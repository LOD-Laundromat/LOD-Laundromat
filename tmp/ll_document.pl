:- module(
  ll_document,
  [
    ll_document/2 % +Hash, +Uri
  ]
).

/** <module> LOD Laundromat: Scrape one document

1. download: url → file [md5(url)]
2. decompress: file → file [md5(md5(url)-entry)]
3. recode: file → file
4. clean: dirty → clean.nq.gz, meta.log.gz

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).
:- use_module(library(settings)).

:- use_module(library(archive_ext)).
:- use_module(library(dict)).
:- use_module(library(default)).
:- use_module(library(hash_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(media_type)).
:- use_module(library(settings)).
:- use_module(library(stream_ext)).
:- use_module(library(string_ext)).
:- use_module(library(sw/rdf_clean)).
:- use_module(library(sw/rdf_deref)).
:- use_module(library(sw/rdf_export)).
:- use_module(library(sw/rdf_guess)).
:- use_module(library(sw/rdf_media_type)).
:- use_module(library(sw/rdf_prefix)).
:- use_module(library(sw/rdf_term)).
:- use_module(library(uri_ext)).
:- use_module(library(xml_ext)).

:- initialization
   set_setting(rdf_term:bnode_prefix_scheme, http),
   set_setting(rdf_term:bnode_prefix_authority, 'lodlaundromat.org').

download_from_entry(Out, Hash, Uri, unknown, In, Meta) :- !,
  download_from_entry_stream(Out, Hash, Uri, In, Meta).
download_from_entry(Out, Hash, Uri, Enc, In, Meta) :-
  recode_stream(Enc, In), !,
  download_from_entry_stream(Out, Hash, Uri, In, Meta).
download_from_entry(Out, Hash, Uri, Enc, In1, Meta) :-
  setup_call_cleanup(
    recode_stream(Enc, In1, In2),
    download_from_entry_stream(Out, Hash, Uri, In2, Meta),
    close(In2)
  ).

download_from_entry_stream(Out, Hash, Uri, In, Meta) :-
  % After recoding, the stream needs to be peeked again.  Not only
  % because the stream can have changed (which could be detected with
  % (==)/2, but also because the same stream can have different
  % settings now.
  peek_string(In, 10 000, String),
  (   rdf_guess_string(String, MediaType)
  ->  bnode_prefix([Hash], BNodePrefix),
      RdfMeta = _{
        number_of_datadumps: 0,
        number_of_quadruples: 0,
        number_of_triples: 0
      },
      rdf_deref_stream(
        Uri,
        In,
        clean_tuples(RdfMeta, Out),
        [bnode_prefix(BNodePrefix),media_type(MediaType)]
      ),
      Meta = RdfMeta.put(_{'serialization-format': MediaType})
  ;   % The entire peeked string can be too long for a warning
      % message.
      (string_prefix(String, 100, Content) -> true ; Content = String),
      print_message(warning, rdf(non_rdf_format(Hash,Content)))
  ).

bnode_prefix(Segments, BNodePrefix) :-
  setting(rdf_term:bnode_prefix_scheme, Scheme),
  setting(rdf_term:bnode_prefix_authority, Auth),
  uri_comps(BNodePrefix, uri(Scheme,Auth,['.well-known',genid|Segments],_,_)).

clean_tuples(Meta, Out, BNodePrefix, Tuples, _) :-
  maplist(clean_tuple(Meta, Out, BNodePrefix), Tuples).

clean_tuple(Meta, Out, BNodePrefix, rdf(S,P,O)) :- !,
  nb_increment_dict(Meta, number_of_triples),
  clean_triple_(Out, BNodePrefix, rdf(S,P,O)).
clean_tuple(Meta, Out, BNodePrefix, rdf(S,P,O,_)) :-
  nb_increment_dict(Meta, number_of_quadruples),
  clean_triple_(Out, BNodePrefix, rdf(S,P,O)).

clean_triple_(Out, BNodePrefix, rdf(S0,P0,O0)) :-
  (   rdf_clean_triple(BNodePrefix, rdf(S0,P0,O0), rdf(S,P,O))
  ->  rdf_write_triple(Out, BNodePrefix, S, P, O)
  ;   true
  ).

upload_metadata(Dir, Seed, Meta) :-
  directory_file_path(Dir, 'metadata.json', File),
  setup_call_cleanup(
    open(File, write, Out),
    json_write_dict(Out, Meta),
    close(Out)
  ),
  file(Seed.organization.name, Seed.dataset.name, _, File).
