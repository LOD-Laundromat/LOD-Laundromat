:- module(
  ll_document,
  [
    ll_documents/3 % +Out, +Seed, -Metadata
  ]
).

/** <module> LOD Laundromat: Scrape one document

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

:- maplist(rdf_assert_prefix, [
     dcat-'http://www.w3.org/ns/dcat#',
     void-'http://rdfs.org/ns/void#'
   ]).

:- thread_local
   dump_url/2.





%! ll_documents(+Out:stream, +Seed:dict, -Metadata:dict) is det.

ll_documents(Out, Seed, Meta) :-
  maplist(assert_dump_url, Seed.documents),
  ll_documents_loop(Out, Seed.hash, [], L),
  Meta = _{documents: L, hash: Seed.hash}.

ll_documents_loop(Out, Hash, T, L) :-
  dump_url(Uri, false), !,
  catch(
    ll_document(Out, Hash, Uri, H),
    E,
    print_message(warning, E)
  ),
  update_dump_url(Uri),
  ll_documents_loop(Out, Hash, [H|T], L).
ll_documents_loop(_, _, L, L).

assert_dump_url(Uri) :-
  dump_url(Uri, _), !.
assert_dump_url(Uri) :-
  assertz(dump_url(Uri,false)).

update_dump_url(Uri) :-
  retract(dump_url(Uri,false)),
  assertz(dump_url(Uri,true)).



%! ll_document(+Out:stream, +Hash:atom, +Directory:atom, +Uri:atom,
%!             -DocMetadata:dict) is det.

ll_document(Out, Hash, Uri0, DocMeta) :-
  % Make sure that non-ASCII Unicode characters are percent encoded.
  uri_normalized(Uri0, Uri),
  % TBD: Assert (HTTP) metadata into LOD-Seedlist.
  http_open2(Uri, In, [failure(-1),metadata(HttpMetas),success('2xx')]),
  HttpMetas = [HttpMeta|_],
  _{status: Status} :< HttpMeta,
  (   between(200, 299, Status)
  ->  call_cleanup(
        (
          % Use a dummy value in case the Media Type cannot be determined.
          call_default_value(MediaType, http_metadata_content_type(HttpMetas), null),
          download_from_uri(Out, Hash, Uri, MediaType, In, EntryMetas)
        ),
        close(In)
      )
  ;   print_message(warning, http(status(Status),Uri))
  ),
  DocMeta = _{http: HttpMetas, entries: EntryMetas, uri: Uri0}.

download_from_uri(Out, Hash, Uri, MediaType, In, EntryMetas) :-
  setup_call_cleanup(
    archive_open(In, Arch),
    download_from_archive(Out, Hash, Uri, MediaType, Arch, EntryMetas),
    archive_close(Arch)
  ).

download_from_archive(Out, Hash, Uri, MediaType, Arch, EntryMetas) :-
  findall(
    EntryMeta,
    (
      archive_data_stream(Arch, In, [meta_data(ArchMetas)]),
      call_cleanup(
        download_from_archive_stream(Out, Hash, Uri, MediaType, ArchMetas, In, EntryMeta),
        close(In)
      )
    ),
    EntryMetas
  ).

download_from_archive_stream(Out, Hash1, Uri, MediaType, ArchMetas, In, Meta2) :-
  ArchMetas = [ArchMeta|_],
  _{name: Entry} :< ArchMeta,
  md5(Hash1-Entry, Hash2),
  peek_string(In, 10 000, String),
  guess_encoding(MediaType, String, Encoding),
  download_from_entry(Out, Hash2, Uri, Encoding, In, Meta1),
  Meta2 = Meta1.put(_{archive: ArchMeta, encoding: Encoding}).

guess_encoding(MediaType, String, Encoding) :-
  ignore(xml_encoding_(String, EncodingXml)),
  ignore(media_type_encoding(MediaType, EncodingMediaType)),
  ignore(guess_encoding_(String, EncodingGuess)),
  choose_encoding(EncodingXml, EncodingMediaType, EncodingGuess, Encoding),
  warn_encoding(EncodingXml, EncodingMediaType, EncodingGuess, Encoding).

guess_encoding_(String, Encoding) :-
  setup_call_cleanup(
    open_string(String, In),
    guess_encoding(In, Encoding),
    close(In)
  ),
  Encoding \== unknown.

xml_encoding_(String, Encoding) :-
  setup_call_cleanup(
    open_string(String, In),
    xml_encoding(In, Encoding),
    close(In)
  ).

% Encoding specified in the XML header.
choose_encoding(Xml, _, _, Xml) :-
  ground(Xml), !.
% Encoding specified in the Media Type.
choose_encoding(_, MediaType, _, MediaType) :-
  ground(MediaType), !.
% Guessed encoding.
choose_encoding(_, _, Guess, Guess).

warn_encoding(A, B, C, D) :-
  maplist(generalize_encoding, [A,B,C], [X,Y,Z]),
  include(ground, [X,Y,Z], L),
  list_to_ord_set(L, S),
  (   S = [_,_|_]
  ->  print_message(warning, unclear_encoding([X,Y,Z],D))
  ;   true
  ).

generalize_encoding(Var, Var) :-
  var(Var), !.
generalize_encoding(ascii, utf8) :- !.
generalize_encoding(unknown, _Var) :- !.
generalize_encoding(Encoding, Encoding).

download_from_entry(Out, Hash, Uri, unknown, In, Meta) :- !,
  download_from_entry_stream(Out, Hash, Uri, In, Meta).
download_from_entry(Out, Hash, Uri, Encoding, In, Meta) :-
  recode_stream(Encoding, In), !,
  download_from_entry_stream(Out, Hash, Uri, In, Meta).
download_from_entry(Out, Hash, Uri, _Encoding, In1, Meta) :-
  setup_call_cleanup(
    recode_stream(Encoding, In1, In2),
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
  clean_triple_(Meta, Out, BNodePrefix, rdf(S,P,O)).
clean_tuple(Meta, Out, BNodePrefix, rdf(S,P,O,_)) :-
  nb_increment_dict(Meta, number_of_quadruples),
  clean_triple_(Meta, Out, BNodePrefix, rdf(S,P,O)).

clean_triple_(Meta, Out, BNodePrefix, rdf(S0,P0,O0)) :-
  (   rdf_clean_triple(BNodePrefix, rdf(S0,P0,O0), rdf(S,P,O))
  ->  rdf_write_triple(Out, BNodePrefix, S, P, O)
  ;   true
  ),
  (   rdf_prefix_memberchk(P, [dcat:downloadURL,void:dataDump]),
      rdf_term_url(O, Uri)
  ->  assert_dump_url(Uri),
      nb_increment_dict(Meta, number_of_datadumps)
  ;   true
  ).

rdf_term_url(Literal, Uri) :-
  rdf_is_literal(Literal), !,
  rdf_literal_lexical_form(Literal, Uri).
rdf_term_url(Uri, Uri).
