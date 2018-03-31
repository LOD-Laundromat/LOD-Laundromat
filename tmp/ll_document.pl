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





%! ll_document(+Hash:atom, +Uri:atom) is det.

ll_document(Hash, Uri) :-
  hash_directory(Hash, Dir),
  catch(download(Dir, Uri, File, Meta), E, true),
  (   var(E)
  ->  decompress(File, Files),
      maplist(ll_entry, Files)
  ;   Meta = _{error: E}
  ),
  store_metadata(Meta).

ll_entry(File1) :-
  recode(File1, File2),
  parse(File2).

download(Dir, Uri, File, Meta2) :-
  directory_file_name(Dir, dirty, File),
  setup_call_cleanup(
    open(File, write, Out),
    download_stream(Out, Uri, Meta1),
    close_metadata(Out, OutMeta)
  ),
  Meta2 = Meta1.put(_{write: OutMeta}).

download_stream(Out, Uri, Meta) :-
  http_open2(Uri, In, [failure(-1),metadata(HttpMetas),success('2xx')]),
  HttpMetas = [HttpMeta|_],
  between(200, 299, HttpMeta.status)
  call_cleanup(
    copy_data_stream(In, Out),
    close_metadata(In, InMeta)
  ),
  Meta = _{http: HttpMetas, read: InMeta}.

% ---

  DocMeta = _{http: HttpMetas, entries: EntryMetas, uri: Uri0}.
  catch(
    ll_document(Out, Hash, Uri, H),
    E,
    print_message(warning, E)
  ),
  setup_call_cleanup(
    gzopen(File, write, Out),
    ll_documents(Out, Seed, Meta),
    close(Out)
  ),
  Meta = _{documents: L, hash: Seed.hash}.



%! ll_document(+Out:stream, +Hash:atom, +Directory:atom, +Uri:atom,
%!             -DocMetadata:dict) is det.

ll_document(Out, Hash, Uri, DocMeta) :-
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
download_from_entry(Out, Hash, Uri, Encoding, In1, Meta) :-
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
