:- module(
  clean,
  [
    clean/0,
    clean_uri/1, % +Uri
    seed_uri/1   % -Uri
  ]
).

/** <module> LOD Laundromat

HASH := MD5(URI + " " + ENTRY-PATH)

Archive:

```
HASH/
  dirty → dirty.data
  dirty.json.gz
  warn.log.gz
```

Entry:

```
HASH/
  clean → clean.nt
  clean.json.gz
  warn.log.gz
```

@author Wouter Beek
@version 2017/04
*/

:- use_module(library(apply)).
:- use_module(library(archive)).
:- use_module(library(check_installation), []).
:- use_module(library(ckan_api)).
:- use_module(library(date_time/date_time)).
:- use_module(library(debug)).
:- use_module(library(dict_ext)).
:- use_module(library(hdt/hdt_api)).
:- use_module(library(http/http_cookie)).
:- use_module(library(http/http_header)).
:- use_module(library(http/http_open)).
:- use_module(library(http/http_ssl_plugin)).
:- use_module(library(http/json)).
:- use_module(library(lists)).
:- use_module(library(md5)).
:- use_module(library(os_ext)).
:- use_module(library(rdf)).
:- use_module(library(semweb/rdf_guess)).
:- use_module(library(semweb/rdf_http_plugin)).
:- use_module(library(semweb/rdf_ntriples)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(semweb/rdfa)).
:- use_module(library(semweb/turtle)).
:- use_module(library(ssl)).
:- use_module(library(xsd/xsd_number)).
:- use_module(library(zlib)).

:- use_module(library(file_ext)).
:- use_module(library(uri/uri_ext)).

:- debug(clean).

:- dynamic
    seed_uri0/1.

%seed_uri0('http://www.portaldocidadao.tce.sp.gov.br/api_rdf_municipios').
%seed_uri0('http://www.portaldocidadao.tce.sp.gov.br/api_rdf_orgaos').
%seed_uri0('http://api.comprasnet.gov.br/sicaf/v1/consulta/fornecedores.rdf?uf=RN').
%seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2000.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2001.zip').
%seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-09-10T17%3A50%3A30.109Z/sp-servicosocialuf.ttl').
%seed_uri0('https://data.sazp.sk/dataset/sk-ld-inspire-bio-geographical-regions').
%seed_uri0('https://data.sazp.sk/dataset/sk-ld-inspire-species-distribution').
%seed_uri0('https://data.sazp.sk/dataset/sk-ld-environmental-burdens-contaminated-sites').
%seed_uri0('https://data.sazp.sk/dataset').
%seed_uri0('https://data.sazp.sk/dataset/sk-ld-inspire-protected-sites').
%seed_uri0('https://data.sazp.sk/dataset/sk-ld-inspire-corine-land-cover').

:- meta_predicate
    store_warnings(+, 0).

:- nodebug(http(_)).

:- public
    ssl_verify/5.

ssl_verify(_SSL, _ProblemCertificate, _AllCertificates, _FirstCertificate, _Error).

:- rdf_register_prefix(bnode, 'https://lodlaundromat.org/.well-known/genid/').





%! clean is det.

clean :-
  forall(
    seed_uri0(Uri),
    clean_uri(Uri)
  ).



%! clean_uri(+BaseUri) is det.

clean_uri(BaseUri) :-
  debug(clean, "Start: ~a", [BaseUri]),
  md5_hash(BaseUri, Hash, []),
  store_warnings(Hash, download_uri(BaseUri, Hash, ArchFile, HttpMediaType)),
  % Two cases: (1) download failed, (2) download succeeded.
  (   var(ArchFile)
  ->  hash_to_file(Hash, dirty, TmpFile),
      % Delete the dirty source file.
      delete_file(TmpFile)
  ;   unpack_file(BaseUri, Hash, HttpMediaType, ArchFile)
  ).



%! clean_file(+BaseUri, +Hash, +MediaType, +File) is det.

clean_file(BaseUri, Hash, HttpMediaType, File1) :-
  ignore(uri_media_type(BaseUri, ExtMediaType)),
  rdf_global_id(bnode:Hash, BNodePrefix),
  hash_to_file(Hash, clean, File2),
  (   setup_call_cleanup(
        (
          open(File1, read, In),
          open(File2, write, Out)
        ),
        (
          rdf_guess(In, MediaTypes),
          choose_media_type(MediaTypes, HttpMediaType, ExtMediaType, MediaType),
          Count = count(0),
          call_statistics(
            clean_stream(Count, In, Out, BNodePrefix, BaseUri, MediaType),
            walltime,
            Walltime
          ),
          arg(1, Count, NumTriples),
          stream_metadata(In, Out, Walltime, StreamDict),
          MediaType = media(Super/Sub,_),
          format(string(MediaType0), "~a/~a", [Super,Sub]),
          Dict = _{
            number_of_triples: NumTriples,
            rdf_media_type: MediaType0,
            stream: StreamDict,
            type: stream
          }
        ),
        (
          close(In),
          close(Out)
        )
      )
  ->  rename_file(File2, nt, File3),
      hdt_prepare_file(File3),
      compress_file(File3),
      write_json(Hash, 'clean.json.gz', Dict)
  ;   File3 = File2,
      print_message(warning, non_rdf)
  ),
  % Delete the uncompressed clean file.
  delete_file(File3).

choose_media_type([MediaType], _, _, MediaType) :- !.
choose_media_type(MediaTypes, HttpMediaType, ExtMediaType, MediaType) :-
  member(MediaType, MediaTypes),
  member(MediaType0, [HttpMediaType,ExtMediaType]),
  generalization(MediaType, MediaType0), !.
choose_media_type(L, X, Y, Z) :-
  maplist(writeln, [X,Y|L]),
  gtrace,
  choose_media_type(L, X, Y, Z).

% N-Quads
clean_stream(Count, In, Out, BNodePrefix, BaseUri, media(application/'n-quads',_)) :- !,
  rdf_process_ntriples(
    In,
    write_clean_tuples(Count, Out),
    [anon_prefix(BNodePrefix),base_uri(BaseUri)]
  ).
% N-Triples
clean_stream(Count, In, Out, BNodePrefix, BaseUri, media(application/'n-triples',_)) :- !,
  rdf_process_ntriples(
    In,
    write_clean_tuples(Count, Out),
    [anon_prefix(BNodePrefix),base_uri(BaseUri)]
  ).
% RDF/XML
clean_stream(Count, In, Out, _BNodePrefix, BaseUri, media(application/'rdf+xml',_)) :- !,
  % @tbd blank nodes
  process_rdf(In, write_clean_tuples(Count, Out), [base_uri(BaseUri)]).
% TriG
clean_stream(Count, In, Out, BNodePrefix, BaseUri, media(application/trig,_)) :- !,
  rdf_process_turtle(
    In,
    write_clean_tuples(Count, Out),
    [
      anon_prefix(BNodePrefix),
      base_uri(BaseUri),
      format(trig),
      resources(iri)
    ]
  ).
% Turtle
clean_stream(Count, In, Out, BNodePrefix, BaseUri, media(text/turtle,_)) :- !,
  rdf_process_turtle(
    In,
    write_clean_tuples(Count, Out),
    [
      anon_prefix(BNodePrefix),
      base_uri(BaseUri),
      format(turtle),
      resources(iri)
    ]
  ).
% RDFa
clean_stream(Count, In, Out, BNodePrefix, BaseUri, MediaType) :-
  memberchk(MediaType, [media(application/'xhtml+xml',_),media(text/html,_)]), !,
  read_rdfa(In, Triples, [anon_prefix(BNodePrefix),base(BaseUri)]),
  maplist(write_clean_tuple(Count, Out), Triples).
% unsupported Media Type
clean_stream(_, _, _, _, _, MediaType) :-
  print_message(warning, unsupported_media_type(MediaType)).



%! download_uri(+Uri, +Hash, -File, -HttpMediaType) is det.
%
% Step 1: Download archive

download_uri(Uri, Hash, File2, HttpMediaType) :-
  hash_to_file(Hash, dirty, File1),
  setup_call_cleanup(
    (
      open(File1, write, Out, [type(binary)]),
      open_uri(Uri, In, HttpDicts)
    ),
    (   var(In)
    ->  Dict = _{http: HttpDicts, type: uri}
    ;   call_statistics(copy_stream_data(In, Out), walltime, Walltime),
        stream_metadata(In, Out, Walltime, StreamDict),
        Dict = _{http: HttpDicts, stream: StreamDict, type: uri}
    ),
    (
      close(In),
      close(Out)
    )
  ),
  rename_file(File1, data, File2),
  (   HttpDicts = [HttpDict|_],
      get_dict(content_type, HttpDict.headers, ContentType)
  ->  http_parse_header_value(content_type, ContentType, HttpMediaType)
  ;   true
  ),
  write_json(Hash, 'dirty.json.gz', Dict).

open_uri(Uri, In, []) :-
  uri_components(Uri, uri_components(file,_,_,_,_)), !,
  uri_file_name(Uri, File),
  open(File, read, In, [type(binary)]).
open_uri(Uri, In2, Dicts2) :-
  open_uri1(Uri, In2, 1, [], Dicts1),
  reverse(Dicts1, Dicts2).

open_uri1(Uri, In2, NumRetries, Visited, [Dict|Dicts]) :-
  rdf_http_plugin:rdf_accept_header_value(_, Accept),
  call_statistics(
    http_open(
      Uri,
      In1,
      [
        authenticate(false),
        cert_verify_hook(cert_accept_any),
        header(location,Location),
        headers(Headers),
        redirect(false),
        request_header(accept,Accept),
        status_code(Status),
        version(Major-Minor)
      ]
    ),
    walltime,
    Walltime
  ),
  maplist(term_to_pair, Headers, Pairs),
  dict_pairs(HeadersDict, Pairs),
  format(string(Version), "~d.~d", [Major,Minor]),
  Dict = _{
    headers: HeadersDict,
    status: Status,
    uri: Uri,
    version: Version,
    walltime: Walltime
  },
  open_uri2(Uri, In1, Location, Status, NumRetries, Visited, In2, Dicts).

term_to_pair(Term, Key-Val2) :-
  Term =.. [Key,Val1],
  with_output_to(string(Val2), write_canonical(Val1)).

% authentication error
open_uri2(_, In, _, Status, _, _, _, []) :-
  Status =:= 401,
  close(In),
  print_message(warning, http_error_code(Status)).
% non-authentication error
open_uri2(Uri, In1, _, Status, NumRetries1, Visited, In2, Dicts) :-
  between(400, 599, Status), !,
  NumRetries2 is NumRetries1 + 1,
  (   NumRetries2 >= 5
  ->  close(In1),
      Dicts = [],
      print_message(warning, http_error_code(Status))
  ;   open_uri1(Uri, In2, NumRetries2, Visited, Dicts)
  ).
% redirect
open_uri2(Uri1, In1, Location, Status, NumRetries, Visited1, In2, Dicts) :-
  between(300, 399, Status), !,
  close(In1),
  uri_resolve(Location, Uri1, Uri2),
  Visited2 = [Uri2|Visited1],
  (   length(Visited2, NumVisited),
      NumVisited >= 5
  ->  close(In1),
      Dicts = [],
      print_message(warning, http_max_redirect(5,Uri2))
  ;   include(==(Uri2), Visited2, Visited3),
      length(Visited3, NumRepeats),
      NumRepeats >= 2
  ->  close(In1),
      Dicts = [],
      print_message(warning, http_redirect_loop(Uri2))
  ;   open_uri1(Uri2, In2, NumRetries, Visited2, Dicts)
  ).
% succes
open_uri2(_, In, _, _, _, _, In, []).



%! seed_uri(-Uri) is nondet.

seed_uri(Uri) :-
  ckan_site_uri(Site),
  ckan_resource(Site, Res),
  atom_string(Format, Res.format),
  (rdf_format(Format) -> atom_string(Uri, Res.url)).



%! unpack_file(+BaseUri, +Hash, +HttpMediaType, +File) is det.
%
% Step 2: Unpack archive file into entry files

unpack_file(BaseUri, Hash, HttpMediaType, File) :-
  findall(format(Format), archive_format(Format, true), Opts),
  setup_call_cleanup(
    archive_open(File, read, Arch, [filter(all)|Opts]),
    forall(
      store_warnings(Hash, archive_data_stream(Arch, In, [meta_data(Dicts)])),
      unpack_stream(BaseUri, Hash, HttpMediaType, File, In, Dicts)
    ),
    archive_close(Arch)
  ),
  % Delete the archive file.
  delete_file(File).



%! unpack_stream(+BaseUri, +Hash, +HttpMediaType, +File, +In, +Dicts) is det.

% raw data
unpack_stream(BaseUri, Hash, HttpMediaType, File, In, [_]) :- !,
  close(In),
  store_warnings(Hash, clean_file(BaseUri, Hash, HttpMediaType, File)).
% more unpacking
unpack_stream(BaseUri, Hash0, HttpMediaType, _, In0, [Dict0,_]) :-
  _{
    filetype: file,
    filters: Filters,
    format: Format,
    mtime: MTime,
    name: EntryName,
    permissions: Permissions,
    size: Size
  } :< Dict0,
  atomic_list_concat([BaseUri,EntryName], ' ', Name),
  md5_hash(Name, Hash, []),
  hash_to_file(Hash, dirty, File1),
  setup_call_cleanup(
    open(File1, write, Out, [type(binary)]),
    (
      call_statistics(copy_stream_data(In0, Out), walltime, Walltime),
      stream_metadata(In0, Out, Walltime, StreamDict)
    ),
    close(Out)
  ),
  close(In0),
  rename_file(File1, data, File2),
  Dict = _{
    filters: Filters,
    format: Format,
    mtime: MTime,
    name: EntryName,
    parent: Hash0,
    permissions: Permissions,
    size: Size,
    stream: StreamDict,
    type: entry
  },
  write_json(Hash, 'dirty.json.gz', Dict),
  unpack_file(BaseUri, Hash, HttpMediaType, File2).





% HELPERS %

%! archive_formats(?Format, ?Active) is nondet.

archive_format('7zip', true).
archive_format(ar, true).
archive_format(cab, true).
archive_format(cpio, true).
archive_format(empty, true).
archive_format(gnutar, true).
archive_format(iso9660, true).
archive_format(lha, true).
archive_format(mtree, false).
archive_format(rar, true).
archive_format(raw, true).
archive_format(tar, true).
archive_format(xar, true).
archive_format(zip, true).



%! clean_graph(+G1, -G2) is det.

clean_graph(G1, G3) :-
  rdf11:post_graph(G2, G1),
  (G2 == user -> G3 = default ; G3 = G2).



%! clean_object(+O1, -O2) is semidet.

clean_object(O1, O2) :-
  rdf11:legacy_literal_components(O1, D, Lex1, LTag1), !,
  (   rdf_equal(rdf:'HTML', D)
  ->  rdf11:write_xml_literal(html, Lex1, Lex2)
  ;   rdf_equal(rdf:'XMLLiteral', D)
  ->  rdf11:write_xml_literal(xml, Lex1, Lex2)
  ;   rdf_equal(xsd:decimal, D)
  ->  string_codes(Lex1, Cs1),
      phrase(decimalLexicalMap(Val), Cs1),
      phrase(decimalCanonicalMap(Val), Cs2),
      atom_codes(Lex2, Cs2)
  ;   Lex2 = Lex1
  ),
  catch(
    (
      rdf11:post_object(O2, O1),
      rdf11:pre_object(O2, O3),
      rdf11:legacy_literal_components(O3, D, Lex3, LTag3)
    ),
    E,
    true
  ),
  % Warn for a non-canonical lexical form.
  (   Lex2 \== Lex3
  ->  print_message(warning, non_canonical_lexical_form(D,Lex2,Lex3))
  ;   true
  ),
  % Warn for a non-canonical language tag.
  (   ground(LTag1),
      LTag1 \== LTag3
  ->  print_message(warning, non_canonical_language_tag(LTag1))
  ;   true
  ),
  % Warn and fail for an incorrect lexical form.
  (var(E) -> true ; print_message(warning, E), fail).
clean_object(O, O).



%! clean_tuple(+Tuple, -Quad) is det.

clean_tuple(rdf(S,P,O1,G1), rdf(S,P,O2,G2)) :- !,
  clean_graph(G1, G2),
  clean_object(O1, O2),
  (rdf_is_subject(S) -> true ; debug(clean, "BUGGY S ~w", [S]), fail),
  (rdf_is_predicate(P) -> true ; debug(clean, "BUGGY P ~w", [P]), fail),
  (rdf_is_object(O2) -> true ; debug(clean, "BUGGY O ~w", [O2]), fail),
  (rdf_is_graph(G2) -> true ; debug(clean, "BUGGY G ~w", [G2]), fail).
clean_tuple(rdf(S,P,O), Quad) :-
  rdf_default_graph(G),
  clean_tuple(rdf(S,P,O,G), Quad).



%! generalization(+MediaType1, +MediaType2) is semidet.

generalization(MediaType, MediaType) :- !.
generalization(MediaType1, MediaType3) :-
  generalization0(MediaType1, MediaType2),
  generalization(MediaType2, MediaType3).

generalization0(
  media(application/trig,Params),
  media(text/turtle,Params)
).
generalization0(
  media(application/'n-quads',Params),
  media(application/'n-triples',Params)
).
generalization0(
  media(text/turtle,Params),
  media(application/'n-triples',Params)
).



%! hash_to_file(+Hash, +Local, -File) is det.

hash_to_file(Hash, Local, File) :-
  hash_to_directory(Hash, Dir),
  create_directory(Dir),
  directory_file_path(Dir, Local, File).



%! hash_to_directory(+Hash, -Dir) is det.

hash_to_directory(Hash, Dir) :-
  atom_codes(Hash, Cs),
  append([H1,H2], T, Cs),
  atom_codes(Dir1, [H1,H2]),
  atom_codes(Dir2, T),
  atomic_list_concat(['',scratch,wbeek,ll,Dir1,Dir2], /, Dir).



%! rdf_format(?Format) is nondet.

rdf_format('RDF').
rdf_format('SPARQL').



%! rdf_is_graph(+G) is semidet.

rdf_is_graph(default) :- !.
rdf_is_graph(G) :-
  rdf_is_iri(G).



%! rename_file(+File1, +Ext, -File2) is det.

rename_file(File1, Ext, File2) :-
  file_name_extension(File1, Ext, File2),
  rename_file(File1, File2).



%! store_warnings(+Hash, :Goal_0) is det.

store_warnings(Hash, Goal_0) :-
  hash_to_file(Hash, 'warn.log.gz', File),
  Count = count(0),
  setup_call_cleanup(
    gzopen(File, append, Out),
    (
      asserta((
        user:thread_message_hook(E,Kind,_) :-
          check_installation:error_kind(Kind),
          write_error(Out, E),
          arg(1, Count, N1),
          N2 is N1 + 1,
          nb_setarg(1, Count, N2)
      )),
      (catch(call(Goal_0), E, true) *-> true ; E = fail(Goal_0)),
      (var(E) -> true ; write_error(Out, E))
    ),
    close(Out)
  ),
  arg(1, Count, N),
  % Delete the warnings file if there are no warnings.
  (N =:= 0, exists_file(File) -> delete_file(File) ; true).

write_error(Out, E) :-
  error_term(E, ETerm),
  with_output_to(Out, write_canonical(ETerm)),
  nl(Out).

error_term(error(ETerm,_), ETerm) :- !.
error_term(ETerm, ETerm).



%! stream_metadata(+Stream, -Dict) is det.

stream_metadata(Stream, Dict) :-
  stream_property(Stream, position(Pos)),
  stream_position_data(byte_count, Pos, NumBytes),
  stream_position_data(char_count, Pos, NumChars),
  stream_position_data(line_count, Pos, NumLines),
  stream_property(Stream, newline(Newline)),
  Dict = _{
    byte_count: NumBytes,
    char_count: NumChars,
    line_count: NumLines,
    newline: Newline
  }.



%! stream_metadata(+In, +Out, +Walltime, -Dict) is det.

stream_metadata(In, Out, Walltime, Dict) :-
  stream_metadata(In, InDict),
  stream_metadata(Out, OutDict),
  Dict = _{
    in: InDict,
    out: OutDict,
    walltime: Walltime
  }.



%! uri_media_type(+Uri, -MediaType) is det.

uri_media_type(Uri, MediaType) :-
  uri_file_extensions(Uri, Exts),
  member(Ext1, Exts),
  (   media_type_ext(MediaType, Ext1)
  ;   alt_ext(Ext1, Ext2),
      media_type_ext(MediaType, Ext2)
  ), !.

media_type_ext(media(application/'ld+json',[]), jsonld).
media_type_ext(media(application/'n-quads',[]), nq).
media_type_ext(media(application/'n-triples',[]), nt).
media_type_ext(media(application/'rdf+xml',[]), rdf).
media_type_ext(media(application/trig,[]), trig).
media_type_ext(media(application/'xhtml+xml',[]), xhtml).
media_type_ext(media(text/html,[]), html).
media_type_ext(media(text/turtle,[]), ttl).

alt_ext(json, jsonld).
alt_ext(nquads, nq).
alt_ext(ntriples, nt).
alt_ext(n3, ttl).
alt_ext(turtle, ttl).



%! write_clean_tuple(+Count, +Out, +Tuple) is det.

write_clean_tuple(Count, Out, Tuple) :-
  clean_tuple(Tuple, rdf(S,P,O,_)), !,
  write_ntriple0(Out, S, P, O),
  write_eot(Out),
  arg(1, Count, N1),
  N2 is N1 + 1,
  nb_setarg(1, Count, N2).
% always succeeds
write_clean_tuple(_, _, _).

write_clean_tuples(Count, Out, Tuples, _) :-
  maplist(write_clean_tuple(Count, Out), Tuples).

write_ntriple0(Out, S, P, O) :-
  write_subject(Out, S),
  write(Out, ' '),
  write_predicate(Out, P),
  write(Out, ' '),
  write_object(Out, O),
  write(Out, ' ').

write_eot(Out) :-
  write(Out, '.\n').

write_graph(Out, G) :-
  write_iri(Out, G).

write_iri(Out, Iri) :-
  turtle:turtle_write_uri(Out, Iri).

write_predicate(Out, P) :-
  write_iri(Out, P).

write_object(Out, S) :-
  write_subject(Out, S), !.
write_object(Out, Lit) :-
  write_literal(Out, Lit).

write_subject(Out, Iri) :-
  rdf_is_iri(Iri), !,
  write_iri(Out, Iri).

write_literal(Out, Val^^D) :- !,
  rdf_lexical_form(Val^^D, Lex^^D),
  turtle:turtle_write_quoted_string(Out, Lex),
  write(Out, '^^'),
  write_iri(Out, D).
write_literal(Out, Lex@LTag) :- !,
  turtle:turtle_write_quoted_string(Out, Lex),
  write(Out, '@'),
  write(Out, LTag).
write_literal(Out, V) :-
  rdf_equal(xsd:string, D),
  write_literal(Out, V^^D).



%! write_json(+Hash, +Local, +Dict) is det.

write_json(Hash, Local, Dict) :-
  hash_to_file(Hash, Local, File),
  setup_call_cleanup(
    gzopen(File, write, Out),
    json_write_dict(Out, Dict),
    close(Out)
  ),
  debug(clean, "Written: ~a", [File]).
