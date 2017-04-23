:- module(
  clean,
  [
    clean_uri/1 % +Uri
  ]
).

/** <module> LOD Laundromat

HASH := MD5(URI + " " + ENTRY-PATH)

Archive:

```
HASH/
  meta.[hdt,nt.gz]
  source → source.data
  warn.log.gz
```

Entry:

```
HASH/
  data → data.[hdt,nt]
  meta.[hdt,nt.gz]
  warn.log.gz
```

@author Wouter Beek
@version 2017/04
*/

:- use_module(library(apply)).
:- use_module(library(archive)).
:- use_module(library(check_installation), []).
:- use_module(library(debug)).
:- use_module(library(dict_ext)).
:- use_module(library(http/http_cookie)).
:- use_module(library(http/http_header)).
:- use_module(library(http/https_open)).
:- use_module(library(lists)).
:- use_module(library(md5)).
:- use_module(library(rdf)).
:- use_module(library(semweb/rdf_guess)).
:- use_module(library(semweb/rdf_http_plugin)).
:- use_module(library(semweb/rdf_ntriples)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(semweb/rdfa)).
:- use_module(library(semweb/turtle)).
:- use_module(library(uuid)).
:- use_module(library(xsd/xsd_number)).
:- use_module(library(zlib)).

:- use_module(library(date_time/date_time)).
:- use_module(library(file_ext)).
:- use_module(library(hdt/hdtio)).
:- use_module(library(os_ext)).
:- use_module(library(rdf/rdfio)).
:- use_module(library(thread_ext)).
:- use_module(library(uri/uri_ext)).

:- debug(clean).

:- meta_predicate
    store_warnings(+, 0).

:- nodebug(http(_)).

:- rdf_register_prefix(bnode, 'https://lodlaundromat.org/.well-known/genid/').
:- rdf_register_prefix(llh, 'https://lodlaundromat.org/http/').
:- rdf_register_prefix(llo, 'https://lodlaundromat.org/ontology/').
:- rdf_register_prefix(llr, 'https://lodlaundromat.org/resource/').
:- rdf_register_prefix(void, 'http://rdfs.org/ns/void#').





%! clean_uri(+BaseUri) is det.

clean_uri(BaseUri) :-
  md5_hash(BaseUri, Hash, []),
  debug(clean, "Starting: ~a ~a", [Hash,BaseUri]),
  store_warnings(Hash, download_uri(BaseUri, Hash, ArchFile, HttpMT)),
  % Two cases: (1) download failed, (2) download succeeded.
  (   var(ArchFile)
  ->  hash_to_file(Hash, source, TmpFile),
      % Delete the source file.
      delete_file(TmpFile)
  ;   atomic_list_concat([a,Hash], :, Alias),
      ignore(uri_media_type(BaseUri, ExtMT)),
      call_in_thread(
        Alias,
        unpack_file(BaseUri, Hash, ExtMT, HttpMT, ArchFile)
      )
  ),
  finish_meta_and_warn(Hash),
  debug(clean, "Ended: ~a ~a", [Hash,BaseUri]).



%! clean_file(+BaseUri, +Hash, +ExtMT, +HttpMT, +EntryFile) is det.

clean_file(BaseUri, Hash, ExtMT, HttpMT, EntryFile) :-
  rdf_global_id(bnode:Hash, BNodePrefix),
  hash_to_file(Hash, data, DataFileTmp),
  (   setup_call_cleanup(
        (
          open(EntryFile, read, In),
          open(DataFileTmp, write, Out)
        ),
        (
          rdf_guess(In, MTs),
          choose_media_type(MTs, HttpMT, ExtMT, MT),
          Count = count(0),
          call_statistics(
            clean_stream(Count, In, Out, BNodePrefix, BaseUri, MT),
            walltime,
            Walltime
          ),
          arg(1, Count, NumTriples),
          stream_metadata(In, Out, Walltime, StreamDict),
          MT = media(Super/Sub,_),
          format(string(MT0), "~a/~a", [Super,Sub])
        ),
        (
          close(In),
          close(Out)
        )
      )
  ->  rename_file(DataFileTmp, nt, DataFile),
      finish_ntriples_file(DataFile),
      hash_to_file(Hash, 'meta.nt', MetaFile),
      setup_call_cleanup(
        open(MetaFile, append, MetaOut),
        (
          rdf_global_id(llr:Hash, Entry),
          write_ntriple(MetaOut, Entry, rdf:type, llo:'Cleaning'),
          write_ntriple(MetaOut, Entry, void:triples,
                        NumTriples^^xsd:nonNegativeInteger),
          write_ntriple(MetaOut, Entry, llo:rdfMT, MT0^^xsd:string),
          write_stream_metadata(MetaOut, Entry, StreamDict)
        ),
        close(MetaOut)
      )
  ;   delete_file(DataFileTmp),
      print_message(warning, non_rdf)
  ).

choose_media_type([MT], _, _, MT) :- !.
choose_media_type(MTs1, HttpMT, ExtMT, MT1) :-
  exclude(var, [HttpMT,ExtMT], MTs2),
  member(MT1, MTs1),
  member(MT2, MTs2),
  generalization(MT1, MT2), !.
choose_media_type(L, X, Y, Z) :-
  maplist(writeln, [X,Y|L]),
  gtrace,
  choose_media_type(L, X, Y, Z).

% N-Quads
clean_stream(Count, In, Out, BNodePrefix, BaseUri,
             media(application/'n-quads',_)) :- !,
  rdf_process_ntriples(
    In,
    write_clean_tuples(Count, Out),
    [anon_prefix(BNodePrefix),base_uri(BaseUri)]
  ).
% N-Triples
clean_stream(Count, In, Out, BNodePrefix, BaseUri,
             media(application/'n-triples',_)) :- !,
  rdf_process_ntriples(
    In,
    write_clean_tuples(Count, Out),
    [anon_prefix(BNodePrefix),base_uri(BaseUri)]
  ).
% RDF/XML
clean_stream(Count, In, Out, _BNodePrefix, BaseUri,
             media(application/'rdf+xml',_)) :- !,
  % @tbd blank nodes
  process_rdf(In, write_clean_tuples(Count, Out), [base_uri(BaseUri)]).
% TriG
clean_stream(Count, In, Out, BNodePrefix, BaseUri,
             media(application/trig,_)) :- !,
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
clean_stream(Count, In, Out, BNodePrefix, BaseUri,
             media(text/turtle,_)) :- !,
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
clean_stream(Count, In, Out, BNodePrefix, BaseUri, MT) :-
  memberchk(
    MT,
    [media(application/'xhtml+xml',_),media(text/html,_)]
  ), !,
  read_rdfa(In, Triples, [anon_prefix(BNodePrefix),base(BaseUri)]),
  maplist(write_clean_tuple(Count, Out), Triples).
% unsupported Media Type
clean_stream(_, _, _, _, _, MT) :-
  print_message(warning, unsupported_media_type(MT)).



%! download_uri(+Uri, +Hash, -File, -HttpMT) is det.
%
% Step 1: Download archive

download_uri(Uri, Hash, File, HttpMT) :-
  hash_to_file(Hash, source, FileTmp),
  setup_call_cleanup(
    (
      open(FileTmp, write, Out, [type(binary)]),
      open_uri(Uri, In, HttpDicts)
    ),
    (   var(In)
    ->  true
    ;   call_statistics(copy_stream_data(In, Out), walltime, Walltime),
        stream_metadata(In, Out, Walltime, StreamDict)
    ),
    (
      close(In),
      close(Out)
    )
  ),
  rename_file(FileTmp, data, File),
  (   HttpDicts = [HttpDict|_],
      get_dict(content_type, HttpDict.headers, ContentType)
  ->  http_parse_header_value(content_type, ContentType, HttpMT)
  ;   true
  ),
  hash_to_file(Hash, 'meta.nt', MetaFile),
  setup_call_cleanup(
    open(MetaFile, append, MetaOut),
    (
      rdf_global_id(llr:Hash, Source),
      write_ntriple(MetaOut, Source, rdf:type, llo:'Download'),
      write_ntriple(MetaOut, Source, llo:uri, Uri^^xsd:anyURI),
      (   var(StreamDict)
      ->  true
      ;   write_stream_metadata(MetaOut, Source, StreamDict)
      ),
      write_http_metadata(MetaOut, Source, HttpDicts)
    ),
    close(MetaOut)
  ).

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
    https_open(
      Uri,
      In1,
      [
        authenticate(false),
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



%! unpack_file(+BaseUri, +Hash, +ExtMT, +HttpMT, +File) is det.
%
% Step 2: Unpack archive file into entry files

unpack_file(BaseUri, Hash, ExtMT, HttpMT, File) :-
  findall(format(Format), archive_format(Format, true), Opts),
  setup_call_cleanup(
    archive_open(File, read, Arch, [filter(all)|Opts]),
    forall(
      store_warnings(Hash, archive_data_stream(Arch, In, [meta_data(Dicts)])),
      unpack_stream(BaseUri, Hash, ExtMT, HttpMT, File, In, Dicts)
    ),
    archive_close(Arch)
  ),
  % Delete the archive file.
  delete_file(File).



%! unpack_stream(+BaseUri, +Hash, +ExtMT, +HttpMT, +File, +In, +Dicts) is det.

% raw data
unpack_stream(BaseUri, Hash, ExtMT, HttpMT, File, In, [_]) :- !,
  close(In),
  store_warnings(Hash, clean_file(BaseUri, Hash, ExtMT, HttpMT, File)).
% more unpacking
unpack_stream(BaseUri, Hash0, ExtMT0, HttpMT, _, In0, [Dict0,_]) :-
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
  hash_to_file(Hash, source, DataFileTmp),
  setup_call_cleanup(
    open(DataFileTmp, write, DataOut, [type(binary)]),
    (
      call_statistics(copy_stream_data(In0, DataOut), walltime, Walltime),
      stream_metadata(In0, DataOut, Walltime, StreamDict)
    ),
    close(DataOut)
  ),
  close(In0),
  rename_file(DataFileTmp, data, DataFile),
  hash_to_file(Hash, 'meta.nt', MetaFile),
  setup_call_cleanup(
    open(MetaFile, append, MetaOut),
    (
      rdf_global_id(llr:Hash, S),
      write_ntriple(MetaOut, S, rdf:type, llo:'Unpack'),
      write_filters_metadata(MetaOut, S, Filters),
      write_ntriple(MetaOut, S, llo:format, Format^^xsd:string),
      write_ntriple(MetaOut, S, llo:mtime, MTime^^xsd:float),
      write_ntriple(MetaOut, S, llo:name, EntryName^^xsd:string),
      rdf_global_id(llr:Hash0, S0),
      write_ntriple(MetaOut, S, llo:parent, S0),
      write_ntriple(MetaOut, S, llo:permissions,
                    Permissions^^xsd:nonNegativeInteger),
      write_ntriple(MetaOut, S, llo:size, Size^^xsd:float),
      write_stream_metadata(MetaOut, S, StreamDict)
    ),
    close(MetaOut)
  ),
  (file_media_type(EntryName, ExtMT) -> true ; ExtMT = ExtMT0),
  atomic_list_concat([e,Hash], :, Alias),
  writeln(Hash),
  call_in_thread(Alias, unpack_file(BaseUri, Hash, ExtMT, HttpMT, DataFile)),
  finish_meta_and_warn(Hash).





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



%! extensions_to_media_type(+Exts, -MT) is det.

extensions_to_media_type(Exts, MT) :-
  member(Ext1, Exts),
  (   media_type_ext(MT, Ext1)
  ;   alt_ext(Ext1, Ext2),
      media_type_ext(MT, Ext2)
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



%! file_media_type(+File, -MT) is det.

file_media_type(File, MT) :-
  file_extensions(File, Exts),
  extensions_to_media_type(Exts, MT).



%! finish_meta(+Hash) is det.

finish_meta(Hash) :-
  hash_to_file(Hash, 'meta.nt', MetaFile),
  finish_ntriples_file(MetaFile).



%! finish_meta_and_warn(+Hash) is det.

finish_meta_and_warn(Hash) :-
  finish_meta(Hash),
  finish_warn(Hash).



%! finish_warn(+Hash) is det.

finish_warn(Hash) :-
  hash_to_file(Hash, 'warn.log', WarnFile),
  wc(WarnFile, NumLines),
  (   NumLines =:= 0,
      exists_file(WarnFile)
  ->  true
  ;   compress_file(WarnFile)
  ),
  delete_file(WarnFile).



%! generalization(+MT1, +MT2) is semidet.

generalization(MT, MT) :- !.
generalization(MT1, MT3) :-
  generalization0(MT1, MT2),
  generalization(MT2, MT3).

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



%! ll_create_bnode(-BNode) is det.

ll_create_bnode(BNode) :-
  uuid(Local),
  rdf_global_id(bnode:Local, BNode).



%! rdf_is_graph(+G) is semidet.

rdf_is_graph(default) :- !.
rdf_is_graph(G) :-
  rdf_is_iri(G).



%! rdf_membership_property(+N, -P) is det.

rdf_membership_property(N, P) :-
  atom_concat('_', N, Local),
  rdf_global_id(rdf:Local, P).



%! rename_file(+File1, +Ext, -File2) is det.

rename_file(File1, Ext, File2) :-
  file_name_extension(File1, Ext, File2),
  rename_file(File1, File2).



%! store_warnings(+Hash, :Goal_0) is det.

store_warnings(Hash, Goal_0) :-
  hash_to_file(Hash, 'warn.log', File),
  setup_call_cleanup(
    open(File, append, Out),
    (
      asserta((
        user:thread_message_hook(E,Kind,_) :-
          check_installation:error_kind(Kind),
          write_error(Out, E)
      )),
      (catch(call(Goal_0), E, true) *-> true ; E = fail(Goal_0)),
      (var(E) -> true ; write_error(Out, E))
    ),
    close(Out)
  ).

write_error(Out, E) :-
  error_term(E, ETerm),
  with_output_to(Out, write_canonical(ETerm)),
  nl(Out).

error_term(error(ETerm,_), ETerm) :- !.
error_term(ETerm, ETerm).



%! stream_metadata(+In, +Out, +Walltime, -Dict) is det.

stream_metadata(In, Out, Walltime, Dict) :-
  stream_metadata(In, InDict),
  stream_metadata(Out, OutDict),
  Dict = _{
    in: InDict,
    out: OutDict,
    walltime: Walltime
  }.

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



%! term_to_pair(+Term, -Pair) is det.

term_to_pair(Term, Key-Val2) :-
  Term =.. [Key,Val1],
  with_output_to(string(Val2), write_canonical(Val1)).



%! uri_media_type(+Uri, -MT) is det.

uri_media_type(Uri, MT) :-
  uri_file_extensions(Uri, Exts),
  extensions_to_media_type(Exts, MT).



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



%! write_filters_metadata(+Out, +S, +Filters) is det.

write_filters_metadata(Out, S, Filters) :-
  write_filters_metadata(Out, S, 1, Filters).

write_filters_metadata(_, _, _, []) :- !.
write_filters_metadata(Out, S, N1, [H|T]) :-
  rdf_membership_property(N1, P),
  write_ntriple(Out, S, P, H^^xsd:string),
  N2 is N1 + 1,
  write_filters_metadata(Out, S, N2, T).



%! write_http_metadata(+Out, +S, +Dicts) is det.

write_http_metadata(Out, S, Dicts) :-
  write_http_metadata(Out, S, 1, Dicts).

write_http_metadata(_, _, _, []) :- !.
write_http_metadata(Out, S, N1, [H|T]) :-
  _{
    headers: Dict,
    status: Status,
    uri: Uri,
    version: Version,
    walltime: Walltime
  } :< H,
  rdf_membership_property(N1, P),
  ll_create_bnode(BNode),
  write_ntriple(Out, S, P, BNode),
  write_ntriple(Out, BNode, rdf:type, llo:'HttpRequest'),
  write_ntriple(Out, BNode, llo:status, Status^^xsd:nonNegativeInteger),
  write_ntriple(Out, BNode, llo:uri, Uri^^xsd:anyURI),
  write_ntriple(Out, BNode, llo:version, Version^^xsd:string),
  write_ntriple(Out, BNode, llo:walltime, Walltime^^xsd:float),
  write_http_headers_metadata(Out, BNode, Dict),
  N2 is N1 + 1,
  write_http_metadata(Out, S, N2, T).

write_http_headers_metadata(Out, S, Dict) :-
  dict_pairs(Dict, _, Pairs),
  maplist(write_http_header_metadata(Out, S), Pairs).

write_http_header_metadata(Out, S, Key-Val) :-
  rdf_global_id(llh:Key, P),
  write_ntriple(Out, S, P, Val^^xsd:string).



%! write_stream_metadata(+Out, +S, +Dict) is det.

write_stream_metadata(Out, S, Dict) :-
  _{
    in: InDict,
    out: OutDict,
    walltime: Walltime
  } :< Dict,
  ll_create_bnode(InStream),
  write_ntriple(Out, S, llo:inputStream, InStream),
  write_iostream_metadata(Out, InStream, InDict),
  ll_create_bnode(OutStream),
  write_ntriple(Out, S, llo:outputStream, OutStream),
  write_iostream_metadata(Out, OutStream, OutDict),
  write_ntriple(Out, S, llo:walltime, Walltime^^xsd:float).

write_iostream_metadata(Out, Stream, Dict) :-
  _{
    byte_count: NumBytes,
    char_count: NumChars,
    line_count: NumLines,
    newline: Newline
  } :< Dict,
  write_ntriple(Out, Stream, llo:numberOfBytes,
                NumBytes^^xsd:nonNegativeInteger),
  write_ntriple(Out, Stream, llo:numberOfCharacters,
                NumChars^^xsd:nonNegativeInteger),
  write_ntriple(Out, Stream, llo:numberOfLines,
                NumLines^^xsd:nonNegativeInteger),
  write_ntriple(Out, Stream, llo:newline, Newline^^xsd:string).
