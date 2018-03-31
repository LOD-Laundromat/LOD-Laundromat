:- module(
  ll_generics,
  [
    call_loop/1,           % :Goal_0
    close_metadata/2,      % +Out, -Metadata
    delete_empty_directories/0,
    find_hash_directory/2, % -Directory, -Hash
    find_hash_file/3,      % +Local, -Hash, -File
    hash_directory/2,      % +Hash, -Directory
    hash_entry_hash/3,     % +Hash1, +Entry, -Hash2
    hash_file/3,           % +Hash, +Local, -File
    seed_base_uri/2,       % +Seed, -BaseUri
    seedlist_request/4,    % +Segments, ?Query, :Goal_1, +Options
    stale_seed/1,          % -Seed
    touch_hash_file/2,     % +Hash, +Local
    write_meta/3,          % +Hash, +PLocal, +Lex
    write_meta_archive/2,  % +Hash, +Metadata
    write_meta_error/2,    % +Hash, +Error
    write_meta_http/2,     % +Hash, +Metadata
    write_meta_now/2,      % +Hash, +PLocal
    write_meta_stream/3,   % +Hash, +PLocal, +Metadata
    write_meta_triple/3    % +Hash, +PLocal, +OLocal
  ]
).

/** <module> LOD Laundromat: Generics

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(http/json)).
:- use_module(library(settings)).
:- use_module(library(zlib)).

:- use_module(library(conf_ext)).
:- use_module(library(dict)).
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(sw/rdf_export)).
:- use_module(library(sw/rdf_prefix)).
:- use_module(library(sw/rdf_term)).
:- use_module(library(thread_ext)).
:- use_module(library(uri_ext)).
:- use_module(library(write_ext)).

:- initialization
   init_ll.

:- maplist(rdf_assert_prefix, [
     def-'https://lodlaundromat.org/def/',
     id-'https://lodlaundromat.org/id/'
   ]).

:- meta_predicate
    call_loop(0),
    running_loop(0),
    seedlist_request(+, ?, 1, +),
    write_meta(+, +, 3).

lod_cloud_portray(Blob, Options) :-
  blob(Blob, Type),
  \+ atom(Blob),
  Type \== reserved_symbol,
  write_term('BLOB'(Type), Options).

% Set global stack to 10GB for larger datasets.
:- set_prolog_stack(global, limit(10*10**9)).

:- setting(authority, any, _,
           "URI scheme of the seedlist server location.").
:- setting(data_directory, any, _,
           "The directory where clean data is stored and where logs are kept.").
:- setting(password, any, _, "").
:- setting(scheme, oneof([http,https]), https,
           "URI scheme of the seedlist server location.").
:- setting(user, any, _, "").





%! call_loop(:Goal_0) is det.

call_loop(Mod:Goal_0) :-
  thread_create(running_loop(Mod:Goal_0), _, [alias(Goal_0),detached(true)]).



%! close_metadata(+Out:stream, -Metadata:dict) is det.

close_metadata(Out, Meta) :-
  stream_metadata(Out, Meta),
  close(Out).



%! delete_empty_directories is det.

delete_empty_directories :-
  setting(data_directory, Dir0),
  forall(
    directory_path(Dir0, Dir),
    (is_empty_directory(Dir) -> delete_directory(Dir) ; true)
  ).



%! find_hash_directory(-Directory:atom, -Hash:atom) is nondet.

find_hash_directory(Dir2, Hash) :-
  setting(data_directory, Root),
  directory_subdirectory(Root, Hash1, Dir1),
  directory_subdirectory(Dir1, Hash2, Dir2),
  atom_concat(Hash1, Hash2, Hash).



%! find_hash_file(+Local:atom, -Hash:atom, -File:atom) is nondet.

find_hash_file(Local, Hash, File) :-
  find_hash_directory(Dir, Hash),
  directory_file_path(Dir, Local, File),
  exists_file(File).



%! hash_directory(+Hash:atom, -Directory:atom) is det.

hash_directory(Hash, Dir) :-
  setting(ll:data_directory, Root),
  hash_directory(Root, Hash, Dir).



%! hash_entry_hash(+Hash1:atom, +Entry:atom, -Hash2:atom) is det.

hash_entry_hash(Hash1, Entry, Hash2) :-
  md5(Hash1-Entry, Hash2).



%! hash_file(+Hash:atom, +Local:atom, -File:atom) is det.

hash_file(Hash, Local, File) :-
  setting(data_directory, Dir),
  hash_file(Dir, Hash, Local, File).



%! running_loop(:Goal_0) is det.

running_loop(Goal_0) :-
  Goal_0, !,
  running_loop(Goal_0).
running_loop(Goal_0) :-
  sleep(1),
  running_loop(Goal_0).



%! seed_base_uri(+Seed:dict, -BaseUri:atom) is det.
% ???

seed_base_uri(Seed, BaseUri) :-
  _{uri: BaseUri} :< Seed, !.
seed_base_uri(Seed1, BaseUri) :-
  _{parent: Parent} :< Seed1,
  seed(Parent, Seed2),
  seed_base_uri(Seed2, BaseUri).



%! seedlist_request_(+Segments:list(atom), ?Query:list(compound), :Goal_1,
%!                   +Options:list(compound)) is semidet.

seedlist_request(Segments, Query, Goal_1, Options) :-
  maplist(
    setting,
    [authority,password,scheme,user],
    [Auth,Password,Scheme,User]
  ),
  uri_comps(Uri, uri(Scheme,Auth,Segments,Query,_)),
  http_call(
    Uri,
    Goal_1,
    [accept(json),authorization(basic(User,Password))|Options]
  ).



%! stale_seed(-Seed:dict) is semidet.

stale_seed(Seed) :-
  seedlist_request([seed,stale], _, seed_(Seed), [failure(404),method(patch)]).

seed_(Seed, In) :-
  call_cleanup(
    (
      json_read_dict(In, Seeds, [value_string_as(atom)]),
      (is_list(Seeds) -> member(Seed, Seeds) ; Seed = Seeds)
    ),
    close(In)
  ).



%! stream_metadata(+In:stream, -Metadata:dict) is det.

stream_metadata(In, Meta) :-
  stream_property(In, position(Pos)),
  stream_position_data(byte_count, Pos, NumBytes),
  stream_position_data(char_count, Pos, NumChars),
  stream_position_data(line_count, Pos, NumLines),
  stream_property(In, newline(Newline)),
  Meta = _{
    bytes: NumBytes,
    characters: NumChars,
    lines: NumLines,
    newline: Newline
  }.



%! touch_hash_file(+Hash:atom, +Local:atom) is det.

touch_hash_file(Hash, Local) :-
  hash_file(Hash, Local, File),
  touch(File).



%! write_meta(+Hash:atom, :Goal_3) is det.

write_meta(Hash, PLocal, Goal_1) :-
  rdf_global_id(id:Hash, S),
  rdf_global_id(def:PLocal, P),
  hash_file(Hash, 'meta.nt', File),
  setup_call_cleanup(
    open(File, append, Out),
    call(Goal_1, Out, S, P),
    close(Out)
  ).



%! write_meta_archive(+Hash:atom, +Metadata:list(dict)) is det.

write_meta_archive(Hash, L) :-
  write_meta(Hash, archive, write_meta_archive_list(L)).

write_meta_archive_list(L, Out, S, P) :-
  rdf_bnode_iri(O),
  rdf_write_triple(Out, S, P, O),
  write_meta_archive_list(O, L, Out).

write_meta_archive_list(Node, [H|T], Out) :-
  rdf_bnode_iri(First),
  rdf_write_triple(Out, Node, rdf:first, First),
  write_meta_archive_item(First, H, Out),
  (   T == []
  ->  rdf_equal(Next, rdf:type)
  ;   rdf_bnode_iri(Next),
      write_meta_http_list(Next, T, Out)
  ),
  rdf_write_triple(Out, Node, rdf:next, Next).

write_meta_archive_item(Item, Meta, Out) :-
  atomic_list_concat(Meta.filters, ' ', Filters),
  maplist(
    atom_number,
    [Mtime,Permissions,Size],
    [Meta.mtime,Meta.permissions,Meta.size]
  ),
  rdf_write_triple(Out, Item, def:filetype, literal(type(xsd:string,Meta.filetype))),
  rdf_write_triple(Out, Item, def:filter, literal(type(xsd:string,Filters))),
  rdf_write_triple(Out, Item, def:format, literal(type(xsd:string,Meta.format))),
  rdf_write_triple(Out, Item, def:mtime, literal(type(xsd:float,Mtime))),
  rdf_write_triple(Out, Item, def:name, literal(type(xsd:string,Meta.name))),
  rdf_write_triple(Out, Item, def:permissions, literal(type(xsd:positiveInteger,Permissions))),
  rdf_write_triple(Out, Item, def:permissions, literal(type(xsd:float,Size))).



%! write_meta_error(+Hash:atom, +Error:compound) is det,

write_meta_error(Hash, E) :-
  gtrace,
  print_message(informational, dummy(Hash,E)).



%! write_meta_http(+Hash:atom, +Metadata:list(dict)) is det,

write_meta_http(Hash, L) :-
  write_meta(Hash, http, write_meta_http_list(L)).

write_meta_http_list(L, Out, S, P) :-
  rdf_bnode_iri(O),
  rdf_write_triple(Out, S, P, O),
  write_meta_http_list(O, L, Out).

write_meta_http_list(Node, [H|T], Out) :-
  rdf_bnode_iri(First),
  rdf_write_triple(Out, Node, rdf:first, First),
  write_meta_http_item(First, H, Out),
  (   T == []
  ->  rdf_equal(Next, rdf:nil)
  ;   rdf_bnode_iri(Next),
      write_meta_http_list(Next, T, Out)
  ),
  rdf_write_triple(Out, Node, rdf:next, Next).

write_meta_http_item(Item, Meta, Out) :-
  dict_pairs(Meta.headers, Pairs),
  maplist(write_meta_http_header(Out, Item), Pairs),
  atom_number(Lex, Meta.status),
  rdf_write_triple(Out, Item, def:status, literal(type(xsd:positiveInteger,Lex))),
  rdf_write_triple(Out, Item, def:url, literal(type(xsd:anyURI,Meta.uri))).

% TBD: Multiple values should emit a warning in `http/http_client2'.
write_meta_http_header(Out, Item, PLocal-[Lex|_]) :-
  rdf_global_id(def:PLocal, P),
  rdf_write_triple(Out, Item, P, literal(type(xsd:string,Lex))).



%! write_meta_now(+Hash:atom, +PLocal:atom) is det.

write_meta_now(Hash, PLocal) :-
  write_meta(Hash, PLocal, write_meta_now_).

write_meta_now_(Out, S, P) :-
  get_time(Now),
  format_time(atom(Lex), "%FT%T%:z", Now),
  rdf_write_triple(Out, S, P, literal(type(xsd:dateTime,Lex))).



%! write_meta_stream(+Hash:atom, +PLocal:atom, +Metadata:dict) is det.

write_meta_stream(Hash, PLocal, Meta) :-
  write_meta(Hash, PLocal, write_meta_stream_(Meta)).

write_meta_stream_(Meta, Out, S, P) :-
  rdf_bnode_iri(O),
  rdf_write_triple(Out, rdf(S,P,O)),
  rdf_write_triple(Out, O, def:newline, literal(type(xsd:string,Meta.newline))),
  maplist(
    atom_number,
    [NumBytes,NumChars,NumLines],
    [Meta.bytes,Meta.characters,Meta.lines]
  ),
  rdf_write_triple(Out, O, def:bytes, literal(type(xsd:string,NumBytes))),
  rdf_write_triple(Out, O, def:characters, literal(type(xsd:string,NumChars))),
  rdf_write_triple(Out, O, def:lines, literal(type(xsd:string,NumLines))).



%! write_meta_triple(+Hash:atom, +PLocal:atom, +OLocal:atom) is det.

write_meta_triple(Hash, PLocal, OLocal) :-
  rdf_global_id(id:OLocal, O),
  write_meta(Hash, PLocal, write_meta_triple_(O)).

write_meta_triple_(O, Out, S, P) :-
  rdf_write_triple(Out, S, P, O).





% INITIALIZATION %

%! init_ll is det.

init_ll :-
  conf_json(Conf),
  % data directory
  create_directory(Conf.'data-directory'),
  set_setting(data_directory, Conf.'data-directory'),
  maplist(
    set_setting,
    [authority,password,scheme,user],
    [
      Conf.seedlist.authority,
      Conf.seedlist.password,
      Conf.seedlist.scheme,
      Conf.seedlist.user
    ]
  ),
  % unless under debug mode
  (    debugging(ll)
  ->   true
  ;    % error and output logs
       init_log(Conf.'data-directory')
  ).

init_log(Dir) :-
  init_out_log(Dir),
  init_err_log(Dir).

init_out_log(Dir) :-
  directory_file_path(Dir, 'out.log', File),
  protocol(File).

init_err_log(Dir) :-
  directory_file_path(Dir, 'err.log.gz', File),
  gzopen(File, write, Out),
  asserta((
    user:message_hook(E1, Kind, _) :-
      memberchk(Kind, [error,warning]),
      thread_self_property(alias(Alias)),
      replace_blobs(E1, E2),
      format(
        Out,
        "~a\t~W\n",
        [Alias,E2,[blobs(portray),portray_goal(lod_cloud_portray),quoted(true)]]
      )
  )),
  asserta(user:at_halt(close(Out))).
