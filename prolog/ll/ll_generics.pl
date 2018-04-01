:- module(
  ll_generics,
  [
    call_loop/1,           % :Goal_0
    delete_empty_directories/0,
    failure_success/4,     % +Hash, +Local, ?Term, +E
    find_hash_directory/2, % -Directory, -Hash
    find_hash_file/3,      % +Local, -Hash, -File
    hash_directory/2,      % +Hash, -Directory
    hash_entry_hash/3,     % +Hash1, +Entry, -Hash2
    hash_file/3,           % +Hash, +Local, -File
    read_term_from_file/2, % +File, -Term
    seedlist_request/4,    % +Segments, ?Query, :Goal_1, +Options
    stale_seed/1,          % -Seed
    touch_hash_file/2      % +Hash, +Local
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
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(thread_ext)).
:- use_module(library(uri_ext)).
:- use_module(library(write_ext)).

:- initialization
   init_ll.

:- meta_predicate
    call_loop(0),
    running_loop(0),
    seedlist_request(+, ?, 1, +).

ll_portray(Blob, Options) :-
  blob(Blob, Type),
  \+ atom(Blob),
  Type \== reserved_symbol,
  write_term('BLOB'(Type), Options).

:- rdf_meta
   rdf_meta_triple(+, r, r, o).

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



%! delete_empty_directories is det.

delete_empty_directories :-
  setting(data_directory, Dir0),
  forall(
    directory_path(Dir0, Dir),
    (is_empty_directory(Dir) -> delete_directory(Dir) ; true)
  ).



%! failure_success(+Hash:atom, +Local:atom, ?Term:term, +E:compound) is det.

% success
failure_success(Hash, Local, Term, E) :-
  var(E), !,
  (   var(Term)
  ->  touch_hash_file(Hash, Local)
  ;   hash_file(Hash, Local, File),
      setup_call_cleanup(
        open(File, write, Out),
        format(Out, "~W\n", [Term,[quoted(true)]]),
        close(Out)
      )
  ).
% failure
failure_success(Hash, _, _, E) :-
  write_meta_error(Hash, E).



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



%! read_term_from_file(+File:atom, -Term:term) is semidet.

read_term_from_file(File, Term) :-
  call_stream_file(File, read_term_from_file_(Term)).

read_term_from_file_(Term, In) :-
  repeat,
  read_line_to_string(In, Line),
  (   Line == end_of_file
  ->  !
  ;   read_term_from_atom(Line, Term, [])
  ).


%! running_loop(:Goal_0) is det.

running_loop(Goal_0) :-
  Goal_0, !,
  running_loop(Goal_0).
running_loop(Goal_0) :-
  sleep(1),
  running_loop(Goal_0).



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



%! touch_hash_file(+Hash:atom, +Local:atom) is det.

touch_hash_file(Hash, Local) :-
  hash_file(Hash, Local, File),
  touch(File).





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
  % worker threads

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
        [Alias,E2,[blobs(portray),portray_goal(ll_portray),quoted(true)]]
      )
  )),
  asserta(user:at_halt(close(Out))).
