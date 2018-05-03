:- module(
  ll_generics,
  [
    copy_task_files/2,     % +FromHash, +ToHash
    end_task/2,            % +Hash, +Local
    find_hash/2,           % ?Hash, +Local
    find_hash_directory/2, % ?Hash, -Directory
    find_hash_file/3,      % ?Hash, +Local, -File
    finish/1,              % +Hash
    hash_directory/2,      % +Hash, -Directory
    hash_entry_hash/3,     % +Hash1, +Entry, -Hash2
    hash_file/3,           % +Hash, +Local, -File
    hash_url/2,            % +Hash, -Url
    read_task_memory/3,    % +Hash, +Local, -Term
    start_seed/1,          % -Seed
    touch_hash_file/2,     % +Hash, +Local
    write_task_memory/3    % +Hash, +Local, +Term
  ]
).

/** <module> LOD Laundromat: Generics

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(http/json)).
:- use_module(library(lists)).

:- use_module(library(call_ext)).
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(uri_ext)).

:- dynamic
    ll:debug/2.

:- meta_predicate
    seedlist_request(+, ?, 1),
    seedlist_request(+, ?, 1, +).





%! copy_task_files(+FromHash:atom, +ToHash:atom) is det.

copy_task_files(Hash1, Hash2) :-
  forall(
    task_file_local(Local),
    (   find_hash_file(Hash1, Local, File1)
    ->  hash_file(Hash2, Local, File2),
        copy_file(File1, File2)
    ;   true
    )
  ).

task_file_local(base_uri).
task_file_local(http_media_type).



%! end_task(+Hash:atom, +Local:atom) is det.

end_task(Hash, Local) :-
  touch_hash_file(Hash, Local).



%! find_hash(+Hash:atom, +Local:atom) is semidet.
%! find_hash(-Hash:atom, +Local:atom) is nondet.

find_hash(Hash, Local) :-
  find_hash_file(Hash, Local, _).



%! find_hash_directory(+Hash:atom, -Directory:atom) is semidet.
%! find_hash_directory(-Hash:atom, -Directory:atom) is nondet.

find_hash_directory(Hash, Dir2) :-
  setting(ll:data_directory, Root),
  (   var(Hash)
  ->  directory_subdirectory(Root, Hash1, Dir1),
      directory_subdirectory(Dir1, Hash2, Dir2),
      atom_concat(Hash1, Hash2, Hash)
  ;   hash_directory(Hash, Dir2),
      exists_directory(Dir2)
  ).



%! find_hash_file(+Hash:atom, +Local:atom, -File:atom) is semidet.
%! find_hash_file(-Hash:atom, +Local:atom, -File:atom) is nondet.

find_hash_file(Hash, Local, File) :-
  find_hash_directory(Hash, Dir),
  directory_file_path(Dir, Local, File),
  exists_file(File).



%! finish(+Hash:atom) is det.

finish(Hash) :-
  touch_hash_file(Hash, finished),
  hash_file(Hash, 'meta.nq', File),
  compress_file(File),
  delete_file(File),
  (   seedlist_request([seed,processing], [hash(Hash)], true)
  ->  seedlist_request([seed,processing], [hash(Hash)], true, [method(patch)])
  ;   true
  ).



%! hash_directory(+Hash:atom, -Directory:atom) is det.

hash_directory(Hash, Dir) :-
  setting(ll:data_directory, Root),
  hash_directory(Root, Hash, Dir).



%! hash_entry_hash(+Hash1:atom, +Entry:atom, -Hash2:atom) is det.

hash_entry_hash(Hash1, Entry, Hash2) :-
  md5(Hash1-Entry, Hash2).



%! hash_file(+Hash:atom, +Local:atom, -File:atom) is det.

hash_file(Hash, Local, File) :-
  setting(ll:data_directory, Dir),
  hash_file(Dir, Hash, Local, File).



%! hash_url(+Hash:atom, -Url:atom) is det.

hash_url(Hash, Url) :-
  seedlist_request([seed], [hash(Hash)], seed_(Seed)),
  Url = Seed.url.



%! read_task_memory(+Hash:atom, +Local:atom, -Term:term) is semidet.

read_task_memory(Hash, Local, Term) :-
  find_hash_file(Hash, Local, File),
  setup_call_cleanup(
    open(File, read, In),
    (
      read_line_to_string(In, Line),
      read_term_from_atom(Line, Term, [])
    ),
    close(In)
  ).



%! seedlist_request(+Segments:list(atom), ?Query:list(compound), :Goal_1) is semidet.
%! seedlist_request(+Segments:list(atom), ?Query:list(compound), :Goal_1,
%!                  +Options:list(compound)) is semidet.

seedlist_request(Segments, Query, Goal_1) :-
  seedlist_request(Segments, Query, Goal_1, []).


seedlist_request(Segments, Query, Goal_1, Options) :-
  setting(ll:authority, Auth),
  setting(ll:password, Password),
  setting(ll:scheme, Scheme),
  setting(ll:user, User),
  uri_comps(Uri, uri(Scheme,Auth,Segments,Query,_)),
  http_call(
    Uri,
    Goal_1,
    [accept(json),authorization(basic(User,Password)),failure(404)|Options]
  ).



%! start_seed(-Seed:dict) is semidet.

start_seed(Seed) :-
  seedlist_request([seed,stale], _, seed_(Seed), [method(patch)]).

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



%! write_task_memory(+Hash:atom, +Local:atom, +Term:term) is det.

write_task_memory(Hash, Local, Term) :-
  hash_file(Hash, Local, File),
  setup_call_cleanup(
    open(File, write, Out),
    format(Out, "~W\n", [Term,[quoted(true)]]),
    close(Out)
  ).
