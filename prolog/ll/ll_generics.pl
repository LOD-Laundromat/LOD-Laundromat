:- module(
  ll_generics,
  [
    end_processing/2,   % +Alias, +Hash
    end_task/3,         % +Hash, +Alias, +State
    finish/2,           % +Hash, +State
    handle_status/4,    % +Hash, +Status, +Alias, +State
    hash_entry_hash/3,  % +Hash1, +Entry, -Hash2
    hash_file/3,        % +Hash, +Local, -File
    processing_file/3,  % ?Alias, ?Hash, -File
    start_processing/2, % +Alias, +Hash
    start_task/3        % +AliasPair, -Hash, -State
  ]
).

/** <module> LOD Laundromat: Generics

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(error)).
:- use_module(library(lists)).

:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(rocks_ext)).
:- use_module(library(semweb/ldfs)).





%! end_processing(+Alias:oneof([download,decompress,recode,parse]), +Hash:atom) is det.

end_processing(Alias, Hash) :-
  processing_file(Alias, Hash, File),
  delete_file(File),
  debug(ll(task), "[END] ~a: ~a", [Alias,Hash]).



%! end_task(+Hash:atom, +Alias:oneof([download,decompress,recode,parse]), +State:dict) is det.

end_task(Hash, Alias, State) :-
  rocks_put(Alias, Hash, State).



%! finish(+Hash:atom, +State:dict) is det.

finish(Hash, State) :-
  hash_file(Hash, finished, File),
  touch(File),
  end_task(Hash, seeds, State).



%! handle_status(+Hash:atom, +Status, +Alias:oneof([download,decompress,recode,parse]), +State:dict) is det.

% Successfully parsed means the job is done.  Status may be
% uninstantiated, e.g., when coming from `catch(some_task, Status,
% true)'.
handle_status(Hash, true, Alias, State) :- !,
  (   % The last task has completed successfully.  The state is added
      % to the seedlist.
      Alias == seed
  ->  finish(Hash, State)
  ;   % A non-last task has completed successfully.
      end_task(Hash, Alias, State)
  ).
% Use the state returned by thread_exit/1 if available.
handle_status(Hash, exited(State), Alias, _) :- !,
  handle_status(Hash, true, Alias, State).
% Unsuccessfully ended the last task menas the job is over.
handle_status(Hash, Status, _, State) :-
  status_error(Status, E),
  write_message(error, Hash, E),
  finish(Hash, State).

status_error(exception(E), E) :- !.
status_error(E, E).



%! hash_entry_hash(+Hash1:atom, +Entry:atom, -Hash2:atom) is det.

hash_entry_hash(Hash1, Entry, Hash2) :-
  md5(Hash1-Entry, Hash2).



%! hash_file(+Hash:atom, +Local:atom, -File:atom) is det.

hash_file(Hash, Local, File) :-
  ldfs_root(Root),
  hash_file(Root, Hash, Local, File).



%! processing_file(?Alias:oneof([download,decompress,recode,parse]), +Hash:atom, -File:atom) is det.
%! processing_file(?Alias:oneof([download,decompress,recode,parse]), -Hash:atom, -File:atom) is nondet.

processing_file(Alias, Hash, File) :-
  (   ground(Alias)
  ->  must_be(oneof([download,decompress,recode,parse]), Alias)
  ;   member(Alias, [download,decompress,recode,parse])
  ),
  ldfs_root(Root),
  directory_file_path(Root, processing, Dir1),
  create_directory(Dir1),
  directory_file_path(Dir1, Alias, Dir2),
  create_directory(Dir2),
  directory_file_path2(Dir2, Hash, File).



%! start_task(+AliasPair:pair(oneof([download,decompress,recode,parse])), -Hash:atom, -State:dict) is det.

start_task(_-ToAlias, Hash, State) :-
  processing_file(ToAlias, Hash, File), !,
  ldfs_directory(Hash, false, Dir),
  delete_directory_and_contents(Dir),
  delete_file(File),
  debug(ll(reset), "~a: ~a", [ToAlias,Hash]).
start_task(FromAlias-_, Hash, State) :-
  with_mutex(ll_generics, (
    rocks_enum(FromAlias, Hash, State),
    rocks_delete(FromAlias, Hash)
  )),
  processing_file(FromAlias, Hash, File),
  touch(File).
