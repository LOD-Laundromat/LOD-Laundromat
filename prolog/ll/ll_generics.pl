:- module(
  ll_generics,
  [
    begin_task/3,      % +Alias, -Hash, -State
    end_task/3,        % +Hash, +Alias, +State
    end_task/4,        % +Hash, +Status, +Alias, +State
    finish_task/2,     % +Hash, +State
    hash_entry_hash/3, % +Hash1, +Entry, -Hash2
    hash_file/3        % +Hash, +Local, -File
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
:- use_module(library(json_ext)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(rocks_ext)).
:- use_module(library(semweb/ldfs)).





%! begin_task(+Alias:oneof([download,decompress,recode,parse]), -Hash:atom, -State:dict) is det.
%
% We are going to begin a task of type `Alias'.

begin_task(Alias, Hash, State) :-
  (   % We can find a task of type `Alias' which was started earlier,
      % but did not complete during a previous run.
      processing_file(Alias, Hash, File)
  ->  json_load(File, State),
      debug(ll(reset), "~a: ~a", [Alias,Hash])
  ;   % We can find a record in the state store that was not run before.
      with_mutex(ll_generics, (
        rocks_enum(Alias, Hash, State),
        rocks_delete(Alias, Hash)
      )),
      processing_file(Alias, Hash, File),
      json_save(File, State)
  ),
  (debugging(ll(offline,Hash)) -> gtrace ; true),
  indent_debug(1, ll(task,Alias), "> ~a ~a", [Alias,Hash]).



%! end_task(+Hash:atom, +Alias:oneof([download,decompress,recode,parse]), +State:dict) is det.
%
% Ends a task of type `Alias'.

end_task(Hash, Alias, State) :-
  % Move the state to the state store that corresponds to the next
  % task type.
  next_task(Alias, Next),
  rocks_put(Next, Hash, State),
  processing_file(Alias, Hash, File),
  delete_file(File),
  debug(ll(task), "[END] ~a: ~a", [Alias,Hash]).



%! end_task(+Hash:atom, +Status, +Alias:oneof([download,decompress,recode,parse]), +State:dict) is det.

% Successfully parsed means the job is done.  Status may be
% uninstantiated, e.g., when coming from `catch(some_task, Status,
% true)'.
end_task(Hash, true, parse, State) :- !,
  % The last task has completed successfully.  The state is added to
  % the seedlist.
  finish_task(Hash, State).
end_task(Hash, true, Alias, State) :- !,
  % A non-last task has completed successfully.
  end_task(Hash, Alias, State).
% Use the state returned by thread_exit/1 if available.
end_task(Hash, exited(State), Alias, _) :- !,
  end_task(Hash, true, Alias, State).
% Unsuccessfully ended the last task menas the job is over.
end_task(Hash, Status, _, State) :-
  status_error(Status, E),
  write_message(error, Hash, E),
  finish_task(Hash, State).

status_error(exception(E), E) :- !.
status_error(E, E).



%! finish_task(+Hash:atom, +State:dict) is det.

finish_task(Hash, State) :-
  hash_file(Hash, finish, File),
  touch(File),
  rocks_put(seed, Hash, State),
  processing_file(Alias, Hash, File),
  delete_file(File),
  debug(ll(finish), "~a ~a", [Alias,Hash]).



%! hash_entry_hash(+Hash1:atom, +Entry:atom, -Hash2:atom) is det.

hash_entry_hash(Hash1, Entry, Hash2) :-
  md5(Hash1-Entry, Hash2).



%! hash_file(+Hash:atom, +Local:atom, -File:atom) is det.

hash_file(Hash, Local, File) :-
  ldfs_root(Root),
  hash_file(Root, Hash, Local, File).





% HELPERS %

%! next_task(+From, -To) is det.
%! next_task(-From, +To) is det.

next_task(From, To) :-
  once(next_task_(From, To)).

next_task_(seed, stale).
next_task_(stale, download).
next_task_(download, decompress).
next_task_(decompress, reode).
next_task_(recode, parse).
next_task_(parse, seed).



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
