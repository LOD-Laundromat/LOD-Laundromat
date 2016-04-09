:- module(
  wm,
  [
    add_wm/0,
    add_wms/1,              % +N
    buggy_seedpoint/1,      % ?Hash
    current_seedpoint/1,    % ?Hash
    current_wm/1,           % ?Alias
    number_of_seedpoints/1, % -N
    number_of_wms/1,        % -N
    reset/0,
    single_wm/0,
    wm_reset/1,             % +Hash
    wm_reset_and_clean/1,   % +Hash
    wm_status/0
  ]
).

/* <module> LOD Laundromat: Washing machine

@author Wouter Beek
@version 2016/01-2016/04
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(error)).
:- use_module(library(filesex)).
:- use_module(library(gen/gen_ntuples)).
:- use_module(library(hash_ext)).
:- use_module(library(http/json)).
:- use_module(library(jsonld/jsonld_metadata)).
:- use_module(library(jsonld/jsonld_read)).
:- use_module(library(lodcli/lodfs)).
:- use_module(library(lodcli/lodhdt)).
:- use_module(library(os/dir_ext)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/process_ext)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pl/pl_term)).
:- use_module(library(print_ext)).
:- use_module(library(prolog_stack)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(rdf/rdf_print)).
:- use_module(library(semweb/rdf11)). % Operators.
:- use_module(library(string_ext)).
:- use_module(library(zlib)).

:- use_module(cpack('LOD-Laundromat'/lclean)).
:- use_module(cpack('LOD-Laundromat'/seedlist)).

prolog_stack:stack_guard('C').
prolog_stack:stack_guard(none).





%! add_wm is det.
% Add a LOD Laundromat thread.

add_wm :-
  add_wms(1).



%! add_wms(+N) is det.

add_wms(0) :- !.
add_wms(M1) :-
  must_be(positive_integer, M1),
  max_wm(N1),
  N2 is N1 + 1,
  atom_concat(wm, N2, Alias),
  thread_create(start_wm0, _, [alias(Alias),detached(false)]),
  M2 is M1 - 1,
  add_wms(M2).



%! buggy_seedpoint(+Hash) is semidet.
%! buggy_seedpoint(-Hash) is nondet.

buggy_seedpoint(Hash) :-
  lunready_hash(Hash),
  \+ current_seedpoint(Hash).



%! current_seedpoint(+Hash) is semidet.
%! current_seedpoint(-Hash) is nondet.

current_seedpoint(Hash) :-
  thread_seed(_, Hash).



%! current_wm(+Alias) is semidet.
%! current_wm(-Alias) is nondet.

current_wm(Alias) :-
  thread_property(Id, alias(Alias)),
  atom_prefix(wm, Alias),
  thread_property(Id, status(running)).



%! max_wm(-N) is det.

max_wm(N) :-
  aggregate_all(
    max(N),
    (
      current_wm(Alias),
      atom_concat(wm, N0, Alias),
      atom_number(N0, N)
    ),
    N
  ), !.
max_wm(0).



%! number_of_seedpoints(-N) is det.

number_of_seedpoints(N) :-
  aggregate_all(count, lunready_hash(_), N).



%! number_of_wms(-N) is det.

number_of_wms(N) :-
  aggregate_all(count, current_wm(_), N).



%! single_wm is det.

single_wm :-
  start_wm0.



%! wm_reset is det.
% Reset the LOD Laundromat.
% This removes all data files and resets the seedlist.

wm_reset :-
  lroot(Root),
  forall(dir_file(Root, Subdir), delete_directory_and_contents(Subdir)),
  absolute_file_name(cpack('LOD-Laundromat'), Dir, [file_type(directory)]),
  run_process(git, ['checkout','seedlist.db'], [cwd(Dir)]),
  retractall(wm_hash0(_,_)).


%! wm_reset(+Hash) is det.

wm_reset(Hash) :-
  % Do not reset seedpoints that are currently being processed by
  % a washing machine.
  \+ current_seedpoint(Hash),
  reset(Hash).



%! wm_reset_and_clean(+Hash) is det.

wm_reset_and_clean(Hash) :-
  reset(Hash),
  clean(Hash).



wm_status :-
  number_of_wms(N1),
  number_of_seedpoints(N2),
  msg_notification(
    "~D washing machines are cleaning ~D seedpoints.~n",
    [N1,N2]
  ).





% HELPERS %

start_wm0 :-
  wm0(_{idle: 0}).


wm0(State) :-
  % Clean one -- arbitrarily chosen -- seed.
  clean, !,
  number_of_wms(N),
  debug(wm(thread), "~D washing machines are currently active.", [N]),
  wm0(State).
wm0(State) :-
  sleep(1),
  dict_inc(idle, State, N),
  thread_name(Alias),
  debug(wm(idle), "ZZZZ Thread ~w idle ~D sec.", [Alias,N]),
  wm0(State).
