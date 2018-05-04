:- module(
  ll_loop,
  [
    run_loop/3  % :Goal_0, +Sleep, +NumberOfThreads
  ]
).

/** <module> LOD Laundromat: Main loop

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).
:- use_module(library(debug)).

:- use_module(library(ll/ll_decompress)).
:- use_module(library(ll/ll_download)).
:- use_module(library(ll/ll_parse)).
:- use_module(library(ll/ll_recode)).

:- initialization
   flag(ll_download, _, 1),
   flag(ll_decompress, _, 1),
   flag(ll_recode, _, 1),
   flag(ll_parse, _, 1).

:- meta_predicate
    run_loop(0, +, +),
    running_loop(+, 0).

:- multifile
    init_loop_hook/1.





%! run_loop(:Goal_0, +Sleep:nonneg, +NumberOfThreads:nonneg) is det.

run_loop(Goal_0, Sleep, M) :-
  strip_module(Goal_0, _, Pred),
  forall(
    between(1, M, _),
    (
      flag(Pred, N, N+1),
      format(atom(Alias), "~a-~D", [Pred,N]),
      thread_create(start_loop(Pred, Sleep, Goal_0), _, [alias(Alias),detached(true)])
    )
  ).


start_loop(Pred, Sleep, Goal_0) :-
  init_loop(Pred),
  running_loop(Sleep, Goal_0).


init_loop(Pred) :-
  init_loop_hook(Pred), !.
init_loop(_).


running_loop(Sleep, Goal_0) :-
  Goal_0, !,
  running_loop(Sleep, Goal_0).
running_loop(Sleep, Goal_0) :-
  (debugging(ll(idle)) -> debug(ll(idle), "💤 ~w", [Goal_0]) ; true),
  sleep(Sleep),
  running_loop(Sleep, Goal_0).
