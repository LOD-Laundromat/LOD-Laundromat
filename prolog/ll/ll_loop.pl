:- module(
  ll_loop,
  [
    run_loop/1, % :Goal_0
    run_loop/2  % :Goal_0, +NumberOfThreads
  ]
).

/** <module> LOD Laundromat: Main loop

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).

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
    run_loop(0),
    run_loop(0, +),
    running_loop(0).





%! run_loop(:Goal_0) is det.
%! run_loop(:Goal_0, +NumberOfThreads:nonneg) is det.

run_loop(Goal_0) :-
  run_loop(Goal_0, 1).


run_loop(Goal_0, M) :-
  strip_module(Goal_0, _, Pred),
  forall(
    between(1, M, _),
    (
      flag(Pred, N, N+1),
      format(atom(Alias), "~a ~D", [Pred,N]),
      thread_create(running_loop(Goal_0), _, [alias(Alias),detached(true)])
    )
  ).

running_loop(Goal_0) :-
  Goal_0, !,
  running_loop(Goal_0).
running_loop(Goal_0) :-
  sleep(1),
  running_loop(Goal_0).
