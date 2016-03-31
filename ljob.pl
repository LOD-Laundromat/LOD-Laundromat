:- module(
  ljob,
  [
    ctuples/1 % -N
  ]
).

/** <module> LOD Laundromat jobs

@author Wouter Beek
@version 2016/03
*/

:- use_module(library(aggregate)).

:- use_module(cpack('LOD-Laundromat'/lcli)).

ctuples(N) :-
  aggregate_all(sum(N), lm(_, llo:unique_tuples, ^^(N,_)), N).
