:- set_prolog_flag(verbose, silent).

:- use_module(library(ll/ll_loop)).

run :-
  thread_get_message(stop).
:- run.
