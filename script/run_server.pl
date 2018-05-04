:- set_prolog_flag(verbose, silent).

:- use_module(library(ll/ll_init)).

:- initialization(run, main).

run :-
  thread_get_message(stop).
