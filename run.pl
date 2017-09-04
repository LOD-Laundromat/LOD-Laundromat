:- reexport(library(ll/ll_analysis)).
:- reexport(library(ll/ll_download)).
:- reexport(library(ll/ll_guess)).
:- reexport(library(ll/ll_parse)).
:- reexport(library(ll/ll_seedlist)).
:- reexport(library(ll/ll_unarchive)).

:- use_module(library(apply)).
:- use_module(library(debug)).

:- debug(ll).

:- meta_predicate
    call_loop(0),
    running_loop(0).

end :-
  clear_all,
  halt.

test :-
  tmon,
  maplist(call_loop, [ll_download,ll_unarchive,ll_guess,ll_parse]).

test1 :-
  add_uri('http://ieee.rkbexplorer.com/models/dump.tgz').

call_loop(Mod:Goal_0) :-
  thread_create(running_loop(Mod:Goal_0), _, [alias(Goal_0),detached(true)]).

running_loop(Goal_0) :- Goal_0, !, running_loop(Goal_0).
running_loop(Goal_0) :- sleep(1), running_loop(Goal_0).
