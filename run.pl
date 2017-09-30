:- reexport(library(ll/ll_analysis)).
:- reexport(library(ll/ll_download)).
:- reexport(library(ll/ll_guess)).
:- reexport(library(ll/ll_index)).
:- reexport(library(ll/ll_parse)).
:- reexport(library(ll/ll_seedlist)).
:- reexport(library(ll/ll_show)).
:- reexport(library(ll/ll_unarchive)).

:- reexport(test/test_seeds).

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(ll/ll_generics)).

:- debug(ll).

end :-
  clear_all,
  halt.

test :-
  tmon,
  maplist(call_loop, [ll_download,ll_unarchive,ll_guess,ll_parse,ll_index]).

test1 :-
  add_uri('http://ieee.rkbexplorer.com/models/dump.tgz').
