:- use_module(library(debug)).
:- use_module(library(ll/ll)).
:- use_module(library(ll/ll_sources)).

:- debug(ll(_)).

test1 :-
  add_seed('http://ieee.rkbexplorer.com/models/dump.tgz').
