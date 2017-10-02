:- use_module(library(debug)).
:- use_module(library(ll/ll)).
:- use_module(library(ll/ll_sources)).

:- debug(ll(_)).

test1 :-
  add_uri('http://ieee.rkbexplorer.com/models/dump.tgz').
test2 :-
  add_uri('http://www.w3.org/ns/auth/acl').
