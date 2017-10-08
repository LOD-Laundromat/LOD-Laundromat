:- use_module(library(debug)).
:- use_module(library(ll/ll)).
:- use_module(library(ll/ll_seedlist)).
:- use_module(library(ll/ll_sources)).

:- debug(ll(_)).

test1 :-
  add_uri('http://ieee.rkbexplorer.com/models/dump.tgz').

test_lov(Uri) :-
  ll_source(lov, Uri),
  add_uri(Uri).
