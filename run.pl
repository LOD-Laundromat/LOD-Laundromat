:- set_prolog_flag(optimise, true).
:- set_prolog_stack(global, limit(150*10**9)).
:- use_module(library(http/http_unix_daemon)).
:- use_module(library(http/thread_httpd)).

:- use_module(prolog/ll).
:- use_module(prolog/ll_seedlist).
:- use_module(prolog/ll_server).

:- use_module(library(debug)).
:- debug(ll(_)).

test :-
  add_uri('http://ieee.rkbexplorer.com/models/dump.tgz').
