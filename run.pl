:- reexport(library(ll/ll_analysis)).
:- reexport(library(ll/ll_download)).
:- reexport(library(ll/ll_seedlist)).
:- reexport(library(ll/ll_unarchive)).

:- use_module(library(debug)).
:- debug(ll).

test :-
  add_uri('http://ieee.rkbexplorer.com/models/dump.tgz'),
  ll_download,
  ll_unarchive,
  true.

end :-
  clear_seedlist,
  halt.
