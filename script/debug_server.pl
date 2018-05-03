:- use_module(library(debug)).
:- use_module(library(thread_ext)).

:- debug(ll(offline)).

:- use_module(library(ll/ll_download)).
:- use_module(library(ll/ll_init)).

debug_uri(Uri) :-
  ll_download(Uri).
