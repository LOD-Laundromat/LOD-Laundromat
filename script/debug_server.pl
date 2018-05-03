:- use_module(library(debug)).
:- use_module(library(thread_ext)).

:- use_module(library(ll/ll_download)).
:- use_module(library(ll/ll_init)).

:- debug(ll(offline)).

debug_uri(Uri) :-
  ll_download(Uri).
