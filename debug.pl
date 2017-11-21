:- use_module(library(debug)).

:- [run].

:- tmon.

init_debug :-
  debug(dot),
  debug(ll),
  debug(ll(download)),
  debug(ll(guess)),
  debug(ll(index)),
  debug(ll(parse)),
  debug(ll(store)).
:- init_debug.

%:- dmon.
