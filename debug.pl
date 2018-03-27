:- use_module(library(debug)).
:- use_module(library(thread_ext)).

:- debug(ll).

:- use_module(library(ll/ll_dataset)).
:- use_module(library(ll/ll_init)).
:- use_module(library(ll/ll_seeder)).

ll_debug(Hash) :-
  seed_by_hash(Hash, Seed),
  ll_dataset(Seed).
