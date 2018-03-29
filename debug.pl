:- use_module(library(debug)).
:- use_module(library(thread_ext)).

:- debug(ll).

:- use_module(library(ll/ll_dataset)).
:- use_module(library(ll/ll_init)).
:- use_module(library(ll/ll_seeder)).

:- dynamic
    ll:skip_error/1.

:- multifile
    ll:skip_error/1.

ll:skip_error(non_canonical_lexical_form(_,_,_)).





%! ll_debug(+Hash:atom) is det.

ll_debug(Hash) :-
  seed_by_hash(Hash, Seed),
  ll_dataset(Seed).
