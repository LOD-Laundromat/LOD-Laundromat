:- module(
  ll_cloud,
  [
    ll_store/0,
    number_of_datasets/1, % -N
    number_of_triples/1   % -N
  ]
).

/** <module> LOD Laundromat: Cloud

@author Wouter Beek
@version 2017/11
*/

:- use_module(library(aggregate)).
:- use_module(library(debug)).
:- use_module(library(tapir)).

:- use_module(ll_seedlist).
:- use_module(ll_generics).



ll_store :-
  with_mutex(ll_store, (
    seed(Seed),
    Hash{status: generated} :< Seed,
    seed_merge(Hash{status: storing})
  )),
  debug(ll(store), "┌─> storing (~a)", [Hash]),
  hash_file(Hash, 'clean.nq.gz', File),
  Properties = _{
    accessLevel: public,
    files: [File]
  },
  dataset_upload(Hash, Properties),
  debug(ll(store), "└─< stored", []),
  with_mutex(ll_store, seed_merge(Hash{status: stored})).



%! number_of_datasets(-N:nonneg) is det.

number_of_datasets(N) :-
  aggregate_all(count, dataset(lodlaundromat, _), N).



%! number_of_triples(-N:nonneg) is det.

number_of_triples(N) :-
  N = 0.
  %aggregate_all(count, (dataset(lodlaundromat, _, Dict), writeln(Dict)), N).
