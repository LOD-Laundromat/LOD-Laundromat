:- module(
  ll_maintenance,
  [
    ll_clear/0
  ]
).

/** <module> LOD Laundromat maintenance

@author Wouter Beek
@version 2018
*/

:- use_module(library(ll/ll_seeder)).
:- use_module(library(tapir)).





%! ll_clear is det.
%
% Delete all data currently stored in the LOD Laundromat.

ll_clear :-
  % Remove all datasets.
  forall(
    dataset(User, Dataset, _),
    dataset_delete(User, Dataset)
  ),
  % Remove all organizations.
  forall(
    organization(Organization, _),
    organization_delete(_, Organization)
  ),
  % Remove all seeds.
  forall(
    seed(Seed),
    (
      _{hash: Hash} :< Seed,
      delete_seed(Hash)
    )
  ).
