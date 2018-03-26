:- module(
  ll_maintenance,
  [
    ll_clear/0,
    ll_reset_processing_seeds/0
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
    delete_seed(Seed)
  ).



%! ll_reset_processing_seeds is det.
%
% TBD: Do not delete these seeds, but put them back to being stale.

ll_reset_processing_seeds :-
  forall(
    processing_seed(Seed),
    delete_seed(Seed)
  ).
