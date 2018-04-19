:- module(
  ll_maintenance,
  [
    delete_empty_directories/0,
    ll_clear/0,
    ll_clear_datasets/0,
    ll_clear_organizations/0,
    ll_clear_seeds/0,
    ll_reset_processing_seeds/0,
    ll_reset_seeds/0
  ]
).

/** <module> LOD Laundromat maintenance

@author Wouter Beek
@version 2018
*/

:- use_module(library(ll/ll_seeder)).
:- use_module(library(tapir)).





%! delete_empty_directories is det.

delete_empty_directories :-
  setting(ll:data_directory, Dir0),
  forall(
    directory_path(Dir0, Dir),
    (is_empty_directory(Dir) -> delete_directory(Dir) ; true)
  ).



%! ll_clear is det.
%
% Delete all data currently stored in the LOD Laundromat.

ll_clear :-
  ll_clear_datasets,
  ll_clear_organizations,
  ll_clear_seeds.



%! ll_clear_datasets is det.

ll_clear_datasets :-
  forall(
    dataset(User, Dataset, _),
    dataset_delete(User, Dataset)
  ).



%! ll_clear_organizations is det.

ll_clear_organizations :-
  forall(
    organization(Organization, _),
    organization_delete(_, Organization)
  ).



%! ll_reset_processing_seeds is det.

ll_reset_processing_seeds :-
  forall(
    processing_seed(Seed),
    reset_seed(Seed)
  ).



%! ll_reset_seeds is det.

ll_reset_seeds :-
  forall(
    seed(Seed),
    reset_seed(Seed)
  ).
