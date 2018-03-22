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

:- use_module(library(tapir)).





%! ll_clear is det.
%
% Delete all data currently stored in the LOD Laundromat.

ll_clear :-
  tapir:user_(Site, User),
  % Remove all datasets.
  forall(
    dataset(Site, User, Dataset, _),
    dataset_delete(Site, User, Dataset)
  ),
  % Remove all organizations.
  forall(
    (
      account(Account, Dict),
      _{type: org} :< Dict
    ),
    organization_delete(wouter, Account)
  ).
