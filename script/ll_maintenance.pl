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
  % Remove all datasets.
  forall(
    dataset(_, Dataset, _),
    dataset_delete(_, Dataset)
  ),
  % Remove all organizations.
  forall(
    organization(Organization),
    organization_delete(_, Organization)
  ).
