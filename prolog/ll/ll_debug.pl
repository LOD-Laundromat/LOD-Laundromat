:- module(
  ll_debug,
  [
    ll_debug/3 % +Directory, +Dataset, +Uris
  ]
).

/** <module> LOD Laundromat: Debug tools

@author Wouter Beek
@version 2018
*/

:- use_module(library(debug)).

:- use_module(library(ll/ll_dataset)).

:- debug(lod_laundromat).





/*
%! ll_dataset_debug(+Directory:atom, +Dataset:atom, +Uris:list(atom)) is det.

ll_dataset_debug(Dir, Dataset, Uris) :-
  seed(Package),
  _{name: Dataset} :< Package, !,
  work_package(Dataset, Package).
*/
