:- module(
  ll_maintenance,
  [
    delete_empty_directories/0
  ]
).

/** <module> LOD Laundromat maintenance

@author Wouter Beek
@version 2018
*/

:- use_module(library(file_ext)).





%! delete_empty_directories is det.

delete_empty_directories :-
  setting(ll:data_directory, Dir0),
  forall(
    directory_path(Dir0, Dir),
    (is_empty_directory(Dir) -> delete_directory(Dir) ; true)
  ).
