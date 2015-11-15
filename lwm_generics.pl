:- module(
  lwm_generics,
  [
    document_directory/2 % +Document:iri
                         % -Directory:atom
  ]
).

/** <module> LOD Laundromat: Generic predicates

@author Wouter Beek
@version 2015/11
*/

:- use_module(library(apply)).
:- use_module(library(filesex)).
:- use_module(library(semweb/rdf_db)).

:- use_module('LOD-Laundromat'(lwm_settings)).





%! document_directory(+Document:iri, -Directory:atom) is det.
% Returns the absolute directory of a specific MD5.

document_directory(Doc, Dir2):-
  document_name(Doc, Md5),
  
  % Outer directory.
  atom_codes(Md5, [X,Y|T]),
  maplist(atom_codes, [DirName1,DirName2], [[X,Y],T]),
  absolute_file_name(
    lwm_data(DirName1),
    Dir1,
    [access(write),file_type(directory)]
  ),
  make_directory_path(Dir1),

  % Inner directory.
  absolute_file_name(
    DirName2,
    Dir2,
    [access(write),file_type(directory),relative_to(Dir1)]
  ),
  make_directory_path(Dir2).
