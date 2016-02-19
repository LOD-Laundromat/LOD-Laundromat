:- module(
  lwm_generics,
  [
    lwm_document_dir/2 % +Document:iri
                       % -Directory:atom
  ]
).

/** <module> LOD Laundromat: Generic predicates

@author Wouter Beek
@version 2015/11
*/

:- use_module(library(apply)).
:- use_module(library(filesex)).
:- use_module(library(lodapi/lodapi_generics)).
:- use_module(library(semweb/rdf_db)).

:- use_module('LOD-Laundromat'(lwm_settings)).





%! lwm_document_dir(+Document:iri, -Directory:atom) is det.
% Returns the absolute directory of a specific MD5.

lwm_document_dir(Doc, Dir):-
  document_path(Doc, Path),
  absolute_file_name(lwm_data, Dir0, [access(write),file_type(directory)]),
  directory_file_path(Dir0, Path, Dir),
  make_directory_path(Dir).
