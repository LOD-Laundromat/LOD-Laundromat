:- module(
  exception_schema,
  [
    exception_schema_assert/1 % +Graph:atom
  ]
).

/** <module> Exception schema

Asserts the exception schema.

@author Wouter Beek
@version 2014/08
*/

:- use_module(library(semweb/rdf_db)).

:- use_module(plRdf(rdfs_build2)).

:- rdf_register_prefix(exception, 'http://lodlaundromat.org/exception/ontology/').



assert_schema(Graph):-
  rdfs_assert_class(
    exception:'Exception',
    rdfs:'Class',
    exception,
    'An action that is not part of ordinary operations or standards.',
    Graph
  ),



