:- module(
  lwm_reset,
  [
    reset_datadoc/1 % +Datadoc:iri
  ]
).

/** <module> LOD Washing Machine: Reset

Reset data documents in the triple store.

@author Wouter Beek
@version 2015/01
*/

:- use_module(library(apply)).
:- use_module(library(lists), except([delete/3,subset/2])).
:- use_module(library(filesex)).

:- use_module(plSparql(update/sparql_update_api)).

:- use_module(lwm(lwm_settings)).
:- use_module(lwm(md5)).
:- use_module(lwm(query/lwm_sparql_query)).

:- rdf_meta(reset_datadoc(r)).





%! reset_datadoc(+Datadoc:iri) is det.

reset_datadoc(Datadoc):-
  datadoc_p_os(Datadoc, llo:warning, Warnings),
  forall(
    member(Warning, Warnings),
    (
      datadoc_p_os(Warning, error:streamPosition, StreamPositions),
      maplist(delete_resource, StreamPositions)
    )
  ),
  maplist(delete_resource, Warnings),
  delete_resource(Datadoc),
  
  datadoc_directory(Datadoc, DatadocDir),
  delete_directory_and_contents(DatadocDir),
  print_message(informational, lwm_reset(Datadoc)).





% HELPERS %

datadoc_directory(Datadoc, Dir):-
  rdf_global_id(ll:Md5, Datadoc),
  md5_directory(Md5, Dir).



%! delete_resource(+Resource:rdf_term) is det.

delete_resource(Resource):-
  sparql_delete_where(
    cliopatria,
    [],
    [rdf(Resource,var(p),var(o))],
    [],
    [],
    []
  ).





% MESSAGES %

:- multifile(prolog:message//1).

prolog:message(lwm_reset(Datadoc)) -->
  ['Successfully reset ',Datadoc,'.'].

