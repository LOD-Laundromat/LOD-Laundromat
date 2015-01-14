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

:- use_module(plSparql(update/sparql_update_api)).

:- use_module(lwm(lwm_settings)).
:- use_module(lwm(query/lwm_sparql_query)).

:- rdf_meta(reset_datadoc(r)).





%! reset_datadoc(+Datadoc:iri) is det.
% @tbd The ClioPatria implementation would be even easier
%      (using the Semweb API).

reset_datadoc(Datadoc):-
  lwm:lwm_server(virtuoso),
  % Remove the metadata triples that were stored for the given data document.
  lwm_version_graph(NG),
  datadoc_p_os(Datadoc, llo:warning, Warnings),
  forall(
    member(Warning, Warnings),
    (
      datadoc_p_os(Warning, error:streamPosition, StreamPositions),
      maplist(delete_resource(NG), StreamPositions)
    )
  ),
  maplist(delete_resource(NG), Warnings),
  delete_resource(NG, Datadoc),
  print_message(informational, lwm_reset(Datadoc,NG)).





% HELPERS %

%! delete_resource(+Graph:atom, +Resource:rdf_term) is det.

delete_resource(Graph, Resource):-
  sparql_delete_where(
    virtuoso_update,
    [ll],
    [rdf(Resource,var(p),var(o))],
    [Graph],
    [],
    []
  ).





% MESSAGES %

:- multifile(prolog:message//1).

prolog:message(lwm_reset(Datadoc,NG)) -->
  ['Successfully reset ',Datadoc,' in graph ',NG,'.'].

