:- module(
  lwm_reset,
  [
    lwm_reset/0
  ]
).

/** <module> LOD Washing Machine: reset

Reset the LOD Washing Machine during debugging.

@author Wouter Beek
@version 2014/08
*/

:- use_module(plSparql_update(sparql_update_api)).

:- use_module(lwm(lwm_settings)).



%! lwm_reset is det.

lwm_reset:-
  lwm_version_graph(Graph),
  sparql_drop_graph(virtuoso_update, Graph, []).

