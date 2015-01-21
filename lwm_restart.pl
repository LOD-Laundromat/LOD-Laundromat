:- module(
  lwm_restart,
  [
    lwm_restart/0
  ]
).

/** <module> LOD Washing Machine: reset

Restart the LOD Washing Machine during debugging.

@author Wouter Beek
@version 2014/08-2014/09, 2015/01
*/

:- use_module(plSparql(update/sparql_update_api)).





%! lwm_restart is det.

lwm_restart:-
  sparql_drop_graph(cliopatria, user, []).

