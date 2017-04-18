:- module(
  lwm_restart,
  [
    lwm_restart/0
  ]
).

/** <module> LOD Washing Machine: reset

Restart the LOD Washing Machine during debugging.

@author Wouter Beek
@version 2014/08-2014/09, 2015/01-2015/02, 2015/06
*/

:- use_module(library(debug)).
:- use_module(library(thread)).





%! lwm_restart is det.

lwm_restart:-
  lwm_settings:setting(endpoint, both), !,
  concurrent(
    2,
    [
      restart_store(cliopatria, Endpoint),
      restart_store(virtuoso, Endpoint)
    ],
    []
  ).
lwm_restart:-
  lwm_settings:setting(endpoint, Endpoint),
  restart_store(Endpoint).

restart_store(cliopatria):- !,
  sparql_drop_graph(cliopatria, user, []).
restart_store(virtuoso):-
  lwm_version_graph(Graph),

  % Virtuoso implements SPARQL Updates so irregularly,
  % that we cannot even use options for it:
  % (1) No support for direct POST bodies (only URL encoded).
  % (2) GET method for DROP GRAPH.
  % (3) Required SILENT keyword.
  sparql_endpoint_location(virtuoso_update, update, Uri0),
  format(atom(Query), 'DROP SILENT GRAPH <~a>', [Graph]),
  iri_add_query_comp(Uri0, query=Query, Uri),
  http_goal(Uri, true, [fail_on_status([404]),status(Status)]), !,
  (   between(200, 299, Status)
  ->  true
  ;   debug(
        lwm,
        '[RESTART FAILED] Status code ~d received.',
        [Status]
      )
  ).

