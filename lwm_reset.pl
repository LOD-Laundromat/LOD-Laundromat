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

:- use_module(plSparql(update/sparql_update_api)).

:- use_module(lwm(lwm_settings)).

:- rdf_meta(reset_datadoc(r)).





%! reset_datadoc(+Datadoc:uri) is det.

reset_datadoc(Datadoc):-
  % Remove the metadata triples that were stored for the given data document.
  lwm_version_graph(NG),
  (   lwm:lwm_server(virtuoso)
  ->  sparql_delete_where(
        virtuoso_update,
        [ll],
        [rdf(Datadoc,var(p),var(o))],
        [NG],
        [],
        []
      ),
      print_message(informational, lwm_reset(Datadoc,NG))
  ;   lwm:lwm_server(cliopatria)
  ->  sparql_delete_where(
        cliopatria_localhost,
        [ll],
        [rdf(Datadoc,var(p),var(o))],
        [NG],
        [],
        []
      )
  ).





% MESSAGES %

:- multifile(prolog:message//1).

prolog:message(lwm_reset(Datadoc,NG)) -->
  ['Successfully reset ',Datadoc,' in graph ',NG,'.'].

