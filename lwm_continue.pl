:- module(
  lwm_continue,
  [
    lwm_continue/0
  ]
).

/** <module> LOD Washing Machine: Continue

Continues an interrupted LOD Washing Machine crawl.

@author Wouter Beek
@version 2014/09, 2015/01
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(filesex)).
:- use_module(library(semweb/rdf_db), except([rdf_node/1])).

:- use_module(plSparql(update/sparql_update_api)).

:- use_module(lwm(lwm_settings)).
:- use_module(lwm(lwm_sparql_query)).
:- use_module(lwm(md5)).



%! lwm_continue is det.

lwm_continue:-
  % Collect zombie data documents.
  aggregate_all(
    set(Datadoc),
    (
      datadoc_unpacking(Datadoc)
    ;
      datadoc_cleaning(Datadoc)
    ),
    Datadocs
  ),

  maplist(reset_datadoc, Datadocs).


%! reset_datadoc(+Datadoc:url) is det.

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
      )
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

