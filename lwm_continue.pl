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
:- use_module(lwm(md5)).
:- use_module(lwm(query/lwm_sparql_enum)).
:- use_module(lwm(query/lwm_sparql_query)).

:- rdf_meta(reset_datadoc(r)).





%! lwm_continue is det.

lwm_continue:-
  % Collect zombie data documents.
  aggregate_all(
    set(Datadoc),
    (   datadoc_enum_unpacking(Datadoc)
    ;   datadoc_enum_cleaning(Datadoc)
    ),
    Datadocs
  ),

  maplist(reset_datadoc, Datadocs),
  reset_datadoc(ll:'425a29576b0fb45899fd0999ca650ca5').


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

