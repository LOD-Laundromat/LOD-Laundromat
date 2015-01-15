:- module(
  lwm_continue,
  [
    lwm_continue/0,
    lwm_retry_unrecognized_format/0
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
:- use_module(library(lists), except([delete/3,subset/2])).
:- use_module(library(semweb/rdf_db), except([rdf_node/1])).

:- use_module(lwm(lwm_reset)).
:- use_module(lwm(lwm_settings)).
:- use_module(lwm(md5)).
:- use_module(lwm(query/lwm_sparql_enum)).
:- use_module(lwm(query/lwm_sparql_generics)).
:- use_module(lwm(query/lwm_sparql_query)).





%! lwm_continue is det.

lwm_continue:-
  % Collect zombie data documents.
  aggregate_all(
    set(Datadoc),
    (   datadoc_enum_unpacking(Datadoc)
    ;   datadoc_enum_cleaning(Datadoc)
    ;   debug_datadoc(Datadoc)
    ;   erroneous_datadoc(Datadoc)
    ),
    Datadocs
  ),

  maplist(reset_datadoc, Datadocs).

debug_datadoc(Datadoc):-
  debug:debug_md5(Md5, _),
  rdf_global_id(ll:Md5, Datadoc).

erroneous_datadoc(Datadoc):-
  lwm_sparql_select(
    [llo,rdf],
    [datadoc],
    [
      rdf(var(datadoc), rdf:type, llo:'ArchiveEntry'),
      not([rdf(var(datadoc))])
    ],
    Datadocs0,
    []
  ),
  flatten(Datadocs0, Datadocs),
  member(Datadoc, Datadocs).



%! lwm_retry_unrecognized_format is det.
% Retrie to download, unpack, and clean all datadocuments that were
% previously classified as having a unrecognized serialization format.
% Useful in case RDF guessing was changed/fixed while crawling.

lwm_retry_unrecognized_format:-
  lwm_sparql_select(
    [error,llo],
    [datadoc],
    [rdf(var(datadoc), llo:serializationFormat, error:unrecognizedFormat)],
    Rows,
    [distinct(true),order(ascending-[datadoc])]
  ),
  flatten(Rows, Datadocs),
  maplist(reset_datadoc, Datadocs).

