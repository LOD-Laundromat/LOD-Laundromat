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
@version 2014/09, 2015/01-2015/02
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(lists), except([delete/3,subset/2])).
:- use_module(library(semweb/rdf_db), except([rdf_node/1])).

:- use_module(lwm(lwm_reset)).
:- use_module(lwm(query/lwm_sparql_enum)).
:- use_module(lwm(query/lwm_sparql_generics)).





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
  length(Datadocs, N),
  reset_datadocs(0, N, Datadocs).

reset_datadocs(_, _, []).
reset_datadocs(M, N, [H|T]).
  reset_datadoc(H),
  format(user_output, '[RESET] ~D/~D', [M,N]),
  NextM is M + 1,
  reset_datadocs(NextM, N, T).

debug_datadoc(Datadoc):-
  debug:debug_md5(Md5, _),
  rdf_global_id(ll:Md5, Datadoc).

erroneous_datadoc(Datadoc):-
  erroneous_datadoc0(Datadocs0),
  flatten(Datadocs0, Datadocs),
  member(Datadoc, Datadocs).

% Unpacked documents that are not clean yet.
erroneous_datadoc0(L):-
  lwm_sparql_select(
    [llo],
    [datadoc],
    [
      rdf(var(datadoc), llo:md5, var(md5)),
      not([rdf(var(datadoc), llo:endClean, var(endClean))])
    ],
    L,
    []
  ).
% Crawled more than once.
erroneous_datadoc0(L):-
  lwm_sparql_select(
    [llo],
    [datadoc],
    [
      rdf(var(datadoc), llo:startUnpack, var(startUnpack1)),
      rdf(var(datadoc), llo:startUnpack, var(startUnpack2)),
      filter(str(var(startUnpack1)) < str(var(startUnpack2)))
    ],
    L,
    []
  ).
% Archives with a datadump location.
erroneous_datadoc0(L):-
  lwm_sparql_select(
    [llo,rdf],
    [datadoc],
    [
      rdf(var(datadoc), rdf:type, llo:'Archive'),
      rdf(var(datadoc), void:dataDump, var(dataDump))
    ],
    L,
    []
  ).


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

