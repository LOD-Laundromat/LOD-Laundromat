:- module(
  lwm_sparql_nondet,
  [
    cleaning/1, % -Document:iri
    pending/2, % -Document:iri
               % -Download:iri
    unpacked/4, % ?Min:nonneg
                % ?Max:nonneg
                % -Document:iri
                % -UnpackedSize:nonneg
    unpacking/1 % -Document:iri
  ]
).

/** <module> LOD Laundromat: Non-deterministic SPARQL queries

SPARQL queries that enumerate various kinds of data documents
for the LOD Washing Machine.

@author Wouter Beek
@version 2015/11, 2016/01
*/

:- use_module(library(lists)).
:- use_module(library(rdf/rdf_term)).

:- use_module('LOD-Laundromat'(query/lwm_sparql_generics)).





%! cleaning(-Document:iri) is nondet.
% Enumerates documents that are currently being cleaned.

cleaning(Document):-
  lwm_sparql_select_iteratively(
    [llo],
    [document],
    [
      rdf(var(document), llo:startClean, var(startClean)),
      not([rdf(var(document), llo:endClean, var(endClean))])
    ],
    1,
    Rows,
    []
  ),
  member([Document], Rows).



%! pending(-Document:iri, -Download:iri) is nondet.
% Enumerates documents that are currently pending.
% This is how a worker fetches a new job from the pool.
%
% @tbd Make sure that at no time two data documents are
%      being downloaded from the same host.
%      This avoids being blocked by servers that do not allow
%      multiple simultaneous requests.
%      ~~~{.pl}
%      (   nonvar(Download)
%      ->  uri_component(Download, host, Host),
%          \+ lwm:current_host(Host),
%          % Set a lock on this host for other unpacking threads.
%          assertz(lwm:current_host(Host))
%      ;   true
%      ), !,
%      ~~~
%      Add argument `Host` for releasing the lock in [lwm_unpack].

pending(Document, Download):-
  lwm_sparql_select_iteratively(
    [llo],
    [document,download],
    [
      rdf(var(document), llo:added, var(added)),
      not([rdf(var(document), llo:startUnpack, var(startUnpack))]),
      optional([rdf(var(document), llo:url, var(download))])
    ],
    1,
    [[Document,Download]],
    [order(descending-added)]
  ).



%! unpacked(
%!   ?Min:nonneg,
%!   ?Max:nonneg,
%!   -Document:iri,
%!   -UnpackedSize:nonneg
%! ) is semidet.
% UnpackedSize is expressed as the number of bytes.

unpacked(Min, Max, Document, UnpackedSize):-
  build_unpacked_query(Min, Max, Query),
  lwm_sparql_select_iteratively(
    [llo],
    [document,unpackedSize],
    Query,
    1,
    [[Document,UnpackedSizeLiteral]],
    [order(descending-added)]
  ),
  rdf_literal_value(UnpackedSizeLiteral, UnpackedSize).



%! unpacking(-Document:iri) is nondet.
% Eniemrates documents that are currently unpacking.

unpacking(Document):-
  lwm_sparql_select_iteratively(
    [llo],
    [document],
    [
      rdf(var(document), llo:startUnpack, var(startUnpack)),
      not([rdf(var(document), llo:endUnpack, var(endUnpack))])
    ],
    1,
    [[Document]],
    []
  ).
