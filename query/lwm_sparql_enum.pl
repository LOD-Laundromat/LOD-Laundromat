:- module(
  lwm_sparql_enum,
  [
    datadoc_enum_cleaning/1, % -Datadoc:iri
    datadoc_enum_pending/2, % -Datadoc:iri
                            % -DirtyUrl:uri
    datadoc_enum_unpacked/4, % ?Min:nonneg
                             % ?Max:nonneg
                             % -Datadoc:iri
                             % -UnpackedSize:nonneg
    datadoc_enum_unpacking/1 % -Datadoc:iri
  ]
).

/** <module> llWashingMachine: SPARQL enumerate

SPARQL queries that enumerate various kinds of data documents
for the LOD Washing Machine.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09, 2014/11, 2015/01-2015/02
*/

:- use_module(library(lists), except([delete/3,subset/2])).

:- use_module(plRdf(term/rdf_literal)).

:- use_module(lwm(query/lwm_sparql_generics)).





%! datadoc_enum_cleaning(-Datadoc:iri) is nondet.

datadoc_enum_cleaning(Datadoc):-
  lwm_sparql_select_iteratively(
    [llo],
    [datadoc],
    [
      rdf(var(datadoc), llo:startClean, var(startClean)),
      not([
        rdf(var(datadoc), llo:endClean, var(endClean))
      ])
    ],
    1,
    Rows,
    []
  ),
  member([Datadoc], Rows).



%! datadoc_enum_pending(-Datadoc:iri, -DirtyUrl:uri) is nondet.
% @tbd Make sure that at no time two data documents are
%      being downloaded from the same host.
%      This avoids being blocked by servers that do not allow
%      multiple simultaneous requests.
%      ~~~{.pl}
%      (   nonvar(DirtyUrl)
%      ->  uri_component(DirtyUrl, host, Host),
%          \+ lwm:current_host(Host),
%          % Set a lock on this host for other unpacking threads.
%          assertz(lwm:current_host(Host))
%      ;   true
%      ), !,
%      ~~~
%      Add argument `Host` for releasing the lock in [lwm_unpack].

datadoc_enum_pending(Datadoc, DirtyUrl):-
  lwm_sparql_select_iteratively(
    [llo],
    [datadoc,dirty],
    [
      rdf(var(datadoc), llo:added, var(added)),
      not([
        rdf(var(datadoc), llo:startUnpack, var(startUnpack))
      ]),
      optional([
        rdf(var(datadoc), llo:url, var(dirty))
      ])
    ],
    1,
    [[Datadoc,DirtyUrl]],
    []
  ).



%! datadoc_enum_unpacked(
%!   ?Min:nonneg,
%!   ?Max:nonneg,
%!   -Datadoc:iri,
%!   -UnpackedSize:nonneg
%! ) is semidet.
% UnpackedSize is expressed as the number of bytes.

datadoc_enum_unpacked(Min, Max, Datadoc, UnpackedSize):-
  build_unpacked_query(Min, Max, Query),
  lwm_sparql_select_iteratively(
    [llo],
    [datadoc,unpackedSize],
    Query,
    1,
    [[Datadoc,UnpackedSizeLiteral]],
    []
  ),
  rdf_literal_data(value, UnpackedSizeLiteral, UnpackedSize).



%! datadoc_enum_unpacking(-Datadoc:iri) is nondet.

datadoc_enum_unpacking(Datadoc):-
  lwm_sparql_select_iteratively(
    [llo],
    [datadoc],
    [
      rdf(var(datadoc), llo:startUnpack, var(startUnpack)),
      not([
        rdf(var(datadoc), llo:endUnpack, var(endUnpack))
      ])
    ],
    1,
    [[Datadoc]],
    []
  ).

