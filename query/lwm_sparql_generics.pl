:- module(
  lwm_sparql_generics,
  [
    build_unpacked_query/3, % ?Min:nonneg
                            % ?Max:nonneg
                            % -Query:atom
    lwm_sparql_ask/3, % +Prefixes:list(atom)
                      % +Bgps:list(compound)
                      % +Options:list(nvpair)
    lwm_sparql_select/3, % +Query:atom
                         % -Result:list(list)
                         % +Options:list(nvpair)
    lwm_sparql_select/5, % +Prefixes:list(atom)
                         % +Variables:list(atom)
                         % +Bgps:list(compound)
                         % -Result:list(list)
                         % +Options:list(nvpair)
    lwm_sparql_select_iteratively/6 % +Prefixes:list(atom)
                                    % +Variables:list(atom)
                                    % +Bgps:list(compound)
                                    % +MaximumNumberOfResults:positive_integer
                                    % -Results:list(list)
                                    % +Options:list(nvpair)
  ]
).

/** <module> LOD Washing Machine: SPARQL generics

Generic support predicates for SPARQL queries conducted by
the LOD Laundromat's washing machine.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09, 2014/11, 2015/01-2015/02
*/

:- use_module(library(option)).

:- use_module(generics(list_ext)).
:- use_module(generics(meta_ext)).

:- use_module(plSparql(query/sparql_query_api)).

:- use_module(lwm(lwm_settings)).

:- predicate_options(lwm_sparql_ask/3, 3, [
     pass_to(sparql_ask/4, 4)
   ]).
:- predicate_options(lwm_sparql_select/3, 3, [
     pass_to(sparql_select/4, 4)
   ]).
:- predicate_options(lwm_sparql_select/5, 5, [
     pass_to(sparql_select/6, 6)
   ]).
:- predicate_options(lwm_sparql_select_iteratively/6, 6, [
     pass_to(sparql_select_iteratively/7, 7)
   ]).





%! build_unpacked_query(?Min:nonneg, ?Max:nonneg, -Query:atom) is det.
% Returns a query in which the given Min and Max integers denote
% the range restriction on the unpacked file size
% in terms of a SPARQL filter.

build_unpacked_query(Min, Max, Query2):-
  Query1 = [
    rdf(var(datadoc), llo:endUnpack, var(endUnpack)),
    not([
      rdf(var(datadoc), llo:startClean, var(startClean))
    ]),
    rdf(var(datadoc), llo:unpackedSize, var(unpackedSize))
  ],

  % Insert the range restriction on the unpacked file size as a filter.
  (   nonvar(Min)
  ->  MinFilter = >(var(unpackedSize),Min)
  ;   true
  ),
  (   nonvar(Max)
  ->  MaxFilter = <(var(unpackedSize),Max)
  ;   true
  ),
  exclude(var, [MinFilter,MaxFilter], FilterComponents),
  (   list_binary_term(FilterComponents, and, FilterContent)
  ->  append(Query1, [filter(FilterContent)], Query2)
  ;   Query2 = Query1
  ).



%! lwm_sparql_ask(
%!   +Prefixes:list(atom),
%!   +Bgps:list(compound),
%!   +Options:list(nvpair)
%! ) is semidet.

lwm_sparql_ask(Prefixes, Bgps, Options1):-
  endpoint_options(Options1, Endpoint, Options2),
  loop_until_true(
    sparql_ask(Endpoint, Prefixes, Bgps, Options2)
  ).



%! lwm_sparql_select(
%!   +Query:atom,
%!   -Result:list(list),
%!   +Options:list(nvpair)
%! ) is det.

lwm_sparql_select(Query, Result, Options1):-
  endpoint_options(Options1, Endpoint, Options2),
  loop_until_true(
    sparql_select(Endpoint, Query, Result, Options2)
  ).



%! lwm_sparql_select(
%!   +Prefixes:list(atom),
%!   +Variables:list(atom),
%!   +Bgps:list(compound),
%!   -Result:list(list),
%!   +Options:list(nvpair)
%! ) is det.

lwm_sparql_select(Prefixes, Variables, Bgps, Result, Options1):-
  endpoint_options(Options1, Endpoint, Options2),
  loop_until_true(
    sparql_select(Endpoint, Prefixes, Variables, Bgps, Result, Options2)
  ).



%! lwm_sparql_select_iteratively(
%!   +Prefixes:list(atom),
%!   +Variables:list(atom),
%!   +Bgps:list(compound),
%!   +MaximumNumberOfResults:positive_integer,
%!   -NResults:list(list),
%!   +Options:list(nvpair)
%! ) is nondet.

lwm_sparql_select_iteratively(
  Prefixes,
  Variables,
  Bgps,
  N,
  NResults,
  Options1
):-
  endpoint_options(Options1, Endpoint, Options2),
  loop_until_true(
    sparql_select_iteratively(
      Endpoint,
      Prefixes,
      Variables,
      Bgps,
      N,
      NResults,
      Options2
    )
  ).





% HELPERS %

%! endpoint_options(
%!   +Options1:list(nvpair),
%!   -Endpoint:oneof([cliopatria,virtuoso_query]),
%!   -Options2:list(nvpair)
%! ) is det.

endpoint_options(Options, cliopatria, Options):-
  lwm_settings:setting(endpoint, cliopatria), !.
% Virtuoso queries are requested within a named graph.
endpoint_options(Options1, virtuoso_query, Options2):-
  lwm_settings:setting(endpoint, virtuoso),
  lod_basket_graph(G1),
  lwm_version_graph(G2),
  merge_options([default_graph(G1),default_graph(G2)], Options1, Options2).

