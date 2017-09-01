:- module(
  lwm_sparql_generics,
  [
    build_unpacked_query/3, % ?Min:nonneg
                            % ?Max:nonneg
                            % -Query:atom
    lwm_sparql_ask/3, % +Prefixes:list(atom)
                      % +Bgps:list(compound)
                      % +Options:list(compound)
    lwm_sparql_select/3, % +Query:atom
                         % -Result:list(list)
                         % +Options:list(compound)
    lwm_sparql_select/5, % +Prefixes:list(atom)
                         % +Variables:list(atom)
                         % +Bgps:list(compound)
                         % -Result:list(list)
                         % +Options:list(compound)
    lwm_sparql_select_iteratively/6 % +Prefixes:list(atom)
                                    % +Variables:list(atom)
                                    % +Bgps:list(compound)
                                    % +MaximumNumberOfResults:positive_integer
                                    % -Results:list(list)
                                    % +Options:list(compound)
  ]
).

/** <module> LOD Washing Machine: SPARQL generics

Generic support predicates for SPARQL queries conducted by
the LOD Laundromat's washing machine.

@author Wouter Beek
@version 2015/11, 2016/01
*/

:- use_module(library(dcg/dcg_ext)).
:- use_module(library(list_ext)).
:- use_module(library(option)).
:- use_module(library(sparql/query/sparql_query)).
:- use_module(library(sparql/query/sparql_query_build)).

:- use_module(lwm_settings).

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
    not([rdf(var(datadoc), llo:startClean, var(startClean))]),
    rdf(var(datadoc), llo:unpackedSize, var(unpackedSize)),
    rdf(var(datadoc), llo:added, var(added))
  ],

  % Insert the range restriction on the unpacked file size as a filter.
  (nonvar(Min) -> MinFilter = >(var(unpackedSize),Min) ; true),
  (nonvar(Max) -> MaxFilter = <(var(unpackedSize),Max) ; true),
  exclude(var, [MinFilter,MaxFilter], FilterComponents),
  (   list_binary_term(FilterComponents, and, FilterContent)
  ->  append(Query1, [filter(FilterContent)], Query2)
  ;   Query2 = Query1
  ).



%! lwm_sparql_ask(
%!   +Prefixes:list(atom),
%!   +Bgps:list(compound),
%!   +Options:list(compound)
%! ) is semidet.

lwm_sparql_ask(Prefixes, Bgps, Opts1):-
  lwm_endpoint_options(Opts1, Opts2),
  atom_phrase(Query, sparql_build_ask(Prefixes, Bgps, Opts2)),
  loop_until_true(sparql_ask(lwm_endpoint, Query, Opts2)).



%! lwm_sparql_select(
%!   +Query:atom,
%!   -Result:list(list),
%!   +Options:list(compound)
%! ) is det.

lwm_sparql_select(Query, Result, Opts1):-
  lwm_endpoint_options(Opts1, Opts2),
  loop_until_true(sparql_select(lwm_endpoint, Query, Result, Opts2)).



%! lwm_sparql_select(
%!   +Prefixes:list(atom),
%!   +Variables:list(atom),
%!   +Bgps:list(compound),
%!   -Result:list(list),
%!   +Options:list(compound)
%! ) is det.

lwm_sparql_select(Prefixes, Vars, Bgps, Result, Opts1):-
  lwm_endpoint_options(Opts1, Opts2),
  atom_phrase(Query, sparql_build_select(Prefixes, Vars, Bgps, Opts2)),
  loop_until_true(sparql_select(lwm_endpoint, Query, Result, Opts2)).



%! lwm_sparql_select_iteratively(
%!   +Prefixes:list(atom),
%!   +Variables:list(atom),
%!   +Bgps:list(compound),
%!   +MaximumNumberOfResults:positive_integer,
%!   -NResults:list(list),
%!   +Options:list(compound)
%! ) is nondet.

lwm_sparql_select_iteratively(Prefixes, Vars, Bgps, N, Results, Opts1):-
  lwm_endpoint_options(Opts1, Opts2),
  atom_phrase(Q, sparql_build_select(Prefixes, Vars, Bgps)),
  sparql_select(lwm_endpoint, Q, Results, Opts2).





% HELPERS %

%! lwm_sparql_options(
%!   +Options1:list(compound),
%!   -Options2:list(compound)
%! ) is det.
% Notice that Virtuoso queries are requested within a named graph.

lwm_endpoint_options(Opts1, Opts2):-
  lod_basket_graph(G1),
  lwm_version_graph(G2),
  merge_options([default_graph(G1),default_graph(G2)], Opts1, Opts2).
