:- module(
  lwm_sparql_generics,
  [
    build_unpacked_query/3, % ?Min:nonneg
                            % ?Max:nonneg
                            % -Query:atom
    lwm_sparql_ask/3, % +Prefixes:list(atom)
                      % +Bgps:list(compound)
                      % +Options:list(nvpair)
    lwm_sparql_select/5, % +Prefixes:list(atom)
                         % +Variables:list(atom)
                         % +Bgps:list(compound)
                         % -Result:list(list)
                         % +Options:list(nvpair)
    lwm_sparql_select_iteratively/5 % +Prefixes:list(atom)
                                    % +Variables:list(atom)
                                    % +Bgps:list(compound)
                                    % -Result:list(list)
                                    % +Options:list(nvpair)
  ]
).

/** <module> llWashingMachine: SPARQL generics

Generic support predicates for SPARQL queries conducted by
the LOD Laundromat's washing machine.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09, 2014/11, 2015/01
*/

:- use_module(library(option)).

:- use_module(generics(list_ext)).
:- use_module(generics(meta_ext)).

:- use_module(plSparql(query/sparql_query_api)).

:- use_module(lwm(lwm_settings)).

:- predicate_options(lwm_sparql_ask/3, 3, [
     pass_to(sparql_ask/4, 4)
   ]).
:- predicate_options(lwm_sparql_select/5, 5, [
     pass_to(sparql_select/6, 6)
   ]).
:- predicate_options(lwm_sparql_select_iteratively/5, 5, [
     pass_to(sparql_select_iteratively/6, 6)
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
  lwm_version_graph(Graph),
  merge_options([named_graph(Graph),sparql_errors(fail)], Options1, Options2),
  (   lwm:lwm_server(virtuoso)
  ->  Endpoint = virtuoso_query
  ;   lwm:lwm_server(cliopatria)
  ->  Endpoint = cliopatria_localhost
  ),
  loop_until_true(
    sparql_ask(Endpoint, Prefixes, Bgps, Options2)
  ).



%! lwm_sparql_select(
%!   +Prefixes:list(atom),
%!   +Variables:list(atom),
%!   +Bgps:list(compound),
%!   -Result:list(list),
%!   +Options:list(nvpair)
%! ) is det.

lwm_sparql_select(Prefixes, Variables, Bgps, Result, Options1):-
  current_endpoint(Endpoint),
  sparql_select_options(Options1, Options2),
  loop_until_true(
    sparql_select(Endpoint, Prefixes, Variables, Bgps, Result, Options2)
  ).



%! lwm_sparql_select_iteratively(
%!   +Prefixes:list(atom),
%!   +Variables:list(atom),
%!   +Bgps:list(compound),
%!   -Result:list(list),
%!   +Options:list(nvpair)
%! ) is nondet.

lwm_sparql_select_iteratively(Prefixes, Variables, Bgps, Result, Options1):-
  current_endpoint(Endpoint),
  sparql_select_options(Options1, Options2),
  loop_until_true(
    sparql_select_iteratively(
      Endpoint,
      Prefixes,
      Variables,
      Bgps,
      Result,
      Options2
    )
  ).





% HELPERS %

%! current_endpoint(-Endpoint:atom) is det.

current_endpoint(Endpoint):-
  lwm:lwm_server(virtuoso), !,
  Endpoint = virtuoso_query.
current_endpoint(Endpoint):-
  lwm:lwm_server(cliopatria), !,
  Endpoint = cliopatria_localhost.



%! sparql_select_options(
%!   +Options1:list(nvpair),
%!   -Options2:list(nvpair)
%! ) is det.

sparql_select_options(Options1, Options2):-
  % Set the RDF Dataset over which SPARQL Queries are executed.
  lod_basket_graph(BasketGraph),
  lwm_version_graph(LwmGraph),
  merge_options(
    [default_graph(BasketGraph),default_graph(LwmGraph),sparql_errors(fail)],
    Options1,
    Options2
  ).
