:- module(
  ll_sparql_api,
  [
    ll_sparql_ask/3, % +Prefixes:list(atom)
                     % +Bgps:or([compound,list(compound)])
                     % +Options:list(nvpair)
    ll_sparql_ask/4, % +Endpoint:atom
                     % +Prefixes:list(atom)
                     % +Bgps:or([compound,list(compound)])
                     % +Options:list(nvpair)
    ll_sparql_drop/1, % +Options:list(nvpair)
    ll_sparql_drop/2, % +Endpoint:atom
                      % +Options:list(nvpair)
    ll_sparql_select/5, % +Prefixes:list(atom)
                        % +Variables:list(atom)
                        % +Bgps:or([compound,list(compound)])
                        % -Result:list(list)
                        % +Options:list(nvpair)
    ll_sparql_select/6 % +Endpoint:atom
                       % +Prefixes:list(atom)
                       % +Variables:list(atom)
                       % +Bgps:or([compound,list(compound)])
                       % -Result:list(list)
                       % +Options:list(nvpair)
  ]
).

/** <module> LOD Washing Machine (LWM): SPARQL

SPARQL constructors for the LOD Washing Machine.

@author Wouter Beek
@version 2014/06, 2014/08
*/

:- use_module(library(option)).

:- use_module(generics(meta_ext)).

:- use_module(plSparql(sparql_api)).

:- use_module(ll_sparql(ll_sparql_endpoint)). % Register SPARQL endpoints.



ll_sparql_ask(Prefixes, Bgps1, Options):-
  once(ll_sparql_endpoint(Endpoint)),
  ll_sparql_default_graph(LwmGraph),
  Bgps2 = [graph(LwmGraph,Bgps1)],
  ll_sparql_ask(Endpoint, Prefixes, Bgps2, Options).

ll_sparql_ask(Endpoint, Prefixes, Bgps, Options1):-
  ll_sparql_endpoint(Endpoint, Options2),
  merge_options(Options1, Options2, Options3),
  loop_until_true(
    sparql_ask(Endpoint, Prefixes, Bgps, Options3)
  ).


ll_sparql_drop(Options):-
  once(ll_sparql_endpoint(Endpoint)),
  ll_sparql_drop(Endpoint, Options).

ll_sparql_drop(Endpoint, Options1):-
  ll_sparql_endpoint(Endpoint, Options2),
  merge_options(Options1, Options2, Options3),
  option(default_graph(DefaultGraph), Options3),
  loop_until_true(
    sparql_drop_graph(Endpoint, DefaultGraph, Options3)
  ).


ll_sparql_select(Prefixes, Variables, Bgps1, Result, Options):-
  once(ll_sparql_endpoint(Endpoint)),
  ll_sparql_default_graph(LwmGraph),
  Bgps2 = [graph(LwmGraph,Bgps1)],
  ll_sparql_select(Endpoint, Prefixes, Variables, Bgps2, Result, Options).

ll_sparql_select(Endpoint, Prefixes, Variables, Bgps, Result, Options1):-
  ll_sparql_endpoint(Endpoint, Options2),
  merge_options(Options1, Options2, Options3),
  loop_until_true(
    sparql_select(Endpoint, Prefixes, Variables, Bgps, Result, Options3)
  ).

