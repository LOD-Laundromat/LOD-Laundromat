:- module(
  lwm_sparql_api,
  [
    lwm_sparql_ask/3, % +Prefixes:list(atom)
                      % +Bgps:or([compound,list(compound)])
                      % +Options:list(nvpair)
    lwm_sparql_ask/4, % +Endpoint:atom
                      % +Prefixes:list(atom)
                      % +Bgps:or([compound,list(compound)])
                      % +Options:list(nvpair)
    lwm_sparql_drop/1, % +Options:list(nvpair)
    lwm_sparql_drop/2, % +Endpoint:atom
                       % +Options:list(nvpair)
    lwm_sparql_select/5, % +Prefixes:list(atom)
                         % +Variables:list(atom)
                         % +Bgps:or([compound,list(compound)])
                         % -Result:list(list)
                         % +Options:list(nvpair)
    lwm_sparql_select/6 % +Endpoint:atom
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
:- use_module(library(semweb/rdf_db)).

:- use_module(generics(meta_ext)).

:- use_module(plSparql(sparql_api)).

:- use_module(lwm(lwm_settings)).
:- use_module(lwm_sparql(lwm_sparql_endpoint)).

:- rdf_register_prefix(ll, 'http://lodlaundromat.org/vocab#').



lwm_sparql_ask(Prefixes, Bgps1, Options):-
  once(lwm_sparql_endpoint(Endpoint)),
  lwm_version_graph(LwmGraph),
  Bgps2 = [graph(LwmGraph,Bgps1)],
  lwm_sparql_ask(Endpoint, Prefixes, Bgps2, Options).

lwm_sparql_ask(Endpoint, Prefixes, Bgps, Options1):-
  lwm_sparql_endpoint(Endpoint, Options2),
  merge_options(Options1, Options2, Options3),
  loop_until_true(
    sparql_ask(Endpoint, Prefixes, Bgps, Options3)
  ).


lwm_sparql_drop(Options):-
  once(lwm_sparql_endpoint(Endpoint)),
  lwm_sparql_drop(Endpoint, Options).

lwm_sparql_drop(Endpoint, Options1):-
  lwm_sparql_endpoint(Endpoint, Options2),
  merge_options(Options1, Options2, Options3),
  option(default_graph(DefaultGraph), Options3),
  loop_until_true(
    sparql_drop_graph(Endpoint, DefaultGraph, Options3)
  ).


lwm_sparql_select(Prefixes, Variables, Bgps1, Result, Options):-
  once(lwm_sparql_endpoint(Endpoint)),
  lwm_version_graph(LwmGraph),
  Bgps2 = [graph(LwmGraph,Bgps1)],
  lwm_sparql_select(Endpoint, Prefixes, Variables, Bgps2, Result, Options).

lwm_sparql_select(Endpoint, Prefixes, Variables, Bgps, Result, Options1):-
  lwm_sparql_endpoint(Endpoint, Options2),
  merge_options(Options1, Options2, Options3),
  loop_until_true(
    sparql_select(Endpoint, Prefixes, Variables, Bgps, Result, Options3)
  ).

