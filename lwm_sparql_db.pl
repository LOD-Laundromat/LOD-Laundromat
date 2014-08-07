:- module(
  lwm_sparql_db,
  [
    lwm_default_graph/1 % -DefaultGraph:iri
  ]
).

/** <module> LOD Washing Machine (LWM): database

Registration of SPARQL endpoints for the LOD Washing Machine.

@author Wouter Beek
@version 2014/08
*/

:- use_module(plSparql(sparql_db)).

:- initialization(init_lwm_endpoints).



%! lwm_default_graph(-DefaultGraph:iri) is det.

lwm_default_graph(DefaultGraph):-
  lwm_version(Version),
  atom_number(Fragment, Version),
  uri_components(
    DefaultGraph,
    uri_components(http, 'lodlaundromat.org', _, _, Fragment)
  ).



% Initialization.

init_lwm_endpoints:-
  init_cliopatria_endpoint,
  init_localhost_endpoint,
  init_virtuoso_endpoint.

% Localhost.
init_localhost_endpoint:-
  sparql_register_endpoint(
    localhost,
    query,
    uri_components(http,'localhost:3020','/sparql/',_,_)
  ),
  sparql_register_endpoint(
    localhost,
    update,
    uri_components(http,'localhost:3020','/sparql/update',_,_)
  ).

% Cliopatria.
init_cliopatria_endpoint:-
  sparql_register_endpoint(
    cliopatria,
    query,
    uri_components(http,'lodlaundry.wbeek.ops.few.vu.nl','/sparql/',_,_)
  ),
  sparql_register_endpoint(
    cliopatria,
    update,
    uri_components(http,'lodlaundry.wbeek.ops.few.vu.nl','/sparql/update',_,_)
  ).

% Virtuoso.
init_virtuoso_endpoint:-
  sparql_register_endpoint(
    virtuoso,
    query,
    uri_components(http,'virtuoso.lodlaundromat.ops.few.vu.nl','/sparql',_,_)
  ).
