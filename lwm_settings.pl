:- module(
  lwm_settings,
  [
    lod_basket_graph/1, % ?Graph:atom
    ll_authority/1, % ?Authority:atom
    ll_scheme/1, % ?Scheme:atom
    lwm_version_directory/1, % -Directory:atom
    lwm_version_graph/1, % -Graph:iri
    lwm_version_number/1 % ?Version:positive_integer
  ]
).

/** <module> LOD Washing Machine: generics

Generic predicates for the LOD Washing Machine.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09
*/

:- use_module(library(filesex)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(uri)).

:- use_module(generics(service_db)).
:- use_module(void(void_db)). % Namespace.

:- use_module(plSparql(sparql_db)).

%! lwm_sparql_endpoint(+Endpoint:atom) is semidet.
%! lwm_sparql_endpoint(-Endpoint:atom) is multi.

:- dynamic(lwm_sparql_endpoint/1).

:- rdf_register_prefix(ll, 'http://lodlaundromat.org/resource/').
:- rdf_register_prefix(llo, 'http://lodlaundromat.org/ontology/').

:- dynamic(sparql_endpoint_option0/3).
:- multifile(sparql_endpoint_option0/3).

lwm:lwm_server(cliopatria).
%%%%lwm:lwm_server(virtuoso).

:- initialization(init_lwm_sparql_endpoints).



%! lod_basket_graph(+Graph:atom) is semidet.
%! lod_basket_graph(-Graph:atom) is det.

lod_basket_graph(Graph):-
  ll_scheme(Scheme),
  ll_authority(Authority),
  lod_basket_path(Path),
  lod_basket_fragment(Fragment),
  uri_components(Graph, uri_components(Scheme,Authority,Path,_,Fragment)).


lod_basket_path('').


lod_basket_fragment(seedlist).


%! ll_authority(+Authortity:atom) is semidet.
%! ll_authority(-Authortity:atom) is det.

ll_authority('lodlaundromat.org').


%! ll_scheme(+Scheme:atom) is semidet.
%! ll_scheme(-Scheme:oneof([http])) is det.

ll_scheme(http).


lwm_fragment(Fragment):-
  lwm_version_number(Version),
  atom_number(Fragment, Version).


lwm_path('').


%! lwm_version_directory(-Directory:atom) is det.
% Returns the absolute directory for the current LOD Washing Machine version.

lwm_version_directory(Dir):-
  % Place data documents in the data subdirectory.
  absolute_file_name(data(.), DataDir, [access(write),file_type(directory)]),

  % Add the LOD Washing Machine version to the directory path.
  lwm_version_number(Version1),
  atom_number(Version2, Version1),
  directory_file_path(DataDir, Version2, Dir),
  make_directory_path(Dir).


%! lwm_version_graph(-Graph:iri) is det.

lwm_version_graph(Graph):-
  ll_scheme(Scheme),
  ll_authority(Authority),
  lwm_path(Path),
  lwm_fragment(Fragment),
  uri_components(
    Graph,
    uri_components(Scheme,Authority,Path,_,Fragment)
  ).


%! lwm_version_number(+Version:positive_integer) is semidet.
%! lwm_version_number(-Version:positive_integer) is det.

lwm_version_number(11).



% Initialization.

init_lwm_sparql_endpoints:-
  % Update (debug tools)
  assert(lwm_sparql_endpoint(cliopatria_localhost)),
  sparql_register_endpoint(
    cliopatria_localhost,
    ['http://localhost:3030'],
    cliopatria
  ),
  register_service(cliopatria_localhost, lwm, lwmlwm),

  % Update (reset, continue)
  assert(lwm_sparql_endpoint(virtuoso_update)),
  sparql_register_endpoint(
    virtuoso_update,
    ['http://localhost:8890/sparql-auth'],
    virtuoso
  ),
  sparql_db:assert(
    sparql_endpoint_option0(virtuoso_update, path_suffix(update), '')
  ),

  % Query.
  assert(lwm_sparql_endpoint(virtuoso_query)),
  sparql_register_endpoint(
    virtuoso_query,
    ['http://sparql.backend.lodlaundromat.org'],
    virtuoso
  ),
  sparql_db:assert(
    sparql_endpoint_option0(virtuoso_query, path_suffix(query), '')
  ),

  % HTTP.
  assert(lwm_sparql_endpoint(virtuoso_http)),
  sparql_register_endpoint(
    virtuoso_http,
    ['http://localhost/sparql/graph'],
    virtuoso
  ),
  sparql_db:assert(
    sparql_endpoint_option0(virtuoso_http, path_suffix(http), '')
  ).

