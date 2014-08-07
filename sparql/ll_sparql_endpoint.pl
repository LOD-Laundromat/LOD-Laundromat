:- module(
  ll_sparql_endpoint,
  [
    ll_sparql_default_graph/2, % +Mode:oneof([collection,dissemination])
                               % -DefaultGraph:iri
    ll_sparql_endpoint/1, % ?Endpoint:atom
    ll_sparql_endpoint/2, % ?Endpoint:atom
                          % -Options:list(nvpair)
    ll_sparql_endpoint_authentication/1 % -Authentication:list(nvpair)
  ]
).

/** <module> LOD Laundromat: endpoint

Registration of SPARQL endpoints for the LOD Laundromat.

@author Wouter Beek
@version 2014/06, 2014/08
*/

:- use_module(library(base64)).
:- use_module(library(option)).
:- use_module(library(uri)).

:- use_module(plSparql(sparql_db)).

:- use_module(ll(ll_generics)).

:- initialization(init_ll_sparql_endpoints).



%! ll_sparql_default_graph(
%!   +Mode:oneof([collection,dissemination]),
%!   -DefaultGraph:iri
%! ) is det.

ll_sparql_default_graph(Mode, DefaultGraph):-
  ll_version(Mode, Version),
  atom_number(Fragment, Version),
  uri_components(
    DefaultGraph,
    uri_components(http, 'lodlaundromat.org', _, _, Fragment)
  ).


%! ll_sparql_endpoint(+Endpoint:atom) is semidet.
%! ll_sparql_endpoint(-Endpoint:atom) is det.

ll_sparql_endpoint(Endpoint):-
  ll_sparql_endpoint(Endpoint, _).

%! ll_sparql_endpoint(-Endpoint:atom, -Options:list(nvpair)) is multi.

ll_sparql_endpoint(Endpoint, Options2):-
  lwm_endpoint0(Endpoint, Options1),
  ll_sparql_default_graph(collection, LwmGraph),
  merge_options([named_graphs([LwmGraph])], Options1, Options2).

lwm_endpoint0(localhost, Options):-
  ll_sparql_endpoint_authentication(AuthenticationOptions),
  merge_options(AuthenticationOptions, [update_method(direct)], Options).
%lwm_endpoint0(cliopatria, Options):-
%  ll_sparql_endpoint_authentication(AuthenticationOptions),
%  merge_options(AuthenticationOptions, [update_method(direct)], Options).
%lwm_endpoint0(virtuoso, [update_method(url_encoded)]).


%! ll_sparql_endpoint_authentication(-Authentication:list(nvpair)) is det.

ll_sparql_endpoint_authentication(
  [request_header('Authorization'=Authentication)]
):-
  ll_sparql_user(User),
  ll_sparql_password(Password),
  atomic_list_concat([User,Password], ':', Plain),
  base64(Plain, Encoded),
  atomic_list_concat(['Basic',Encoded], ' ', Authentication).


ll_sparql_password(lwmlwm).


ll_sparql_user(lwm).



% Initialization.

init_ll_sparql_endpoints:-
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

