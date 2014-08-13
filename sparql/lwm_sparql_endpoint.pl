:- module(
  lwm_sparql_endpoint,
  [
    lwm_sparql_endpoint/1, % ?Endpoint:atom
    lwm_sparql_endpoint/2, % ?Endpoint:atom
                          % -Options:list(nvpair)
    lwm_sparql_endpoint_authentication/1 % -Authentication:list(nvpair)
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

:- use_module(lwm(lwm_settings)).

:- initialization(init_lwm_sparql_endpoints).



%! lwm_sparql_endpoint(+Endpoint:atom) is semidet.
%! lwm_sparql_endpoint(-Endpoint:atom) is det.

lwm_sparql_endpoint(Endpoint):-
  lwm_sparql_endpoint(Endpoint, _).

%! lwm_sparql_endpoint(-Endpoint:atom, -Options:list(nvpair)) is multi.

lwm_sparql_endpoint(Endpoint, Options2):-
  lwm_endpoint0(Endpoint, Options1),
  lwm_version_object(LwmGraph),
  merge_options([named_graphs([LwmGraph])], Options1, Options2).

lwm_endpoint0(localhost, Options):-
  lwm_sparql_endpoint_authentication(AuthenticationOptions),
  merge_options(AuthenticationOptions, [update_method(direct)], Options).
%lwm_endpoint0(cliopatria, Options):-
%  lwm_sparql_endpoint_authentication(AuthenticationOptions),
%  merge_options(AuthenticationOptions, [update_method(direct)], Options).
%lwm_endpoint0(virtuoso, [update_method(url_encoded)]).


%! lwm_sparql_endpoint_authentication(-Authentication:list(nvpair)) is det.

lwm_sparql_endpoint_authentication(
  [request_header('Authorization'=Authentication)]
):-
  lwm_sparql_user(User),
  lwm_sparql_password(Password),
  atomic_list_concat([User,Password], ':', Plain),
  base64(Plain, Encoded),
  atomic_list_concat(['Basic',Encoded], ' ', Authentication).


lwm_sparql_password(lwmlwm).


lwm_sparql_user(lwm).



% Initialization.

init_lwm_sparql_endpoints:-
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

