:- module(
  lwm_sparql_endpoint,
  [
    lwm_sparql_endpoint/2, % ?Endpoint:atom
                           % ?Mode:oneof([http,sparql,update])
    lwm_sparql_endpoint/3, % ?Endpoint:atom
                           % ?Mode:oneof([http,sparql,update])
                           % -Options:list(nvpair)
    lwm_sparql_endpoint_authentication/1 % -Authentication:list(nvpair)
  ]
).

/** <module> LOD Laundromat: endpoint

Registration of SPARQL endpoints for the LOD Laundromat.

### Test Virtuose

~~~{.sh}
curl --data "INSERT DATA {\n<http://lodlaundromat.org/vocab#674f08039170b9f33b9451f96f89f057> <http://lodlaundromat.org/vocab#start_unpack> \"2014-08-20T15:41:13.4511661529541015625-02:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n}\n" -L "http://sparql.backend.lodlaundromat.org/update?named-graph-uri=http://lodlaundromat.org%2311&using-named-graph-uri=http://lodlaundromat.org%2311"
~~~

@author Wouter Beek
@version 2014/06, 2014/08
*/

:- use_module(library(base64)).
:- use_module(library(option)).

:- use_module(plSparql(sparql_db)).

:- use_module(lwm(lwm_settings)).

:- initialization(init_lwm_sparql_endpoints).



%! lwm_sparql_endpoint(
%!   +Endpoint:atom,
%!   +Mode:oneof([http,sparql,update])
%! ) is semidet.
%! lwm_sparql_endpoint(
%!   +Endpoint:atom,
%!   -Mode:oneof([http,sparql,update])
%! ) is nondet.
%! lwm_sparql_endpoint(
%!   -Endpoint:atom,
%!   +Mode:oneof([http,sparql,update])
%! ) is nondet.
%! lwm_sparql_endpoint(
%!   -Endpoint:atom,
%!   -Mode:oneof([http,sparql,update])
%! ) is nondet.

lwm_sparql_endpoint(Endpoint, Mode):-
  lwm_endpoint(Endpoint, Mode, _).

%! lwm_sparql_endpoint(
%!   +Endpoint:atom,
%!   +Mode:oneof([http,query,update]),
%!   -Options:list(nvpair)
%! ) is semidet.

lwm_sparql_endpoint(Endpoint, Mode, Options2):-
  % Make sure that the endpoint is register with the required mode.
  sparql_endpoint(Endpoint, Mode, _),
  
  % The LOD Washing Machine can specify endpoint-specific options.
  findall(
    Option,
    lwm_endpoint_option(Endpoint, Mode, Option),
    Options1
  ),
  
  % The named graph option is endpoint-independent.
  merge_options([named_graphs([LwmGraph])], Options1, Options2).


%! lwm_endpoint_option(+Endpoint:atom, -Option:nvpair) is nondet.
% Endpoint-specific options.

lwm_endpoint_option(cliopatria, _, Options):-
  lwm_endpoint_auth_option(Option).
lwm_endpoint_option(cliopatria, update, update_method(direct)).


%! lwm_endpoint_auth_option(-AuthenticationOption:nvpair) is det.

lwm_endpoint_auth_option(request_header('Authorization'=Authentication)):-
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
  init_virtuoso_endpoint.

% ClioPatria
init_cliopatria_endpoint:-
  sparql_register_endpoint(
    cliopatria,
    query,
    uri_components(http,'localhost:3020','/sparql/',_,_)
  ),
  sparql_register_endpoint(
    cliopatria,
    update,
    uri_components(http,'localhost:3020','/sparql/update',_,_)
  ).

% Virtuoso.
init_virtuoso_endpoint:-
  sparql_register_endpoint(
    virtuoso,
    http,
    uri_components(http,localhost,'/sparql/graph',_,_)
  ),
  sparql_register_endpoint(
    virtuoso,
    query,
    uri_components(http,'sparql.backend.lodlaundromat.org','/',_,_)
  ),
  sparql_register_endpoint(
    virtuoso,
    update,
    uri_components(http,'sparql.backend.lodlaundromat.org','/update',_,_)
  ).

