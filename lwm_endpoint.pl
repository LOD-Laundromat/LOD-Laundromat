:- module(
  lwm_endpoint,
  [
    lwm_endpoint/1, % ?Endpoint:atom
    lwm_endpoint/2, % ?Endpoint:atom
                    % -Options:list(nvpair)
    lwm_endpoint_authentication/1 % -Authentication:list(nvpair)
  ]
).

/** <module> LOD Washing Machine (LWM): endpoint

Endpoint registration and usage for the LOD Washing Machine.

@author Wouter Beek
@version 2014/08
*/

:- use_module(library(base64)).
:- use_module(library(option)).

:- use_module(lwm(lwm_sparql_db)).



%! lwm_endpoint(+Endpoint:atom) is semidet.
%! lwm_endpoint(-Endpoint:atom) is det.

lwm_endpoint(Endpoint):-
  lwm_endpoint(Endpoint, _).

%! lwm_endpoint(-Endpoint:atom, -Options:list(nvpair)) is multi.

lwm_endpoint(Endpoint, Options2):-
  lwm_endpoint0(Endpoint, Options1),
  lwm_default_graph(LwmGraph),
  merge_options([named_graphs([LwmGraph])], Options1, Options2).

lwm_endpoint0(localhost, Options):-
  lwm_endpoint_authentication(AuthenticationOptions),
  merge_options(AuthenticationOptions, [update_method(direct)], Options).
%lwm_endpoint0(cliopatria, Options):-
%  lwm_endpoint_authentication(AuthenticationOptions),
%  merge_options(AuthenticationOptions, [update_method(direct)], Options).
%lwm_endpoint0(virtuoso, [update_method(url_encoded)]).


%! lwm_endpoint_authentication(-Authentication:list(nvpair)) is det.

lwm_endpoint_authentication([request_header('Authorization'=Authentication)]):-
  lwm_user(User),
  lwm_password(Password),
  atomic_list_concat([User,Password], ':', Plain),
  base64(Plain, Encoded),
  atomic_list_concat(['Basic',Encoded], ' ', Authentication).


lwm_password(lwmlwm).


lwm_user(lwm).
