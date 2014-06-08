:- module(
  configure,
  [
    endpoint/4, % ?EndpointName:atom
                % ?EndpointUrl:url
                % ?Enabled:boolean
                % ?Authentication:boolean
    endpoint_authentication/2 % +EndpointName:atom
                              % -Authentication:list(nvpair)
  ]
).

/** <module> Configure

Configuration settings for project LOD-Washing-Machine.

@author Wouter Beek
@version 2014/06
*/

:- use_module(library(base64)).
:- use_module(library(uri)).

:- use_module(lwm(lwm_generics)).



%! endpoint(
%!   ?EndpointName:atom,
%!   ?EndpointUrl:url,
%!   ?Enabled:boolean,
%!   ?Authentication:boolean
%! ) is nondet.

endpoint(cliopatria, Url, true, true):-
  uri_query_components(Search, [format('rdf+xml')]),
  uri_components(
    Url,
    uri_components(
      http,
      'lodlaundry.wbeek.ops.few.vu.nl',
      '/sparql/update',
      Search,
      _
    )
  ).
endpoint(stardog, Url, false, true):-
  uri_components(
    Url,
    uri_components(
      http,
      'stardog.lodlaundromat.ops.few.vu.nl',
      '/laundromat/update',
      _,
      _
    )
  ).
endpoint(virtuoso, Url, true, false):-
  lwm_version(Version),
  atom_number(Fragment, Version),
  uri_components(Graph, uri_components(http, laundromat, _, _, Fragment)),
  uri_query_components(Search, ['using-graph-uri'=Graph]),
  uri_components(
    Url,
    uri_components(
      http,
      'virtuoso.lodlaundromat.ops.few.vu.nl',
      '/sparql',
      Search,
      _
    )
  ).


%! endpoint_authentication(
%!   +EndpointName:atom,
%!   -Authentication:list(nvpair)
%! ) is det.

endpoint_authentication(
  EndpointName,
  [request_header('Authorization'=Authentication)]
):-
  endpoint(EndpointName, _, _, true), !,
  user(User),
  password(Password),
  atomic_list_concat([User,Password], ':', Plain),
  base64(Plain, Encoded),
  atomic_list_concat(['Basic',Encoded], ' ', Authentication).
endpoint_authentication(_, []).


%! password(-Password:atom) is det.

password(lwmlwm).


%! user(-User:name) is det.

user(lwm).

