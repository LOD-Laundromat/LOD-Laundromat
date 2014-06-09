:- module(
  lwm_db,
  [
    lwm_endpoint/1, % ?Endpoint:atom
    lwm_endpoint_authentication/1 % -Authentication:list(nvpair)
  ]
).

/** <module> Configure

Configuration settings for project LOD-Washing-Machine.

@author Wouter Beek
@version 2014/06
*/

:- use_module(library(base64)).
:- use_module(library(uri)).

:- use_module(sparql(sparql_db)).
:- use_module(xml(xml_namespace)).

:- use_module(lwm(lwm_generics)).

:- xml_register_namespace(ap, 'http://www.wouterbeek.com/ap.owl#').

:- initialization(init_lwm).
init_lwm:-
  % Localhost.
  default_graph(DefaultGraph),
  uri_query_components(Search1, [format('rdf+xml'),'using-graph-uri'=DefaultGraph]),
  uri_components(Url11, uri_components(http,'localhost:3020','/sparql',Search1,_)),
  sparql_register_endpoint(localhost, query, Url11),
  uri_components(Url12, uri_components(http,'localhost:3020','/sparql/update',Search1,_)),
  sparql_register_endpoint(localhost, update, Url12),
  
  % Cliopatria.
  uri_components(Url21, uri_components(http,'lodlaundry.wbeek.ops.few.vu.nl','/sparql',Search1,_)),
  sparql_register_endpoint(cliopatria, query, Url21),
  uri_components(Url22, uri_components(http,'lodlaundry.wbeek.ops.few.vu.nl','/sparql/update',Search1,_)),
  sparql_register_endpoint(cliopatria, update, Url22),
  
  % Virtuoso.
  uri_query_components(Search3, ['using-graph-uri'=DefaultGraph]),
  uri_components(Url3, uri_components(http,'virtuoso.lodlaundromat.ops.few.vu.nl','/sparql',Search3,_)),
  sparql_register_endpoint(virtuoso, Url3).


lwm_endpoint(localhost).
%lwm_endpoint(cliopatria).
%lwm_endpoint(virtuoso).


%! lwm_endpoint_authentication(-Authentication:list(nvpair)) is det.

lwm_endpoint_authentication([request_header('Authorization'=Authentication)]):-
  user(User),
  password(Password),
  atomic_list_concat([User,Password], ':', Plain),
  base64(Plain, Encoded),
  atomic_list_concat(['Basic',Encoded], ' ', Authentication).

password(lwmlwm).

user(lwm).

