:- module(
  lwm_db,
  [
    lwm_base/2, % +Md5:atom
                % -Base:triple(atom)
    lwm_bnode_base/2, % +Md5:atom
                      % -Base:triple(atom)
    lwm_endpoint/1, % ?Endpoint:atom
    lwm_endpoint/2, % ?Endpoint:atom
                    % -Options:list(nvpair)
    lwm_endpoint_authentication/1, % -Authentication:list(nvpair)
    lwm_sparql_ask/4, % +Endpoint:atom
                      % ?Regime:oneof([owl])
                      % +Prefixes:list(atom)
                      % +Bbps:or([compound,list(compound)])
    lwm_sparql_drop/1, % +Endpoint:atom
    lwm_sparql_select/10, % +Endpoint:atom
                          % ?Regime:oneof([owl])
                          % +Prefixes:list(atom)
                          % +Distinct:boolean
                          % +Variables:list(atom)
                          % +BGPs:or([compound,list(compound)])
                          % ?Limit:or([nonneg,oneof([inf])])
                          % ?Offset:nonneg
                          % ?Order:pair(oneof([asc]),list(atom))
                          % -Result:list(list)
    lwm_sparql_update/3 % +Endpoint:atom
                        % +Mode:oneof([delete,insert])
                        % +Triples:list(list(or([bnode,iri,literal])))
  ]
).

/** <module> Configure

Configuration settings for project LOD-Washing-Machine.

@author Wouter Beek
@version 2014/06
*/

:- use_module(library(base64)).
:- use_module(library(uri)).

:- use_module(sparql(sparql_api)).
:- use_module(sparql(sparql_db)).
:- use_module(xml(xml_namespace)).

:- use_module(lwm(lwm_generics)).

:- xml_register_namespace(lwm, 'http://lodlaundromat.org/vocab#').

:- initialization(init_lwm).
init_lwm:-
  % Localhost.
  uri_components(Url11, uri_components(http,'localhost:3040','/sparql/',_,_)),
  sparql_register_endpoint(localhost, query, Url11),
  uri_components(Url12, uri_components(http,'localhost:3040','/sparql/update',_,_)),
  sparql_register_endpoint(localhost, update, Url12),

  % Cliopatria.
  uri_components(Url21, uri_components(http,'lodlaundry.wbeek.ops.few.vu.nl','/sparql/',_,_)),
  sparql_register_endpoint(cliopatria, query, Url21),
  uri_components(Url22, uri_components(http,'lodlaundry.wbeek.ops.few.vu.nl','/sparql/update',_,_)),
  sparql_register_endpoint(cliopatria, update, Url22),

  % Virtuoso.
  uri_components(Url3, uri_components(http,'virtuoso.lodlaundromat.ops.few.vu.nl','/sparql',_,_)),
  sparql_register_endpoint(virtuoso, Url3).


%! lwm_authortity(?Authortity:atom) is semidet.

lwm_authortity('lodlaundromat.org').


%! lwm_base(+Md5:atom, -Base:triple(atom)) is det.

lwm_base(Md5, Base):-
  lwm_scheme(Scheme),
  lwm_authortity(Authority),
  atomic_list_concat(['',Md5], '/', Path1),
  atomic_concat(Path1, '#', Path2),
  uri_components(Base, uri_components(Scheme,Authority,Path2,_,_)).


%! lwm_bnode_base(+Md5:atom, -Base:triple(atom)) is det.

lwm_bnode_base(Md5, Scheme-Authority-Md5):-
  lwm_scheme(Scheme),
  lwm_authortity(Authority).


lwm_endpoint(Endpoint):-
  lwm_endpoint(Endpoint, _).

%lwm_endpoint(localhost, [update_method(direct)|AuthOpts]):-
%  lwm_endpoint_authentication(AuthOpts).
lwm_endpoint(
  virtuoso,
  [default_graph(DefaultGraph),update_method(url_encoded)]
):-
  default_graph(DefaultGraph).

%lwm_endpoint(cliopatria, [update_method(direct)|AuthOpts]):-
%  lwm_endpoint_authentication(AuthOpts).


%! lwm_endpoint_authentication(-Authentication:list(nvpair)) is det.

lwm_endpoint_authentication([request_header('Authorization'=Authentication)]):-
  lwm_user(User),
  lwm_password(Password),
  atomic_list_concat([User,Password], ':', Plain),
  base64(Plain, Encoded),
  atomic_list_concat(['Basic',Encoded], ' ', Authentication).


lwm_password(lwmlwm).


%! lwm_scheme(?Scheme:atom) is semidet.

lwm_scheme(http).


lwm_sparql_ask(Endpoint, Regime, Prefixes, Bbps):-
  lwm_endpoint(Endpoint, Options),
  sparql_ask(Endpoint, Regime, Prefixes, Bbps, Options).


lwm_sparql_drop(Endpoint):-
  lwm_endpoint(Endpoint, Options),
  option(default_graph(DefaultGraph), Options),
  sparql_drop(Endpoint, DefaultGraph, Options).


lwm_sparql_select(
  Endpoint,
  Regime,
  Prefixes,
  Distinct,
  Variables,
  BGPs,
  Limit,
  Offset,
  Order,
  Result
):-
  lwm_endpoint(Endpoint, Options),
  sparql_select(
    Endpoint,
    Regime,
    Prefixes,
    Distinct,
    Variables,
    BGPs,
    Limit,
    Offset,
    Order,
    Result,
    Options
  ).


lwm_sparql_update(Endpoint, Mode, Triples):-
  lwm_endpoint(Endpoint, Options),
  sparql_update(Endpoint, Mode, Triples, Options).


lwm_user(lwm).

