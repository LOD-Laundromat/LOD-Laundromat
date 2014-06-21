:- module(
  lwm_generics,
  [
    data_directory/1, % -DataDirectory:atom
    lod_accept_header_value/1, % -Value
    lwm_base/2, % +Md5:atom
                % -Base:triple(atom)
    lwm_bnode_base/2, % +Md5:atom
                      % -Base:triple(atom)
    lwm_datadoc_location/2, % +Md5:atom
                            % -Location:url
    lwm_default_graph/1, % -DefaultGraph:iri
    lwm_endpoint/1, % ?Endpoint:atom
    lwm_endpoint/2, % ?Endpoint:atom
                    % -Options:list(nvpair)
    lwm_endpoint_authentication/1, % -Authentication:list(nvpair)
    lwm_sparql_ask/3, % +Prefixes:list(atom)
                      % +Bbps:or([compound,list(compound)])
                      % +Options:list(nvpair)
    lwm_sparql_ask/4, % +Endpoint:atom
                      % +Prefixes:list(atom)
                      % +Bbps:or([compound,list(compound)])
                      % +Options:list(nvpair)
    lwm_sparql_drop/1, % +Options:list(nvpair)
    lwm_sparql_drop/2, % +Endpoint:atom
                       % +Options:list(nvpair)
    lwm_sparql_select/5, % +Prefixes:list(atom)
                         % +Variables:list(atom)
                         % +Bgps:or([compound,list(compound)])
                         % -Result:list(list)
                         % +Options:list(nvpair)
    lwm_sparql_select/6, % +Endpoint:atom
                         % +Prefixes:list(atom)
                         % +Variables:list(atom)
                         % +Bgps:or([compound,list(compound)])
                         % -Result:list(list)
                         % +Options:list(nvpair)
    lwm_version/1, % -Version:positive_integer
    md5_to_dir/2, % +Md5:atom
                  % -Md5Directory:atom
    set_data_directory/1 % +DataDirectory:atom
  ]
).

/** <module> Download LOD generics

Generic predicates that are used in the LOD download process.

Also contains Configuration settings for project LOD-Washing-Machine.

@author Wouter Beek
@version 2014/04-2014/06
*/

:- use_module(library(base64)).
:- use_module(library(filesex)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_open)).
:- use_module(library(option)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(uri)).

:- use_module(generics(db_ext)).
:- use_module(generics(uri_ext)).
:- use_module(xml(xml_namespace)).

:- use_module(plSparql(sparql_api)).
:- use_module(plSparql(sparql_db)).

:- xml_register_namespace(lwm, 'http://lodlaundromat.org/vocab#').

%! data_directory(?DataDirectory:atom) is semidet.

:- dynamic(data_directory/1).

%! ssl_verify(+SSL, +ProblemCert, +AllCerts, +FirstCert, +Error)
% Currently we accept all certificates.

:- public(ssl_verify/5).
   ssl_verify(_, _, _, _, _).

:- initialization(init_lwm).
init_lwm:-
  % Localhost.
  uri_components(Url11, uri_components(http,'localhost:3020','/sparql/',_,_)),
  sparql_register_endpoint(localhost, query, Url11),
  uri_components(Url12, uri_components(http,'localhost:3020','/sparql/update',_,_)),
  sparql_register_endpoint(localhost, update, Url12),

  % Cliopatria.
  uri_components(Url21, uri_components(http,'lodlaundry.wbeek.ops.few.vu.nl','/sparql/',_,_)),
  sparql_register_endpoint(cliopatria, query, Url21),
  uri_components(Url22, uri_components(http,'lodlaundry.wbeek.ops.few.vu.nl','/sparql/update',_,_)),
  sparql_register_endpoint(cliopatria, update, Url22),

  % Virtuoso.
  uri_components(Url3, uri_components(http,'virtuoso.lodlaundromat.ops.few.vu.nl','/sparql',_,_)),
  sparql_register_endpoint(virtuoso, Url3).



%! lod_accept_header_value(-Value:atom) is det.

lod_accept_header_value(Value):-
  findall(
    Value,
    (
      lod_content_type(ContentType, Q),
      format(atom(Value), '~a; q=~1f', [ContentType,Q])
    ),
    Values
  ),
  atomic_list_concat(Values, ', ', Value).


%! lod_content_type(?ContentType:atom, ?QValue:between(0.0,1.0)) is nondet.

% RDFa
lod_content_type('text/html',              0.3).
% N-Quads
lod_content_type('application/n-quads',    0.8).
% N-Triples
lod_content_type('application/n-triples',  0.8).
% RDF/XML
lod_content_type('application/rdf+xml',    0.7).
lod_content_type('text/rdf+xml',           0.7).
lod_content_type('application/xhtml+xml',  0.3).
lod_content_type('application/xml',        0.3).
lod_content_type('text/xml',               0.3).
lod_content_type('application/rss+xml',    0.5).
% Trig
lod_content_type('application/trig',       0.8).
lod_content_type('application/x-trig',     0.5).
% Turtle
lod_content_type('text/turtle',            0.9).
lod_content_type('application/x-turtle',   0.5).
lod_content_type('application/turtle',     0.5).
lod_content_type('application/rdf+turtle', 0.5).
% N3
lod_content_type('text/n3',                0.8).
lod_content_type('text/rdf+n3',            0.5).
% All
lod_content_type('*/*',                    0.1).


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


lwm_datadoc_location(Md5, Location):-
  atomic_list_concat([Md5,'clean.nt.gz'], '/', Path),
  http_link_to_id(clean, path_postfix(Path), Location).


%! lwm_default_graph(-DefaultGraph:iri) is det.

lwm_default_graph(DefaultGraph):-
  lwm_version(Version),
  atom_number(Fragment, Version),
  uri_components(
    DefaultGraph,
    uri_components(http, 'lodlaundromat.org', _, _, Fragment)
  ).


lwm_endpoint(Endpoint):-
  lwm_endpoint(Endpoint, _).

%lwm_endpoint(localhost, [update_method(direct)|AuthOpts]):-
%  lwm_endpoint_authentication(AuthOpts).
lwm_endpoint(cliopatria, [update_method(direct)|AuthOpts]):-
  lwm_endpoint_authentication(AuthOpts).
%lwm_endpoint(
%  virtuoso,
%  [named_graph(DefaultGraph),update_method(url_encoded)]
%):-
%  lwm_default_graph(DefaultGraph).


%! lwm_endpoint_authentication(-Authentication:list(nvpair)) is det.

lwm_endpoint_authentication([request_header('Authorization'=Authentication)]):-
  lwm_user(User),
  lwm_password(Password),
  atomic_list_concat([User,Password], ':', Plain),
  base64(Plain, Encoded),
  atomic_list_concat(['Basic',Encoded], ' ', Authentication).


lwm_password(lwmlwm).


lwm_scheme(http).


lwm_sparql_ask(Prefixes, Bbps, Options):-
  once(lwm_endpoint(Endpoint)),
  lwm_sparql_ask(Endpoint, Prefixes, Bbps, Options).

lwm_sparql_ask(Endpoint, Prefixes, Bbps, Options1):-
  lwm_endpoint(Endpoint, Options2),
  merge_options(Options1, Options2, Options3),
  sparql_ask(Endpoint, Prefixes, Bbps, Options3).


lwm_sparql_drop(Options):-
  once(lwm_endpoint(Endpoint)),
  lwm_sparql_drop(Endpoint, Options).

lwm_sparql_drop(Endpoint, Options1):-
  lwm_endpoint(Endpoint, Options2),
  merge_options(Options1, Options2, Options3),
  option(default_graph(DefaultGraph), Options3),
  sparql_drop_graph(Endpoint, DefaultGraph, Options3).


lwm_sparql_select(Prefixes, Variables, Bgps, Result, Options):-
  once(lwm_endpoint(Endpoint)),
  lwm_sparql_select(Endpoint, Prefixes, Variables, Bgps, Result, Options).

lwm_sparql_select(Endpoint, Prefixes, Variables, Bgps, Result, Options1):-
  lwm_endpoint(Endpoint, Options2),
  merge_options(Options1, Options2, Options3),
  sparql_select(Endpoint, Prefixes, Variables, Bgps, Result, Options3).


lwm_user(lwm).


lwm_version(8).


%! md5_to_dir(+Md5:atom, -Md5Directory:atom) is det.

md5_to_dir(Md5, Md5Dir):-
  absolute_file_name(data(.), DataDir, [access(write),file_type(directory)]),
  directory_file_path(DataDir, Md5, Md5Dir),
  make_directory_path(Md5Dir).


%! set_data_directory(+DataDirectory:atom) is det.

set_data_directory(DataDir):-
  % Assert the data directory.
  db_replace_novel(data_directory(DataDir), [e]).

