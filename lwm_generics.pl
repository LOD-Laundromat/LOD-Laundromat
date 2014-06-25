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
    lwm_file_extension/2, % +Md5:atom
                          % -FileExtension:atom
    lwm_sparql_ask/3, % +Prefixes:list(atom)
                      % +Bgps:or([compound,list(compound)])
                      % +Options:list(nvpair)
    lwm_sparql_ask/4, % +Endpoint:atom
                      % +Prefixes:list(atom)
                      % +Bgps:or([compound,list(compound)])
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
    lwm_url/2, % +Md5:atom
               % -Url:url
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
:- use_module(library(option)).
:- use_module(library(uri)).

:- use_module(generics(db_ext)).
:- use_module(generics(meta_ext)).
:- use_module(xml(xml_namespace)).

:- use_module(plRdf_term(rdf_literal)).

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
  init_cliopatria,
  init_localhost,
  init_virtuoso.

% Localhost.
init_localhost:-
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
init_cliopatria:-
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
init_virtuoso:-
  sparql_register_endpoint(
    virtuoso,
    query,
    uri_components(http,'virtuoso.lodlaundromat.ops.few.vu.nl','/sparql',_,_)
  ).



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

lwm_endpoint(Endpoint, Options2):-
  lwm_endpoint0(Endpoint, Options1),
  lwm_default_graph(LwmGraph),
  merge_options([named_graphs([LwmGraph])], Options1, Options2).

%lwm_endpoint0(localhost, [update_method(direct)|AuthOpts]):-
%  lwm_endpoint_authentication(AuthOpts).
lwm_endpoint0(cliopatria, [update_method(direct)|AuthOpts]):-
  lwm_endpoint_authentication(AuthOpts).
%lwm_endpoint0(virtuoso, [update_method(url_encoded)]).


%! lwm_endpoint_authentication(-Authentication:list(nvpair)) is det.

lwm_endpoint_authentication([request_header('Authorization'=Authentication)]):-
  lwm_user(User),
  lwm_password(Password),
  atomic_list_concat([User,Password], ':', Plain),
  base64(Plain, Encoded),
  atomic_list_concat(['Basic',Encoded], ' ', Authentication).


%! lwm_file_extension(+Md5:atom, -FileExtension:atom) is det.

lwm_file_extension(Md5, FileExtension):-
  lwm_sparql_select([lwm], [file_extension],
      [rdf(var(md5res),lwm:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5res),lwm:file_extension,var(file_extension))],
      [[Literal]], [limit(1)]),
  rdf_literal(Literal, FileExtension, _).


lwm_password(lwmlwm).


lwm_scheme(http).


%! lwm_size(+Md5:atom, -NumberOfGigabytes:between(0.0,inf)) is det.

lwm_size(Md5, NumberOfGigabytes):-
  lwm_sparql_select([lwm], [size],
      [rdf(var(datadoc),lwm:md5,literal(type(xsd:string,Md5))),
       rdf(var(datadoc),lwm:size,var(size))],
      [[Literal]], [limit(1)]), !,
  rdf_literal(Literal, NumberOfBytes1, _),
  atom_number(NumberOfBytes1, NumberOfBytes2),
  NumberOfGigabytes is NumberOfBytes2 / (1024 ** 3).


%! lwm_source(+Md5:atom, -Source:atom) is det.
% Returns the original source of the given datadocument.
%
% This is either a URL simpliciter,
% or a URL suffixed by an archive entry path.

lwm_source(Md5, Url):-
  lwm_url(Md5, Url), !.
lwm_source(Md5, Source):-
  lwm_sparql_select([lwm], [md5parent,path],
      [rdf(var(ent),lwm:md5,literal(type(xsd:string,Md5))),
       rdf(var(ent),lwm:path,var(path)),
       rdf(var(parent),lwm:contains_entry,var(md5ent)),
       rdf(var(parent),lwm:md5,var(md5parent))],
      [[Literal1,Literal2]], [limit(1)]), !,
  maplist(rdf_literal, [Literal1,Literal2], [ParentMd5,Path], _),
  lwm_source(ParentMd5, ParentSource),
  atomic_concat(ParentSource, Path, Source).
lwm_source(_, 'UNKNOWN SOURCE').


lwm_sparql_ask(Prefixes, Bgps1, Options):-
  once(lwm_endpoint(Endpoint)),
  lwm_default_graph(LwmGraph),
  Bgps2 = [graph(LwmGraph,Bgps1)],
  lwm_sparql_ask(Endpoint, Prefixes, Bgps2, Options).

lwm_sparql_ask(Endpoint, Prefixes, Bgps, Options1):-
  lwm_endpoint(Endpoint, Options2),
  merge_options(Options1, Options2, Options3),
  loop_until_true(
    sparql_ask(Endpoint, Prefixes, Bgps, Options3)
  ).


lwm_sparql_drop(Options):-
  once(lwm_endpoint(Endpoint)),
  lwm_sparql_drop(Endpoint, Options).

lwm_sparql_drop(Endpoint, Options1):-
  lwm_endpoint(Endpoint, Options2),
  merge_options(Options1, Options2, Options3),
  option(default_graph(DefaultGraph), Options3),
  loop_until_true(
    sparql_drop_graph(Endpoint, DefaultGraph, Options3)
  ).


lwm_sparql_select(Prefixes, Variables, Bgps1, Result, Options):-
  once(lwm_endpoint(Endpoint)),
  lwm_default_graph(LwmGraph),
  Bgps2 = [graph(LwmGraph,Bgps1)],
  lwm_sparql_select(Endpoint, Prefixes, Variables, Bgps2, Result, Options).

lwm_sparql_select(Endpoint, Prefixes, Variables, Bgps, Result, Options1):-
  lwm_endpoint(Endpoint, Options2),
  merge_options(Options1, Options2, Options3),
  loop_until_true(
    sparql_select(Endpoint, Prefixes, Variables, Bgps, Result, Options3)
  ).


%! lwm_url(+Md5:atom, -Url:url) is det.

lwm_url(Md5, Url):-
  lwm_sparql_select([lwm], [url],
      [rdf(var(md5res),lwm:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5res),lwm:url,var(url))],
      [[Url]], [limit(1)]).


lwm_user(lwm).


lwm_version(10).


%! md5_to_dir(+Md5:atom, -Md5Directory:atom) is det.

md5_to_dir(Md5, Md5Dir):-
  absolute_file_name(data(.), DataDir, [access(write),file_type(directory)]),
  directory_file_path(DataDir, Md5, Md5Dir),
  make_directory_path(Md5Dir).


%! set_data_directory(+DataDirectory:atom) is det.

set_data_directory(DataDir):-
  % Assert the data directory.
  db_replace_novel(data_directory(DataDir), [e]).



% Messages

:- multifile(prolog:message/1).

prolog:message(lwm_end(Mode,Md5,Source,Status,Messages)) -->
  status(Md5, Status),
  messages(Messages),
  ['[END '],
  lwm_mode(Mode),
  ['] [~w] [~w]'-[Md5,Source]].

prolog:message(lwm_sparql_retry(Mode,Exception)) -->
  ['Retrying '],
  sparql_mode(Mode),
  [': ~w'-[Exception]].

prolog:message(lwm_start(Mode,Md5,Source)) -->
  {lwm_source(Md5, Source)},
  ['[START '],
  lwm_mode(Mode),
  ['] [~w] [~w]'-[Md5,Source]],
  lwm_start_mode_specific_suffix(Md5, Mode).

lines([]) --> [].
lines([H|T]) -->
  [H],
  lines(T).

lwm_mode(clean) --> ['CLEAN'].
lwm_mode(metadata) --> ['METADATA'].
lwm_mode(unpack) --> ['UNPACK'].

lwm_start_mode_specific_suffix(Md5, clean) --> !,
  {lwm_size(Md5, NumberOfGigabytes)},
  [' [~f]'-[NumberOfGigabytes]].
lwm_start_mode_specific_suffix(_, unpack) --> [].

message(message(_,Kind,Lines)) -->
  ['    [MESSAGE(~w)] '-[Kind]],
  lines(Lines),
  [nl].

messages([]) --> !, [].
messages([H|T]) -->
  message(H),
  messages(T).

sparql_mode(ask) --> ['ASK'].
sparql_mode(drop) --> ['DROP'].
sparql_mode(select) --> ['SELECT'].

% @tbd Send an email whenever an MD5 fails.
status(_, false) --> !,
  ['    [STATUS] FALSE',nl].
status(_, true) --> !.
status(_, Status) -->
  ['    [STATUS] ~w'-[Status],nl].

