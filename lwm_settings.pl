:- module(
  lwm_settings,
  [
    init_lwm_settings/1, % +Port:nonneg
    ll_authority/1, % ?Authority:atom
    ll_scheme/1, % ?Scheme:atom
    lod_basket_graph/1, % -Graph:atom
    lwm_version_graph/1 % -Graph:atom
  ]
).

/** <module> LOD Washing Machine: generics

Generic predicates for the LOD Washing Machine.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09, 2015/01-2015/02
*/

:- use_module(library(filesex)).
:- use_module(library(semweb/rdf_db), except([rdf_node/1])).
:- use_module(library(settings)).
:- use_module(library(uri)).

:- use_module(generics(service_db)).

:- use_module(plSparql(sparql_db)).

:- dynamic(user:prolog_file_type/2).
:- multifile(user:prolog_file_type/2).

user:prolog_file_type(conf, configuration).

:- rdf_register_prefix(
     error,
     'http://lodlaundromat.org/error/ontology/'
   ).
:- rdf_register_prefix(ll, 'http://lodlaundromat.org/resource/').
:- rdf_register_prefix(llo, 'http://lodlaundromat.org/ontology/').

:- setting(
     endpoint,
     oneof([both,cliopatria,virtuoso]),
     both,
     'The endpoint that is used to store the crawling metadata in.'
   ).
:- setting(
     keep_old_datadoc,
     boolean,
     true,
     'Whether the original data document is stored or not.'
   ).
:- setting(
     max_number_of_warnings,
     nonneg,
     100,
     'The maximum number of warnings that is stored per data document.'
   ).
:- setting(
     number_of_large_cleaning_threads,
     nonneg,
     1,
     'The number of threads for cleaning large data documents.'
   ).
:- setting(
     number_of_medium_cleaning_threads,
     nonneg,
     1,
     'The number of threads for cleaning medium data documents.'
   ).
:- setting(
     number_of_small_cleaning_threads,
     nonneg,
     1,
     'The number of threads for cleaning small data documents.'
   ).
:- setting(
     number_of_sorting_threads,
     nonneg,
     1,
     'The number of threads for sorting data documents.'
   ).
:- setting(
     number_of_unpacking_threads,
     nonneg,
     1,
     'The number of threads for downloading and unpacking data documents.'
   ).





%! lod_basket_graph(-Graph:atom) is det.

lod_basket_graph(Graph):-
  ll_scheme(Scheme),
  ll_authority(Authority),
  uri_components(Graph, uri_components(Scheme,Authority,'',_,seedlist)).



%! lwm_version_graph(-Graph:atom) is det.

lwm_version_graph(Graph):-
  ll_scheme(Scheme),
  ll_authority(Authority),
  uri_components(
    Graph,
    uri_components(Scheme,Authority,'',_,'12')
  ).





% HELPERS %

%! ll_authority(+Authortity:atom) is semidet.
%! ll_authority(-Authortity:atom) is det.

ll_authority('lodlaundromat.org').


%! ll_scheme(+Scheme:atom) is semidet.
%! ll_scheme(-Scheme:oneof([http])) is det.

ll_scheme(http).





% INITIALIZATION %

init_lwm_settings(Port):-
  (   absolute_file_name(
        lwm(settings),
        File,
        [access(read),file_errors(fail),file_type(configuration)]
      )
  ->  load_settings(File)
  ;   true
  ),

  % Register the ClioPatria SPARQL endpoint.
  uri_authority_components(Authority, uri_authority(_,_,localhost,Port)),
  uri_components(Uri, uri_components(http,Authority,'/',_,_)),
  sparql_register_endpoint(cliopatria, [Uri], cliopatria),

  % Virtuoso (1/3): Update (reset, continue).
  sparql_register_endpoint(
    virtuoso_update,
    ['http://localhost:8890/sparql-auth'],
    virtuoso
  ),
  sparql_db:assert(
    sparql_endpoint_option0(virtuoso_update, path_suffix(update), '')
  ),

  % Virtuoso (2/3): Query.
  sparql_register_endpoint(
    virtuoso_query,
    ['http://sparql.backend.lodlaundromat.org'],
    virtuoso
  ),
  sparql_db:assert(
    sparql_endpoint_option0(virtuoso_query, path_suffix(query), '')
  ),

  % Virtuoso (3/3): HTTP.
  sparql_register_endpoint(
    virtuoso_http,
    ['http://localhost:8890/sparql-graph-crud'],
    virtuoso
   ),
  sparql_db:assert(
    sparql_endpoint_option0(virtuoso_http, path_suffix(http), '')
  ).

