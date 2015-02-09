:- module(
  lwm_settings,
  [
    init_lwm_settings/1, % +Port:nonneg
    ll_authority/1, % ?Authority:atom
    ll_scheme/1 % ?Scheme:atom
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
  uri_authority_components(Authority, uri_authority(_,_,localhost,Port)),
  uri_components(Uri, uri_components(http,Authority,'/',_,_)),
  % Register the ClioPatria SPARQL endpoint.
  sparql_register_endpoint(cliopatria, [Uri], cliopatria).

