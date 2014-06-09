:- module(
  noRdf_store,
  [
    post_rdf_triples/0,
    store_triple/4 % +Subject:or([bnode,iri])
                   % +Predicate:iri
                   % +Object:or([bnode,iri,literal])
                   % +Graph:atom
  ]
).

/** <module> no-RDF store

A small-scale and simple RDF-like store that does not use
the built-in triple store for RDF in Semweb.
This means that we can use RDF transactions + snapshots
and at the same time send small RDF messages using SPARQL Update requests.

@author Wouter Beek
@version 2014/05-2014/06
*/

:- use_module(library(http/http_client)).
:- use_module(library(semweb/rdf_db)).

:- use_module(sparql(sparql_db)).

:- use_module(plRdf_ser(rdf_ntriples_write)).

:- use_module(lwm(lwm_db)).
:- use_module(lwm(lwm_messages)).

:- rdf_meta(store_triple(r,r,o,+)).

%! rdf_triple(
%!   ?Subject:or([bnode,iri]),
%!   ?Predicate:iri,
%!   ?Object:or([bnode,iri,literal]),
%!   ?DataDocument:url
%! ) is nondet.
% Since threads load data in RDF transactions with snapshots,
% we cannot use the triple store for anything else during
% the load-save cycle of a data document.
% Therefore, we store triples that arise during this cycle
% as thread-specific Prolog assertions.

:- thread_local(rdf_triple/4).



%! post_rdf_triples is det.
% Sends a SPARQL Update requests to the SPARQL endpoints that are
% registered and enabled.
% The thread-local rdf_triple/4 statements form the contents
% of the update request.

post_rdf_triples:-
  lwm_endpoint(Endpoint),
  post_rdf_triples(Endpoint),
  fail.
post_rdf_triples.

%! post_rdf_triples(+EndpointName:atom) is det.

post_rdf_triples(Endpoint):-
  sparql_endpoint(Endpoint, update, Location),
  setup_call_cleanup(
    forall(
      rdf_triple(S, P, O, _),
      rdf_assert(S, P, O)
    ),
    (
      with_output_to(codes(Codes), sparql_insert_data([])),
      lwm_endpoint_authentication(Authentication),
      http_post(
        Location,
        codes('application/sparql-update', Codes),
        Reply,
        [request_header('Accept'='application/json')|Authentication]
      ),
      print_message(informational, sent_to_endpoint(Endpoint,Reply))
    ),
    (
      rdf_retractall(_, _, _),
      retractall(rdf_triple(_, _, _, _))
    )
  ).


%! store_triple(
%!   +Subject:or([bnode,iri]),
%!   +Predicate:iri,
%!   +Object:or([bnode,iri,literal]),
%!   +Graph:atom
%! ) is det.

store_triple(S, P, O, G):-
  assert(rdf_triple(S, P, O, G)).

