:- module(
  noRdf_store,
  [
    post_rdf_triples/0,
    store_triple/3, % +Subject:or([bnode,iri])
                    % +Predicate:iri
                    % +Object:or([bnode,iri,literal])
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
:- use_module(library(option)).
:- use_module(library(semweb/rdf_db)).

:- use_module(plSparql(sparql_api)).

:- use_module(lwm(lwm_db)).
:- use_module(lwm(lwm_messages)).

:- rdf_meta(store_triple(r,r,o,+)).

%! rdf_triple(
%!   ?Subject:or([bnode,iri]),
%!   ?Predicate:iri,
%!   ?Object:or([bnode,iri,literal])
%! ) is nondet.
%! rdf_triple(
%!   ?Subject:or([bnode,iri]),
%!   ?Predicate:iri,
%!   ?Object:or([bnode,iri,literal]),
%!   ?Graph:atom
%! ) is nondet.
% Since threads load data in RDF transactions with snapshots,
% we cannot use the triple store for anything else during
% the load-save cycle of a data document.
% Therefore, we store triples that arise during this cycle
% as thread-specific Prolog assertions.

:- thread_local(rdf_triple/3).
:- thread_local(rdf_triple/4).



%! post_rdf_triples is det.
% Sends a SPARQL Update requests to the SPARQL endpoints that are
% registered and enabled.
% The thread-local rdf_triple/[3,4] statements form the contents
% of the update request.

post_rdf_triples:-
  setup_call_cleanup(
    aggregate_all(
      set(Triple),
      rdf_triple(Triple),
      Triples
    ),
    forall(
      lwm_endpoint(Endpoint, Options),
      sparql_insert_data(Endpoint, Triples, Options)
    ),
    retractall(rdf_triple(_, _, _, _))
  ).


rdf_triple([S,P,O]):-
  rdf_triple(S, P, O).
rdf_triple([S,P,O,G]):-
  rdf_triple(S, P, O, G).


%! store_triple(
%!   +Subject:or([bnode,iri]),
%!   +Predicate:iri,
%!   +Object:or([bnode,iri,literal])
%! ) is det.

store_triple(S1, P1, O1):-
  maplist(rdf_term_map, [S1,P1,O1], [S2,P2,O2]),
  assert(rdf_triple(S2, P2, O2)).

%! store_triple(
%!   +Subject:or([bnode,iri]),
%!   +Predicate:iri,
%!   +Object:or([bnode,iri,literal]),
%!   +Graph:atom
%! ) is det.

store_triple(S1, P1, O1, G):-
  maplist(rdf_term_map, [S1,P1,O1], [S2,P2,O2]),
  assert(rdf_triple(S2, P2, O2, G)).


rdf_term_map(X-Y, Z):- !,
  rdf_global_id(X:Y, Z).
rdf_term_map(X, X).

