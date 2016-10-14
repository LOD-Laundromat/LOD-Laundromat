:- module(
  noRdf_store,
  [
    post_nordf_triples/0,
    nordf_triple/3,       % ?S, ?P, ?O
    store_nordf_triple/3  % +S, +P, +O
  ]
).

/** <module> no-RDF store

A small-scale and simple RDF-like store that does not use
the built-in triple store for RDF in Semweb.
This means that we can use RDF transactions + snapshots
and at the same time send small RDF messages using SPARQL Update requests.

@author Wouter Beek
@version 2016/01
*/

:- use_module(library(debug)).
:- use_module(library(sparql/sparql_graph_store)).

:- use_module(lwm_settings).

%! nordf_triple0(?S, ?P, ?O) is nondet.
% Since threads load data in RDF transactions with snapshots,
% we cannot use the triple store for anything else during
% the load-save cycle of a data document.
% Therefore, we store triples that arise during this cycle
% as thread-specific Prolog assertions.

:- thread_local
   nordf_triple0/3.

:- rdf_meta
   nordf_triple(r,r,o).





%! post_nordf_triples is det.
% Sends a SPARQL Update request to the LOD Laundromat Endpoint
% containing all current noRdf triples, for a particular MD5.
%
% The thread-local nordf_triple0/3 statements form the contents
% of the update request.

post_nordf_triples:-
  lwm_version_graph(G),
  with_mutex(lwm_endpoint_access, (
    aggregate_all(set(rdf(S,P,O)), nordf_triple0(S, P, O), Ts),
    sparql_post_graph_statements(virtuoso_http, G, Ts),
    retractall(nordf_triple0(_,_,_))
  )).



%! nordf_triple(?S, ?P, ?O) is nondet.

nordf_triple(S, P, O) :-
  nordf_triple0(S, P, O).



%! store_nordf_triple(+S, +P, +O) is det.

store_nordf_triple(S1, P1, O1) :-
  maplist(rdf_term_map, [S1,P1,O1], [S2,P2,O2]),
  assert(nordf_triple0(S2, P2, O2)).

rdf_term_map(X-Y0, Z) :- !,
  (number(Y0) -> atom_number(Y, Y0) ; Y = Y0),
  rdf_global_id(X:Y, Z).
rdf_term_map(literal(type(X-Y,Q)), Q^^Z) :- !,
  rdf_global_id(X:Y, Z).
rdf_term_map(X, X).
