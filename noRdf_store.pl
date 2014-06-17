:- module(
  noRdf_store,
  [
    post_rdf_triples/1, % +Md5:atom
    store_triple/3, % +Subject
                    % +Predicate
                    % +Object
    store_triple/4 % +Subject
                   % +Predicate
                   % +Object
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
:- use_module(library(uri)).

:- use_module(plSparql(sparql_api)).

:- use_module(lwm(lwm_db)).
:- use_module(lwm(lwm_generics)).
:- use_module(lwm(lwm_messages)).

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



%! post_rdf_triples(+Md5:atom) is det.
% Sends a SPARQL Update requests to the SPARQL endpoints that are
% registered and enabled.
% The thread-local rdf_triple/[3,4] statements form the contents
% of the update request.

post_rdf_triples(Md5):-
  lwm_bnode_base(Md5, BNodeBase),
  setup_call_cleanup(
    aggregate_all(
      set(Triple),
      rdf_triple(Triple),
      Triples
    ),
    forall(
      lwm_endpoint(Endpoint, Options1),
      (
        merge_options(
          [bnode_base(BNodeBase),reset_bnode_map(false)],
          Options1,
          Options2
        ),
        sparql_insert_data(Endpoint, Triples, Options2)
      )
    ),
    (
      retractall(rdf_triple(_, _, _)),
      retractall(rdf_triple(_, _, _, _))
    )
  ).


rdf_triple([S,P,O]):-
  rdf_triple(S, P, O).
rdf_triple([S,P,O,G]):-
  rdf_triple(S, P, O, G).


%! store_triple(+Subject, +Predicate, +Object) is det.

store_triple(S, P, O):-
  lwm_default_graph(DefaultGraph),
  store_triple(S, P, O, DefaultGraph).

%! store_triple(+Subject, +Predicate, +Object, +Graph) is det.

store_triple(S1, P1, O1, G1):-
  maplist(rdf_term_map, [S1,P1,O1], [S2,P2,O2]),
  versioned_graph(G1, G2),
  assert(rdf_triple(S2, P2, O2, G2)).


rdf_term_map(X-Y, Z):- !,
  rdf_global_id(X:Y, Z).
rdf_term_map(literal(type(X-Y,Q)), literal(type(Z,Q))):- !,
  rdf_global_id(X:Y, Z).
rdf_term_map(X, X).


%! versioned_graph(+Graph:atom, -VersionedGraph:atom) is det.

versioned_graph(G1, G2):-
  lwm_bnode_base(G1, Scheme-Authority-Hash1),
  lwm_version(Version),
  atomic_concat([Hash1,Version], '#', Path1),
  atomic_list_concat(['',Path1], '/', Path2),
  uri_components(G2, uri_components(Scheme,Authority,Path2,_,_)).

