:- module(
  noRdf_store,
  [
    post_rdf_triples/0,
    store_triple/3 % +Subject
                   % +Predicate
                   % +Object
  ]
).

/** <module> no-RDF store

A small-scale and simple RDF-like store that does not use
the built-in triple store for RDF in Semweb.
This means that we can use RDF transactions + snapshots
and at the same time send small RDF messages using SPARQL Update requests.

@author Wouter Beek
@version 2014/05-2014/06, 2014/08-2014/09
*/

:- use_module(library(semweb/rdf_db)).

:- use_module(plRdf_ser(rdf_bnode_write)).

:- use_module(plSparql_http(sparql_graph_store)).

:- use_module(lwm(lwm_settings)).

%! rdf_triple(
%!   ?Subject:or([bnode,iri]),
%!   ?Predicate:iri,
%!   ?Object:or([bnode,iri,literal])
%! ) is nondet.
% Since threads load data in RDF transactions with snapshots,
% we cannot use the triple store for anything else during
% the load-save cycle of a data document.
% Therefore, we store triples that arise during this cycle
% as thread-specific Prolog assertions.

:- thread_local(rdf_triple/3).



%! post_rdf_triples is det.
% Sends a SPARQL Update request to the LOD Laundromat Endpoint
% containing all current noRdf triples, for a particular MD5.
%
% The thread-local rdf_triple/3 statements form the contents
% of the update request.

post_rdf_triples:-
  % Named graph argument.
  lwm_version_graph(NG),

  setup_call_cleanup(
    % Collect contents.
    aggregate_all(
      set(rdf(S,P,O)),
      rdf_triple(S, P, O),
      Triples
    ),
    % Use HTTP Graph Store on Virtuoso.
    (gtrace,
      with_mutex(lod_washing_machine, (
        sparql_post_named_graph(
          virtuoso_http,
          NG,
          Triples,
          [status_code(Code)]
        )
      )),

      % DEB
      (   between(200, 299, Code)
      ->  true
      ;   gtrace,
          format(user_output, '~w\n', [Code])
      )
    ),
    retractall(rdf_triple(_,_,_))
  ).


%! store_triple(+Subject, +Predicate, +Object) is det.

store_triple(S1, P1, O1):-
  maplist(rdf_term_map, [S1,P1,O1], [S2,P2,O2]),
  assert(rdf_triple(S2, P2, O2)).


rdf_term_map(X-Y0, Z):- !,
  (   number(Y0)
  ->  atom_number(Y, Y0)
  ;   Y = Y0
  ),
  rdf_global_id(X:Y, Z).
rdf_term_map(literal(type(X-Y,Q)), literal(type(Z,Q))):- !,
  rdf_global_id(X:Y, Z).
rdf_term_map(X, X).

