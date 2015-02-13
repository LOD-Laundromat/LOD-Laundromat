:- module(
  noRdf_store,
  [
    post_rdf_triples/0,
    rdf_triple/3, % +Subject
                  % +Predicate
                  % +Object
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
@version 2014/05-2014/06, 2014/08-2014/09, 2015/01-2015/02
*/

:- use_module(library(semweb/rdf_db), except([rdf_node/1])).

:- use_module(plSparql(http/sparql_graph_store)).
:- use_module(plSparql(update/sparql_update_api)).

:- use_module(lwm(lwm_settings)).

%! rdf_triple0(?S, ?P, ?O) is nondet.
% Since threads load data in RDF transactions with snapshots,
% we cannot use the triple store for anything else during
% the load-save cycle of a data document.
% Therefore, we store triples that arise during this cycle
% as thread-specific Prolog assertions.

:- thread_local(rdf_triple0/3).

:- rdf_meta(rdf_triple(r,r,o)).





%! post_rdf_triples is det.
% Sends a SPARQL Update request to the LOD Laundromat Endpoint
% containing all current noRdf triples, for a particular MD5.
%
% The thread-local rdf_triple0/3 statements form the contents
% of the update request.

post_rdf_triples:-
  % Collect contents.
  aggregate_all(
    set(rdf(S,P,O)),
    rdf_triple0(S, P, O),
    Triples
  ),
  
  post_rdf_triples(Triples, Code),
  
  % Debug
  (   between(100, 599, Code)
  ->  true
  ;   writeln(Code),
      maplist(writeln, Triples),
      gtrace, %DEB
      post_rdf_triples
  ),

  % Cleanup.
  retractall(rdf_triple0(_,_,_)).



%! rdf_triple(
%!   ?Subject:or([bnode,iri]),
%!   ?Predicate:iri,
%!   ?Object:or([bnode,iri,literal])
%! ) is nondet.

rdf_triple(S, P, O):-
  rdf_triple0(S0, P0, O0),
  maplist(rdf_term_map, [S0,P0,O0], [S,P,O]).



%! store_triple(+Subject, +Predicate, +Object) is det.

store_triple(S1, P1, O1):-
  maplist(rdf_term_map, [S1,P1,O1], [S2,P2,O2]),
  assert(rdf_triple0(S2, P2, O2)).





% HELPERS %

%! post_rdf_triples(+Triples:list(compound), -Code:nonneg) is det.

post_rdf_triples(Triples, Code):-
  lwm_settings:setting(endpoint, both), !,
  with_mutex(lwm_endpoint_access,
    concurrent(
      2,
      [
        post_rdf_triples(cliopatria, Triples, Code),
        post_rdf_triples(virtuoso, Triples, Code)
      ],
      []
    )
  ).
post_rdf_triples(Triples, Code):-
  lwm_settings:setting(endpoint, Endpoint),
  post_rdf_triples(Endpoint, Triples, Code).

% Use SPARQL Update on ClioPatria.
post_rdf_triples(cliopatria, Triples, Code):- !,
  sparql_insert_data(
    cliopatria,
    Triples,
    [],
    [],
    [status_code(Code)]
  ).
post_rdf_triples(virtuoso, Triples, Code):-
  lwm_version_graph(NG),
  sparql_post_named_graph(
    virtuoso_http,
    NG,
    Triples,
    [status_code(Code)]
  ).

rdf_term_map(X-Y0, Z):- !,
  (   number(Y0)
  ->  atom_number(Y, Y0)
  ;   Y = Y0
  ),
  rdf_global_id(X:Y, Z).
rdf_term_map(literal(type(X-Y,Q)), literal(type(Z,Q))):- !,
  rdf_global_id(X:Y, Z).
rdf_term_map(X, X).

