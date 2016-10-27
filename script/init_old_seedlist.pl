:- use_module(library(lists)).
:- use_module(library(sparql/sparql_query_client)).

:- use_module(llw(seedlist)).

init_old_seedlist :-
  % Extract all seeds from the old LOD Laundromat server and store
  % them locally as a seedlist.  This is intended for debugging
  % purposes only.
  Q = '\c
PREFIX llo: <http://lodlaundromat.org/ontology/>\n\c
SELECT ?url\n\c
WHERE {\n\c
  ?doc llo:url ?url\n\c
}\n',
  forall(
    sparql_select('http://sparql.backend.lodlaundromat.org', Q, Rows),
    forall(member([From], Rows), add_seed(From))
  ).
