:- module(
  seedlist_legacy,
  [
    add_old_iris/0
  ]
).

/** <module> Legacy support for LOD Laundromat ≤12 seeds

@author Wouter Beek
@version 2016/02-2016/03
*/

:- use_module(library(lists)).
:- use_module(library(os/gnu_sort)).
:- use_module(library(sparql/sparql_db)).
:- use_module(library(sparql/sparql_query)).

:- use_module(cpack('LOD-Laundromat/seedlist')).

:- sparql_register_endpoint(
     lod_laundromat,
     ['http://sparql.backend.lodlaundromat.org'],
     virtuoso
   ).





%! add_old_iris is det.
% Extracts all seeds from the old LOD Laundromat server and stores them locally
% as a seedlist.  This is intended for debugging purposes only.

add_old_iris :-
  gtrace,
  absolute_file_name('seedlist.db', File, [access(write)]),
  Q = '\c
PREFIX llo: <http://lodlaundromat.org/ontology/>\n\c
SELECT ?url\n\c
WHERE {\n\c
  ?doc llo:url ?url\n\c
}\n',
  setup_call_cleanup(
    open(File, write, Write),
    forall(
      sparql_select(lod_laundromat, Q, Rows),
      forall(member([Iri], Rows), add_iri(Iri))
    ),
    close(Write)
  ),
  sort_file(File).
