:- module(old_seedlist, []).

:- use_module(library(debug)).
:- use_module(library(lists)).
:- use_module(library(os/gnu_sort)).
:- use_module(library(sparql/sparql_db)).
:- use_module(library(sparql/query/sparql_query)).

:- debug(sparql(_)).

:- sparql_register_endpoint(
     ll_endpoint,
     ['http://sparql.backend.lodlaundromat.org'],
     virtuoso
   ).

:- initialization(run).

run :-
  absolute_file_name('seedlist.db', File, [access(write)]),
  Q = 'PREFIX llo: <http://lodlaundromat.org/ontology/>\nSELECT ?url WHERE { ?doc llo:url ?url }\n',
  setup_call_cleanup(
    open(File, write, Write),
    forall(
      sparql_select(ll_endpoint, Q, Rows),
      forall(member([Url], Rows), format(Write, '~a~n', [Url]))
    ),
    close(Write)
  ),
  sort_file(File),
  halt.
