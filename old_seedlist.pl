:- module(old_seedlist, []).

:- use_module(library(debug)).
:- use_module(library(lists)).
:- use_module(library(os/gnu_sort)).
:- use_module(library(rdf11/rdf11)). % Operators.
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
  absolute_file_name(seedlist, File, [access(write),file_type(prolog)]),
  Q = '\c
PREFIX llo: <http://lodlaundromat.org/ontology/>\n\c
SELECT ?url\n\c
WHERE {\n\c
  ?doc llo:url ?url\n\c
}\n',
  get_time(Now),
  setup_call_cleanup(
    open(File, write, Write),
    forall(
      sparql_select(ll_endpoint, Q, Rows),
      forall(member([Url], Rows),   format(Write, 'seed(\'~a\',~w,0.0,0.0)~n', [Url,Now]))
    ),
    close(Write)
  ),
  sort_file(File),
  halt.
