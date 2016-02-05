:- module(old_seedlist, []).

:- use_module(library(debug)).
:- use_module(library(lists)).
:- use_module(library(os/gnu_sort)).
:- use_module(library(rdf11/rdf11)).
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
SELECT ?url ?added\n\c
WHERE {\n\c
  ?doc llo:url ?url ;\n\c
       llo:added ?added\n\c
}\n',
  setup_call_cleanup(
    open(File, write, Write),
    forall(
      sparql_select(ll_endpoint, Q, Rows),
      forall(member([Url,Added], Rows), assert_seed(Write, Url, Added))
    ),
    close(Write)
  ),
  sort_file(File),
  halt.

assert_seed(Write, Url, Added1) :-
  gtrace,
  rdf11:post_object(Added2, Added1),
  date_time_stamp(Added2, Added3),
  format(Write, 'seed(~w,~w,0.0,0.0)~n', [Url,Added3]).
