:- module(
     ll_cli,
     [
       number_of_triples/0
     ]
   ).

:- reexport(library(hdt/hdt_ext)).
:- reexport(library(q/q_rdf)).
:- reexport(library(semweb/rdf11)).

:- use_module(library(aggregate)).
:- use_module(library(iri/iri_ext)).
:- use_module(library(q/q_cli)).
:- use_module(library(q/q_io), []).
:- use_module(library(q/q_iri)).
:- use_module(library(settings)).

:- set_setting(iri:data_auth, 'lodlaundromat.org').
:- set_setting(iri:data_scheme, http).

:- set_setting(q_io:source_dir, '/scratch/wbeek/crawls/13/source/').
:- set_setting(q_io:store_dir, '/scratch/wbeek/crawls/13/store/').

:- q_init_ns.

number_of_triples :-
  aggregate_all(
    sum(NumTriples),
    hdt(meta, _, nsdef:numberOfTriples, NumTriples^^xsd:nonNegativeInteger),
    NumTriples
  ),
  format(user_output, "~D~n", [NumTriples]).
