:- module(
  ll2_api,
  [
    gen_term_index/0,
    gen_stat/0,
    number_of_triples/0,
    term_index/2 % ?Term, ?Hash
  ]
).

:- reexport(library(hdt/hdt_ext)).
:- reexport(library(q/q_cli)).
:- reexport(library(q/q_rdf)).
:- reexport(library(semweb/rdf11)).
:- reexport(library(service/rocks_ext)).

/** <module> LOD Laundromat 2 API

@author Wouter Beek
@version 2017/01
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(hdt/hdt_ext)).
:- use_module(library(iri/iri_ext)).
:- use_module(library(lists)).
:- use_module(library(q/q_fs)).
:- use_module(library(q/q_io), []).
:- use_module(library(q/q_iri)).
:- use_module(library(q/q_term)).
:- use_module(library(q/qb)).
:- use_module(library(rdf/rdf__io)).
:- use_module(library(settings)).
:- use_module(library(yall)).

:- debug(stat).

:- set_setting(iri:data_auth, 'lodlaundromat.org').
:- set_setting(iri:data_scheme, http).

:- set_setting(q_io:source_dir, '/scratch/wbeek/crawls/13/source/').
:- set_setting(q_io:store_dir, '/scratch/wbeek/crawls/13/store/').
:- set_setting(rocks_ext:index_dir, '/scratch/wbeek/crawls/13/index/').

:- q_init_ns.



gen_term_index :-
  call_on_rocks(term_hashes, set(atom), gen_term_index).

gen_term_index(Alias) :-
  q(hdt, S, P, O, G),
  q_graph_hash(G, Hash),
  maplist(add_term_index(Alias, Hash), [S,P,O]),
  flag(number_of_triples, NumTriples, NumTriples + 1),
  format(user_output, "~D~n", [NumTriples]),
  fail.
gen_term_index(_).

add_term_index(Alias, Hash, Term) :-
  q_term_to_atom(Term, A),
  rocks_merge(Alias, A, [Hash]).



gen_stat :-
  call_on_rocks(stat, int, gen_stat).

gen_stat(Alias) :-
  init_stat(Alias),
  forall(
    stat_file_todo(DataFile, G, StatFile),
    (
      hdt_call_on_file(DataFile, gen_stat(Alias)),
      rdf_call_to_ntriples(StatFile, rdf_write_stat(Alias, G)),
      hdt_prepare_file(StatFile, HdtStatFile),
      maplist(q_file_touch_ready, [StatFile,HdtStatFile])
    )
  ).

rdf_write_stat(Alias, G, State, Out) :-
  forall(
    rocks_pull(Alias, Key, Val),
    (
      rdf_global_id(nsdef:Key, P),
      debug(stat, "~a: ~D", [Key,Val]),
      qb(stream(State,Out), G, P, Val^^xsd:nonNegativeInteger)
    )
  ).

gen_stat(Alias, Hdt) :-
  q(hdt0, S, P, O, Hdt),
  forall(
    update_stat(Key, S, P, O),
    rocks_merge(Alias, Key, 1)
  ),
  fail.
gen_stat(_, _).

init_stat(Alias) :-
  forall(
    update_stat(Key, _, _, _),
    rocks_merge(Alias, Key, 0)
  ).

update_stat(literal, _, _, O) :-
  q_is_literal(O).



%! stat_file_todo(-DataFile, -G, -StatFile) is nondet.

stat_file_todo(DataFile, G, StatFile) :-
  q_dir_file(Dir, data, hdt, DataFile),
  q_file_is_ready(DataFile),
  directory_file_path(Dir, 'stat.nt.gz', StatFile),
  q_dir_graph(Dir, data, G),
  \+ q_file_is_ready(StatFile).
  


%! number_of_triples is det.

number_of_triples :-
  aggregate_all(
    sum(NumTriples),
    hdt(meta, _, nsdef:numberOfTriples, NumTriples^^xsd:nonNegativeInteger),
    NumTriples
  ),
  format(user_output, "~D~n", [NumTriples]).



%! term_index(+Term, +Hash) is semidet.
%! term_index(+Term, -Hash) is nondet.
%! term_index(-Term, -Hash) is nondet.

term_index(Term, Hash) :-
  call_on_rocks(term_hashes, set(atom), term_index(Term, Hash)).

term_index(Term, Hash, Alias) :-
  (   var(Term)
  ->  rocks_enum(Alias, Term, Hashes)
  ;   rocks_get(Alias, Term, Hashes)
  ),
  member(Hash, Hashes).
