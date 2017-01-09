:- module(
  ll2_api,
  [
    gen_stat/0,
    gen_term_index/0,
    number_of_triples/0,
    print_todo_process/1, % +Local
    term_index/2,         % ?Term, ?Hash
    term_index_size/1     % -NumTerms
  ]
).

:- reexport(library(hdt/hdt_ext)).
:- reexport(library(os/file_ext)).
:- reexport(library(q/q_cli)).
:- reexport(library(q/q_rdf)).
:- reexport(library(semweb/rdf11)).
:- reexport(library(service/rocks_api)).

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
:- use_module(library(print_ext)).
:- use_module(library(q/q_fs)).
:- use_module(library(q/q_io), []).
:- use_module(library(q/q_iri)).
:- use_module(library(q/q_term)).
:- use_module(library(q/qb)).
:- use_module(library(rdf/rdf__io)).
:- use_module(library(settings)).
:- use_module(library(yall)).

:- debug(stat).

:- meta_predicate
    call_todo(+, 2).

:- rlimit(nofile, _, 50000).

:- set_setting(iri:data_auth, 'lodlaundromat.org').
:- set_setting(iri:data_scheme, http).
:- q_init_ns.
:- set_setting(q_io:source_dir, '/scratch/wbeek/crawls/13/source/').
:- set_setting(q_io:store_dir, '/scratch/wbeek/crawls/13/store/').
:- set_setting(rocks_api:index_dir, '/scratch/wbeek/crawls/13/index/').





%! call_todo(+Local, :Goal_2) is det.

call_todo(Local, Goal_2) :-
  forall(
    todo_file(Local, DataFile, TodoFile),
    (   call(Goal_2, DataFile, TodoFile)
    ->  file_touch_ready(TodoFile)
    ;   msg_warning("Could not create ‘~a’.~n", [TodoFile])
    )
  ).



%! data_file_ready(-DataFile) is nondet.

data_file_ready(DataFile) :-
  q_file(data, hdt, DataFile),
  file_is_ready(DataFile).



%! gen_stat is det.

gen_stat :-
  call_on_rocks(stat, int, gen_stat).

gen_stat(Alias) :-
  forall(update_stat(Key, _, _, _), rocks_merge(Alias, Key, 0)),
  call_todo('stat.nt.gz', gen_stat_file(Alias)).

gen_stat_file(Alias, DataFile, StatFile) :-
  hdt_call_on_file(DataFile, gen_stat(Alias)),
  q_file_graph(DataFile, G),
  rdf_call_to_ntriples(StatFile, rdf_write_stat(Alias, G)),
  hdt_prepare_file(StatFile, HdtStatFile),
  file_touch_ready(HdtStatFile).

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

update_stat(literal, _, _, O) :-
  q_is_literal(O).



%! gen_term_index is det.

gen_term_index :-
  call_on_rocks(term_hashes, set(atom), gen_term_index).

gen_term_index(Alias) :-
  call_todo(Alias, gen_term_index_file(Alias)).

gen_term_index_file(Alias, DataFile, _) :-
  q_file_hash(DataFile, Hash),
  hdt_call_on_file(DataFile, gen_term_index_hdt(Alias, Hash)).

gen_term_index_hdt(Alias, Hash, Hdt) :-
  forall(
    hdt0(S, P, O, Hdt),
    maplist(add_term_index(Alias, Hash), [S,P,O])
  ).

add_term_index(Alias, Hash, Term) :-
  q_term_to_atom(Term, A),
  rocks_merge(Alias, A, [Hash]),
  flag(number_of_keys, NumKeys, NumKeys + 1),
  (NumKeys mod 100000 =:= 0 -> format(user_output, "~D~n", [NumKeys]) ; true).



%! number_of_triples is det.

number_of_triples :-
  aggregate_all(
    sum(NumTriples),
    hdt(meta, _, nsdef:numberOfTriples, NumTriples^^xsd:nonNegativeInteger),
    NumTriples
  ),
  format(user_output, "~D~n", [NumTriples]).



%! print_todo_process(+Local) is det.

print_todo_process(Local) :-
  aggregate_all(count, todo_file(Local, _, _), NumTodos),
  aggregate_all(count, data_file_ready(_), NumAll),
  NumDone is NumAll - NumTodos,
  msg_notification("(~D/~D)~n", [NumDone,NumAll]).


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



%! term_index_size(-NumTerms) is det.

term_index_size(NumTerms) :-
  call_on_rocks(term_hashes, set(atom), term_index_size(NumTerms)).

term_index_size(NumTerms, Alias) :-
  aggregate_all(count, rocks_key(Alias, _), NumTerms).



%! todo_file(+Local, -DataFile, -TodoFile) is nondet.
%
% Enumerates Local files that are not ready, but whose corresponding
% data file is.

todo_file(Local, DataFile, TodoFile) :-
  data_file_ready(DataFile),
  file_directory_name(DataFile, Dir),
  directory_file_path(Dir, Local, TodoFile),
  \+ file_is_ready(TodoFile).
