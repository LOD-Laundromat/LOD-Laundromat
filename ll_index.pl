:- module(
  ll_index,
  [
    gen_stat_index/0,
    gen_term_index/0,
    print_todo_progress/1 % +Local
  ]
).

/** <module> LOD Laundromat 2 index

@author Wouter Beek
@version 2017/01
*/

:- use_module(library(debug)).
:- use_module(library(file_ext)).
:- use_module(library(hdt/hdt_api)).
:- use_module(library(lists)).
:- use_module(library(print_ext)).
:- use_module(library(q/q_fs)).
:- use_module(library(rdf/rdf__io)).
:- use_module(library(rdf/rdf_api)).
:- use_module(library(service/rocks_api)).

:- debug(stat).

:- meta_predicate
    call_todo(+, 2).





% GENERATORS %

%! gen_stat_index is det.

gen_stat_index :-
  call_to_rocks(stat, int, gen_stat_index).

gen_stat_index(Alias) :-
  forall(
    update_stat(Key, _, _, _),
    rocks_merge(Alias, Key, 0)
  ),
  call_todo('stat.nt.gz', gen_stat_index_file(Alias)).

gen_stat_index_file(Alias, DataFile, StatFile) :-
  hdt_call_on_file(DataFile, gen_stat_index(Alias)),
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

gen_stat_index(Alias, Hdt) :-
  q(hdt0, S, P, O, Hdt),
  forall(
    update_stat(Key, S, P, O),
    rocks_merge(Alias, Key, 1)
  ),
  fail.
gen_stat_index(_, _).

update_stat(literal, _, _, O) :-
  rdf_is_literal(O).



%! gen_term_index is det.

gen_term_index :-
  call_to_rocks(term_hashes, set(atom), gen_term_index).

gen_term_index(Alias) :-
  call_todo(Alias, gen_term_index_file(Alias)).

gen_term_index_file(Alias, DataFile, _) :-
  q_file_hash(DataFile, Hash),
  hdt_call_on_file(DataFile, gen_term_index_hdt(500000, Alias, Hash)),
  format(user_output, "Processed ~a~n", [Hash]).

gen_term_index_hdt(BatchSize, Alias, Hash, Hdt) :-
  forall(
    findnsols(
      BatchSize,
      Command,
      gen_term_index_command(Hash, Command, Hdt),
      Commands
    ),
    (
      rocks_batch(Alias, Commands),
      flag(number_of_keys, NumCommands, NumCommands + BatchSize),
      format(user_output, "Executed ~D commands.~n", [NumCommands])
    )
  ).

gen_term_index_command(Hash, Command, Hdt) :-
  hdt0(S, P, O, Hdt),
  member(Term, [S,P,O]),
  rdf_term_to_atom(Term, A),
  Command = merge(A, [Hash]).





% API %

%! print_todo_progress(+Local) is det.

print_todo_progress(Local) :-
  aggregate_all(count, todo_file(Local, _, _), NumTodos),
  aggregate_all(count, data_file_ready(_), NumAll),
  NumDone is NumAll - NumTodos,
  msg_notification("(~D/~D)~n", [NumDone,NumAll]).





% HELPERS %

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



%! todo_file(+Local, -DataFile, -TodoFile) is nondet.
%
% Enumerates Local files that are not ready, but whose corresponding
% data file is.

todo_file(Local, DataFile, TodoFile) :-
  data_file_ready(DataFile),
  file_directory_name(DataFile, Dir),
  directory_file_path(Dir, Local, TodoFile),
  \+ file_is_ready(TodoFile).
