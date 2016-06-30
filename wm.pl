:- module(
  wm,
  [
    add_wm/0,
    add_wms/1,              % +N
    buggy_seedpoint/1,      % ?Hash
    current_wm/1,           % ?Alias
    number_of_seedpoints/1, % -N
    number_of_wms/1,        % -N
    single_wm/0,
    washing_seed/1,         % ?Hash
    wm_reset/1,             % +Hash
    wm_reset_and_clean/1,   % +Hash
    wm_restore/0,
    wm_status/0,
    wm_table/0
  ]
).

/* <module> LOD Laundromat: Washing machine

@author Wouter Beek
@version 2016/01-2016/05
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(dcg/dcg_ext)).
:- use_module(library(dcg/dcg_table)).
:- use_module(library(dict_ext)).
:- use_module(library(error)).
:- use_module(library(filesex)).
:- use_module(library(gen/gen_ntuples)).
:- use_module(library(hash_ext)).
:- use_module(library(http/json)).
:- use_module(library(jsonld/jsonld_metadata)).
:- use_module(library(jsonld/jsonld_read)).
:- use_module(library(lodcli/lodfs)).
:- use_module(library(lodcli/lodhdt)).
:- use_module(library(os/dir_ext)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/process_ext)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pair_ext)).
:- use_module(library(pl_term)).
:- use_module(library(print_ext)).
:- use_module(library(prolog_stack)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_ext)).
:- use_module(library(rdf/rdfio)).
:- use_module(library(semweb/rdf11)). % Operators.
:- use_module(library(string_ext)).
:- use_module(library(thread)).
:- use_module(library(z/z_print)).
:- use_module(library(zlib)).

:- use_module(lclean).
:- use_module(seedlist).

prolog_stack:stack_guard('C').
prolog_stack:stack_guard(none).





%! add_wm is det.
%! add_wms(+N) is det.
% Add a LOD Laundromat thread.

add_wm :-
  add_wms(1).


add_wms(0) :- !.
add_wms(M1) :-
  must_be(positive_integer, M1),
  max_wm(N1),
  N2 is N1 + 1,
  atom_concat(wm, N2, Alias),
  thread_create(start_wm0, _, [alias(Alias),detached(false)]),
  M2 is M1 - 1,
  add_wms(M2).



%! buggy_seedpoint(+Hash) is semidet.
%! buggy_seedpoint(-Hash) is nondet.

buggy_seedpoint(Hash) :-
  lunready_hash(Hash),
  \+ washing_seed(Hash).



%! current_wm(+Alias) is semidet.
%! current_wm(-Alias) is nondet.

current_wm(Alias) :-
  thread_property(Id, alias(Alias)),
  atom_prefix(Alias, wm),
  thread_property(Id, status(running)).



%! max_wm(-N) is det.

max_wm(N) :-
  aggregate_all(
    max(N),
    (
      current_wm(Alias),
      atom_concat(wm, N0, Alias),
      atom_number(N0, N)
    ),
    N
  ), !.
max_wm(0).



%! number_of_seedpoints(-N) is det.

number_of_seedpoints(N) :-
  aggregate_all(count, lunready_hash(_), N).



%! number_of_wms(-N) is det.

number_of_wms(N) :-
  aggregate_all(count, current_wm(_), N).



%! single_wm is det.

single_wm :-
  start_wm0.



%! washing_seed(+Hash) is semidet.
%! washing_seed(-Hash) is nondet.

washing_seed(Hash) :-
  thread_seed(_, Hash).



%! wm_reset is det.
% Reset the LOD Laundromat.
% This removes all data files and resets the seedlist.

wm_reset :-
  lroot(Root),
  forall(dir_file(Root, Subdir), delete_directory_and_contents(Subdir)),
  absolute_file_name(cpack('LOD-Laundromat'), Dir, [file_type(directory)]),
  run_process(git, ['checkout','seedlist.db'], [cwd(Dir)]),
  retractall(wm_hash0(_,_)).


%! wm_reset(+Hash) is det.

wm_reset(Hash) :-
  % Do not reset seedpoints that are currently being processed by
  % a washing machine.
  \+ washing_seed(Hash),
  reset(Hash).



%! wm_reset_and_clean(+Hash) is det.

wm_reset_and_clean(Hash) :-
  wm_reset(Hash),
  clean(Hash).



%! wm_restore is det.

wm_restore :-
  findall(Hash, lunready_hash(Hash), Hashs),
  concurrent_maplist(wm_reset_and_clean, Hashs).



%! wm_status is det.

wm_status :-
  number_of_wms(N1),
  number_of_seedpoints(N2),
  msg_notification(
    "~D washing machines are cleaning ~D seedpoints.~n",
    [N1,N2]
  ).



wm_table :-
  findall(
    Global-[Alias,Global,Hash],
    (
      current_wm(Alias),
      thread_statistics(Alias, global, Global),
      thread_seed(Alias, Hash)
    ),
    Pairs
  ),
  desc_pairs_values(Pairs, Rows),
  dcg_with_output_to(user_output, dcg_table(Rows)).





% HELPERS %

start_wm0 :-
  wm0(_{idle: 0}).


wm0(State) :-
  % Clean one -- arbitrarily chosen -- seed.
  clean, !,
  number_of_wms(N),
  debug(wm(thread), "~D washing machines are currently active.", [N]),
  wm0(State).
wm0(State) :-
  sleep(1),
  dict_inc(idle, State, N),
  thread_name(Alias),
  debug(wm(idle), "ZZZZ Thread ~w idle ~D sec.", [Alias,N]),
  wm0(State).
