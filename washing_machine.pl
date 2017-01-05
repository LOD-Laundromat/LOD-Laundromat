:- module(
  washing_machine,
  [
    add_wm/0,
    add_wms/1,                 % +NumWashingMachines
    buggy_seedpoint/1,         % ?Hash
    current_wm/1,              % ?Alias
    number_of_seedpoints/1,    % -NumSeeds
    number_of_wms/1,           % -NumWachingMachines
    single_wm/0,
    washing_hash/1,            % ?Hash
    wm_clean/0,
    wm_reset/0,
    wm_reset_and_clean_hash/1, % +Hash
    wm_restore/0,
    wm_status/0,
    wm_table/0
  ]
).

/* <module> LOD Laundromat: Washing machine API

@author Wouter Beek
@version 2016/01-2016/05, 2016/08, 2016/10, 2016/12-2017/01
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(call_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(dcg/dcg_ext)).
:- use_module(library(dcg/dcg_table)).
:- use_module(library(dict_ext)).
:- use_module(library(error)).
:- use_module(library(filesex)).
:- use_module(library(hash_ext)).
:- use_module(library(http/json)).
:- use_module(library(jsonld/jsonld_metadata)).
:- use_module(library(jsonld/jsonld_read)).
:- use_module(library(os/file_ext)).
:- use_module(library(os/process_ext)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pair_ext)).
:- use_module(library(pl_ext)).
:- use_module(library(print_ext)).
:- use_module(library(prolog_stack)).
:- use_module(library(q/q_print)).
:- use_module(library(rdf/rdf__io)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(service/es_api)).
:- use_module(library(service/rocks_ext)).
:- use_module(library(settings)).
:- use_module(library(string_ext)).
:- use_module(library(thread)).
:- use_module(library(zlib)).

:- use_module(ll(lclean)).
:- use_module(ll(api/seedlist)).

prolog_stack:stack_guard('C').
prolog_stack:stack_guard(none).





%! add_wm is det.
%! add_wms(+NumWashingMachines) is det.
%
% Add a LOD Laundromat thread.

add_wm :-
  max_wm(Id0),
  Id is Id0 + 1,
  atomic_list_concat([wm0,Id], :, Alias),
  thread_create(start_wm0, _, [alias(Alias),detached(false)]).


add_wms(0) :- !.
add_wms(N1) :-
  N2 is N1 - 1,
  add_wm,
  add_wms(N2).



%! buggy_seedpoint(+Hash) is semidet.
%! buggy_seedpoint(-Hash) is nondet.
%
% @tbd

buggy_seedpoint(Hash) :-
  seed_by_status(started, Hash),
  \+ washing_hash(Hash).



%! current_wm(+Alias) is semidet.
%! current_wm(-Alias) is nondet.

current_wm(Alias) :-
  thread_property(Id, alias(Alias)),
  atom_prefix(Alias, 'wm0:'),
  thread_property(Id, status(running)).



%! max_wm(-N) is det.

max_wm(N) :-
  aggregate_all(
    max(N),
    (
      current_wm(Alias),
      atomic_list_concat([wm0,N0], :, Alias),
      atom_number(N0, N)
    ),
    N
  ), !.
max_wm(0).



%! number_of_seedpoints(-NumSeeds) is det.
%
% The number of pending seedpoints.

number_of_seedpoints(NumSeeds) :-
  aggregate_all(count, seed_by_status(added, _), NumSeeds).



%! number_of_wms(-NumWashingMachines) is det.

number_of_wms(NumWashingMachines) :-
  aggregate_all(count, current_wm(_), NumWashingMachines).



%! single_wm is det.

single_wm :-
  start_wm0.



%! washing_hash(+Hash) is semidet.
%! washing_hash(-Hash) is nondet.

washing_hash(Hash) :-
  current_wm(Alias),
  atomic_list_concat([wm,Hash], :, Alias).



%! wm_clean is det.

wm_clean :-
  forall(
    (
      seed_by_status(started, Seed),
      dict_pairs(Seed, Hash, _)
    ),
    reset_seed(Hash)
  ).



%! wm_reset is det.
%
% Reset the LOD Laundromat, removing all data files and resetting the
% seedlist.

:- [script/init_old_seedlist].
wm_reset :-
  % Reset the data store.
  setting(q_io:store_dir, Dir1),
  delete_directory_and_contents_msg(Dir1),
  % Reset the web site index.
  rocks_rm(llw),
  rocks_open(llw, int),
  rocks_put(llw, number_of_documents, 0),
  rocks_put(llw, number_of_tuples, 0),
  % Reset the seedlist.
  retry0(es_rm([ll])),
  init_old_seedlist.



%! wm_reset_and_clean_hash(+Hash) is det.

wm_reset_and_clean_hash(Hash) :-
  % Do not reset seedpoints that are currently being processed by a
  % washing machine.
  \+ washing_hash(Hash),
  reset_and_clean_hash(Hash).



%! wm_restore is det.
%
% @tbd

wm_restore :-
  findall(Hash, seed_by_status(started, Hash), Hashs),
  concurrent_maplist(wm_reset_and_clean_hash, Hashs).



%! wm_status is det.

wm_status :-
  number_of_wms(NumWashingMachines),
  number_of_seedpoints(NumSeeds),
  msg_notification(
    "~D washing machines are cleaning ~D seedpoints.~n",
    [NumWashingMachines,NumSeeds]
  ).



wm_table :-
  findall(
    Global-[Alias,Global,Hash],
    (
      current_wm(Alias),
      thread_statistics(Alias, global, Global),
      get_thread_seed(Alias, Hash)
    ),
    Pairs
  ),
  desc_pairs_values(Pairs, Rows),
  print_table(Rows).





% HELPERS %

start_wm0 :-
  wm0(_{idle: 0}).


wm0(State) :-
  % Clean one, arbitrarily chosen, seed.
  clean, !,
  number_of_wms(NumWashingMachines),
  debug(
    wm(thread),
    "~D washing machines are currently active.",
    [NumWashingMachines]
  ),
  wm0(State).
wm0(State) :-
  sleep(1),
  dict_inc(idle, State, NumWashingMachines),
  thread_name(Alias),
  debug(
    wm(idle),
    "ZZZ Thread ~w idle ~D sec.",
    [Alias,NumWashingMachines]
  ),
  wm0(State).
