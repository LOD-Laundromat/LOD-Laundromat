:- module(
  wm,
  [
    add_wm/0,
    add_wms/1,              % +NumWms
    buggy_seedpoint/1,      % ?Hash
    current_wm/1,           % ?Alias
    number_of_seedpoints/1, % -NumSeeds
    number_of_wms/1,        % -NumWms
    single_wm/0,
    washing_seed/1,         % ?Hash
    wm_reset/0,
    wm_reset_and_clean/1,   % +Hash
    wm_restore/0,
    wm_status/0,
    wm_table/0
  ]
).

/* <module> LOD Laundromat: Washing machine

@author Wouter Beek
@version 2016/01-2016/05, 2016/08, 2016/11
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
:- use_module(library(hash_ext)).
:- use_module(library(http/json)).
:- use_module(library(jsonld/jsonld_metadata)).
:- use_module(library(jsonld/jsonld_read)).
:- use_module(library(os/file_ext)).
:- use_module(library(os/process_ext)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pair_ext)).
:- use_module(library(pl_term)).
:- use_module(library(print_ext)).
:- use_module(library(prolog_stack)).
:- use_module(library(q/q_print)).
:- use_module(library(rdf/rdf__io)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(service/es_api)).
:- use_module(library(string_ext)).
:- use_module(library(thread)).
:- use_module(library(zlib)).

:- use_module(lclean).
:- use_module(seedlist).

prolog_stack:stack_guard('C').
prolog_stack:stack_guard(none).





%! add_wm is det.
%! add_wms(+NumWms) is det.
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
%
% @tbd

buggy_seedpoint(Hash) :-
  seed_by_status(started, Hash),
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



%! number_of_seedpoints(-NumSeeds) is det.
%
% The number of pending seedpoints.

number_of_seedpoints(NumSeeds) :-
  aggregate_all(count, seed_by_status(added, _), NumSeeds).



%! number_of_wms(-NumWms) is det.

number_of_wms(NumWms) :-
  aggregate_all(count, current_wm(_), NumWms).



%! single_wm is det.

single_wm :-
  start_wm0.



%! washing_seed(+Hash) is semidet.
%! washing_seed(-Hash) is nondet.

washing_seed(Hash) :-
  get_thread_seed(_, Hash).



%! wm_reset is det.
%
% Reset the LOD Laundromat, removing all data files and resetting the
% seedlist.

wm_reset :-
  setting(q_io:store_dir, Dir1),
  delete_directory_and_contents_msg(Dir1),
  setting('conf_LOD-Laundromat-Web':llw_index, Dir2),
  delete_directory_and_contents_msg(Dir2),
  es_rm_pp([llw]).



%! wm_reset_and_clean(+Hash) is det.

wm_reset_and_clean(Hash) :-
  % Do not reset seedpoints that are currently being processed by a
  % washing machine.
  \+ washing_seed(Hash),
  reset_and_clean(Hash).



%! wm_restore is det.
%
% @tbd

wm_restore :-
  findall(Hash, seed_by_status(started, Hash), Hashs),
  concurrent_maplist(wm_reset_and_clean, Hashs).



%! wm_status is det.

wm_status :-
  number_of_wms(NumWms),
  number_of_seedpoints(NumSeeds),
  msg_notification(
    "~D washing machines are cleaning ~D seedpoints.~n",
    [NumWms,NumSeeds]
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
  number_of_wms(NumWms),
  debug(wm(thread), "~D washing machines are currently active.", [NumWms]),
  wm0(State).
wm0(State) :-
  sleep(1),
  dict_inc(idle, State, NumWms),
  thread_name(Alias),
  debug(wm(idle), "ZZZZ Thread ~w idle ~D sec.", [Alias,NumWms]),
  wm0(State).
