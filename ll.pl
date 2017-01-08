:- module(
  ll,
  [
    add_wm/0,
    add_wms/1,   % +NumWMs
    ll_rm/0,
    ll_stack/0,
    ll_start/0,
    ll_status/0,
    ll_stop/0,
    number_of_wms/1 % -NumWMs
  ]
).

/** <module> LOD Laundromat

@author Wouter Beek
@version 2017/01
*/

:- use_module(library(call_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(lists)).
:- use_module(library(os/file_ext)).
:- use_module(library(pair_ext)).
:- use_module(library(sparql/sparql_query_client)).

:- use_module(seedlist).
:- use_module(wm).

:- at_halt(ll_stop).





%! add_wm is det.
%
% Add one more washing machine to the LOD Laundromat.

add_wm :-
  max_wm(N0),
  N is N0 + 1,
  atomic_list_concat([m,N], :, Alias),
  thread_create(wm_run, _, [alias(Alias),detached(false)]).



%! add_wms(+NumWMs) is det.
%
% Add the given number of washing machines to LOD Laundromat.


add_wms(0) :- !.
add_wms(N1) :-
  N2 is N1 - 1,
  add_wm,
  add_wms(N2).



%! buggy_hash(+Hash) is semidet.
%! buggy_hash(-Hash) is nondet.
%
% A buggy hash is one that is ‘started’ according to the seedlist, but
% that is not currently being processed by a washing machine.

buggy_hash(Hash) :-
  seed_by_status(started, Seed),
  dict_tag(Seed, Hash),
  \+ wm_thread_postfix(a, Hash).



%! ll_rm is det.
%
% Remove everything that was every cleaned by the LOD Laundromat.
% After all the data, metadata and indices are removed, the seedlist
% is re-initialized to its original contents.

ll_rm :-
  ll_stop,
  ll_rm_store,
  ll_rm_index,
  ll_rm_seedlist.



%! ll_rm_index is det.
%
% Remove the LOD Laundromat web site index.

ll_rm_index :-
  rocks_rm(llw),
  rocks_open(llw, int),
  rocks_put(llw, number_of_documents, 0),
  rocks_put(llw, number_of_tuples, 0).



%! ll_rm_seedlist is det.
%
% Remove and re-populate the LOD Laundromat seedlist.

ll_rm_seedlist :-
  retry0(es_rm([ll])),
  init_old_seedlist.

init_old_seedlist :-
  % Extract all seeds from the old LOD Laundromat server and store
  % them locally as a seedlist.  This is intended for debugging
  % purposes only.
  Q = '\c
PREFIX llo: <http://lodlaundromat.org/ontology/>\n\c
SELECT ?url\n\c
WHERE {\n\c
  ?doc llo:url ?url\n\c
}\n',
  forall(
    sparql_select('http://sparql.backend.lodlaundromat.org', Q, Rows),
    forall(member([From], Rows), add_seed(From))
  ).




%! ll_rm_store is det.
%
% Remove all LOD Laundromat data and metadata file. 

ll_rm_store :-
  setting(q_io:store_dir, Dir),
  delete_directory_and_contents_msg(Dir).



%! ll_stack is det.

ll_stack :-
  findall(
    Global-[Alias,Global],
    (
      thread_property(Id, alias(Alias)),
      thread_property(Id, status(running)),
      thread_statistics(Id, global, Global)
    ),
    Pairs
  ),
  asc_pairs_values(Pairs, Rows),
  msg_notification("LSIYCKUTTMDHE (Let’s See If You Can Keep Up This Time My Dear Hardware Engineers):~n"),
  print_table([head(["Alias","Global stack"])|Rows]).



%! ll_start is det.
%
% Start by cleaning the rubbish from last time.

ll_start :-
  forall(
    buggy_hash(Hash),
    reset_seed(Hash)
  ).



%! ll_status is det.

ll_status :-
  findall(
    Global-[Alias,Global,Hash],
    (
      wm_thread_alias(a, Alias),
      atomic_list_concat([a,Hash], :, Alias),
      thread_statistics(Alias, global, Global)
    ),
    Pairs
  ),
  desc_pairs_values(Pairs, Rows),
  print_table([head(["Thread","Global stack","Seed"])|Rows], [indexed(true)]),
  aggregate_all(count, wm_thread_alias(m, _), NumWMs),
  number_of_seeds_by_status(added, NumSeeds),
  msg_notification(
    "~D washing machines are cleaning ~D seedpoints.~n",
    [NumWMs,NumSeeds]
  ),
  aggregate_all(count, buggy_hash(_), NumBuggyHashes),
  (   NumBuggyHashes =:= 0
  ->  msg_success("No buggy seedpoints.~n")
  ;   msg_warning("~D buggy seedpoints.~n", [NumBuggyHashes])
  ).



%! ll_stop is det.
%
% Stop all processes for the currently running LOD Laundromat.

ll_stop :-
  forall(wm_thread_alias(m, Alias), thread_signal(Alias, abort)).



%! max_wm(-N) is det.
%
% The highest washing machine identifier.

max_wm(N) :-
  aggregate_all(
    max(N),
    (
      wm_thread_postfix(m, N0),
      atom_number(N0, N)
    ),
    N
  ), !.
max_wm(0).



%! number_of_wms(-NumWMs) is det.

number_of_wms(NumWMs) :-
  aggregate_all(count, wm_thread_alias(m, _), NumWMs).



%! reset_and_clean_hash(+Hash) is det.

reset_and_clean_hash(Hash) :-
  % Do not reset seedpoints that are currently being processed by a
  % washing machine.
  \+ wm_thread_postfix(a, Hash),
  reset_seed(Hash),
  clean_hash(Hash).
