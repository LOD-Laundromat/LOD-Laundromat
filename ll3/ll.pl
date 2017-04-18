:- module(
  ll,
  [
    ll_add_wm/0,
    ll_add_wms/1,              % +NumWMs
    ll_clean_hash/1,           % +Hash
    ll_clean_one_seed/0,
    ll_clean_one_seed/1,       % -Seed
    ll_reset_all/0,
    ll_reset_and_clean_hash/1, % +Hash
    ll_stack/0,
    ll_status/0,
    ll_stop/0,
    ll_thread_alias/2,         % +Prefix, -Alias
    number_of_wms/1            % -NumWMs
  ]
).

/** <module> LOD Laundromat

The following debug flags are defined:
  * ll(finish)
  * ll(idle)

@author Wouter Beek
@version 2017/01-2017/02
*/

:- use_module(library(call_ext)).
:- use_module(library(debug)).
:- use_module(library(dict_ext)).
:- use_module(library(file_ext)).
:- use_module(library(lists)).
:- use_module(library(pair_ext)).
:- use_module(library(print_ext)).
:- use_module(library(q/q_fs)).
:- use_module(library(random)).
:- use_module(library(service/es_api)).
:- use_module(library(service/rocks_api)).
:- use_module(library(sparql/sparql_client2)).
:- use_module(library(thread_ext)).
:- use_module(library(wm)).

:- use_module(seedlist).

:- at_halt((ll_stop, rocks_close(llw))).

:- initialization((ll_init, init_llw_index)).

ll_init :-
  forall(
    buggy_hash(Hash),
    ll_reset_hash(Hash)
  ).

init_llw_index :-
  rocks_open(llw, int, write),
  rocks_merge(llw, number_of_documents, 0),
  rocks_merge(llw, number_of_tuples, 0).





%! buggy_hash(-Hash) is nondet.
%
% Enumerates Hashes for archive, entry and data directories that do
% not contain a single ‘.ready’ file.  These are certainly buggy.

buggy_hash(Hash) :-
  q_dir(Dir),
  forall(
    directory_file(Dir, File),
    \+ file_name_extension(_, ready, File)
  ),
  q_dir_hash(Dir, Hash).



%! ll_add_wm is det.
%
% Add one more washing machine to the LOD Laundromat.

ll_add_wm :-
  max_wm(N0),
  N is N0 + 1,
  atomic_list_concat([m,N], :, Alias),
  thread_create(ll_loop(_{idle: 0}), _, [alias(Alias),detached(false)]).



%! ll_add_wms(+NumWMs) is det.
%
% Add the given number of washing machines to LOD Laundromat.


ll_add_wms(0) :- !.
ll_add_wms(N1) :-
  N2 is N1 - 1,
  ll_add_wm,
  ll_add_wms(N2).



%! ll_clean_hash(+Hash) is det.
%
% Clean a specific seed from the seedlist.  Does not re-clean
% documents.
%
% @throws existence_error If the seed is not in the seedlist.

ll_clean_hash(Hash) :-
  q_file_hash(File, data, [nt,gz], Hash),
  (   file_is_ready(File)
  ->  msg_notification("Already cleaned ~a", [Hash])
  ;   seed_by_hash(Hash, Seed),
      ll_clean_seed(Seed)
  ).



%! ll_clean_seed(+Seed) is det.

ll_clean_seed(Seed) :-
  atom_string(Uri, Seed.from),
  dict_tag(Seed, Hash),
  currently_debugging(Hash),
  begin_seed_hash(Hash),
  wm_clean_inner(Uri, Hash),
  % @tbd rocks_merge(llw, number_of_tuples, NumTuples),
  % @tbd rocks_merge(llw, number_of_documents, 1),
  end_seed_hash(Hash),
  debug(ll(finish), "Finished ~a", [Hash]).



%! ll_clean_one_seed is det.
%! ll_clean_one_seed(-Seed) is det.
%
% Clean one, arbitrarily chosen, seedpoint.

ll_clean_one_seed :-
  ll_clean_one_seed(_).


ll_clean_one_seed(Seed2) :-
  once(seeds_by_status(added, Result)),
  % @note By taking a random member from th result set we have less
  %       collisions than we would have had if we had used
  %       seed_by_status/2.
  Results = Result.results,
  random_member(Seed1, Results),
  % @note There may still be concurrency problems in the seedlist.
  catch(ll_clean_seed(Seed1), E, true),
  (var(E) -> Seed2 = Seed1 ; ll_clean_one_seed(Seed2)).



%! ll_loop(+State) is det.

ll_loop(State) :-
  ll_clean_one_seed, !,
  ll_loop(State).
ll_loop(State) :-
  sleep(1),
  dict_inc(idle, State, NumWMs),
  thread_name(Alias),
  debug(ll(idle), "💤 thread ~w (machine ~D)", [Alias,NumWMs]),
  ll_loop(State).



%! ll_reset_and_clean_hash(+Hash) is det.

ll_reset_and_clean_hash(Hash) :-
  % Do not reset seedpoints that are currently being processed by a
  % washing machine.
  \+ (
    ll_thread_alias(a, Alias),
    atomic_list_concat([a,Hash], :, Alias)
  ),
  ll_reset_hash(Hash),
  ll_clean_hash(Hash).



%! ll_reset_hash(+Hash) is det.
%
% Resets _any_ hash, i.e., archive/seed, entry, and data.

ll_reset_hash(Hash) :-
  wm_reset_hash(Hash),
  % If a seed, reset it in the seedlist.
  (is_seed_hash(Hash) -> reset_seed(Hash) ; true).



%! ll_reset_all is det.
%
% Remove everything that was every cleaned by the LOD Laundromat.
% After all the data, metadata and indices are removed, the seedlist
% is re-initialized to its original contents.

ll_reset_all :-
  ll_stop,
  ll_reset_store,
  ll_reset_index,
  ll_reset_seedlist.



%! ll_reset_index is det.
%
% Remove and re-initialize the LOD Laundromat web site index.

ll_reset_index :-
  rocks_rm(llw),
  rocks_open(llw, int, write),
  rocks_put(llw, number_of_documents, 0),
  rocks_put(llw, number_of_tuples, 0).



%! ll_reset_seedlist is det.
%
% Remove and re-populate the LOD Laundromat seedlist.

ll_reset_seedlist :-
  % Ignore covers the case in which the ElasticSearch ‘ll’ index is
  % already gone, i.e., 404.
  ignore(es_delete([ll])),
  % Extract all seeds from the old LOD Laundromat server and store
  % them locally as a seedlist.  This is intended for debugging
  % purposes only.
  Q = "\c
PREFIX llo: <http://lodlaundromat.org/ontology/>\n\c
SELECT ?url\n\c
WHERE {\n\c
  ?doc llo:url ?url\n\c
}\n",
  forall(
    sparql_get(
      'http://sparql.backend.lodlaundromat.org',
      string(Q),
      media(application/'sparql-results+json',[]),
      Result
    ),
    (
      Result = select(_VarNames,Rows),
      forall(
        member([Uri], Rows),
        add_seed(Uri)
      )
    )
  ).



%! ll_reset_store is det.
%
% Remove all LOD Laundromat data and metadata file.

ll_reset_store :-
  q_store_dir(Dir),
  delete_directory_and_contents_silent(Dir).



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



%! ll_status is det.

ll_status :-
  findall(
    Global-[Alias,Global,Hash],
    (
      ll_thread_alias(a, Alias),
      atomic_list_concat([a,Hash], :, Alias),
      thread_statistics(Alias, global, Global)
    ),
    Pairs
  ),
  desc_pairs_values(Pairs, Rows),
  print_table([head(["Thread","Global stack","Seed"])|Rows], [indexed(true)]),
  aggregate_all(count, ll_thread_alias(m, _), NumWMs),
  number_of_seeds_by_status(added, NumSeeds),
  msg_notification(
    "~D washing machines are cleaning ~D seedpoints.~n",
    [NumWMs,NumSeeds]
  ),
  rocks_get(llw, number_of_documents, NumDocs),
  rocks_get(llw, number_of_tuples, NumTuples),
  msg_notification(
    "Cleaned ~D documents containing ~D statements.~n",
    [NumDocs,NumTuples]
  ).



%! ll_stop is det.
%
% Stop all processes for the currently running LOD Laundromat.

ll_stop :-
  forall(
    ll_thread_alias(m, Alias),
    thread_signal(Alias, abort)
  ).



%! ll_thread_alias(+Prefix:oneof([a,e,m]), -Alias) is nondet.
%
% @arg Prefix Either `a` (archive), `e` (entry) or `m` (machine).

ll_thread_alias(Prefix, Alias) :-
  thread_property(Id, status(running)),
  thread_property(Id, alias(Alias)),
  atomic_list_concat([Prefix|_], :, Alias).



%! max_wm(-N) is det.
%
% The highest washing machine identifier.

max_wm(N) :-
  aggregate_all(
    max(N),
    (
      ll_thread_alias(m, Alias),
      atomic_list_concat([m,N0], :, Alias),
      atom_number(N0, N)
    ),
    N
  ), !.
max_wm(0).



%! number_of_wms(-NumWMs) is det.

number_of_wms(NumWMs) :-
  aggregate_all(count, ll_thread_alias(m, _), NumWMs).





% DEBUG %

%! currently_debugging(+Hash) is det.

currently_debugging(Hash) :-
  deb0(Hash), !,
  ansi_format(user_output, [bold], "~a", [Hash]),
  gtrace. %DEB
currently_debugging(_).

:- dynamic
    deb0/1.

%%%%deb0('6de4d9c7e59ab7aae94f059133620827').
