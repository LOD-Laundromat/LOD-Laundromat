:- module(
  ll,
  [
    add_wm/0,
    add_wms/1,                   % +NumWMs
    clean_file/1,                % +File
    clean_hash/1,                % +Hash
    ll_rm/0,
    ll_stack/0,
    ll_start/0,
    ll_status/0,
    ll_stop/0,
    number_of_wms/1,             % -NumWMs
    reset_and_clean_hash/1,      % +Hash
    reset_archive_and_entries/1, % +Hash
    wm_run/0,
    wm_thread_alias/2,           % +Prefix, -Alias
    wm_thread_postfix/2          % +Prefix, -Hash
  ]
).

:- use_module(library(q/q_iri)).
:- q_init_ns.

/** <module> LOD Laundromat

@author Wouter Beek
@version 2017/01
*/

:- use_module(library(call_ext)).
:- use_module(library(debug)).
:- use_module(library(dict_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(iri/iri_ext)).
:- use_module(library(lists)).
:- use_module(library(os/archive_ext)).
:- use_module(library(os/file_ext)).
:- use_module(library(os/io)).
:- use_module(library(pair_ext)).
:- use_module(library(q/q_fs)).
:- use_module(library(q/q_io), []).
:- use_module(library(service/rocks_api)).
:- use_module(library(settings)).
:- use_module(library(sparql/sparql_query_client)).

:- use_module(seedlist).
:- use_module(wm).

:- at_halt((ll_stop, rocks_close(llw))).

%:- debug(es_api).
%:- debug(http(send_request)).
%:- debug(http(reply)).
%:- debug(http_io).
%:- debug(io(close)).
%:- debug(io(open)).
%:- debug(seedlist(_)).
% @tbd Document that ‘wm(idle)’ overrules ‘wm(_)’.
:- debug(wm(_)).

:- initialization((ll_start, init_llw_index)).

:- set_setting(iri:data_auth, 'lodlaundromat.org').
:- set_setting(iri:data_scheme, http).
:- set_setting(q_io:store_dir,  '/scratch/wbeek/crawls/13/store/' ).
:- set_setting(rocks_api:index_dir, '/scratch/wbeek/crawls/13/index/').

init_llw_index :-
  rocks_open(llw, int),
  rocks_merge(llw, number_of_documents, 0),
  rocks_merge(llw, number_of_tuples, 0).





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



%! clean_hash(+Hash) is det.
%
% Clean a specific seed from the seedlist.  Does not re-clean
% documents.
%
% @throws existence_error If the seed is not in the seedlist.

clean_hash(Hash) :-
  q_file_hash(File, data, ntriples, Hash),
  (   file_is_ready(File)
  ->  msg_notification("Already cleaned ~a", [Hash])
  ;   seed_by_hash(Hash, Seed),
      clean_seed(Seed)
  ).



%! clean_seed(+Seed) is det.

clean_seed(Seed) :-
  atom_string(From, Seed.from),
  dict_tag(Seed, Hash),
  currently_debugging(Hash),
  begin_seed_hash(Hash),
  clean_inner(From, Hash),
  end_seed_hash(Hash),
  debug(wm(finish), "Finished ~a", [Hash]).



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
    reset_hash(Hash)
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
  reset_hash(Hash),
  clean_hash(Hash).



%! reset_archive_and_entries(+Hash) is det.

reset_archive_and_entries(Hash) :-
  seed_by_hash(Hash, Seed),
  atom_string(From, Seed.from),
  % Remove the directories for all seed entries, if any.
  ignore(
    forall(
      call_on_stream(uri(From), reset_seed_entry(From)),
      true
    )
  ),
  % Remove the directory for the seed,
  q_dir_hash(Dir, Hash),
  with_mutex(ll, delete_directory_and_contents_msg(Dir)).

reset_seed_entry(From, _, InPath, InPath) :-
  path_entry_name(InPath, EntryName),
  md5(From-EntryName, EntryHash),
  q_dir_hash(EntryDir, EntryHash),
  with_mutex(ll, delete_directory_and_contents_msg(EntryDir)).



%! reset_hash(+Hash) is det.
%
% Resets _any_ hash, i.e., archive/seed, entry, and data.

reset_hash(Hash) :-
  wm_reset_hash(Hash),
  % If a seed, reset it in the seedlist.
  (is_seed_hash(Hash) -> reset_seed(Hash) ; true).



%! wm_run is det.

wm_run :-
  wm_loop(_{idle: 0}).


wm_loop(State) :-
  % Clean one, arbitrarily chosen, seed.
  once(seed_by_status(added, Seed)),
  clean_seed(Seed), !,
  wm_loop(State).
wm_loop(State) :-
  sleep(1),
  dict_inc(idle, State, NumWMs),
  thread_name(Alias),
  debug(wm(idle), "ZZZ Thread ~w idle ~D sec.", [Alias,NumWMs]),
  wm_loop(State).



%! wm_thread_alias(+Prefix:oneof([a,e,m]), -Hash) is nondet.
%
% @arg Prefix Either `a` (archive), `e` (entry) or `m` (machine).

wm_thread_alias(Prefix, Alias) :-
  thread_property(Id, alias(Alias)),
  atomic_list_concat([Prefix|_], :, Alias),
  thread_property(Id, status(running)).



%! wm_thread_postfix(+Prefix:oneof([a,e,m]), -Postfix) is nondet.
%
% @arg Prefix Either `a` (archive), `e` (entry) or `m` (machine).

wm_thread_postfix(Prefix, Postfix) :-
  thread_property(Id, alias(Alias)),
  atomic_list_concat([Prefix,Postfix], :, Alias),
  thread_property(Id, status(running)).





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
