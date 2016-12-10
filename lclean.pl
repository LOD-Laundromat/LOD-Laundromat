:- module(
  lclean,
  [
    clean/0,
    clean/1,           % +Hash
    clean_seed/1,      % +Seed
    get_thread_seed/2, % +Alias, -Hash
    reset_and_clean/1  % +Hash
  ]
).

/** <module> LOD Laundromat: Data cleaning

@author Wouter Beek
@version 2016/03-2016/05, 2016/08-2016/10
*/

:- use_module(library(apply)).
:- use_module(library(debug_ext)).
:- use_module(library(default)).
:- use_module(library(dict_ext)).
:- use_module(library(filesex)).
:- use_module(library(gen/gen_ntuples)).
:- use_module(library(hdt/hdt_ext)).
:- use_module(library(lists)).
:- use_module(library(option)).
:- use_module(library(os/compress_ext)).
:- use_module(library(os/file_ext)).
:- use_module(library(os/gnu_sort)).
:- use_module(library(os/io)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pl_ext)).
:- use_module(library(print_ext)).
:- use_module(library(q/qb)).
:- use_module(library(q/q_fs)).
:- use_module(library(rdf/rdf__io)).
:- use_module(library(rdf/rdf_error)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(semweb/rdf11_containers)).
:- use_module(library(service/es_api)).
:- use_module(library(service/rocksdb_ext)).

:- use_module(ll(api/seedlist)).

:- meta_predicate
    rdf_store_messages(+, +, 0).

:- rdf_meta
   rdf_store_messages(+, r, :).

:- dynamic
    currently_debugging0/1,
    thread_seed/2.

:- multifile
    currently_debugging0/1.





%! clean is det.
%
% Clean an arbitrarily chosen seedpoint.

clean :-
  once(seed_by_status(added, Seed)),
  clean_seed(Seed).


%! clean(+Hash) is det.
%
% Clean a specific seed from the seedlist.  Does not re-clean
% documents.
%
% @throws existence_error If the seed is not in the seedlist.

clean(Hash) :-
  q_file_hash(File, data, ntriples, Hash),
  q_file_is_ready(File), !,
  msg_notification("Already cleaned ~a", [Hash]).
clean(Hash) :-
  seed_by_hash(Hash, Seed),
  clean_seed(Seed).



%! clean_seed(+Seed) is det.

clean_seed(Seed) :-
  begin_seed(Seed),
  thread_name(Alias),
  dict_tag(Seed, Hash),
  set_thread_seed(Alias, Hash),
  debug(lclean, "Thread ~a is cleaning ~a ~a", [Alias,Hash,Seed.from]),
  clean_seed(Seed, Hash),
  debug(lclean, "Thread ~a has cleaned ~a ~a", [Alias,Hash,Seed.from]),
  end_seed(Seed).

clean_seed(Seed, Hash) :-
  currently_debugging(Hash),
  q_dir_hash(Dir, Hash),
  with_mutex(lclean,
    (   % This seed has already been processed.
        exists_directory(Dir)
    ->  Done = true
    ;   make_directory_path(Dir),
        Done = false
    )
  ),
  (Done == true -> true ; Done == false -> clean_seed_dir(Seed, Hash, Dir)).

clean_seed_dir(Seed, Hash, Dir) :-
  q_dir_file(Dir, meta, ntriples, MetaFile),
  q_dir_file(Dir, warn, ntriples, WarnFile),
  atom_string(From, Seed.from),
  call_to_streams(
    MetaFile,
    WarnFile,
    clean_streams0(Dir, Hash, From, CleanFile),
    [compression(false)],
    [compression(false),metadata(Path)]
  ),
  q_dir_file(Dir, data, ntriples, DataFile),
  create_file_link(DataFile, CleanFile),
  (var(Path) -> NumWarns = 0 ; Path = [Entry|_], NumWarns = Entry.line_count),
  hdt_prepare_file(MetaFile, HdtMetaFile),
  q_file_touch_ready(MetaFile),
  (   once(
        hdt_call_on_file(
          HdtMetaFile,
          hdt0(_, nsdef:numberOfWarnings, NumWarns^^xsd:nonNegativeInteger)
        )
      ),
      NumWarns >= 1
  ->  hdt_prepare_file(WarnFile)
  ;   true
  ),
  q_file_touch_ready(WarnFile),
  (   once(
        hdt_call_on_file(
          HdtMetaFile,
          hdt0(_, nsdef:numberOfTuples, NumTuples^^xsd:nonNegativeInteger)
        )
      ),
      NumTuples >= 1,
      rocks_merge(ll_index, number_of_tuples, NumTuples)
  ->  hdt_prepare_file(CleanFile, HdtCleanFile),
      q_dir_file(Dir, data, hdt, HdtDataFile),
      create_file_link(HdtDataFile, HdtCleanFile),
      q_file_touch_ready(HdtCleanFile),
      es_update_pp([ll,seedlist,Hash], _{doc: _{number_of_tuples: NumTuples}})
  ;   true
  ),
  (var(CleanFile) -> true ; q_file_touch_ready(CleanFile)),
  rocks_merge(ll_index, number_of_documents, 1).
  %%%%absolute_file_name('dirty.gz', DirtyTo, Opts),
  %%%%call_collect_messages(rdf_download_to_file(From, DirtyTo)).

clean_streams0(Dir, Hash, From, CleanFile, MetaOut, WarnOut) :-
  State = _{meta: MetaOut, warn: WarnOut, warns: 0},
  InOpts = [parse_headers(true),warn(WarnOut)],
  rdf_store_messages(
    State,
    Hash,
    rdf_clean0(Dir, From, CleanFile, InOpts, InPath, OutEntry)
  ),
  rdf_store_metadata(State, Hash, InPath, OutEntry, CleanFile).

rdf_clean0(Dir, From, CleanFile, InOpts1, InPath, OutEntry2) :-
  merge_options(InOpts1, [compression(false),metadata(InPath)], InOpts2),
  absolute_file_name(cleaning, TmpFile0, [access(write),relative_to(Dir)]),
  thread_file(TmpFile0, TmpFile),
  call_to_ntriples(
    TmpFile,
    dummy1(From, InOpts2),
    [
      compression(false),
      md5(CleanHash),
      metadata([OutEntry1]),
      quads(NumQuads),
      triples(NumTriples),
      tuples(NumTuples)
    ]
  ),
  sort_file(TmpFile),
  OutEntry2 = OutEntry1.put(_{
    number_of_quads: NumQuads,
    number_of_triples: NumTriples,
    number_of_tuples: NumTuples
  }),
  q_file_hash(CleanFile, data, ntriples, CleanHash),
  (   exists_file(CleanFile)
  ->  true
  ;   create_file_directory(CleanFile),
      compress_file(TmpFile, CleanFile)
  ),
  delete_file(TmpFile).

dummy1(From, InOpts, State, Out) :-
  rdf_call_on_tuples(From, dummy2(State, Out), InOpts).

dummy2(State, Out, _, S, P, O, G) :-
  gen_ntuple(S, P, O, G, State, Out).

currently_debugging(Hash) :-
  currently_debugging0(Hash), !,
  ansi_format(user_output, [bold], "~a", [Hash]),
  gtrace. %DEB
currently_debugging(_).



%! reset_and_clean(+Hash) is det.

reset_and_clean(Hash) :-
  % Remove directory and contents from disk.
  q_dir_hash(Dir, Hash),
  with_mutex(lclean, delete_directory_and_contents_msg(Dir)),
  % Reset the seedpoint in the seedlist.
  reset_seed(Hash),
  clean(Hash).



%! get_thread_seed(?Alias, ?Hash) is nondet.

get_thread_seed(Alias, Hash) :-
  with_mutex(thread_seed,
    thread_seed(Alias, Hash)
  ).



%! set_thread_seed(+Alias, +Hash) is det.

set_thread_seed(Alias, Hash) :-
  with_mutex(thread_seed, (
    retractall(thread_seed(Alias, _)),
    assert(thread_seed(Alias, Hash))
  )).





% HELPERS %

%! rdf_store_messages(+State, +Hash, :Goal_0) is det.
%
% Run Goal, unify Result with `true`, `false` or `exception(Error)`
% and messages with a list of generated error and warning messages.
% Each message is a term `message(Term, Kind, Lines)`.

rdf_store_messages(State, Hash, Goal_0) :-
  q_graph_hash(MetaG, meta, Hash),
  asserta((
    user:thread_message_hook(Term,Kind,_) :-
      error_kind(Kind),
      dict_inc(warns, State),
      rdf_store_warning(State.warn, MetaG, Term)
  )),
  (catch(Goal_0, E, true) -> true ; E = fail),
  (   var(E)
  ->  End0 = true
  ;   End0 = E,
      msg_warning("[FAILED] ~w ~w~n", [End0,Hash])
  ),
  with_output_to(string(End), write_term(End0)),
  q_graph_hash(DataG, data, Hash),
  write_ntriple(State.meta, DataG, nsdef:end, End^^xsd:string).



%! rdf_store_metadata(+State, +Hash, +InPath, +OutEntry, +CleanFile) is det.

rdf_store_metadata(State, Hash, InPath, OutEntry, CleanFile) :-
  q_file_graph(CleanFile, DataG),
  call_to_ntriples(
    State.meta,
    rdf_store_metadata(State.warns, Hash, InPath, OutEntry, DataG)
  ).


rdf_store_metadata(NumWarns, Hash, InPath, OutEntry, DataG, State, Out) :-
  M = stream(State,Out),
  q_graph_hash(MetaG, meta, Hash),
  qb(M, MetaG, nsdef:dataGraph, DataG),
  qb(M, MetaG, nsdef:numberOfWarnings, NumWarns^^xsd:nonNegativeInteger),
  InPath = [FirstInEntry|_],
  qb(M, MetaG, nsdef:numberOfReadBytes, FirstInEntry.byte_count^^xsd:nonNegativeInteger),
  qb(M, MetaG, nsdef:numberOfWrittenBytes, OutEntry.byte_count^^xsd:nonNegativeInteger),
  qb(M, MetaG, nsdef:numberOfReadCharacters, FirstInEntry.char_count^^xsd:nonNegativeInteger),
  qb(M, MetaG, nsdef:numberOfWrittenCharacters, OutEntry.char_count^^xsd:nonNegativeInteger),
  qb(M, MetaG, nsdef:numberOfReadLines, FirstInEntry.line_count^^xsd:nonNegativeInteger),
  qb(M, MetaG, nsdef:numberOfWrittenLines, OutEntry.line_count^^xsd:nonNegativeInteger),
  dict_get(number_of_quads, OutEntry, 0, NumQuads),
  qb(M, MetaG, nsdef:numberOfQuads, NumQuads^^xsd:nonNegativeInteger),
  dict_get(number_of_triples, OutEntry, 0, NumTriples),
  qb(M, MetaG, nsdef:numberOfTriples, NumTriples^^xsd:nonNegativeInteger),
  dict_get(number_of_tuples, OutEntry, 0, NumTuples),
  qb(M, MetaG, nsdef:numberOfTuples, NumTuples^^xsd:nonNegativeInteger),
  NumDuplicates is NumTuples - OutEntry.line_count + 1,
  qb(M, MetaG, nsdef:numberOfDuplicates, NumDuplicates^^xsd:nonNegativeInteger),
  rdf_media_type(FirstInEntry.rdf_media_type, Format, _),
  qb(M, MetaG, nsdef:rdfFormat, Format),
  forall(
    nth1(N, InPath, InEntry),
    rdf_store_metadata_entry(N, InEntry, MetaG, M)
  ).


rdf_store_metadata_entry(N, InEntry, MetaG, M) :-
  rdfs_container_membership_property(P, N),
  atom_concat('_:', N, BNode),
  qb(M, MetaG, P, BNode),
  dict_pairs(InEntry, InPairs),
  maplist(rdf_store_metadata_entry_pair(M, BNode), InPairs).


% Properties that are skipped.
rdf_store_metadata_entry_pair(_, _, byte_count-_).
rdf_store_metadata_entry_pair(_, _, char_count-_).
rdf_store_metadata_entry_pair(_, _, filetype-_).
rdf_store_metadata_entry_pair(_, _, line_count-_).
rdf_store_metadata_entry_pair(_, _, mtime-_).
rdf_store_metadata_entry_pair(_, _, name-_).
rdf_store_metadata_entry_pair(_, _, newline-_).
rdf_store_metadata_entry_pair(_, _, rdf_media_type-_).
rdf_store_metadata_entry_pair(_, _, size-_).


% Properties for which metadata is stored.
rdf_store_metadata_entry_pair(M, BNode, filters-Filters) :-
  atomic_list_concat(Filters, ',', A),
  qb(M, BNode, nsdef:filters, A^^xsd:string).
rdf_store_metadata_entry_pair(M, BNode, format-Format) :-
  (   Format == raw
  ->  true
  ;   qb(M, BNode, nsdef:format, Format^^xsd:string)
  ).
rdf_store_metadata_entry_pair(M, BNode, headers-Dict) :-
  dict_pairs(Dict, Pairs),
  forall(
    member(Key-Vals, Pairs),
    forall(
      member(Val, Vals),
      (
        rdf_global_id(nsdef:Key, P),
        qb(M, BNode, P, Val.raw^^xsd:string)
      )
    )
  ).
rdf_store_metadata_entry_pair(M, BNode, iri-Iri) :-
  qb(M, BNode, nsdef:iri, Iri).
rdf_store_metadata_entry_pair(M, BNode, permissions-Mask) :-
  qb(M, BNode, nsdef:permissions, Mask^^xsd:string).
rdf_store_metadata_entry_pair(M, BNode, status-Status) :-
  qb(M, BNode, nsdef:status, Status^^xsd:positiveInteger).
rdf_store_metadata_entry_pair(M, BNode, time-Time) :-
  qb(M, BNode, nsdef:time, Time^^xsd:float).
rdf_store_metadata_entry_pair(M, BNode, version-Dict) :-
  atomic_list_concat([Dict.major,Dict.minor], ., A),
  qb(M, BNode, nsdef:version, A^^xsd:string).
rdf_store_metadata_entry_pair(_, _, Key-Val) :-
  gtrace, %DEB
  writeln(Key-Val).
