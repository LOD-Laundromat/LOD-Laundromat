:- module(
  lclean,
  [
    clean/0,
    clean_hash/1,          % +Hash
    clean_seed/1,          % +Seed
    reset_and_clean_hash/1 % +Hash
  ]
).

/** <module> LOD Laundromat: Data cleaning

@author Wouter Beek
@version 2016/03-2016/05, 2016/08-2016/10, 2016/12
*/

:- use_module(library(apply)).
:- use_module(library(call_ext)).
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

:- dynamic
    currently_debugging0/1.

:- multifile
    currently_debugging0/1.





%! clean is det.
%
% Clean an arbitrarily chosen seedpoint.

clean :-
  once(seed_by_status(added, Seed)),
  clean_seed(Seed).


%! clean_hash(+Hash) is det.
%
% Clean a specific seed from the seedlist.  Does not re-clean
% documents.
%
% @throws existence_error If the seed is not in the seedlist.

clean_hash(Hash) :-
  q_file_hash(File, data, ntriples, Hash),
  q_file_is_ready(File), !,
  msg_notification("Already cleaned ~a", [Hash]).
clean_hash(Hash) :-
  seed_by_hash(Hash, Seed),
  clean_seed(Seed).



%! clean_seed(+Seed) is det.

clean_seed(Seed) :-
  dict_tag(Seed, Hash),
  begin_seed_hash(Hash),
  currently_debugging(Hash),
  q_dir_hash(Dir, Hash),
  % Only the Boolean needs to be determined under mutex.
  with_mutex(
    lclean,
    (   % This seed has already been processed.
        exists_directory(Dir)
    ->  Done = true
    ;   make_directory_path(Dir),
        Done = false
    )
  ),
  % If the source directory already exists, then the seed was already
  % crawled.
  (   Done == true
  ->  true
  ;   Done == false
  ->  atomic_list_concat([wm,Hash], :, Alias),
      call_in_thread(Alias, clean_seed_in_dir(Seed, Hash, Dir))
  ),
  
  end_seed_hash(Hash).


:- meta_predicate
    call_in_thread(+, 0).

call_in_thread(Alias, Goal_0) :-
  thread_create(profile(Goal_0), Id, [alias(Alias)]),
  thread_join(Id, true).


clean_seed_in_dir(Seed, Hash, Dir) :-
  atom_string(From, Seed.from),
  q_graph_hash(MetaG, meta, Hash),
  q_dir_file(Dir, meta, ntriples, MetaFile),
  q_dir_file(Dir, warn, ntriples, WarnFile),
  GenOpts = [rdf_media_type(application/'n-triples')],
  setup_call_cleanup(
    (
      open(MetaFile, write, MetaOut0),
      zopen(MetaOut0, MetaOut, [format(gzip)]),
      gen_ntuples:gen_ntuples_begin(MetaState, GenOpts),
      open(WarnFile, write, WarnOut0),
      zopen(WarnOut0, WarnOut, [format(gzip)]),
      gen_ntuples:gen_ntuples_begin(WarnState, GenOpts)
    ),
    (
      % Count the number of warnings.
      Counter = _{number_of_warnings: 0},
      % Assert the warnings.
      asserta((
        user:thread_message_hook(Term,Kind,_) :-
          error_kind(Kind),
          dict_inc(number_of_warnings, Counter),
          rdf_store_warning(stream(WarnState,WarnOut), MetaG, Term)
      )),
      (   catch(
            rdf_clean0(Dir, From, InPath, OutEntry, NtCleanFile),
            Exception,
            true
          )
      ->  true
      ;   % This should not occur.  Look these up in the metadata.
          Exception = fail
      ),
      % Store the status of the cleaning process (‘true’, ‘false’, or
      % compound error term).
      (   var(Exception)
      ->  Status0 = true
      ;   Status0 = Exception,
          msg_warning("[FAILED] ~w ~w~n", [Status0,Hash])
      ),
      with_output_to(string(Status), write_term(Status0)),
      
      % Assert metadata.
      % Explicitly turn off compression.  Otherwise we compress twice.
      rdf_store_metadata(
        Counter.number_of_warnings,
        Hash,
        Status,
        InPath,
        OutEntry,
        NtCleanFile,
        stream(MetaState,MetaOut)
      )
    ),
    (
      gen_ntuples:gen_ntuples_end(MetaState, GenOpts),
      close(MetaOut),
      gen_ntuples:gen_ntuples_end(WarnState, GenOpts),
      close(WarnOut)
    )
  ),

  % Handle the metadata file.
  hdt_prepare_file(MetaFile, HdtMetaFile),
  q_file_touch_ready(MetaFile),

  % Handle the warnings file.
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

  % Handle the cleaned data file.
  (   var(NtCleanFile)
  ->  true
  ;   q_file_touch_ready(NtCleanFile),
      once(
        hdt_call_on_file(
          HdtMetaFile,
          hdt0(_, nsdef:numberOfTuples, NumTuples^^xsd:nonNegativeInteger)
        )
      ),
      hdt_prepare_file(NtCleanFile),
      file_directory_name(NtCleanFile, CleanDir),
      
      % Link to the data.
      link0(Dir, 'data.hdt', CleanDir),
      link0(Dir, 'data.hdt.index', CleanDir),
      link0(Dir, 'data.nt.gz', CleanDir),
      link0(Dir, 'data.nt.gz.ready', CleanDir),

      % Store the number of tuples in RocksDB and ElasticSearch.
      rocks_merge(ll_index, number_of_tuples, NumTuples),
      retry0(
        es_update(
          [ll,seedlist,Hash],
          _{doc: _{number_of_tuples: NumTuples}}
        )
      )
  ),

  % That's it!
  rocks_merge(ll_index, number_of_documents, 1).


link0(Dir1, Local, Dir2) :-
  directory_file_path(Dir1, Local, File1),
  directory_file_path(Dir2, Local, File2),
  create_file_link(File1, File2).


rdf_clean0(Dir, From, InPath, OutEntry2, CleanFile) :-
  absolute_file_name(
    cleaning,
    TmpFile0,
    [access(write),relative_to(Dir)]
  ),
  thread_file(TmpFile0, TmpFile),
  call_to_ntriples(
    TmpFile,
    dummy1(From, [metadata(InPath)]),
    [
      compression(false),
      md5(CleanHash),
      metadata([OutEntry1]),
      quads(NumQuads),
      triples(NumTriples),
      tuples(NumTuples)
    ]
  ),

  % Sort the N-Triples on disk.
  sort_file(TmpFile),
  OutEntry2 = OutEntry1.put(_{
    number_of_quads: NumQuads,
    number_of_triples: NumTriples,
    number_of_tuples: NumTuples
  }),
  q_file_hash(CleanFile, data, ntriples, CleanHash),

  % Compress the cleaned file.
  (   exists_file(CleanFile)
  ->  true
  ;   create_file_directory(CleanFile),
      compress_file(TmpFile, CleanFile)
  ),
  
  % Delete the temporary file.
  delete_file(TmpFile).

dummy1(From, InOpts, State, Out) :-
  rdf_call_on_tuples(From, dummy2(State, Out), InOpts).

dummy2(State, Out, _, S, P, O, G) :-
  gen_ntuple(S, P, O, G, State, Out).



%! reset_and_clean_hash(+Hash) is det.

reset_and_clean_hash(Hash) :-
  % Remove directory and contents from disk.
  q_dir_hash(Dir, Hash),
  with_mutex(lclean, delete_directory_and_contents_msg(Dir)),
  % Reset the seedpoint in the seedlist.
  reset_seed(Hash),
  clean_hash(Hash).





% HELPERS %

currently_debugging(Hash) :-
  currently_debugging0(Hash), !,
  ansi_format(user_output, [bold], "~a", [Hash]),
  gtrace. %DEB
currently_debugging(_).



rdf_store_metadata(NumWarns, Hash, Status, InPath, OutEntry, CleanFile, M) :-
  q_graph_hash(MetaG, meta, Hash),
  qb(M, MetaG, nsdef:end, Status^^xsd:string),
  (   var(CleanFile)
  ->  true
  ;   q_graph_hash(DataG, data, Hash),
      qb(M, MetaG, nsdef:dataGraph, DataG)
  ),
  qb(M, MetaG, nsdef:numberOfWarnings, NumWarns^^xsd:nonNegativeInteger),
  (   var(InPath)
  ->  true
  ;   InPath = [FirstInEntry|_],
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
      once(rdf_media_type(FirstInEntry.rdf_media_type, Format, _)),
      qb(M, MetaG, nsdef:rdfFormat, Format),
      forall(
        nth1(N, InPath, InEntry),
        rdf_store_metadata_entry(N, InEntry, MetaG, M)
      )
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
