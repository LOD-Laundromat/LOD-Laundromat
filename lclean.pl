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
@version 2016/03-2017/01
*/

:- use_module(library(apply)).
:- use_module(library(call_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(default)).
:- use_module(library(dict_ext)).
:- use_module(library(filesex)).
:- use_module(library(hash_ext)).
:- use_module(library(hdt/hdt_ext)).
:- use_module(library(lists)).
:- use_module(library(option)).
:- use_module(library(os/archive_ext)).
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

:- thread_local
   number_of_warnings/1.





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
  dict_tag(Seed, SeedHash),
  begin_seed_hash(SeedHash),
  currently_debugging(SeedHash),
  atom_string(From, Seed.from),
  % Iterate over all entries inside the document stored at From.  We
  % need to catch TCP exceptions, because there will not be an input
  % stream to run the cleaning goal on.
  (   catch(
        forall(
          rdf_call_on_stream(uri(From), clean_seed_entry(From, SeedHash)),
          true
        ),
        Exception,
        true
      )
  ->  true
  ;   % This should not occur.  Look these up in the metadata.
      Exception = fail
  ),
  (   var(Exception)
  ->  true
  ;   msg_warning("[FAILED] ~w ~w~n", [Exception,SeedHash]),
      with_output_to(string(Status), write_term(Exception)),
      q_graph_hash(MetaG, meta, SeedHash),
      q_dir_hash(Dir, SeedHash),
      make_directory_path(Dir),
      q_dir_file(Dir, meta, ntriples, MetaFile),
      q_dir_file(Dir, warn, ntriples, WarnFile),
      setup_call_cleanup(
        (
          open_output_stream(MetaFile, meta, MetaOut, MetaState),
          open_output_stream(WarnFile, warn, WarnOut, WarnState)
        ),
        (
          debug(lclean, "[START] ~a (~a)", [From,SeedHash]),
          MetaM = stream(MetaState,MetaOut),
          qb(MetaM, MetaG, nsdef:end, Status^^xsd:string),
          qb(MetaM, MetaG, nsdef:numberOfWarnings, 1^^xsd:nonNegativeInteger),
          rdf_store_warning(stream(WarnState,WarnOut), MetaG, Exception),
          rdf__io:rdf_write_ntuples_end(MetaState, []),
          rdf__io:rdf_write_ntuples_end(WarnState, []),
          hdt_prepare_file(MetaFile),
          q_file_touch_ready(MetaFile),
          hdt_prepare_file(WarnFile),
          q_file_touch_ready(WarnFile),
          rocks_merge(ll_index, number_of_documents, 1),
          debug(lclean, "[END] ~a (~a)", [From,SeedHash])
        ),
        (
          close(MetaOut),
          close(WarnOut)
        )
      )
  ),
  end_seed_hash(SeedHash).


%! clean_seed_entry(+From, +SeedHash, +In, +InPath1, -InPath2) is det.

clean_seed_entry(From, SeedHash, In, InPath, InPath) :-
  path_entry_name(InPath, EntryName),
  md5(From-EntryName, EntryHash),
  q_dir_hash(EntryDir, EntryHash),
  with_mutex(lclean,
    (   exists_directory(EntryDir)
    ->  EntryDirExists = true
    ;   make_directory_path(EntryDir),
        EntryDirExists = false
    )
  ),
  (   EntryDirExists = true
  ->  true
  ;   atomic_list_concat([wm,SeedHash], :, SeedAlias),
      % We start a new thread with the _seed_ hash as alias.  This
      % makes it easy to see which seed caused a thread to fail.
      call_in_thread(
        SeedAlias,
        clean_seed_entry_in_thread(
          From,
          In,
          InPath,
          EntryName,
          EntryHash,
          EntryDir
        )
      )
  ).


%! clean_seed_entry_in_thread(
%!   +From,
%!   +In,
%!   +InPath,
%!   +EntryName,
%!   +EntryHash,
%!   +Dir
%! ) is det.

clean_seed_entry_in_thread(From, In, InPath, EntryName, EntryHash, Dir) :-
  debug(lclean, "[START] ~a:~a (~a)", [From,EntryName,EntryHash]),
  %gtrace,
  q_dir_hash(Dir, EntryHash),
  make_directory_path(Dir),
  q_dir_file(Dir, meta, ntriples, MetaFile),
  q_dir_file(Dir, warn, ntriples, WarnFile),
  % Let's do this!
  setup_call_cleanup(
    (
      open_output_stream(MetaFile, meta, MetaOut, MetaState),
      open_output_stream(WarnFile, warn, WarnOut, WarnState)
    ),
    (
      % Count the number of warnings as we go along.
      flag(EntryHash, _, 0),
      % Assert the warnings.  Warnings are attributed to the metadata
      % graph.  The metadata graph acts as the (From,EntryName)
      % dataset identifier (not sure whether this is good or not).
      q_graph_hash(MetaG, meta, EntryHash),
      asserta((
        user:thread_message_hook(Term,Kind,_) :-
          error_kind(Kind),
          flag(EntryHash, N, N + 1),
          % @bug: WarnState gets reset each time, e.g., ‘triples: 1’.
          rdf_store_warning(stream(WarnState,WarnOut), MetaG, Term)
      )),
      gtrace,
      (   catch(
            rdf_clean0(Dir, In, InPath, OutEntry, NtCleanFile),
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
          msg_warning("[FAILED] ~w ~w~n", [Status0,EntryHash])
      ),
      with_output_to(string(Status), write_term(Status0)),
      
      % Assert metadata.  Explicitly turn off compression (otherwise
      % we compress twice).
      flag(EntryHash, NumWarns, 0),
      rdf_store_metadata(
        NumWarns,
        EntryHash,
        Status,
        InPath,
        OutEntry,
        NtCleanFile,
        stream(MetaState,MetaOut)
      ),
      rdf__io:rdf_write_ntuples_end(MetaState, []),
      rdf__io:rdf_write_ntuples_end(WarnState, [])
    ),
    (
      close(MetaOut),
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
      rocks_merge(ll_index, number_of_tuples, NumTuples)
  ),
  rocks_merge(ll_index, number_of_documents, 1),

  % That's it!
  debug(lclean, "[END] ~a:~a (~a)", [From,EntryName,EntryHash]).


link0(Dir1, Local, Dir2) :-
  directory_file_path(Dir1, Local, File1),
  directory_file_path(Dir2, Local, File2),
  create_file_link(File1, File2).


rdf_clean0(Dir, In, InPath, OutEntry2, CleanFile) :-
  absolute_file_name(
    cleaning,
    TmpFile0,
    [access(write),relative_to(Dir)]
  ),
  thread_file(TmpFile0, TmpFile),
  rdf_call_to_ntriples(
    TmpFile,
    dummy1(In, InPath),
    [
      compression(false),
      md5(CleanHash),
      metadata(OutPath),
      quads(NumQuads),
      triples(NumTriples),
      tuples(NumTuples)
    ]
  ),
  % Sort the N-Triples on disk.
  sort_file(TmpFile),
  OutPath = [OutEntry1|_],
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

dummy1(In, InPath, State, Out) :-
  rdf_call_on_tuples_stream(In, dummy2(State, Out), InPath).

dummy2(State, Out, _, S, P, O, G) :-
  rdf_write_ntuple(S, P, O, G, State, Out).



%! reset_and_clean_hash(+Hash) is det.

reset_and_clean_hash(Hash) :-
  reset_seed(Hash),
  clean_hash(Hash).





% HELPERS %

%! currently_debugging(+Hash) is det.

currently_debugging(Hash) :-
  currently_debugging0(Hash), !,
  ansi_format(user_output, [bold], "~a", [Hash]),
  gtrace. %DEB
currently_debugging(_).



%! open_output_stream(+File, +Name, -Out, -State) is det.

open_output_stream(File, Name, Out, State) :-
  open(File, write, Out0),
  zopen(Out0, Out, [format(gzip)]),
  rdf__io:rdf_write_ntuples_begin(
    State,
    [name(Name),rdf_media_type(application/'n-quads')]
  ).



rdf_store_metadata(
  NumWarns,
  EntryHash,
  Status,
  InPath,
  OutEntry,
  CleanFile,
  M
) :-
  q_graph_hash(MetaG, meta, EntryHash),
  qb(M, MetaG, nsdef:end, Status^^xsd:string),
  (   var(CleanFile)
  ->  true
  ;   q_graph_hash(DataG, data, EntryHash),
      qb(M, MetaG, nsdef:dataGraph, DataG)
  ),
  qb(M, MetaG, nsdef:numberOfWarnings, NumWarns^^xsd:nonNegativeInteger),
  InPath = [FirstInEntry|_],
  %%%%qb(M, MetaG, nsdef:numberOfReadBytes, FirstInEntry.byte_count^^xsd:nonNegativeInteger),
  qb(M, MetaG, nsdef:numberOfWrittenBytes, OutEntry.byte_count^^xsd:nonNegativeInteger),
  %%%%qb(M, MetaG, nsdef:numberOfReadCharacters, FirstInEntry.char_count^^xsd:nonNegativeInteger),
  qb(M, MetaG, nsdef:numberOfWrittenCharacters, OutEntry.char_count^^xsd:nonNegativeInteger),
  %%%%qb(M, MetaG, nsdef:numberOfReadLines, FirstInEntry.line_count^^xsd:nonNegativeInteger),
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
rdf_store_metadata_entry_pair(M, S, '@id'-Uri) :-
  qb(M, S, nsdef:uri, Uri).
rdf_store_metadata_entry_pair(M, S, '@type'-Local) :-
  rdf_global_id(nsdef:Local, C),
  qb(M, S, rdf:type, C).
rdf_store_metadata_entry_pair(M, S, filters-Filters) :-
  atomic_list_concat(Filters, ',', A),
  qb(M, S, nsdef:filters, A^^xsd:string).
rdf_store_metadata_entry_pair(M, S, format-Format) :-
  (   Format == raw
  ->  true
  ;   qb(M, S, nsdef:format, Format^^xsd:string)
  ).
rdf_store_metadata_entry_pair(M, S, headers-Dict) :-
  dict_pairs(Dict, Pairs),
  forall(
    member(Key-Vals, Pairs),
    forall(
      member(Val, Vals),
      (
        rdf_global_id(nsdef:Key, P),
        qb(M, S, P, Val.raw^^xsd:string)
      )
    )
  ).
rdf_store_metadata_entry_pair(M, S, permissions-Mask) :-
  qb(M, S, nsdef:permissions, Mask^^xsd:string).
rdf_store_metadata_entry_pair(M, S, status-Status) :-
  qb(M, S, nsdef:status, Status^^xsd:positiveInteger).
rdf_store_metadata_entry_pair(M, S, time-Time) :-
  qb(M, S, nsdef:time, Time^^xsd:float).
rdf_store_metadata_entry_pair(M, S, version-Dict) :-
  atomic_list_concat([Dict.major,Dict.minor], ., A),
  qb(M, S, nsdef:version, A^^xsd:string).
rdf_store_metadata_entry_pair(_, _, Key-Val) :-
  gtrace, %DEB
  writeln(Key-Val).
