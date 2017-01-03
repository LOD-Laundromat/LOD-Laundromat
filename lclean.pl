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
@tbd Include byte_count, char_count, lines_count in metadata.
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

:- meta_predicate
    encapsulate(+, +, 2),
    encapsulate_inner(+, 2).

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
  (   q_file_is_ready(File)
  ->  msg_notification("Already cleaned ~a", [Hash])
  ;   seed_by_hash(Hash, Seed),
      clean_seed(Seed)
  ).



%! clean_seed(+Seed) is det.

clean_seed(Seed) :-
  atom_string(From, Seed.from),
  dict_tag(Seed, ArchiveHash),
  currently_debugging(ArchiveHash),
  begin_seed_hash(ArchiveHash),
  archive_label(From, ArchiveHash, Lbl),
  encapsulate(a-Lbl, ArchiveHash, clean_archive(From)),
  end_seed_hash(ArchiveHash).

clean_archive(From, _, _) :-
  % Iterate over all entries inside the document stored at From.
  % We need to catch TCP exceptions, because there will not be an
  % input stream to run the cleaning goal on.
  forall(
    rdf_call_on_stream(From, clean_entry(From)),
    true
  ).

clean_entry(From, In, InPath, InPath) :-
  path_entry_name(InPath, EntryName),
  md5(From-EntryName, EntryHash),
  entry_label(From, EntryName, EntryHash, Lbl),
  encapsulate(e-Lbl, EntryHash, clean_stream(In, InPath)).

clean_stream(In, InPath, EntryHash, MetaM) :-
  q_dir_hash(EntryDir, EntryHash),
  q_graph_hash(MetaG, meta, EntryHash),
  clean_stream_inner(EntryDir, OutPath, TmpFile, CleanHash, In, InPath),
  % Handle the cleaned data file.  Notice that there may not be a
  % cleaned data file.
  (   var(TmpFile)
  ->  true
  ;   q_graph_hash(DataG, data, CleanHash),
      qb(MetaM, MetaG, nsdef:dataGraph, DataG),
      % Compress the cleaned file.
      q_file_hash(CleanFile, data, ntriples, CleanHash),
      (   exists_file(CleanFile)
      ->  true
      ;   create_file_directory(CleanFile),
          compress_file(TmpFile, CleanFile)
      ),
      q_file_touch_ready(CleanFile),
      q_file_hash(HdtMetaFile, meta, hdt, EntryHash),
      once(
        hdt_call_on_file(
          HdtMetaFile,
          hdt0(_, nsdef:tuples, NumTuples^^xsd:nonNegativeInteger)
        )
      ),
      hdt_prepare_file(CleanFile),
      file_directory_name(CleanFile, CleanDir),
      % Link to the data.
      link_dirs(EntryDir, 'data.hdt', CleanDir),
      link_dirs(EntryDir, 'data.hdt.index', CleanDir),
      link_dirs(EntryDir, 'data.nt.gz', CleanDir),
      link_dirs(EntryDir, 'data.nt.gz.ready', CleanDir),
      % Store the number of tuples in RocksDB and ElasticSearch.
      rocks_merge(ll_index, tuples, NumTuples)
  ),
  % Explicitly turn off compression when asserting metadata, otherwise
  % we compress twice.
  OutPath = [OutEntry|_],
  qb(MetaM, MetaG, nsdef:bytes, OutEntry.byte_count^^xsd:nonNegativeInteger),
  qb(MetaM, MetaG, nsdef:chars, OutEntry.char_count^^xsd:nonNegativeInteger),
  qb(MetaM, MetaG, nsdef:lines, OutEntry.line_count^^xsd:nonNegativeInteger),
  dict_get(quads, OutEntry, 0, NumQuads),
  qb(MetaM, MetaG, nsdef:quads, NumQuads^^xsd:nonNegativeInteger),
  dict_get(triples, OutEntry, 0, NumTriples),
  qb(MetaM, MetaG, nsdef:triples, NumTriples^^xsd:nonNegativeInteger),
  dict_get(tuples, OutEntry, 0, NumTuples),
  qb(MetaM, MetaG, nsdef:tuples, NumTuples^^xsd:nonNegativeInteger),
  NumDuplicates is NumTuples - OutEntry.line_count + 1,
  qb(MetaM, MetaG, nsdef:duplicates, NumDuplicates^^xsd:nonNegativeInteger),
  dicts_get(rdf_media_type, InPath, Format),
  qb(MetaM, MetaG, nsdef:rdfFormat, Format^^xsd:string),
  forall(
    nth1(N, InPath, InEntry),
    rdf_store_metadata_entry(N, InEntry, MetaG, MetaM)
  ),
  rocks_merge(ll_index, documents, 1).

clean_stream_inner(EntryDir, OutPath2, TmpFile, CleanHash, In, InPath):-
  absolute_file_name(
    cleaning,
    TmpFile0,
    [access(write),relative_to(EntryDir)]
  ),
  thread_file(TmpFile0, TmpFile),
  rdf_call_to_ntriples(
    TmpFile,
    clean_stream_inner(In, InPath),
    [
      compression(false),
      md5(CleanHash),
      metadata(OutPath1),
      quads(NumQuads),
      triples(NumTriples),
      tuples(NumTuples)
    ]
  ),
  % Sort the N-Triples on disk.
  sort_file(TmpFile),
  OutPath1 = [OutEntry1|OutPath],
  OutEntry2 = OutEntry1.put(_{
    quads: NumQuads,
    triples: NumTriples,
    tuples: NumTuples
  }),
  OutPath2 = [OutEntry2|OutPath],
  % Delete the temporary file.
  delete_file(TmpFile).

clean_stream_inner(In, InPath, State, Out) :-
  rdf_call_on_tuples_stream(In, clean_tuple(State, Out), InPath).

clean_tuple(State, Out, _, S, P, O, G) :-
  rdf_write_ntuple(S, P, O, G, State, Out).



%! reset_and_clean_hash(+Hash) is det.

reset_and_clean_hash(Hash) :-
  reset_seed(Hash),
  clean_hash(Hash).





% HELPERS %

%! archive_label(+From, +ArchiveHash, -ArchiveLbl) is det.

archive_label(From, ArchiveHash, ArchiveLbl) :-
  format(string(ArchiveLbl), "~a (~a)", [From,ArchiveHash]).



%! close_output_stream(+File, +State) is det.
%! close_output_stream(+File, +State, -HdtFile) is det.

close_output_stream(File, State) :-
  close_output_stream(File, State, _).


close_output_stream(File, State, HdtFile) :-
  rdf__io:rdf_write_ntuples_end(State, []),
  hdt_prepare_file(File, HdtFile),
  q_file_touch_ready(File).



%! currently_debugging(+Hash) is det.

currently_debugging(Hash) :-
  currently_debugging0(Hash), !,
  ansi_format(user_output, [bold], "~a", [Hash]),
  gtrace. %DEB
currently_debugging(_).



%! encapsulate(+Debug, +Hash, :Goal_2) is det.

encapsulate(Mode-Lbl, Hash, Goal_2) :-
  q_dir_hash(Dir, Hash),
  with_mutex(lclean, existed_dir(Dir, Existed)),
  (   Existed == true
  ->  debug(ll(done), "No need to recrawl ~s", [Lbl])
  ;   atomic_list_concat([wm,Mode,Hash], :, Alias),
      debug(lclean, "»~a ~s", [Mode,Lbl]),
      call_in_thread(Alias, encapsulate_inner(Hash, Goal_2)),
      debug(lclean, "«~a ~s", [Mode,Lbl])
  ).

%! encapsulate_inner(+Hash, :Goal_2) is det.
%
% Call Goal_2(+Hash,+MetaM) and store all metadata and warnings inside
% directory Dir.  This should be the same for archives and entries.
%
% Assert the warnings.  Warnings are attributed to the metadata graph.
% The metadata graph acts as the (From,EntryName) dataset identifier
% (not sure whether this is good or not).

encapsulate_inner(Hash, Goal_2) :-
  q_graph_hash(MetaG, meta, Hash),
  flag(Hash, _, 0),
  asserta((
    user:thread_message_hook(Term,Kind,_) :-
      error_kind(Kind),
      flag(Hash, NumWarns, NumWarns + 1),
      % @bug WarnState gets reset each time, e.g., ‘triples: 1’.
      rdf_store_warning(stream(WarnState,WarnOut), MetaG, Term)
  )),
  q_dir_hash(Dir, Hash),
  q_dir_file(Dir, meta, ntriples, MetaFile),
  q_dir_file(Dir, warn, ntriples, WarnFile),
  setup_call_cleanup(
    (
      open_output_stream(MetaFile, meta, MetaOut, MetaState),
      open_output_stream(WarnFile, warn, WarnOut, WarnState)
    ),
    (
      MetaM = stream(MetaState,MetaOut),
      (catch(call(Goal_2, Hash, MetaM), E, true) -> true ; E = fail),
      (   var(E)
      ->  End = true
      ;   with_output_to(string(End), write_term(E)),
          msg_warning("[FAILED] ~s (~a)~n", [End,Hash])
      ),
      qb(MetaM, MetaG, nsdef:end, End^^xsd:string),
      flag(Hash, NumWarns, 0),
      qb(MetaM, MetaG, nsdef:warnings, NumWarns^^xsd:nonNegativeInteger),
      close_output_stream(MetaFile, MetaState),
      close_output_stream(WarnFile, WarnState)
    ),
    (
      close(MetaOut),
      close(WarnOut)
    )
  ).



%! entry_label(+From, +EntryName, +EntryHash, -EntryLbl) is det.

entry_label(From, EntryName, EntryHash, EntryLbl) :-
  format(string(EntryLbl), "~a:~a (~a)", [From,EntryName,EntryHash]).



%! existed_dir(+Dir, -Existed) is det.
%
% Ensures that directory Dir is created and returns whether or not it
% already existed before.

existed_dir(Dir, true) :-
  exists_directory(Dir), !.
existed_dir(Dir, false) :-
  make_directory_path(Dir).



%! http_check_for_success(+InPath) is det.
%
% @throws http_status/2 exception

http_check_for_success(InPath) :-
  % The last HTTP status code must have been success.
  (   dicts_getchk(status, InPath, Status)
  ->  (   http_status_is_success(Status)
      ->  true
      ;   throw(http_status(Status, '2xx'))
      )
  ;   true
  ).



%! link_dirs(+Dir1, +Local, +Dir2) is det.

link_dirs(Dir1, Local, Dir2) :-
  directory_file_path(Dir1, Local, File1),
  directory_file_path(Dir2, Local, File2),
  create_file_link(File1, File2).



%! open_output_stream(+File, +Name, -Out, -State) is det.

open_output_stream(File, Name, Out, State) :-
  open(File, write, Out0),
  zopen(Out0, Out, [format(gzip)]),
  rdf__io:rdf_write_ntuples_begin(
    State,
    [name(Name),rdf_media_type(application/'n-quads')]
  ).



rdf_store_metadata_entry(N, InEntry, G, M) :-
  rdfs_container_membership_property(P, N),
  atom_concat('_:', N, S),
  qb(M, G, P, S),
  dict_pairs(InEntry, InPairs),
  maplist(rdf_store_metadata_entry_pair(M, S), InPairs).


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
