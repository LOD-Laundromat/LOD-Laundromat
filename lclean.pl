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
@tbd Can we also count (byte_count, char_count, lines_count) what is
     _read_?
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
:- use_module(library(http/http_io)).
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
    call_meta_warn(+, 2),
    call_meta_warn(+, +, 2),
    call_meta_warn_streams(+, 2, +, +).

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
  call_meta_warn(a-Lbl, ArchiveHash, clean_archive(From)),
  end_seed_hash(ArchiveHash).

clean_archive(From, _, _) :-
  % Iterate over all entries inside the document stored at From.  We
  % need to catch TCP exceptions, because there will not be an input
  % stream to run the cleaning goal on.
  forall(
    rdf_call_on_stream(From, clean_entry(From)),
    true
  ).

clean_entry(From, In, InPath, InPath) :-
  % Make sure that the HTTP status code is in the 2xx range.
  http_check_for_success(InPath),
  path_entry_name(InPath, EntryName),
  md5(From-EntryName, EntryHash),
  entry_label(From, EntryName, EntryHash, Lbl),
  call_meta_warn(e-Lbl, EntryHash, clean_stream1(In, InPath)).

clean_stream1(In, InPath, EntryHash, MetaM) :-
  q_dir_hash(EntryDir, EntryHash),
  q_graph_hash(MetaG, meta, EntryHash),
  clean_stream2(EntryDir, OutPath, TmpFile, CleanHash, In, InPath),
  OutPath = [OutEntry|_],
  % Handle the cleaned data file, if any.
  (   var(TmpFile)
  ->  true
  ;   q_graph_hash(DataG, data, CleanHash),
      qb(MetaM, MetaG, nsdef:dataGraph, DataG),
      % Compress the cleaned file.
      q_file_hash(CleanFile, data, ntriples, CleanHash),
      (   exists_file(CleanFile)
      ->  true
      ;   create_file_directory(CleanFile),
          compress_file(TmpFile, CleanFile),
          delete_file(TmpFile)
      ),
      hdt_prepare_file(CleanFile),
      q_file_touch_ready(CleanFile),
      % Link the entry directory to the data directory.
      file_directory_name(CleanFile, CleanDir),
      link_dirs(EntryDir, 'data.hdt', CleanDir),
      link_dirs(EntryDir, 'data.hdt.index', CleanDir),
      link_dirs(EntryDir, 'data.nt.gz', CleanDir),
      link_dirs(EntryDir, 'data.nt.gz.ready', CleanDir),
      % Store the number of tuples in RocksDB and ElasticSearch.
      get_dict(tuples, OutEntry, NumTuples),
      rocks_merge(ll_index, tuples, NumTuples)
  ),
  % Explicitly turn off compression when asserting metadata, otherwise
  % we compress twice.
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
  dicts_getchk(rdf_media_type, InPath, MT),
  once(rdf_media_type(MT, Format, _)),
  qb(MetaM, MetaG, nsdef:rdfFormat, Format),
  forall(
    nth1(N, InPath, InEntry),
    rdf_store_metadata_entry(N, InEntry, MetaG, MetaM)
  ),
  rocks_merge(ll_index, documents, 1).

clean_stream2(EntryDir, OutPath2, TmpFile, CleanHash, In, InPath):-
  absolute_file_name(
    cleaning,
    TmpFile0,
    [access(write),relative_to(EntryDir)]
  ),
  thread_file(TmpFile0, TmpFile),
  rdf_call_to_ntriples(
    file(TmpFile),
    clean_stream3(In, InPath),
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
  OutPath2 = [OutEntry2|OutPath].

clean_stream3(In, InPath, State, Out) :-
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



%! call_meta_warn(+Debug, +Hash, :Goal_2) is det.
%
% Call `Goal_2(+Hash,+MetaM)` while storing metadata and warnings to
% files.

call_meta_warn(Mode-Lbl, Hash, Goal_2) :-
  q_dir_hash(Dir, Hash),
  with_mutex(lclean, existed_dir(Dir, Existed)),
  (   Existed == true
  ->  debug(ll(done), "No need to recrawl ~s", [Lbl])
  ;   atomic_list_concat([wm,Mode,Hash], :, Alias),
      debug(lclean, "»~a ~s", [Mode,Lbl]),
      call_in_thread(Alias, call_meta_warn(Hash, Goal_2)),
      debug(lclean, "«~a ~s", [Mode,Lbl])
  ).

%! call_meta_warn(+Hash, :Goal_2) is det.
%
% Call `Goal_2(+Hash,+MetaM)` and store all metadata and warnings
% inside directory Dir.  This should be the same for archives and
% entries.
%
% Assert the warnings.  Warnings are attributed to the metadata graph.
% The metadata graph acts as the (From,EntryName) dataset identifier
% (not sure whether this is good or not).

call_meta_warn(Hash, Goal_2) :-
  % Use the Hash directory Dir to assert metadata (MetaFile) and
  % warnings (WarnFile).
  q_dir_hash(Dir, Hash),
  q_dir_file(Dir, meta, ntriples, MetaFile),
  q_dir_file(Dir, warn, ntriples, WarnFile),
  call_to_streams(
    file(MetaFile),
    file(WarnFile),
    call_meta_warn_streams(Hash, Goal_2)
  ),
  hdt_prepare_file(MetaFile),
  q_file_touch_ready(MetaFile),
  hdt_prepare_file(WarnFile),
  q_file_touch_ready(WarnFile).

call_meta_warn_streams(Hash, Goal_2, MetaOut, WarnOut) :-
  Opts = [rdf_media_type(application/'n-quads')],
  MetaOpts = [name(meta)|Opts],
  rdf__io:rdf_write_ntuples_begin(MetaState, MetaOpts),
  MetaM = stream(MetaState,MetaOut),
  WarnOpts = [name(warn)|Opts],
  rdf__io:rdf_write_ntuples_begin(WarnState, WarnOpts),
  % @hack Count the number of warnings.  WarnState should be able to
  %       do this as well.
  flag(Hash, _, 0),
  q_graph_hash(MetaG, meta, Hash),
  % Assert all warnings as RDF.
  asserta((
    user:thread_message_hook(Term,Kind,_) :-
      error_kind(Kind),
      flag(Hash, NumWarns, NumWarns + 1),
      % @bug WarnState gets reset each time, e.g., ‘triples: 1’.
      rdf_store_warning(stream(WarnState,WarnOut), MetaG, Term)
  )),
  (catch(call(Goal_2, Hash, MetaM), E, true) -> true ; E = fail),
  (   var(E)
  ->  End = true
  ;   with_output_to(string(End), write_term(E)),
      msg_warning("[FAILED] ~s (~a)~n", [End,Hash])
  ),
  qb(MetaM, MetaG, nsdef:end, End^^xsd:string),
  flag(Hash, NumWarns, 0),
  qb(MetaM, MetaG, nsdef:warnings, NumWarns^^xsd:nonNegativeInteger),
  rdf__io:rdf_write_ntuples_end(MetaState, MetaOpts),
  rdf__io:rdf_write_ntuples_end(WarnState, WarnOpts).



%! currently_debugging(+Hash) is det.

currently_debugging(Hash) :-
  currently_debugging0(Hash), !,
  ansi_format(user_output, [bold], "~a", [Hash]),
  gtrace. %DEB
currently_debugging(_).



%! entry_label(+From, +EntryName, +EntryHash, -EntryLbl) is det.

entry_label(From, EntryName, EntryHash, EntryLbl) :-
  format(string(EntryLbl), "~a ~a (~a)", [From,EntryName,EntryHash]).



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
rdf_store_metadata_entry_pair(M, S, version-Version) :-
  qb(M, S, nsdef:version, Version^^xsd:string).
rdf_store_metadata_entry_pair(_, _, Key-Val) :-
  gtrace, %DEB
  writeln(Key-Val).
