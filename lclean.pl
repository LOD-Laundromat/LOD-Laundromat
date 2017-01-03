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
  dict_tag(Seed, ArchiveHash),
  begin_seed_hash(ArchiveHash),
  currently_debugging(ArchiveHash),
  atom_string(From, Seed.from),
  % 1. Make sure the directory exists.
  with_mutex(lclean, existed_dir(ArchiveDir, Existed)),
  % 2. Start a thread.
  (   Existed == true
  ->  true
  ;   atomic_list_concat([wm,a,ArchiveHash], :, ArchiveAlias),
      call_in_thread(ArchiveAlias, clean_archive(From, ArchiveHash))
  ),
  end_seed_hash(ArchiveHash).

% 3. Encapsulate
clean_archive(From, ArchiveHash) :-
  % Iterate over all entries inside the document stored at From.
  % We need to catch TCP exceptions, because there will not be an
  % input stream to run the cleaning goal on.
  encapsulate(
    ArchiveHash,
    forall(
      rdf_call_on_stream(From, clean_entry(From)),
      true
    )
  ).

:- meta_predicate
    encapsulate(+, 0).

%! encapsulate(+Hash, :Goal_0) is det.
%
% Call Goal_0 and store all metadata and warnings inside directory
% Dir.  This should be the same for archives and entries.
%
% Assert the warnings.  Warnings are attributed to the metadata graph.
% The metadata graph acts as the (From,EntryName) dataset identifier
% (not sure whether this is good or not).

encapsulate(Hash, Goal_0) :-
  q_graph_hash(MetaG, meta, Hash),%
  flag(Hash, _, 0),
  asserta((
    user:thread_message_hook(Term,Kind,_) :-
      error_kind(Kind),
      flag(Hash, NumWarns, NumWarns + 1),
      % @bug WarnState gets reset each time, e.g., ‘triples: 1’.
      rdf_store_warning(stream(WarnState,WarnOut), MetaG, Term)
  )),
  q_dir_hash(Dir, Hash),
  q_dir_file(Dir, meta, ntriples, MetaFile),%
  q_dir_file(Dir, warn, ntriples, WarnFile),%
  setup_call_cleanup(
    (
      open_output_stream(MetaFile, meta, MetaOut, MetaState),%
      open_output_stream(WarnFile, warn, WarnOut, WarnState)%
    ),
    (
      MetaM = stream(MetaState,MetaOut),
      (catch(Goal_0, E, true) -> true ; E = fail),
      (   var(E)
      ->  End = true
      ;   msg_warning("[FAILED] ~w (~a)~n", [E,Hash]),
          with_output_to(string(End), write_term(E))
      ),
      qb(MetaM, MetaG, nsdef:end, End^^xsd:string),
      flag(Hash, NumWarns, 0),
      qb(MetaM, MetaG, nsdef:warnings, NumWarns^^xsd:nonNegativeInteger),
      rdf_store_metadata(Hash, Status, InPath, Entry, CleanFIle
      close_output_stream(MetaFile, MetaState, HdtMetaFile),
      close_output_stream(WarnFile, WarnState, _)
    ),
    (
      close(MetaOut),
      close(WarnOut)
    )
  ).


%! clean_entry(+From, +In, +InPath1, -InPath2) is det.

clean_entry(From, In, InPath, InPath) :-
  % Find the name of the current entry.
  path_entry_name(InPath, EntryName),
  % The clean hash is based on the pair (From,EntryName).
  md5(From-EntryName, EntryHash),
  q_dir_hash(EntryDir, EntryHash),
  with_mutex(lclean, existed_dir(EntryDir, Existed)),
  entry_label(From, EntryName, EntryHash, EntryLbl),
  (   Existed == true
  ->  debug(ll(done), "No need to recrawl ~s", [EntryLbl])
  ;   atomic_list_concat([wm,e,EntryHash], :, EntryAlias),
      debug(lclean, "»ENTRY ~s", [EntryLbl]),
      call_in_thread(
        EntryAlias,
        clean_entry(From, In, InPath, EntryHash)
      ),
      debug(lclean, "«ENTRY ~s", [EntryLbl])
  ).


%! clean_entry(+From, +In, +InPath, +EntryHash) is det.

clean_entry(From, In, InPath, EntryHash) :-
  encapsulate(EntryHash, clean_stream(From, In, InPath)).

clean_stream(From, In, InPath) :-
  (   var(CleanFile)
  ->  true
  ;   q_graph_hash(DataG, data, Hash),
      qb(M, G, nsdef:dataGraph, DataG)
  ),
  % Assert metadata.  Explicitly turn off compression (otherwise
  % we compress twice).
  rdf_store_metadata(
    NumWarns,
    EntryHash,
    Status,
    InPath,
    OutEntry,
    NtCleanFile,
    stream(MetaState,MetaOut)
  ),
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
  rocks_merge(ll_index, number_of_documents, 1).


link0(Dir1, Local, Dir2) :-
  directory_file_path(Dir1, Local, File1),
  directory_file_path(Dir2, Local, File2),
  create_file_link(File1, File2).


clean_stream(Dir, In, InPath, OutEntry2, CleanFile) :-
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
  % Remove directory and contents from disk.
  q_dir_hash(Dir, Hash),
  with_mutex(lclean, delete_directory_and_contents_msg(Dir)),
  % Reset the seedpoint in the seedlist.
  reset_seed(Hash),
  clean_hash(Hash).





% HELPERS %

%! close_output_stream(+File, +State, -HdtFile) is det.

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



%! open_output_stream(+File, +Name, -Out, -State) is det.

open_output_stream(File, Name, Out, State) :-
  open(File, write, Out0),
  zopen(Out0, Out, [format(gzip)]),
  rdf__io:rdf_write_ntuples_begin(
    State,
    [name(Name),rdf_media_type(application/'n-quads')]
  ).



rdf_store_metadata(Hash, InPath, OutEntry, CleanFile, M, G) :-
  qb(M, G, nsdef:writtenBytes, OutEntry.byte_count^^xsd:nonNegativeInteger),
  qb(M, G, nsdef:writtenChars, OutEntry.char_count^^xsd:nonNegativeInteger),
  qb(M, G, nsdef:writtenLines, OutEntry.line_count^^xsd:nonNegativeInteger),
  dict_get(number_of_quads, OutEntry, 0, NumQuads),
  qb(M, G, nsdef:quads, NumQuads^^xsd:nonNegativeInteger),
  dict_get(number_of_triples, OutEntry, 0, NumTriples),
  qb(M, G, nsdef:triples, NumTriples^^xsd:nonNegativeInteger),
  dict_get(number_of_tuples, OutEntry, 0, NumTuples),
  qb(M, G, nsdef:tuples, NumTuples^^xsd:nonNegativeInteger),
  NumDuplicates is NumTuples - OutEntry.line_count + 1,
  qb(M, G, nsdef:duplicates, NumDuplicates^^xsd:nonNegativeInteger),
  dicts_get(rdf_media_type, InPath, Format),
  qb(M, G, nsdef:rdfFormat, Format^^xsd:string),
  forall(
    nth1(N, InPath, InEntry),
    rdf_store_metadata_entry(N, InEntry, G, M)
  ).


rdf_store_metadata_entry(N, InEntry, G, M) :-
  rdfs_container_membership_property(P, N),
  atom_concat('_:', N, BNode),
  qb(M, G, P, BNode),
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
