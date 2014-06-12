:- module(
  lwm_cleaning,
  [
    clean/1 % +Md5:atom
  ]
).

/** <module> LOD Washing Machine: cleaning

The cleaning process performed by the LOD Washing Machine.

@author Wouter Beek
@version 2014/03-2014/06
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(lists)).
:- use_module(library(pairs)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(uri)).
:- use_module(library(zlib)).

:- use_module(http(http_download)).
:- use_module(os(archive_ext)).
:- use_module(os(file_ext)).
:- use_module(pl(pl_log)).
:- use_module(void(void_db)). % XML namespace.

:- use_module(plRdf_ser(rdf_detect)).
:- use_module(plRdf_ser(rdf_ntriples_write)).
:- use_module(plRdf_term(rdf_literal)).

:- use_module(lwm(lod_basket)).
:- use_module(lwm(lwm_db)).
:- use_module(lwm(lwm_generics)).
:- use_module(lwm(lwm_messages)).
:- use_module(lwm(lwm_store_triple)).
:- use_module(lwm(noRdf_store)).

%! seen_dataset(?Url:url) is nondet.

:- thread_local(seen_dataset/1).



%! clean(+Md5:atom) is det.

clean(Md5):-
  print_message(informational, start_cleaning(X,Md5)),
  run_collect_messages(
    clean_md5(Md5),
    Status,
    Messages
  ),
  store_status(Md5, Status),
  maplist(store_message(Md5), Messages),

  store_end(Md5),
  print_message(informational, end_cleaning(X,Md5,Status,Messages)).


%! clean_datadoc0(+Md5:atom) is det.

% The given Md5 denote an archive entry.
clean_md5(Md5):-
  once(lwm_endpoint(Endpoint)),
  lwm_sparql_select(Endpoint, _, [lwm], true, [md5,path],
        [rdf(var(md5ent),lwm:md5,literal(xsd:string,Md5)),
         rdf(var(md5ent),lwm:path,var(path)),
         rdf(var(md5url),lwm:contains_entry,var(md5ent)),
         rdf(var(md5url),lwm:md5,var(md5))],
        inf, _, _, [[ParentMd50,EntryPath0]]),
  maplist(rdf_literal, [ParentMd50,EntryPath0], [ParentMd5,EntryPath], _), !,

  % Move the entry file from the parent directory into
  % an MD5 directory of its own.
  md5_to_dir(ParentMd5, Md5ParentDir),
  relative_file_path(EntryFile1, Md5ParentDir, EntryPath),
  md5_to_dir(Md5, Md5Dir),
  relative_file_path(EntryFile2, Md5Dir, EntryPath),
  create_file_directory(EntryFile2),
  mv(EntryFile1, EntryFile2),

  clean_file(Md5, EntryFile2).
% The given Md5 denotes a URL.
clean_md5(Md5):-
  once(lwm_endpoint(Endpoint)),
  lwm_sparql_select(Endpoint, _, [lwm], true, [url],
      [rdf(var(md5res),lwm:url,var(url)),
       rdf(var(md5res),lwm:md5,literal(xsd:string,Md5))],
      inf, _, _, [[Url]]), !,

  % Create a directory for the dirty version of the given Md5.
  md5_to_dir(Md5, Md5Dir),

  % Download the dirty file for the given Md5.
  directory_file_path(Md5Dir, dirty, File),
  lod_accept_header_value(AcceptValue),
  download_to_file(
    Url,
    File,
    [cert_verify_hook(ssl_verify),
     % Always redownload.
     freshness_lifetime(0.0),
     header(content_length, ContentLength),
     header(content_type, ContentType),
     header(last_modified, LastModified),
     request_header('Accept'=AcceptValue)]
  ),

  % Store the file size of the dirty file.
  size_file(File, ByteSize),
  store_triple(lwm-Md5, lwm:size, literal(type(xsd:integer,ByteSize)), ap),

  % Store HTTP statistics.
  store_http(Md5, ContentLength, ContentType, LastModified),

  clean_file(Md5, File).


clean_file(Md5, File1):-
  % Extract archive.
  archive_extract(File1, _, ArchiveFilters, EntryPairs),
  store_archive_filters(Md5, ArchiveFilters),

  (
    EntryPairs = [data-EntryProperties],
    memberchk(format(raw),EntryProperties)
  ->
    file_alternative(File1, _, dirty, _, File2),
    (
      File1 == File2
    ->
      true
    ;
      mv(File1, File2)
    ),
    clean_datafile(Md5, File2),
    % :-(
    delete_file(File2)
  ;
    pairs_keys_values(EntryPairs, EntryPaths, EntryProperties1),
    maplist(
      selectchk(format(ArchiveFormat)),
      EntryProperties1,
      EntryProperties2
    ),
    store_triple(lwm-Md5, lwm:archive_format,
        literal(type(xsd:string,ArchiveFormat)), ap),
    maplist(store_archive_entry(Md5), EntryPaths, EntryProperties2)
  ).


clean_datafile(Md5, File):-
  setup_call_cleanup(
    open(File, read, Read),
    (
      rdf_transaction(
        clean_datastream(Md5, File, Read),
        _,
        [snapshot(true)]
      ),
      store_stream(Md5,Read)
    ),
    close(Read)
  ).


clean_datastream(Md5, File, Read):-
  % File extension.
  file_name_extensions(_, FileExtensions, base),
  atomic_list_concat(FileExtensions, '.', FileExtension),
  store_triple(lwm-Md5, lwm:file_extension,
      literal(type(xsd:string,FileExtension)), ap),

  % Guess serialization format.
  rdf_guess_format(Read, Format, []),
  store_triple(lwm-Md5, lwm:serialization_format,
      literal(type(xsd:string,Format)), ap),

  % Load triples in serialization format.
  lwm_base(Md5, Base),
  rdf_load(
    stream(Read),
    [base_uri(Base),format(Format),register_namespaces(false)]
  ),

  % We are not in between loading and saving the data.
  % This means that we can count the number of triples,
  % including duplicates.
  aggregate_all(
    count,
    rdf(_, _, _, _),
    TIn
  ),

  % Save triples using the N-Triples serialization format.
  file_directory_name(File, Dir),
  directory_file_path(Dir, 'clean.nt.gz', Path),
  lwm_bnode_base(Md5, BNodeBase),
  setup_call_cleanup(
    gzopen(Path, write, Write),
    rdf_ntriples_write(
      Write,
      [bnode_base(BNodeBase),number_of_triples(TOut)]
    ),
    close(Write)
  ),

  % Asssert some statistics.
  store_number_of_triples(Md5, Path, TIn, TOut),
  store_void_triples,

  % Make sure any VoID datadumps are added to the LOD Basket.
  register_void_datasets.



% Helpers

%! md5_to_dir(+Md5:atom, -Md5Directory:atom) is det.

md5_to_dir(Md5, Md5Dir):-
  absolute_file_name(data(.), DataDir, [access(write),file_type(directory)]),
  directory_file_path(DataDir, Md5, Md5Dir),
  make_directory_path(Md5Dir).


%! register_void_datasets is det.

register_void_datasets:-
  aggregate_all(
    set(Url),
    (
      rdf(_, void:dataDump, Url),
      % @tbd Create a shortcur for this: only a single SPARQL query,
      % matching `lwm:added`.
      \+ is_cleaned(Md5),
      \+ is_pending(Md5)
    ),
    Urls
  ),
  print_message(informational, found_void_datadumps(Urls)),
  maplist(add_to_basket, Urls).

