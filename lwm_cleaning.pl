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
:- use_module(library(archive)).
:- use_module(library(http/http_client)).
:- use_module(library(lists)).
:- use_module(library(pairs)).
:- use_module(library(semweb/rdf_db)).

:- use_module(generics(uri_ext)).
:- use_module(http(http_download)).
:- use_module(os(archive_ext)).
:- use_module(os(file_ext)).
:- use_module(os(remote_ext)).
:- use_module(pl(pl_log)).
:- use_module(sparql(sparql_api)).
:- use_module(void(void_db)). % XML namespace.
:- use_module(xsd(xsd_dateTime_ext)).

:- use_module(plRdf_ser(rdf_ntriples_write)).
:- use_module(plRdf_ser(rdf_serial)).
:- use_module(plRdf_term(rdf_literal)).

:- use_module(lwm(lod_basket)).
:- use_module(lwm(lwm_db)).
:- use_module(lwm(lwm_generics)).
:- use_module(lwm(lwm_store_triple)).
:- use_module(lwm(noRdf_store)).

%! seen_dataset(?Url:url) is nondet.

:- thread_local(seen_dataset/1).



%! clean(+Md5:atom) is det.

clean(Md5):-
  run_collect_messages(
    clean_md5(Md5),
    Status,
    Messages
  ),
  store_status(Md5, Status),
  maplist(store_message(Md5), Messages),

  store_finished(Md5),
  post_rdf_triples.


%! clean_datadoc0(+Md5:atom) is det.

% The given Md5 denote an archive entry.
clean_md5(Md5):-
  once(lwm_endpoint(Endpoint)),
  sparql_select(Endpoint, _, [lwm], true, [md5,path],
        [rdf(var(md5ent),lwm:md5,literal(xsd:string,Md5)),
         rdf(var(md5ent),lwm:path,var(path)),
         rdf(var(md5url),lwm:has_entry,var(md5ent)),
         rdf(var(md5url),lwm:md5,var(md5))],
        inf, _, _, [[Md5Literal,EntryPath]]),
  rdf_literal(Md5Literal, ParentMd5, _), !,

  % Move the entry file from the parent directory into
  % an MD5 directory of its own.
  md5_to_dir(ParentMd5, Md5ParentDir),
  relative_file_path(EntryFile1, Md5ParentDir, EntryPath),
  md5_to_dir(Md5, Md5Dir),
  relative_file_path(EntryFile2, Md5Dir, EntryPath),
  mv(EntryFile1, EntryFile2),

  clean_file(Md5, EntryFile2).
% The given Md5 denotes a URL.
clean_md5(Md5):-
  once(lwm_endpoint(Endpoint)),
  sparql_select(Endpoint, _, [lwm], true, [url],
      [rdf(var(md5res),lwm:url,var(url)),
       rdf(var(md5res),lwm:md5,literal(xsd:string,Md5))],inf, _, _, [[Url]]), !,

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

  % Store HTTP statistics.
  store_http(Md5, ContentLength, ContentType, LastModified),

  clean_file(Md5, File).


clean_file(Md5, File):-
  archive_extract2(File, _, EntryPairs),

  (
    EntryPairs = [data-EntryProperties],
    memberchk(format(raw),EntryProperties)
  ->
    clean_datafile(Md5, File)
  ;
    pairs_keys_values(EntryPaths, EntryPaths, EntryProperties1),
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
  maplist(writeln, [Md5,File]).



% Helpers

%! md5_to_dir(+Md5:atom, -Md5Directory:atom) is det.

md5_to_dir(Md5, Md5Dir):-
  absolute_file_name(data(.), DataDir, [access(write),file_type(directory)]),
  directory_file_path(DataDir, Md5, Md5Dir),
  make_directory_path(Md5Dir).


/*
%! clean_datastream(+Url:url, +Coordinate:list(nonneg), +Md5:atom, +Read:blob) is det.

clean_datastream(Url, Coord, Md5, Read):-
  % Open an archive on the given stream.
  setup_call_cleanup(
    archive_open(
      Read,
      Archive,
      [close_parent(false),filter(all),format(all),format(raw)]
    ),
    clean_archive(Url, Coord, Md5, Archive),
    archive_close(Archive)
  ).


%! clean_archive(+Url:url, +Coordinate:list(nonneg), +Md5:atom, +Archive:blob) is det.

clean_archive(Url, [], Md5, Archive):- !,
  archive_next_header(Archive, EntryName),
  archive_open_entry(Archive, Read),
  clean_datastream_logged(Url, Md5, EntryName, Read).
clean_archive(Url, [H|T], Md5, Archive):-
  % Scroll to the archive entry indicated by the given coordinate.
  setup_call_cleanup(
    archive_nth0_entry(H, Archive, EntryName, Read),
    (
      writeln(EntryName),
      clean_datastream(Url, T, Md5, Read)
    ),
    close(Read)
  ).



% Step 2: We have a data stream.

%! clean_datastream_logged(+Url:url, +Md5:atom, +Read:blob) is det.
% This logs the status, all warnings, and all informational messages
% that are emitted while processing a file.

% The recusive contents of an archive are added to the LOD Basket.
clean_datastream_logged(Url, Md5, _, Read):-
  archive_tree_coords(Read, Coords),
  Coords \== [], !,
  store_archive_entries(Url, Md5, Coords),
  maplist(add_to_lod_basket(Url), Coords).
% Not an archive, proceed.
clean_datastream_logged(_, _Md5, _, _Read):- !.
  %%%%store_location_properties(Url0, Location, Url),

clean_datastream(Md5, Read):-

%! download_lod_file(
%!   +Url:url,
%!   +DataDirectory:or([atom,compound]),
%!   -Status:oneof([false,true])
%! ) is det.

download_lod_file(Url0, DataDir, Status):-
  print_message(informational, lod_download_start(X,Url)),

  % Make sure the remote directory exists.
  url_flat_directory(DataDir, Url, UrlDir),
  make_remote_directory_path(UrlDir),
  % Clear any previous, incomplete results.
  clear_remote_directory(UrlDir),

  % Process individual RDF files in a separate RDF transaction and snapshot.
  run_collect_messages(
    call_cleanup(
      rdf_transaction(
        download_lod_file_transaction(Url, Read, UrlDir, Location),
        _,
        [snapshot(true)]
      ),
      (
        store_stream_properties(Url, Read),
        close(Read)
      )
    ),
    Status,
    Messages
  ),
  (
    Status == false
  -> !,
    post_rdf_triples
  ;
    store_status(Url, Status),
    maplist(store_message(Url), Messages),
    print_message(informational, lod_downloaded_file(Url,X,Status,Messages)),

    post_rdf_triples,
    % Unpack the next entry by backtracking.
    fail
  ).
download_lod_file(_, _, true).


%! download_lod_file_transaction(
%!   +Url:url,
%!   +Read:stream,
%!   +UrlDirectory:atom,
%!   +Location:dict
%! ) is det.

download_lod_file_transaction(Url, Read, UrlDir, Location):-
  % Guess the serialization format that is used in the given stream.
  rdf_guess_format([], Read, Location, Base, Format),
  store_triple(Url, lwm:serialization_format, literal(type(xsd:string,Format)), ap),
  set_stream(Read, file_name(Base)),
  %%%%store_triple(Url, lwm:base_iri, Base, ap),

  % Load triples in any serialization format.
  rdf_load(
    stream(Read),
    [base_uri(Base),format(Format),register_namespaces(false)]
  ),
  % Count the number of triples including duplicates
  % (in between loading and saving the data).
  aggregate_all(
    count,
    rdf(_, _, _, _),
    TIn
  ),

  % Save triples using the N-Triples serialization format.
  directory_file_path(UrlDir, 'clean.nt.gz', Path),
  setup_call_cleanup(
    remote_open(Path, append, Write, [filter(gzip)]),
    rdf_ntriples_write(Write, [bnode_base(Base),number_of_triples(TOut)]),
    close(Write)
  ),

  % Asssert some statistics.
  store_number_of_triples(Url, Path, TIn, TOut),
  store_void_triples(Url),

  % Make sure any VoID datadumps are considered as well.
  register_void_datasets.

%! register_void_datasets is det.

register_void_datasets:-
  % Add all VoID datadumps to the TODO list.
  aggregate_all(
    set(Url),
    (
      rdf(_, void:dataDump, Url),
      \+ seen_dataset(Url)
    ),
    Urls
  ),
  print_message(informational, found_void_lod_urls(Urls)),
  forall(
    member(Url, Urls),
    add_to_lod_basket(Url, [])
  ).
*/

