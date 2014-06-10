:- module(
  lwm_cleaning,
  [
    clean_datadoc/1 % +Md5:atom
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
:- use_module(library(semweb/rdf_db)).

:- use_module(generics(uri_ext)).
:- use_module(http(http_download)).
:- use_module(os(archive_ext)).
:- use_module(os(remote_ext)).
:- use_module(pl(pl_log)).
:- use_module(sparql(sparql_api)).
:- use_module(void(void_db)). % XML namespace.
:- use_module(xsd(xsd_dateTime_ext)).

:- use_module(plRdf_ser(rdf_ntriples_write)).
:- use_module(plRdf_ser(rdf_serial)).

:- use_module(lwm(lod_basket)).
:- use_module(lwm(lwm_db)).
:- use_module(lwm(lwm_generics)).
:- use_module(lwm(lwm_store_triple)).

%! seen_dataset(?Url:url) is nondet.

:- thread_local(seen_dataset/1).



%! clean_datadoc(+Md5:atom) is det.

clean_datadoc(Md5):-
  run_collect_messages(
    clean_datadoc0(Md5),
    Status,
    Messages
  ),
  store_status(Md5, Status),
  maplist(store_message(Md5), Messages),

  store_finished(Md5).


%! clean_datadoc0(+Md5:atom) is det.

clean_datadoc0(Md5):-
  once(lwm_endpoint(Endpoint)),
  sparql_select(Endpoint, _, [lwm], true, [url,path],
        [rdf(Md5,lwm:path,var(path)),
         rdf(var(md5_url),lwm:has_entry,Md5),
         rdf(var(md5_url),lwm:url,var(url))], inf, _, _, [Url,Path]), !,
  url_nested_file(data(.), Url, Dir1),
  absolute_file_name(
    Path,
    Dir2,
    [access(read),file_type(directory),relative_to(Dir1)]
  ),
  make_directory_path(Dir2),
  directory_file_path(Dir2, data, File),
  clean_datadoc0(Md5, File).
clean_datadoc0(Md5):-
  once(lwm_endpoint(Endpoint)),
  sparql_select(Endpoint, _, [lwm], true, [url],
      [rdf(Md5,lwm:url,var(url))], inf, _, _, [Url]), !,
  url_nested_file(data(.), Url, Dir),
  make_directory_path(Dir),
  directory_file_path(Dir, data, File),

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
  store_http(
    Md5,
    [content_length(ContentLength),
     content_type(ContentType),
     last_modified(LastModified)]
  ),
  archive_extract2(File, _, EntryPaths),
  maplist(add_pair(Url), EntryPaths, Sources),
  maplist(add_source_to_basket, Sources),
  clean_datadoc0(Md5, File).

clean_datadoc0(Md5, File):-
  maplist(writeln, [Md5,File]).


add_pair(X, Y, X-Y).

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

