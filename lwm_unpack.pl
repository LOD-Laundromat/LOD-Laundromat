:- module(
  lwm_unpack,
  [
    lwm_unpack_loop/0
  ]
).

/** <module> LOD Washing Machine: Unpack

Unpacks files for the LOD Washing Machine to clean.

@author Wouter Beek
@version 2014/06
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(lists)).
:- use_module(library(pairs)).
:- use_module(library(semweb/rdf_db)).
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
:- use_module(lwm(lwm_generics)).
:- use_module(lwm(lwm_messages)).
:- use_module(lwm(lwm_store_triple)).
:- use_module(lwm(noRdf_store)).



lwm_unpack_loop:-
  % Pick a new source to process.
  catch(pick_pending(Md5), E, writeln(E)),

  % Process the URL we picked.
  lwm_unpack(Md5),

  % Intermittent loop.
  lwm_unpack_loop.
% Done for now. Check whether there are new jobs in one seconds.
lwm_unpack_loop:-
  sleep(1),
  lwm_unpack_loop.


%! lwm_unpack(+Md5:atom) is det.

lwm_unpack(Md5):-
  print_message(informational, start_cleaning(X,Md5)),

  run_collect_messages(
    unpack_md5(Md5),
    Status,
    Messages
  ),
  store_status(Md5, Status),
  maplist(store_message(Md5), Messages),

  store_end_unpack(Md5),
  print_message(informational, end_cleaning(X,Md5,Status,Messages)).


%! unpack_md5(+Md5:atom) is det.

% The given MD5 denotes an archive entry.
unpack_md5(Md5):-
  lwm_sparql_select([lwm], [md5,path],
      [rdf(var(md5ent),lwm:md5,literal(xsd:string,Md5)),
       rdf(var(md5ent),lwm:path,var(path)),
       rdf(var(md5url),lwm:contains_entry,var(md5ent)),
       rdf(var(md5url),lwm:md5,var(md5))],
      [[ParentMd50,EntryPath0]], [distinct(true)]),
  maplist(rdf_literal, [ParentMd50,EntryPath0], [ParentMd5,EntryPath], _), !,

  % Move the entry file from the parent directory into
  % an MD5 directory of its own.
  md5_to_dir(ParentMd5, Md5ParentDir),
  relative_file_path(EntryFile1, Md5ParentDir, EntryPath),
  md5_to_dir(Md5, Md5Dir),
  relative_file_path(EntryFile2, Md5Dir, EntryPath),
  create_file_directory(EntryFile2),
  mv(EntryFile1, EntryFile2),

  unpack_file(Md5, EntryFile2).
% The given MD5 denotes a URL.
unpack_md5(Md5):-
  lwm_sparql_select([lwm], [url],
      [rdf(var(md5res),lwm:md5,literal(xsd:string,Md5)),
       rdf(var(md5res),lwm:url,var(url))],
      [[Url]], [distinct(true)]), !,

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
  store_triple(lwm-Md5, lwm-size, literal(type(xsd-integer,ByteSize))),

  % Store HTTP statistics.
  store_http(Md5, ContentLength, ContentType, LastModified),

  unpack_file(Md5, File).

%! unpack_file(+Md5:atom, +File:atom) is det.

unpack_file(Md5, File1):-
  % Extract archive.
  archive_extract(File1, _, ArchiveFilters, EntryPairs),
  store_archive_filters(Md5, ArchiveFilters),

  (
    (
      EntryPairs == []
    ;
      EntryPairs = [data-EntryProperties],
      memberchk(format(raw),EntryProperties)
    )
  ->
    file_alternative(File1, _, dirty, _, File2),
    (
      File1 == File2
    ->
      true
    ;
      mv(File1, File2)
    ),
    
    % The file is now ready for cleaning!
    
    % :-(
    delete_file(File2)
  ;
    % Store the archive entries for future processing.
    pairs_keys_values(EntryPairs, EntryPaths, EntryProperties1),
    maplist(
      selectchk(format(ArchiveFormat)),
      EntryProperties1,
      EntryProperties2
    ),
    store_triple(lwm-Md5, lwm-archive_format,
        literal(type(xsd-string,ArchiveFormat))),
    maplist(store_archive_entry(Md5), EntryPaths, EntryProperties2)
  ).

