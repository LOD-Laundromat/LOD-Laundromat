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

:- use_module(library(apply)).
:- use_module(library(lists)).
:- use_module(library(pairs)).

:- use_module(generics(uri_ext)).
:- use_module(http(http_download)).
:- use_module(os(archive_ext)).
:- use_module(os(file_ext)).
:- use_module(pl(pl_log)).
:- use_module(void(void_db)). % XML namespace.

:- use_module(plRdf_term(rdf_literal)).

:- use_module(lwm(lod_basket)).
:- use_module(lwm(lwm_generics)).
:- use_module(lwm(lwm_store_triple)).
:- use_module(lwm(noRdf_store)).



lwm_unpack_loop:-
  % Pick a new source to process.
  catch(pick_pending(Md5), Exception, var(Exception)),

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
  %%%%print_message(informational, lwm_start(unpack,Md5,Source)),

  run_collect_messages(
    unpack_md5(Md5),
    Status,
    Messages
  ),

  %%%%print_message(informational, lwm_end(unpack,Md5,Source,Status,Messages)),
  maplist(store_message(Md5), Messages),
  store_end_unpack(Md5, Status).


%! unpack_md5(+Md5:atom) is det.

% The given MD5 denotes an archive entry.
unpack_md5(Md5):-
  lwm_sparql_select([lwm], [md5,path],
      [rdf(var(md5ent),lwm:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5ent),lwm:path,var(path)),
       rdf(var(md5parent),lwm:contains_entry,var(md5ent)),
       rdf(var(md5parent),lwm:md5,var(md5))],
      [[ParentMd50,EntryPath0]], [limit(1)]),
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
  lwm_url(Md5, Url), !,

  % Create a directory for the dirty version of the given Md5.
  md5_to_dir(Md5, Md5Dir),

  % Extracting and store the file extensions from the download URL.
  url_file_extensions(Url, FileExtensions),
  atomic_list_concat(FileExtensions, '.', FileExtension),

  % Construct the download file.
  file_name_extension(download, FileExtension, LocalDownloadFile),
  directory_file_path(Md5Dir, LocalDownloadFile, DownloadFile),

  % Download the dirty file for the given Md5.
  lod_accept_header_value(AcceptValue),
  download_to_file(
    Url,
    DownloadFile,
    [cert_verify_hook(ssl_verify),
     % Always redownload.
     freshness_lifetime(0.0),
     header(content_length, ContentLength),
     header(content_type, ContentType),
     header(last_modified, LastModified),
     request_header('Accept'=AcceptValue)]
  ),

  % Store the file size of the dirty file.
  size_file(DownloadFile, ByteSize),
  store_triple(lwm-Md5, lwm-size, literal(type(xsd-integer,ByteSize))),

  % Store HTTP statistics.
  store_http(Md5, ContentLength, ContentType, LastModified),

  unpack_file(Md5, DownloadFile).

%! unpack_file(+Md5:atom, +ArchiveFile:atom) is det.

unpack_file(Md5, ArchiveFile):-
  % Store the file extensions.
  file_name_extensions(_, FileExtensions, ArchiveFile),
  store_file_extensions(Md5, FileExtensions),

  % Extract archive.
  archive_extract(ArchiveFile, _, ArchiveFilters, EntryPairs),
  store_archive_filters(Md5, ArchiveFilters),

  (
    EntryPairs == []
  ->
    % The file is now ready for cleaning!
    true
  ;
    EntryPairs = [data-EntryProperties],
    memberchk(format(raw),EntryProperties)
  ->
    % Construct the data file name.
    file_directory_name(ArchiveFile, ArchiveDir),
    directory_file_path(ArchiveDir, data, DataFile),

    % Construct the dirty file name.
    md5_to_dir(Md5, Md5Dir),
    directory_file_path(Md5Dir, dirty, DirtyFile),

    % Move the data file outside of the its entry path,
    % and put it directly inside its MD5 directory.
    mv(DataFile, DirtyFile)

    % The file is now ready for cleaning!
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
    maplist(store_archive_entry(Md5), EntryPaths, EntryProperties2),
    store_end_unpack_and_skip_clean(Md5)
  ),
  % Remove the archive file.
  delete_file(ArchiveFile).

