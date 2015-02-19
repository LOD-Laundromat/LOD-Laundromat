:- module(
  lwm_unpack,
  [
    lwm_unpack/1, % +Datadoc:iri
    lwm_unpack_loop/0
  ]
).

/** <module> LOD Washing Machine: Unpack

Unpacks files for the LOD Washing Machine to clean.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09, 2014/11, 2015/01-2015/02
*/

:- use_module(library(apply)).
:- use_module(library(lists), except([delete/3,subset/2])).

:- use_module(generics(list_script)).
:- use_module(os(archive_ext)).
:- use_module(os(file_ext)).
:- use_module(os(file_gnu)).
:- use_module(pl(pl_log)).

:- use_module(plUri(uri_ext)).

:- use_module(plHttp(download_to_file)).

:- use_module(plRdf(management/rdf_file_db)).

:- use_module(lwm(md5)).
:- use_module(lwm(lwm_debug_message)).
:- use_module(lwm(lwm_store_triple)).
:- use_module(lwm(noRdf_store)).
:- use_module(lwm(query/lwm_sparql_enum)).
:- use_module(lwm(query/lwm_sparql_query)).

:- dynamic(debug:debug_md5/2).
:- multifile(debug:debug_md5/2).





%! lwm_unpack_loop is det.

lwm_unpack_loop:-
  % Pick a new source to process.
  % If some exception is thrown here, the catch/3 makes it
  % silently fail. This way, the unpacking thread is able
  % to wait in case a SPARQL endpoint is temporarily down.
  catch(
    with_mutex(lwm_endpoint_access, (
      % `DirtyUrl` is only instantiated if `Datadoc`
      % is not an archive entry.
      datadoc_enum_pending(Datadoc, DirtyUrl),
      % Update the database, saying we are ready
      % to begin downloading+unpacking this data document.
      store_start_unpack(Datadoc)
    )),
    Exception,
    var(Exception)
  ),
  lwm_unpack(Datadoc, DirtyUrl),
  % Intermittent loop.
  lwm_unpack_loop.
% Done for now. Check whether there are new jobs in one seconds.
lwm_unpack_loop:-
  sleep(5),
  lwm_debug_message(lwm_idle_loop(unpack)), % DEB
  lwm_unpack_loop.



%! lwm_unpack(+Datadoc:iri) is det.

lwm_unpack(Datadoc):-
  % Update the database, saying we are ready
  % to begin downloading+unpacking this data document.
  store_start_unpack(Datadoc),

  datadoc_location(Datadoc, DirtyUrl),
  lwm_unpack(Datadoc, DirtyUrl).

%! lwm_unpack(+Datadoc:iri, +DirtyUrl:uri) is det.

lwm_unpack(Datadoc, DirtyUrl):-
  % We sometimes need the MD5 atom.
  rdf_global_id(ll:Md5, Datadoc),

  % DEB
  (   debug:debug_md5(Md5, unpack)
  ->  gtrace %DEB
  ;   true
  ),

  % DEB: *start* of downloading+unpacking..
  lwm_debug_message(
    lwm_progress(unpack),
    lwm_start(unpack,Md5,Datadoc,Source)
  ),

  % Downloading+unpacking of a specific data document.
  run_collect_messages(
    unpack_datadoc(Md5, Datadoc, DirtyUrl),
    Status,
    Warnings
  ),
  (Status == false -> gtrace ; true), %DEB

  % DEB: *end* of downloading+unpacking.
  lwm_debug_message(
    lwm_progress(unpack),
    lwm_end(unpack,Md5,Source,Status,Warnings)
  ),

  % Store the warnings and status as metadata.
  maplist(store_warning(Datadoc), Warnings),
  store_end_unpack(Md5, Datadoc, Status),
  
  % Remove the archive file.
  delete_file(ArchiveFile).


%! unpack_datadoc(+Md5:atom, +Datadoc:iri, ?DirtyUrl:uri, -File:atom) is det.

% The given MD5 denotes an archive entry.
unpack_datadoc(Md5, Datadoc, DirtyUrl, File):-
  % Uninstantiated SPARQL variable (using keyword OPTIONAL).
  DirtyUrl == '$null$', !,

  % Entries occur in a path.
  md5_directory(Md5, Md5Dir),
  datadoc_archive_entry(Datadoc, _, EntryPath),
  relative_file_path(File, Md5Dir, EntryPath),

  % Further unpack the archive entry.
  unpack_file(Md5, Md5Dir, Datadoc, File).
% The given MD5 denotes a URL.
unpack_datadoc(Md5, Datadoc, DirtyUrl, DownloadFile):-
  % Create a directory for the dirty version of the given Md5.
  md5_directory(Md5, Md5Dir),

  % Extracting and store the file extensions from the download URL, if any.
  (   uri_file_extension(DirtyUrl, FileExtension)
  ->  true
  ;   FileExtension = ''
  ),

  % Construct the download file.
  file_name_extension(download, FileExtension, LocalDownloadFile),
  directory_file_path(Md5Dir, LocalDownloadFile, DownloadFile),

  % Download the dirty file for the given Md5.
  rdf_accept_header_value(AcceptValue),
  download_to_file(
    DirtyUrl,
    DownloadFile,
    [
      cert_verify_hook(ssl_verify),
      % Always redownload.
      freshness_lifetime(0.0),
      header(content_length, ContentLength),
      header(content_type, ContentType),
      header(last_modified, LastModified),
      request_header('Accept'=AcceptValue)
    ]
  ),

  % Store the file size of the dirty file.
  size_file(DownloadFile, DownloadSize),
  store_triple(
    Datadoc,
    llo-downloadSize,
    literal(type(xsd-nonNegativeInteger,DownloadSize))
  ),

  % Store HTTP statistics.
  store_http(Datadoc, ContentLength, ContentType, LastModified),

  unpack_file(Md5, Md5Dir, Datadoc, DownloadFile).


%! unpack_file(
%!   +Md5:atom,
%!   +Md5Dir:atom,
%!   +Datadoc:iri,
%!   +ArchiveFile:atom
%! ) is det.

unpack_file(Md5, Md5Dir, Datadoc, ArchiveFile):-
  % Store the file extension, if any.
  file_name_extension(_, FileExtension, ArchiveFile),
  (   FileExtension == ''
  ->  true
  ;   store_file_extension(Datadoc, FileExtension)
  ),

  % Extract archive.
  archive_extract(ArchiveFile, _, ArchiveFilters, EntryPairs),
  store_archive_filters(Datadoc, ArchiveFilters),

  (   % Case 1: There is no archive file to unpack.
      EntryPairs == []
  ->  % To keep the process simple / consistent with other cases,
      % we create an empty dirty file
      % (acting as the 'content' of the absent archive file.
      directory_file_path(Md5Dir, dirty, DirtyFile),
      touch_file(DirtyFile)
  ;   % Case 2: The archive file is present, but it is a raw data file
      %         (so not an archive proper).
      % Since the file is completely unpacked it can now go into
      % the cleaning phase unaltered.
      EntryPairs = [data-EntryProperties],
      memberchk(format(raw), EntryProperties)
  ->  % Rename the data file to `dirty`, which indicates
      % that it can act as input for the cleaning stage.
      % Notice that the data file may appear in an entry path.
      directory_file_path(ArchiveDir, _, ArchiveFile),
      directory_file_path(ArchiveDir, data, DataFile),
      directory_file_path(Md5Dir, dirty, DirtyFile),
      gnu_mv(DataFile, DirtyFile),

      % Store the size of the dirty data file after unpacking.
      size_file(DirtyFile, UnpackedSize),
      store_triple(
        Datadoc,
        llo-unpackedSize,
        literal(type(xsd-nonNegativeInteger,UnpackedSize))
      )
  ;   % Case 3: The file is a proper archive
      %         containing a number of entries.

      % Create the archive entries
      % and copy the entry files to their own MD5 dirs.
      list_script(
        process_entry_pair(Md5, Md5Dir, Datadoc),
        EntryPairs,
        [message('LWM ArchiveEntry')]
      ),

      % Archives cannot be cleaned,
      % so skip the cleaning phase by using metadata.
      store_skip_clean(Md5, Datadoc)
  ).

%! process_entry_pair(
%!   +ParentMd5:atom,
%!   +ParentMd5Dir:atom,
%!   +Datadoc:atom,
%!   EntryPair:pair(atom,list(nvpair))
%! ) is det.

process_entry_pair(_, _, _, _-EntryProperties):-
  memberchk(filetype(directory), EntryProperties), !.
process_entry_pair(
  ParentMd5,
  ParentMd5Dir,
  Datadoc,
  EntryPath-EntryProperties
):-
  % Establish the entry name.
  create_entry_hash(ParentMd5, EntryPath, EntryMd5),
  rdf_global_id(ll:EntryMd5, Entry),

  % Move the file before the metadata is send to the server.
  relative_file_path(FromEntryFile, ParentMd5Dir, EntryPath),
  md5_directory(EntryMd5, EntryMd5Dir),
  relative_file_path(ToEntryFile, EntryMd5Dir, EntryPath),
  directory_file_path(Dir, _, ToEntryFile),
  make_directory_path(Dir),
  gnu_mv(FromEntryFile, ToEntryFile),

  % Store the metadata.
  store_archive_entry(Datadoc, EntryMd5, Entry, EntryPath, EntryProperties).

%! create_entry_hash(+ParentMd5:atom, +EntryPath:atom, -EntryMd5:atom) is det.

create_entry_hash(ParentMd5, EntryPath, EntryMd5):-
  atomic_list_concat([ParentMd5,EntryPath], ' ', Temp),
  rdf_atom_md5(Temp, 1, EntryMd5).

