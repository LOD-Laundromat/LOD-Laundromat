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
:- use_module(library(ordsets)).
:- use_module(library(pairs)).

:- use_module(generics(atom_ext)).
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
  sleep(60),
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

  % DEB: *end* of downloading+unpacking.
  lwm_debug_message(
    lwm_progress(unpack),
    lwm_end(unpack,Md5,Source,Status,Warnings)
  ),

  % Store the warnings and status as metadata.
  maplist(store_warning(Datadoc), Warnings),
  store_end_unpack(Md5, Datadoc, Status).


%! unpack_datadoc(+Md5:atom, +Datadoc:iri, ?DirtyUrl:uri) is det.

% The given MD5 denotes an archive entry.
unpack_datadoc(Md5, Datadoc, DirtyUrl):-
  % Uninstantiated SPARQL variable (using keyword OPTIONAL).
  DirtyUrl == '$null$', !,
  
  % Create a directory for the entry.
  md5_directory(Md5, Md5Dir),
  
  % Move the entry file over from the partent Md5 directory
  % to its own directory.
  datadoc_archive_entry(Datadoc, ParentMd5, EntryPath),
  md5_directory(ParentMd5, ParentMd5Dir),
  relative_file_path(OldEntryFile, ParentMd5Dir, EntryPath),
  relative_file_path(EntryFile, Md5Dir, EntryPath),
  create_file_directory(EntryFile),
  gnu_mv(OldEntryFile, EntryFile),
  
  % Continue with unpacking the entry.
  unpack_file(Md5, Md5Dir, Datadoc, EntryFile).
% The given MD5 denotes a URL.
unpack_datadoc(Md5, Datadoc, DirtyUrl):-
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
      directory_file_path(Md5Dir, data, DataFile),
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
      pairs_keys_values(EntryPairs, EntryPaths, EntryProperties1),

      % DEB
      (   debugging(lwm_unpack)
      ->  length(EntryPairs, NumberOfEntries),
          debug(lwm_unpack, '[~a], ~D entries', [Md5,NumberOfEntries])
      ;   true
      ),

      % Store the archive format.
      filter_archive_formats(
        EntryProperties1,
        ArchiveFormats,
        EntryProperties2
      ),
      distill_archive_format(ArchiveFormats, ArchiveFormat),
      store_triple(
        Datadoc,
        llo-archiveFormat,
        literal(type(xsd-string,ArchiveFormat))
      ),

      % Create the archive entries
      % and copy the entry files to their own MD5 dirs.
      maplist(
        store_archive_entry(Md5, Datadoc),
        EntryPaths,
        EntryProperties2
      ),

      % Archives cannot be cleaned, so
      store_skip_clean(Md5, Datadoc)
  ),

  % Remove the archive file.
  delete_file(ArchiveFile).





% HELPERS %

%! distill_archive_format(+Formats:ordset(atom), -Format:atom) is det.

distill_archive_format([H0], H):- !,
  strip_atom([' '], H0, H).
distill_archive_format([H1,H2|T], Format):-
  common_atom_prefix(H1, H2, Prefix),
  distill_archive_format([Prefix|T], Format).



%! filter_archive_formats(
%!   +Lists1:list(list(nvpair)),
%!   -Formats:ordset(atom),
%!   -Lists2:list(list(nvpair))
%! ) is det.

filter_archive_formats([], [], []).
filter_archive_formats([L1|Ls1], Fs1, [L2|Ls2]):-
  selectchk(format(F), L1, L2),
  filter_archive_formats(Ls1, Fs2, Ls2),
  ord_add_element(Fs2, F, Fs1).

