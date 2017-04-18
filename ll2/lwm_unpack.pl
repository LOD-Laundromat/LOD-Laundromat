:- module(
  lwm_unpack,
  [
    lwm_unpack/1, % +Document:iri
    lwm_unpack_loop/0
  ]
).

/** <module> LOD Washing Machine: Unpack

Unpacks files for the LOD Washing Machine to clean.

@author Wouter Beek
@version 2015/11, 2016/01
*/

:- use_module(library(apply)).
:- use_module(library(dcg/dcg_debug)).
:- use_module(library(debug_ext)).
:- use_module(library(http/http_ext)).
:- use_module(library(lists)).
:- use_module(library(lodapi/lodapi_generics)).
:- use_module(library(rdf/rdf__io)).

:- use_module(lwm_debug_message).
:- use_module(lwm_store_triple).
:- use_module(noRdf_store).

:- dynamic
    debug:debug_md5/2.
:- multifile
    debug:debug_md5/2.





%! lwm_unpack_loop is det.

lwm_unpack_loop:-
  % Pick a new source to process.
  % If some exception is thrown here, the catch/3 makes it
  % silently fail. This way, the unpacking thread is able
  % to wait in case a SPARQL endpoint is temporarily down.
  catch(
    with_mutex(lwm_endpoint_access, (
      % `Download` is only instantiated if `Document` is not an archive entry.
      pending(Doc, Origin),
      % Update the database, saying we are ready
      % to begin downloading+unpacking this data document.
      store_start_unpack(Doc)
    )),
    E,
    var(E)
  ),
  lwm_unpack(Doc, Origin),
  % Intermittent loop.
  lwm_unpack_loop.
% Done for now. Check whether there are new jobs in one seconds.
lwm_unpack_loop:-
  sleep(100),
  increment_counter(number_of_idle_loops(Category), N),
  "[IDLE ", category(Category), "] ", thousands(N).
  dcg_debug(unpack(idle), idle_loop(unpack)),
  lwm_unpack_loop.



%! lwm_unpack(+Document:iri) is det.
% Manual way of downloading+unpacking a data document.

lwm_unpack(Doc):-
  % Update the database, saying we are ready
  % to begin downloading+unpacking this data document.
  store_start_unpack(Doc),
  datadoc_original_location(Doc, Origin),
  lwm_unpack(Doc, Origin).



%! lwm_unpack(+Document:iri, +Download:iri) is det.

lwm_unpack(Doc, Origin):-
  % DEBUG: Allow the debugger to pop up for specific MD5s.
  if_debug(lwm(unpack), (
    document_name(Doc, Md5),
    debug:debug_md5(Md5, unpack)
  )),

  % DEBUG: mention that downloading+unpacking has started.
  dcg_debug(lwm(progress(unpack)), start_process(unpack, Doc, Origin)),

  % Downloading+unpacking of a specific data document.
  call_collect_messages(unpack_datadoc(Doc, Origin, ArchiveFile), Result, Warnings),

  % DEB: *end* of downloading+unpacking.
  dcg_debug(lwm(unpack(progress)),
    end_process(unpack, Doc, Origin, Result, Warnings)
  ),

  % Store the warnings and status as metadata.
  maplist(store_warning(Doc), Warnings),
  store_end_unpack(Doc, Result),

  % Remove the archive file.
  (ground(ArchiveFile) -> delete_file(ArchiveFile) ; true).


%! unpack_datadoc(+Document:iri, ?Origin:url, -File:atom) is det.

% We are dealing with an archive entry.
unpack_datadoc(Doc, Uri, File):-
  % Uninstantiated SPARQL variable (due to the use of the `OPTIONAL` keyword).
  Uri == '$null$', !,

  % Entries occur in a path.
  lwm_document_dir(Doc, Dir),
  document_archive_entry(Doc, EntryPath),
  relative_file_path(File, Dir, EntryPath),

  % Further unpack the archive entry.
  unpack_file(Dir, Doc, File).
% We are dealing with a URL.
unpack_datadoc(Doc, Uri, File):-
  lwm_document_dir(Doc, Dir),

  % Extract and store the file extensions from the download IRI, if any.
  (uri_file_extension(Uri, Ext) -> true ; Ext = ""),

  % Construct the download file.
  file_name_extension(download, Ext, Base),
  directory_file_path(Dir, Base, File),

  % Download the dirty file of the document.
  rdf_download_to_file(Uri, File, [freshness_lifetime(0.0),metadata(M)], []),
  
  % Store the file size of the dirty file.
  size_file(File, Size),
  store_triple(Doc, llo-downloadSize, literal(type(xsd-nonNegativeInteger,Size))),

  % Store HTTP statistics.
  store_http(Doc, M),

  % Process the HTTP status code.
  (   is_http_error(M.http.status_code)
  ->  throw(error(http_status(M.http.status_code), lwm_unpack:unpack_datadoc/4))
  ;   unpack_file(Dir, Doc, File)
  ).


%! unpack_file(+Directory:atom, +Document:iri, +ArchiveFile:atom) is det.

unpack_file(Dir, Doc, ArchiveFile):-
  % Store the file extension, if any.
  file_name_extension(_, Ext, ArchiveFile),
  (Ext == "" -> true ; store_file_extension(Doc, Ext)),

  % Extract archive.
  archive_extract(ArchiveFile),
  store_archive_filters(Doc, ArchiveFilters),

  (   % Case 1: There is no archive file to unpack.
      EntryPairs == []
  ->  % To keep the process simple / consistent with other cases,
      % we create an empty dirty file
      % (acting as the 'content' of the absent archive file.
      directory_file_path(Dir, dirty, DirtyFile),
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
      directory_file_path(Dir, dirty, DirtyFile),
      gnu_mv(DataFile, DirtyFile),

      % Store the size of the dirty data file after unpacking.
      size_file(DirtyFile, UnpackedSize),
      store_triple(
        Doc,
        llo-unpackedSize,
        literal(type(xsd-nonNegativeInteger,UnpackedSize))
      )
  ;   % Case 3: The file is a proper archive
      %         containing a number of entries.

      % Create the archive entries
      % and copy the entry files to their own MD5 directories.
      list_script(
        process_entry_pair(Md5, Dir, Doc),
        EntryPairs,
        [message('LWM ArchiveEntry')]
      ),

      % Archives cannot be cleaned,
      % so skip the cleaning phase by using metadata.
      store_skip_clean(Md5, Doc)
  ).

%! process_entry_pair(
%!   +ParentMd5:atom,
%!   +ParentMd5Dir:atom,
%!   +Document:atom,
%!   -EntryPair:pair(atom,list(pair))
%! ) is det.

process_entry_pair(_, _, _, _-EntryProperties):-
  memberchk(filetype(directory), EntryProperties), !.
process_entry_pair(
  ParentMd5,
  ParentMd5Dir,
  Doc,
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
  store_archive_entry(Doc, Entry, EntryPath, EntryProperties).


%! create_entry_hash(+ParentMd5:atom, +EntryPath:atom, -EntryMd5:atom) is det.

create_entry_hash(ParentMd5, EntryPath, EntryMd5):-
  atomic_list_concat([ParentMd5,EntryPath], ' ', Temp),
  rdf_atom_md5(Temp, 1, EntryMd5).
