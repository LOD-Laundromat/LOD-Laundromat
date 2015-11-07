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
@version 2015/11
*/

:- use_module(library(apply)).
:- use_module(library(dcg/dcg_debug)).
:- use_module(library(debug_ext)).
:- use_module(library(http/http_info)).
:- use_module(library(lists)).
:- use_module(library(rdf/rdf_download)).

:- use_module('LOD-Laundromat'(md5)).
:- use_module('LOD-Laundromat'(lwm_debug_message)).
:- use_module('LOD-Laundromat'(lwm_store_triple)).
:- use_module('LOD-Laundromat'(noRdf_store)).
:- use_module('LOD-Laundromat'(query/lwm_sparql_enum)).
:- use_module('LOD-Laundromat'(query/lwm_sparql_query)).

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
      % `Download` is only instantiated if `Document` is not an archive entry.
      pending(Document, Download),
      % Update the database, saying we are ready
      % to begin downloading+unpacking this data document.
      store_start_unpack(Document)
    )),
    W,
    var(E)
  ),
  lwm_unpack(Document, DirtyIn),
  % Intermittent loop.
  lwm_unpack_loop.
% Done for now. Check whether there are new jobs in one seconds.
lwm_unpack_loop:-
  sleep(100),
  lwm_debug_message(lwm_idle_loop(unpack)), % DEB
  lwm_unpack_loop.



%! lwm_unpack(+Document:iri) is det.
% Manual way of downloading+unpacking a data document.

lwm_unpack(Document):-
  % Update the database, saying we are ready
  % to begin downloading+unpacking this data document.
  store_start_unpack(Document),
  datadoc_location(Document, Download),
  lwm_unpack(Document, Download).



%! lwm_unpack(+Document:iri, +Download:iri) is det.

lwm_unpack(Document, Download):-
  % Allow the debugger to pop up for specific MD5s.
  (   lwm_debugging,
      rdf_global_id(ll:Md5, Document),
      debug:debug_md5(Md5, unpack)
  ->  gtrace
  ;   true
  ),

  % DEB: *start* of downloading+unpacking..
  lwm_debug_message(
    lwm_progress(unpack),
    lwm_start(unpack,Document,Source)
  ),

  % Downloading+unpacking of a specific data document.
  call_collect_messages(
    unpack_datadoc(Document, Download, ArchiveFile),
    Status,
    Warnings
  ),
  (   Status == false
  ->  dcg_debug(lwm(unpack(status), ("[UNPACKING FAILED] ", document_name(Document)))
  ;   Status == true
  ->  true
  ;   debug(lwm(unpack(status)), "[STATUS] ~w", [Status])
  ),

  % DEB: *end* of downloading+unpacking.
  lwm_debug_message(
    lwm(unpack(progress)),
    lwm_end(unpack,Source,Status,Warnings)
  ),

  % Store the warnings and status as metadata.
  maplist(store_warning(Document), Warnings),
  store_end_unpack(Document, Status),

  % Remove the archive file.
  (ground(ArchiveFile) -> delete_file(ArchiveFile) ; true).


%! unpack_datadoc(+Document:iri, ?Download:iri, -File:atom) is det.

% We are dealing with an archive entry.
unpack_datadoc(Document, Download, File):-
  % Uninstantiated SPARQL variable (due to the use of the `OPTIONAL` keyword).
  Download == '$null$', !,

  % Entries occur in a path.
  document_directory(Document, Dir),
  document_archive_entry(Document, EntryPath),
  relative_file_path(File, Dir, EntryPath),

  % Further unpack the archive entry.
  unpack_file(Dir, Document, File).
% We are dealing with an IRI.
unpack_datadoc(Document, Download, File):-
  document_directory(Document, Dir),

  % Extract and store the file extensions from the download IRI, if any.
  (uri_file_extension(Download, Ext) -> true ; Ext = ""),

  % Construct the download file.
  file_name_extension(download, Ext, Base),
  directory_file_path(Dir, Base, File),

  % Download the dirty file of the document.
  rdf_download(Download, File, [freshness_lifetime(0.0),metadata(M)]),
  
  % Store the file size of the dirty file.
  size_file(File, Size),
  store_triple(
    Document,
    llo-downloadSize,
    literal(type(xsd-nonNegativeInteger,Size))
  ),

  % Store HTTP statistics.
  store_http(Document, M),

  % Process the HTTP status code.
  (   is_http_error(Status)
  ->  throw(error(http_status(Status), lwm_unpack:unpack_datadoc/4))
  ;   unpack_file(Dir, Document, File)
  ).


%! unpack_file(+Directory:atom, +Document:iri, +ArchiveFile:atom) is det.

unpack_file(Dir, Document, ArchiveFile):-
  % Store the file extension, if any.
  file_name_extension(_, Ext, ArchiveFile),
  (Ext == "" -> true ; store_file_extension(Document, Ext)),

  % Extract archive.
  archive_extract(ArchiveFile, _, ArchiveFilters, EntryPairs),
  store_archive_filters(Document, ArchiveFilters),

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
        Document,
        llo-unpackedSize,
        literal(type(xsd-nonNegativeInteger,UnpackedSize))
      )
  ;   % Case 3: The file is a proper archive
      %         containing a number of entries.

      % Create the archive entries
      % and copy the entry files to their own MD5 dirs.
      list_script(
        process_entry_pair(Md5, Md5Dir, Document),
        EntryPairs,
        [message('LWM ArchiveEntry')]
      ),

      % Archives cannot be cleaned,
      % so skip the cleaning phase by using metadata.
      store_skip_clean(Md5, Document)
  ).

%! process_entry_pair(
%!   +ParentMd5:atom,
%!   +ParentMd5Dir:atom,
%!   +Document:atom,
%!   -EntryPair:pair(atom,list(nvpair))
%! ) is det.

process_entry_pair(_, _, _, _-EntryProperties):-
  memberchk(filetype(directory), EntryProperties), !.
process_entry_pair(
  ParentMd5,
  ParentMd5Dir,
  Document,
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
  store_archive_entry(Document, EntryMd5, Entry, EntryPath, EntryProperties).

%! create_entry_hash(+ParentMd5:atom, +EntryPath:atom, -EntryMd5:atom) is det.

create_entry_hash(ParentMd5, EntryPath, EntryMd5):-
  atomic_list_concat([ParentMd5,EntryPath], ' ', Temp),
  rdf_atom_md5(Temp, 1, EntryMd5).
