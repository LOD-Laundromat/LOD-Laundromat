:- module(
  lwm_unpack,
  [
    lwm_unpack_loop/0
  ]
).

/** <module> LOD Washing Machine: Unpack

Unpacks files for the LOD Washing Machine to clean.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09
*/

:- use_module(library(apply)).
:- use_module(library(lists)).
:- use_module(library(ordsets)).
:- use_module(library(pairs)).

:- use_module(generics(atom_ext)).
:- use_module(generics(uri_ext)).
:- use_module(http(http_download)).
:- use_module(os(archive_ext)).
:- use_module(os(file_ext)).
:- use_module(pl(pl_log)).

:- use_module(lwm(md5)).
:- use_module(lwm(lwm_basket)).
:- use_module(lwm(lwm_messages)).
:- use_module(lwm(lwm_sparql_query)).
:- use_module(lwm(lwm_store_triple)).
:- use_module(lwm(noRdf_store)).

:- dynamic(debug:debug_md5/1).
:- multifile(debug:debug_md5/1).



% Do not unpack more documents if the pending pool is already big enough.
lwm_unpack_loop:-
  flag(number_of_pending_md5s, Id, Id),
  Id > 100, !,
  lwm_unpack_loop.
lwm_unpack_loop:-
  % Pick a new source to process.
  catch(pick_pending(Md5), Exception, var(Exception)), !,

  % DEB
  (   debug:debug_md5(Md5)
  ->  gtrace
  ;   true
  ),

  % Process the URL we picked.
  lwm_unpack(Md5),

  % Intermittent loop.
  lwm_unpack_loop.
% Done for now. Check whether there are new jobs in one seconds.
lwm_unpack_loop:-
  sleep(1),

  % DEB
  (   debugging(lwm_idle_loop(unpack))
  ->  print_message(warning, lwm_idle_loop(unpack))
  ;   true
  ),

  lwm_unpack_loop.


%! lwm_unpack(+Md5:atom) is det.

lwm_unpack(Md5):-
  % DEB
  (   debugging(lwm_progress(unpack))
  ->  print_message(informational, lwm_start(unpack,Md5,Source))
  ;   true
  ),

  run_collect_messages(
    unpack_md5(Md5),
    Status,
    Warnings
  ),

  % DEB
  (   debugging(lwm_progress(unpack))
  ->  print_message(informational, lwm_end(unpack,Md5,Source,Status,Warnings))
  ;   true
  ),

  maplist(store_warning(Md5), Warnings),
  store_end_unpack(Md5, Status).


%! unpack_md5(+Md5:atom) is det.

% The given MD5 denotes an archive entry.
unpack_md5(Md5):-
  md5_archive_entry(Md5, ParentMd5, EntryPath), !,

  % Move the entry file from the parent directory into
  % an MD5 directory of its own.
  md5_directory(ParentMd5, Md5ParentDir),
  relative_file_path(EntryFile1, Md5ParentDir, EntryPath),
  md5_directory(Md5, Md5Dir),
  relative_file_path(EntryFile2, Md5Dir, EntryPath),
  create_file_directory(EntryFile2),
  mv2(EntryFile1, EntryFile2),

  unpack_file(Md5, EntryFile2).
% The given MD5 denotes a URL.
unpack_md5(Md5):-
  md5_url(Md5, Url), !,
  store_triple(ll-Md5, rdf-type, llo-'URL'),

  % Create a directory for the dirty version of the given Md5.
  md5_directory(Md5, Md5Dir),

  % Extracting and store the file extensions from the download URL, if any.
  (   url_file_extension(Url, FileExtension)
  ->  true
  ;   FileExtension = ''
  ),

  % Construct the download file.
  file_name_extension(download, FileExtension, LocalDownloadFile),
  directory_file_path(Md5Dir, LocalDownloadFile, DownloadFile),

  % Download the dirty file for the given Md5.
  lod_accept_header_value(AcceptValue),
  download_to_file(
    Url,
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
  size_file(DownloadFile, ByteSize),
  store_triple(ll-Md5, llo-size, literal(type(xsd-integer,ByteSize))),

  % Store HTTP statistics.
  store_http(Md5, ContentLength, ContentType, LastModified),

  unpack_file(Md5, DownloadFile).

%! unpack_file(+Md5:atom, +ArchiveFile:atom) is det.

unpack_file(Md5, ArchiveFile):-
  % Store the file extension, if any.
  file_name_extension(_, FileExtension, ArchiveFile),
  (   FileExtension == ''
  ->  true
  ;   store_file_extension(Md5, FileExtension)
  ),

  % Extract archive.
  archive_extract(ArchiveFile, _, ArchiveFilters, EntryPairs),
  store_archive_filters(Md5, ArchiveFilters),

  md5_directory(Md5, Md5Dir),
  (
    EntryPairs == []
  ->
    % There is no file for cleaning.
    % To keep the process simple / consistent with other cases,
    % we create an empty dirty file.
    directory_file_path(Md5Dir, dirty, DirtyFile),
    touch_file(DirtyFile)
  ;
    % Exactly one raw file.
    % This file is completely unarchived
    % and should be moved to the cleaning phase.
    EntryPairs = [data-EntryProperties],
    memberchk(format(raw), EntryProperties)
  ->
    % Construct the data file name.
    file_directory_name(ArchiveFile, ArchiveDir),
    directory_file_path(ArchiveDir, data, DataFile),

    % Construct the dirty file name.
    directory_file_path(Md5Dir, dirty, DirtyFile),

    % Move the data file outside of the its entry path,
    % and put it directly inside its MD5 directory.
    mv2(DataFile, DirtyFile)

    % The file is now ready for cleaning!
  ;
    % Store the archive entries for future processing.
    pairs_keys_values(EntryPairs, EntryPaths, EntryProperties1),

    % Store the archive format.
    filter_archive_formats(
      EntryProperties1,
      ArchiveFormats,
      EntryProperties2
    ),
    distill_archive_format(ArchiveFormats, ArchiveFormat),
    store_triple(
      ll-Md5,
      llo-archiveFormat,
      literal(type(xsd-string,ArchiveFormat))
    ),

    maplist(store_archive_entry(Md5), EntryPaths, EntryProperties2),
    store_skip_clean(Md5)
  ),
  % Remove the archive file.
  delete_file(ArchiveFile).



% Helpers

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


%! lod_accept_header_value(-Value:atom) is det.

lod_accept_header_value(Value):-
  findall(
    Value,
    (
      lod_content_type(ContentType, Q),
      format(atom(Value), '~a; q=~1f', [ContentType,Q])
    ),
    Values
  ),
  atomic_list_concat(Values, ', ', Value).


%! lod_content_type(?ContentType:atom, ?QValue:between(0.0,1.0)) is nondet.

% RDFa
lod_content_type('text/html',              0.3).
% N-Quads
lod_content_type('application/n-quads',    0.8).
% N-Triples
lod_content_type('application/n-triples',  0.8).
% RDF/XML
lod_content_type('application/rdf+xml',    0.7).
lod_content_type('text/rdf+xml',           0.7).
lod_content_type('application/xhtml+xml',  0.3).
lod_content_type('application/xml',        0.3).
lod_content_type('text/xml',               0.3).
lod_content_type('application/rss+xml',    0.5).
% Trig
lod_content_type('application/trig',       0.8).
lod_content_type('application/x-trig',     0.5).
% Turtle
lod_content_type('text/turtle',            0.9).
lod_content_type('application/x-turtle',   0.5).
lod_content_type('application/turtle',     0.5).
lod_content_type('application/rdf+turtle', 0.5).
% N3
lod_content_type('text/n3',                0.8).
lod_content_type('text/rdf+n3',            0.5).
% All
lod_content_type('*/*',                    0.1).

