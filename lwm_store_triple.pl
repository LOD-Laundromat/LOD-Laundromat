:- module(
  lwm_store_triple,
  [
    store_added/1, % +Datadoc:url
    store_archive_entry/4, % +ParentMd5:atom
                           % +Parent:url
                           % +EntryPath:atom
                           % +EntryProperties:list(nvpair)
    store_archive_filters/2, % +Datadoc:url
                             % +ArchiveFilters:list(atom)
    store_end_clean/2, % +Md5:atom
                       % +Datadoc:url
    store_end_unpack/3, % +Md5:atom
                        % +Datadoc:url
                        % +Status
    store_exception/2, % +Datadoc:url
                       % +Status:or([boolean,compound]))
    store_file_extension/2, % +Datadoc:url
                            % +FileExtension:atom
    store_http/4, % +Datadoc:url
                  % ?ContentLength:nonneg
                  % ?ContentType:atom
                  % ?LastModified:nonneg
    store_new_urls/1, % +Urls:list(atom)
    store_number_of_triples/4, % +Category:atom
                               % +Datadoc:url
                               % +ReadTriples:nonneg
                               % +WrittenTriples:nonneg
    store_skip_clean/1, % +Datadoc:url
    store_warning/2, % +Datadoc:url
                     % +Warning:compound
    store_start_clean/1, % +Datadoc:url
    store_start_unpack/1, % +Datadoc:url
    store_stream/2 % +Datadoc:url
                   % +Stream:stream
  ]
).

/** <module> LOD Washing Machine: store triples

Temporarily store triples into the noRDF store.
Whenever a dirty item has been fully cleaned,
the stored triples are sent in a SPARQL Update request
(see module [noRdf_store].

@author Wouter Beek
@version 2014/04-2014/06, 2014/08-2014/09
*/

:- use_module(library(apply)).
:- use_module(library(lists)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(uri)).
:- use_module(library(semweb/rdf_db)).

:- use_module(pl(pl_control)).
:- use_module(pl(pl_log)).

:- use_module(plXsd_datetime(xsd_dateTime_ext)).

:- use_module(lwm(lwm_debug_message)).
:- use_module(lwm(lwm_sparql_query)).
:- use_module(lwm(noRdf_store)).
:- use_module(lwm(store_lod_error)).



%! store_added(+Datadoc:url) is det.
% Datetime at which the URL was added to the LOD Basket.

store_added(Datadoc):-
  get_dateTime(Added),
  store_triple(Datadoc, llo-added, literal(type(xsd-dateTime,Added))),
  post_rdf_triples.


%! store_archive_entry(
%!   +ParentMd5:atom,
%!   +Parent:url,
%!   +EntryPath:atom,
%!   +EntryProperties:list(nvpair)
%! ) is det.

store_archive_entry(ParentMd5, Parent, EntryPath, EntryProperties1):-
  atomic_list_concat([ParentMd5,EntryPath], ' ', Temp),
  rdf_atom_md5(Temp, 1, EntryMd5),
  rdf_global_id(ll:EntryMd5, Entry),
  store_triple(Entry, rdf-type, llo-'ArchiveEntry'),
  store_triple(Entry, llo-path, literal(type(xsd-string,EntryPath))),

  store_triple(Parent, rdf-type, llo-'Archive'),
  store_triple(Parent, llo-containsEntry, Entry),

  selectchk(mtime(LastModified), EntryProperties1, EntryProperties2),
  % @tbd Store as xsd:dateTime.
  store_triple(
    Entry,
    llo-archiveLastModified,
    literal(type(xsd-integer,LastModified))
  ),

  selectchk(size(ByteSize), EntryProperties2, EntryProperties3),
  store_triple(
    Entry,
    llo-archiveSize,
    literal(type(xsd-integer,ByteSize))
  ),

  selectchk(filetype(ArchiveFileType), EntryProperties3, []),
  store_triple(
    Entry,
    llo-archiveFileType,
    literal(type(xsd-string,ArchiveFileType))
  ),

  store_added(Entry).


%! store_archive_filters(+Datadoc:url, +ArchiveFilters:list(atom)) is det.

store_archive_filters(_, []):- !.
store_archive_filters(Datadoc, ArchiveFilters):-
  rdf_bnode(BNode),
  store_triple(Datadoc, llo-archiveFilters, BNode),
  store_archive_filters0(BNode, ArchiveFilters).

store_archive_filters0(BNode, [H]):- !,
  store_triple(BNode, rdf-first, literal(type(xsd-string,H))),
  store_triple(BNode, rdf-rest, rdf-nil).
store_archive_filters0(BNode1, [H|T]):-
  store_triple(BNode1, rdf-first, literal(type(xsd-string,H))),
  rdf_bnode(BNode2),
  store_triple(BNode1, rdf-rest, BNode2),
  store_archive_filters0(BNode2, T).


%! store_end_clean(+Md5:atom, +Datadoc:url) is det.

store_end_clean(Md5, Datadoc):-
  store_end_clean0(Md5, Datadoc),
  post_rdf_triples.

store_end_clean0(Md5, Datadoc):-
  get_dateTime(Now),
  store_triple(Datadoc, llo-endClean, literal(type(xsd-dateTime,Now))),
  
  % Construct the download URL.
  atom_concat('/', Md5, Path),
  uri_components(
    Datadump,
    uri_components(http,'download.lodlaundromat.org',Path,_,_)
  ),
  
  store_triple(Datadoc, void-dataDump, Datadump).


%! store_end_unpack(
%!   +Md5:atom,
%!   +Datadoc:url,
%!   +Status:or([boolean,compound])
%! ) is det.

% Only unpacking actions with status `true` proceed to cleaning.
store_end_unpack(_, Datadoc, true):- !,
  store_end_unpack0(Datadoc),
  post_rdf_triples.
% Skip cleaning if unpacking failed or throw a critical exception.
store_end_unpack(Md5, Datadoc, Status):-
  store_end_unpack0(Datadoc),
  store_start_clean0(Datadoc),
  store_end_clean0(Md5, Datadoc),
  store_exception(Datadoc, Status),
  post_rdf_triples.

store_end_unpack0(Datadoc):-
  get_dateTime(Now),
  store_triple(Datadoc, llo-endUnpack, literal(type(xsd-dateTime,Now))).


%! store_exception(+Datadoc:url, +Status:or([boolean,compound])) is det.

% Not an exception.
store_exception(_, true):- !.
% Format exceptions.
store_exception(Datadoc, exception(Error)):-
  store_lod_error(Datadoc, exception, Error).


%! store_file_extension(+Datadoc:url, +FileExtension:atom) is det.

store_file_extension(Datadoc, FileExtension):-
  store_triple(
    Datadoc,
    llo-fileExtension,
    literal(type(xsd-string,FileExtension))
  ).


%! store_http(
%!   +Datadoc:url,
%!   ?ContentLength:nonneg,
%!   ?ContentType:atom,
%!   ?LastModified:nonneg
%! ) is det.

store_http(Datadoc, ContentLength, ContentType, LastModified):-
  unless(
    ContentLength == '',
    store_triple(
      Datadoc,
      llo-contentLength,
      literal(type(xsd-integer,ContentLength))
    )
  ),
  unless(
    ContentType == '',
    store_triple(
      Datadoc,
      llo-contentType,
      literal(type(xsd-string,ContentType))
    )
  ),
  % @tbd Store as xsd:dateTime
  unless(
    LastModified == '',
    store_triple(
      Datadoc,
      llo-lastModified,
      literal(type(xsd-string,LastModified))
    )
  ).


%! store_new_urls(+Url:list(atom)) is det.

store_new_urls(Urls):-
  maplist(store_new_url, Urls),
  post_rdf_triples.

store_new_url(Url):-
  rdf_atom_md5(Url, 1, Datadoc),
  store_triple(Datadoc, rdf-type, llo-'URL'),
  store_triple(Datadoc, llo-url, Url),
  get_dateTime(Added),
  store_triple(Datadoc, llo-added, literal(type(xsd-dateTime,Added))).


%! store_number_of_triples(
%!   +Category:atom,
%!   +Datadoc:url,
%!   +ReadTriples:nonneg,
%!   +WrittenTriples:nonneg
%! ) is det.

store_number_of_triples(Category, Datadoc, TIn, TOut):-
  store_triple(Datadoc, llo-triples, literal(type(xsd-integer,TOut))),
  TDup is TIn - TOut,
  store_triple(Datadoc, llo-duplicates, literal(type(xsd-integer,TDup))),
  
  % DEB
  lwm_debug_message(
    lwm_progress(Category),
    ctriples_written(Category,TOut,TDup)
  ).


%! store_skip_clean(+Datadoc:url) is det.

store_skip_clean(Datadoc):-
  store_start_clean0(Datadoc),
  store_end_clean0(Md5, Datadoc),
  post_rdf_triples(Datadoc).


%! store_start_clean(+Datadoc:url) is det.

store_start_clean(Datadoc):-
  store_start_clean0(Datadoc),
  post_rdf_triples(Datadoc).

store_start_clean0(Datadoc):-
  get_dateTime(Now),
  store_triple(Datadoc, llo-startClean, literal(type(xsd-dateTime,Now))).


%! store_start_unpack(+Datadoc:url) is det.

store_start_unpack(Datadoc):-
  get_dateTime(Now),
  store_triple(Datadoc, llo-startUnpack, literal(type(xsd-dateTime,Now))),
  post_rdf_triples.


%! store_stream(+Datadoc:url, +Stream:stream) is det.

store_stream(Datadoc, Stream):-
  stream_property(Stream, position(Position)),

  stream_position_data(byte_count, Position, ByteCount),
  store_triple(Datadoc, llo-byteCount, literal(type(xsd-integer,ByteCount))),

  stream_position_data(char_count, Position, CharCount),
  store_triple(Datadoc, llo-charCount, literal(type(xsd-integer,CharCount))),

  stream_position_data(line_count, Position, LineCount),
  store_triple(Datadoc, llo-lineCount, literal(type(xsd-integer,LineCount))).


%! store_warning(+Datadoc:url, +Warning:compound) is det.
% @tbd Should we distinguish between `warning` and `error`
%      in the second argument here?

store_warning(Datadoc, message(Term,Kind,_)):-
  store_lod_error(Datadoc, Kind, Term).

