:- module(
  lwm_store_triple,
  [
    store_added/1, % +Md5:atom
    store_archive_entry/3, % +ParentMd5:atom
                           % +EntryPath:atom
                           % +EntryProperties:list(nvpair)
    store_archive_filters/2, % +Md5:atom
                             % +ArchiveFilters:list(atom)
    store_end_clean/1, % +Md5:atom
    store_end_unpack/2, % +Md5:atom
                        % +Status
    store_exception/2, % +Md5:atom
                       % +Status:or([boolean,compound]))
    store_file_extension/2, % +Md5:atom
                            % +FileExtension:atom
    store_http/4, % +Md5:atom
                  % ?ContentLength:nonneg
                  % ?ContentType:atom
                  % ?LastModified:nonneg
    store_new_urls/1, % +Urls:list(atom)
    store_number_of_triples/4, % +Category:atom
                               % +Md5:atom
                               % +ReadTriples:nonneg
                               % +WrittenTriples:nonneg
    store_skip_clean/1, % +Md5:atom
    store_warning/2, % +Md5:atom
                     % +Warning:compound
    store_start_clean/1, % +Md5:atom
    store_start_unpack/1, % +Md5:atom
    store_stream/2 % +Md5:atom
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



%! store_added(+Md5:atom) is det.
% Datetime at which the URL was added to the LOD Basket.

store_added(Md5):-
  get_dateTime(Added),
  store_triple(ll-Md5, llo-added, literal(type(xsd-dateTime,Added))),
  post_rdf_triples(Md5).


%! store_archive_entry(
%!   +ParentMd5:atom,
%!   +EntryPath:atom,
%!   +EntryProperties:list(nvpair)
%! ) is det.

store_archive_entry(ParentMd5, EntryPath, EntryProperties1):-
  atomic_list_concat([ParentMd5,EntryPath], ' ', Temp),
  rdf_atom_md5(Temp, 1, EntryMd5),
  store_triple(ll-EntryMd5, rdf-type, llo-'ArchiveEntry'),
  store_triple(ll-EntryMd5, llo-md5, literal(type(xsd-string,EntryMd5))),
  store_triple(ll-EntryMd5, llo-path, literal(type(xsd-string,EntryPath))),

  store_triple(ll-ParentMd5, rdf-type, llo-'Archive'),
  store_triple(ll-ParentMd5, llo-containsEntry, ll-EntryMd5),

  selectchk(mtime(LastModified), EntryProperties1, EntryProperties2),
  % @tbd Store as xsd:dateTime.
  store_triple(
    ll-EntryMd5,
    llo-archiveLastModified,
    literal(type(xsd-integer,LastModified))
  ),

  selectchk(size(ByteSize), EntryProperties2, EntryProperties3),
  store_triple(
    ll-EntryMd5,
    llo-archiveSize,
    literal(type(xsd-integer,ByteSize))
  ),

  selectchk(filetype(ArchiveFileType), EntryProperties3, []),
  store_triple(
    ll-EntryMd5,
    llo-archiveFileType,
    literal(type(xsd-string,ArchiveFileType))
  ),

  store_added(EntryMd5).


%! store_archive_filters(+Md5:atom, +ArchiveFilters:list(atom)) is det.

store_archive_filters(_, []):- !.
store_archive_filters(Md5, ArchiveFilters):-
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-archiveFilters, BNode),
  store_archive_filters0(BNode, ArchiveFilters).

store_archive_filters0(BNode, [H]):- !,
  store_triple(BNode, rdf-first, literal(type(xsd-string,H))),
  store_triple(BNode, rdf-rest, rdf-nil).
store_archive_filters0(BNode1, [H|T]):-
  store_triple(BNode1, rdf-first, literal(type(xsd-string,H))),
  rdf_bnode(BNode2),
  store_triple(BNode1, rdf-rest, BNode2),
  store_archive_filters0(BNode2, T).


%! store_end_clean(+Md5:atom) is det.

store_end_clean(Md5):-
  store_end_clean0(Md5),
  post_rdf_triples(Md5).

store_end_clean0(Md5):-
  get_dateTime(Now),
  store_triple(ll-Md5, llo-endClean, literal(type(xsd-dateTime,Now))),
  atom_concat('/', Md5, Path),
  uri_components(
    Datadump,
    uri_components(http,'download.lodlaundromat.org',Path,_,_)
  ),
  store_triple(ll-Md5, void-dataDump, Datadump).


%! store_end_unpack(+Md5:atom, +Status:or([boolean,compound])) is det.

% Only unpacking actions with status `true` proceed to cleaning.
store_end_unpack(Md5, true):- !,
  store_end_unpack0(Md5),
  post_rdf_triples(Md5).
% Skip cleaning if unpacking failed or throw a critical exception.
store_end_unpack(Md5, Status):-
  store_end_unpack0(Md5),
  store_start_clean0(Md5),
  store_end_clean0(Md5),
  store_exception(Md5, Status),
  post_rdf_triples(Md5).

store_end_unpack0(Md5):-
  get_dateTime(Now),
  store_triple(ll-Md5, llo-endUnpack, literal(type(xsd-dateTime,Now))).


%! store_exception(+Md5:atom, +Status:or([boolean,compound])) is det.

% Not an exception.
store_exception(_, true):- !.
% Format exceptions.
store_exception(Md5, exception(Error)):-
  store_lod_error(Md5, exception, Error).


%! store_file_extension(+Md5:atom, +FileExtension:atom) is det.

store_file_extension(Md5, FileExtension):-
  store_triple(
    ll-Md5,
    llo-fileExtension,
    literal(type(xsd-string,FileExtension))
  ).


%! store_http(
%!   +Md5:atom,
%!   ?ContentLength:nonneg,
%!   ?ContentType:atom,
%!   ?LastModified:nonneg
%! ) is det.

store_http(Md5, ContentLength, ContentType, LastModified):-
  unless(
    ContentLength == '',
    store_triple(
      ll-Md5,
      llo-contentLength,
      literal(type(xsd-integer,ContentLength))
    )
  ),
  unless(
    ContentType == '',
    store_triple(
      ll-Md5,
      llo-contentType,
      literal(type(xsd-string,ContentType))
    )
  ),
  % @tbd Store as xsd:dateTime
  unless(
    LastModified == '',
    store_triple(
      ll-Md5,
      llo-lastModified,
      literal(type(xsd-string,LastModified))
    )
  ).


%! store_new_urls(+Url:list(atom)) is det.

store_new_urls(Urls):-
  maplist(store_new_url, Urls),
  store_added(Md5).

store_new_url(Url):-
  rdf_atom_md5(Url, 1, Md5),
  store_triple(ll-Md5, rdf-type, llo-'URL'),
  store_triple(ll-Md5, llo-md5, literal(type(xsd-string,Md5))),
  store_triple(ll-Md5, llo-url, Url).


%! store_number_of_triples(
%!   +Category:atom,
%!   +Md5:atom,
%!   +ReadTriples:nonneg,
%!   +WrittenTriples:nonneg
%! ) is det.

store_number_of_triples(Category, Md5, TIn, TOut):-
  store_triple(ll-Md5, llo-triples, literal(type(xsd-integer,TOut))),
  TDup is TIn - TOut,
  store_triple(ll-Md5, llo-duplicates, literal(type(xsd-integer,TDup))),
  
  % DEB
  lwm_debug_message(
    lwm_progress(Category),
    ctriples_written(Category,TOut,TDup)
  ).


%! store_skip_clean(+Md5:atom) is det.

store_skip_clean(Md5):-
  store_start_clean0(Md5),
  store_end_clean0(Md5),
  post_rdf_triples(Md5).


%! store_start_clean(+Md5:atom) is det.

store_start_clean(Md5):-
  store_start_clean0(Md5),
  post_rdf_triples(Md5).

store_start_clean0(Md5):-
  get_dateTime(Now),
  store_triple(ll-Md5, llo-startClean, literal(type(xsd-dateTime,Now))).


%! store_start_unpack(+Md5:atom) is det.

store_start_unpack(Md5):-
  get_dateTime(Now),
  store_triple(ll-Md5, llo-startUnpack, literal(type(xsd-dateTime,Now))),
  post_rdf_triples(Md5).


%! store_stream(+Md5:atom, +Stream:stream) is det.

store_stream(Md5, Stream):-
  stream_property(Stream, position(Position)),

  stream_position_data(byte_count, Position, ByteCount),
  store_triple(ll-Md5, llo-byteCount, literal(type(xsd-integer,ByteCount))),

  stream_position_data(char_count, Position, CharCount),
  store_triple(ll-Md5, llo-charCount, literal(type(xsd-integer,CharCount))),

  stream_position_data(line_count, Position, LineCount),
  store_triple(ll-Md5, llo-lineCount, literal(type(xsd-integer,LineCount))).


%! store_warning(+Md5:atom, +Warning:compound) is det.
% @tbd Should we distinguish between `warning` and `error`
%      in the second argument here?

store_warning(Md5, message(Term,Kind,_)):-
  store_lod_error(Md5, Kind, Term).

