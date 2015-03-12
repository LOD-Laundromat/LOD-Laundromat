:- module(
  lwm_store_triple,
  [
    store_archive_entry/5, % +Parent:atom
                           % +EntryMd5:atom
                           % +Entry:atom
                           % +EntryPath:atom
                           % +EntryProperties:list(nvpair)
    store_archive_filters/2, % +Datadoc:uri
                             % +ArchiveFilters:list(atom)
    store_end_clean/2, % +Md5:atom
                       % +Datadoc:uri
    store_end_unpack/3, % +Md5:atom
                        % +Datadoc:uri
                        % +Status
    store_exception/2, % +Datadoc:uri
                       % +Status:or([boolean,compound]))
    store_file_extension/2, % +Datadoc:uri
                            % +FileExtension:atom
    store_http/5, % +Datadoc:uri
                  % +Status:nonneg
                  % ?ContentLength:nonneg
                  % ?ContentType:atom
                  % ?LastModified:nonneg
    store_number_of_triples/4, % +Category:atom
                               % +Datadoc:uri
                               % +NumberOfTriples:nonneg
                               % +NumberOfUniqueTriples:nonneg
    store_skip_clean/2, % +Md5:atom
                        % +Datadoc:uri
    store_seedpoint/1, % +Uri:atom
    store_start_clean/1, % +Datadoc:uri
    store_start_unpack/1, % +Datadoc:uri
    store_stream/2, % +Datadoc:uri
                    % +Stream:stream
    store_warning/2 % +Datadoc:uri
                    % +Warning:compound
  ]
).

/** <module> LOD Washing Machine: store triples

Temporarily store triples into the noRDF store.
Whenever a dirty item has been fully cleaned,
the stored triples are sent in a SPARQL Update request
(see module [noRdf_store].

@author Wouter Beek
@version 2014/04-2014/06, 2014/08-2014/09, 2015/01-2015/02
*/

:- use_module(library(lists), except([delete/3,subset/2])).
:- use_module(library(semweb/rdf_db), except([rdf_node/1])).
:- use_module(library(uri)).

:- use_module(plc(prolog/pl_control)).

:- use_module(plXsd(xsd)).
:- use_module(plXsd(dateTime/xsd_dateTime_functions)).

:- use_module(lwm(lwm_debug_message)).
:- use_module(lwm(noRdf_store)).
:- use_module(lwm(store_lod_error)).
:- use_module(lwm(query/lwm_sparql_query)).





%! store_added(+Datadoc:uri, +Md5:atom) is det.
% Datetime at which the seedpoint URI was added to the LOD Basket.

store_added(Datadoc, Md5):-
  get_dateTime_lexical(Added),
  store_triple(Datadoc, llo-added, literal(type(xsd-dateTime,Added))),
  store_triple(Datadoc, llo-md5, literal(type(xsd-string,Md5))),
  post_rdf_triples.



%! store_archive_entry(
%!   +Parent:atom,
%!   +EntryMd5:atom,
%!   +Entry:atom,
%!   +EntryPath:atom,
%!   +EntryProperties:list(nvpair)
%! ) is det.

store_archive_entry(Parent, EntryMd5, Entry, EntryPath, EntryProperties0):-
  store_triple(Entry, rdf-type, llo-'ArchiveEntry'),
  store_triple(Entry, llo-path, literal(type(xsd-string,EntryPath))),

  store_triple(Parent, rdf-type, llo-'Archive'),
  store_triple(Parent, llo-containsEntry, Entry),

  selectchk(format(ArchiveFormat), EntryProperties0, EntryProperties1),
  store_triple(
    Entry,
    llo-archiveFormat,
    literal(type(xsd-string,ArchiveFormat))
  ),

  selectchk(mtime(LastModified), EntryProperties1, EntryProperties2),
  % @tbd Store as xsd:dateTime.
  store_triple(
    Entry,
    llo-archiveLastModified,
    literal(type(xsd-string,LastModified))
  ),

  selectchk(size(ByteSize), EntryProperties2, EntryProperties3),
  store_triple(
    Entry,
    llo-archiveSize,
    literal(type(xsd-nonNegativeInteger,ByteSize))
  ),

  selectchk(filetype(ArchiveFileType), EntryProperties3, []),
  store_triple(
    Entry,
    llo-archiveFileType,
    literal(type(xsd-string,ArchiveFileType))
  ),

  store_added(Entry, EntryMd5).



%! store_archive_filters(+Datadoc:uri, +ArchiveFilters:list(atom)) is det.

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



%! store_end_clean(+Md5:atom, +Datadoc:uri) is det.

store_end_clean(Md5, Datadoc):-
  store_end_clean0(Md5, Datadoc),
  post_rdf_triples.

store_end_clean0(Md5, Datadoc):-
  get_dateTime_lexical(Now),
  store_triple(Datadoc, llo-endClean, literal(type(xsd-dateTime,Now))),

  % Construct the download URL for non-archive files.
  (   datadoc_has_triples(Datadoc)
  ->  atom_concat('/', Md5, Path),
      uri_components(
        Datadump,
        uri_components(http,'download.lodlaundromat.org',Path,_,_)
      ),
      store_triple(Datadoc, void-dataDump, Datadump)
  ;   true
  ).



%! store_end_unpack(
%!   +Md5:atom,
%!   +Datadoc:uri,
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
  get_dateTime_lexical(Now),
  store_triple(Datadoc, llo-endUnpack, literal(type(xsd-dateTime,Now))).



%! store_exception(+Datadoc:uri, +Status:or([boolean,compound])) is det.

% Not an exception.
store_exception(_, true):- !.
% Format exceptions.
store_exception(Datadoc, exception(Error)):-
  store_lod_error(Datadoc, exception, Error).



%! store_file_extension(+Datadoc:uri, +FileExtension:atom) is det.

store_file_extension(Datadoc, FileExtension):-
  store_triple(
    Datadoc,
    llo-fileExtension,
    literal(type(xsd-string,FileExtension))
  ).



%! store_http(
%!   +Datadoc:uri,
%!   +Status:nonneg,
%!   ?ContentLength:nonneg,
%!   ?ContentType:atom,
%!   ?LastModified:nonneg
%! ) is det.

store_http(Datadoc, Status, ContentLength, ContentType, LastModified):-
  atom_number(Status, Status0),
  store_triple(Datadoc, llo-status, httpo-Status0),
  unless(
    ContentLength == '',
    store_triple(
      Datadoc,
      llo-contentLength,
      literal(type(xsd-nonNegativeInteger,ContentLength))
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



%! store_number_of_triples(
%!   +Category:atom,
%!   +Datadoc:uri,
%!   +NumberOfTriples:nonneg,
%!   +NumberOfUniqueTriples:nonneg
%! ) is det.

store_number_of_triples(
  Category,
  Datadoc,
  NumberOfTriples,
  NumberOfUniqueTriples
):-
  % Store the number of unique triples.
  store_triple(
    Datadoc,
    llo-triples,
    literal(type(xsd-nonNegativeInteger,NumberOfUniqueTriples))
  ),

  % Store the number of duplicate triples.
  NumberOfDuplicateTriples is NumberOfTriples - NumberOfUniqueTriples,
  store_triple(
    Datadoc,
    llo-duplicates,
    literal(type(xsd-nonNegativeInteger,NumberOfDuplicateTriples))
  ),

  % DEB
  lwm_debug_message(
    lwm_progress(Category),
    ctriples_written(Category,NumberOfUniqueTriples,NumberOfDuplicateTriples)
  ).



%! store_seedpoint(+Uri:atom) is det.

store_seedpoint(Uri):-
  rdf_atom_md5(Uri, 1, Md5),
  rdf_global_id(ll:Md5, Datadoc),
  (   datadoc_exists(Datadoc)
  ->  true
  ;   store_triple(Datadoc, rdf-type, llo-'URI'),
      store_triple(Datadoc, llo-url, Uri),
      store_added(Datadoc, Md5)
  ).



%! store_skip_clean(+Md5:atom, +Datadoc:uri) is det.

store_skip_clean(Md5, Datadoc):-
  store_start_clean0(Datadoc),
  store_end_clean0(Md5, Datadoc),
  post_rdf_triples.



%! store_start_clean(+Datadoc:uri) is det.

store_start_clean(Datadoc):-
  store_start_clean0(Datadoc),
  post_rdf_triples.

store_start_clean0(Datadoc):-
  get_dateTime_lexical(Now),
  store_triple(Datadoc, llo-startClean, literal(type(xsd-dateTime,Now))).



%! store_start_unpack(+Datadoc:uri) is det.

store_start_unpack(Datadoc):-
  get_dateTime_lexical(Now),
  store_triple(Datadoc, llo-startUnpack, literal(type(xsd-dateTime,Now))),
  post_rdf_triples.



%! store_stream(+Datadoc:uri, +Stream:stream) is det.

store_stream(Datadoc, Stream):-
  stream_property(Stream, position(Position)),

  stream_position_data(byte_count, Position, ByteCount),
  store_triple(
    Datadoc,
    llo-byteCount,
    literal(type(xsd-nonNegativeInteger,ByteCount))
  ),

  stream_position_data(char_count, Position, CharCount),
  store_triple(
    Datadoc,
    llo-charCount,
    literal(type(xsd-nonNegativeInteger,CharCount))
  ),

  stream_position_data(line_count, Position, LineCount),
  store_triple(
    Datadoc,
    llo-lineCount,
    literal(type(xsd-nonNegativeInteger,LineCount))
  ).



%! store_warning(+Datadoc:uri, +Warning:compound) is det.
% @tbd Should we distinguish between `warning` and `error`
%      in the second argument here?

store_warning(Datadoc, message(Term,Kind0,_)):-
  once(rename_kind(Kind0, Kind)),
  store_lod_error(Datadoc, Kind, Term).

rename_kind(error, warning).
rename_kind(Kind, Kind).





% HELPERS %

get_dateTime_lexical(Added0):-
  get_dateTime(Added),
  xsd_canonical_map(xsd:dateTime, Added, Added0).

