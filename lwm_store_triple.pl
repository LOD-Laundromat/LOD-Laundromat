:- module(
  lwm_store_triple,
  [
    store_archive_entry/5, % +Parent:atom
                           % +EntryMd5:atom
                           % +Entry:atom
                           % +EntryPath:atom
                           % +EntryProperties:list(nvpair)
    store_archive_filters/2, % +Document:iri
                             % +ArchiveFilters:list(atom)
    store_end_clean/2, % +Md5:atom
                       % +Document:iri
    store_end_unpack/3, % +Md5:atom
                        % +Document:iri
                        % +Status
    store_exception/2, % +Document:iri
                       % +Status:or([boolean,compound]))
    store_file_extension/2, % +Document:iri
                            % +FileExtension:atom
    store_http/5, % +Document:iri
                  % +Status:nonneg
                  % ?ContentLength:nonneg
                  % ?ContentType:atom
                  % ?LastModified:nonneg
    store_number_of_triples/4, % +Category:atom
                               % +Document:iri
                               % +NumberOfTriples:nonneg
                               % +NumberOfUniqueTriples:nonneg
    store_skip_clean/2, % +Md5:atom
                        % +Document:iri
    store_seedpoint/1, % +Iri:atom
    store_start_clean/1, % +Document:iri
    store_start_unpack/1, % +Document:iri
    store_stream/2, % +Document:iri
                    % +Stream:stream
    store_warning/2 % +Document:iri
                    % +Warning:compound
  ]
).

/** <module> LOD Washing Machine: store triples

Temporarily store triples into the noRDF store.
Whenever a dirty item has been fully cleaned,
the stored triples are sent in a SPARQL Update request
(see module [noRdf_store].

@author Wouter Beek
@version 2014/04-2014/06, 2014/08-2014/09, 2015/01-2015/02, 2015/06
*/

:- use_module(library(lists), except([delete/3,subset/2])).
:- use_module(library(semweb/rdf_db), except([rdf_node/1])).
:- use_module(library(uri)).

:- use_module(plc(os/date_ext)).
:- use_module(plc(prolog/pl_control)).

:- use_module(plXsd(xsd)).
:- use_module(plXsd(dateTime/xsd_dateTime_functions)).

:- use_module(lwm(lwm_debug_message)).
:- use_module(lwm(noRdf_store)).
:- use_module(lwm(store_lod_error)).
:- use_module(lwm(query/lwm_sparql_query)).





%! store_added(+Document:iri, +Md5:atom) is det.
% Datetime at which the seedpoint IRI was added to the LOD Basket.

store_added(Document, Md5):-
  get_dateTime_lexical(Added),
  store_triple(Document, llo-added, literal(type(xsd-dateTime,Added))),
  store_triple(Document, llo-md5, literal(type(xsd-string,Md5))),
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



%! store_archive_filters(+Document:iri, +ArchiveFilters:list(atom)) is det.

store_archive_filters(_, []):- !.
store_archive_filters(Document, ArchiveFilters):-
  rdf_bnode(BNode),
  store_triple(Document, llo-archiveFilters, BNode),
  store_archive_filters0(BNode, ArchiveFilters).

store_archive_filters0(BNode, [H]):- !,
  store_triple(BNode, rdf-first, literal(type(xsd-string,H))),
  store_triple(BNode, rdf-rest, rdf-nil).
store_archive_filters0(BNode1, [H|T]):-
  store_triple(BNode1, rdf-first, literal(type(xsd-string,H))),
  rdf_bnode(BNode2),
  store_triple(BNode1, rdf-rest, BNode2),
  store_archive_filters0(BNode2, T).



%! store_end_clean(+Md5:atom, +Document:iri) is det.

store_end_clean(Md5, Document):-
  store_end_clean0(Md5, Document),
  post_rdf_triples.

store_end_clean0(Md5, Document):-
  get_dateTime_lexical(Now),
  store_triple(Document, llo-endClean, literal(type(xsd-dateTime,Now))),

  % Construct the download URL for non-archive files.
  (   datadoc_has_triples(Document)
  ->  atom_concat('/', Md5, Path),
      uri_components(
        Datadump,
        uri_components(http,'download.lodlaundromat.org',Path,_,_)
      ),
      store_triple(Document, void-dataDump, Datadump)
  ;   true
  ).



%! store_end_unpack(
%!   +Md5:atom,
%!   +Document:iri,
%!   +Status:or([boolean,compound])
%! ) is det.

% Only unpacking actions with status `true` proceed to cleaning.
store_end_unpack(_, Document, true):- !,
  store_end_unpack0(Document),
  post_rdf_triples.
% Skip cleaning if unpacking failed or throw a critical exception.
store_end_unpack(Md5, Document, Status):-
  store_end_unpack0(Document),
  store_start_clean0(Document),
  store_end_clean0(Md5, Document),
  store_exception(Document, Status),
  post_rdf_triples.

store_end_unpack0(Document):-
  get_dateTime_lexical(Now),
  store_triple(Document, llo-endUnpack, literal(type(xsd-dateTime,Now))).



%! store_exception(+Document:iri, +Status:or([boolean,compound])) is det.

% Not an exception.
store_exception(_, true):- !.
% Format exceptions.
store_exception(Document, exception(Error)):-
  store_lod_error(Document, exception, Error).



%! store_file_extension(+Document:iri, +FileExtension:atom) is det.

store_file_extension(Document, FileExtension):-
  store_triple(
    Document,
    llo-fileExtension,
    literal(type(xsd-string,FileExtension))
  ).



%! store_http(
%!   +Document:iri,
%!   +Status:nonneg,
%!   ?ContentLength:nonneg,
%!   ?ContentType:atom,
%!   ?LastModified:nonneg
%! ) is det.

store_http(Document, Status, ContentLength, ContentType, LastModified):-
  atom_number(Status, Status0),
  store_triple(Document, llo-status, httpo-Status0),
  unless(
    ContentLength == '',
    store_triple(
      Document,
      llo-contentLength,
      literal(type(xsd-nonNegativeInteger,ContentLength))
    )
  ),
  unless(
    ContentType == '',
    store_triple(
      Document,
      llo-contentType,
      literal(type(xsd-string,ContentType))
    )
  ),
  % @tbd Store as xsd:dateTime
  unless(
    LastModified == '',
    store_triple(
      Document,
      llo-lastModified,
      literal(type(xsd-string,LastModified))
    )
  ).



%! store_number_of_triples(
%!   +Category:atom,
%!   +Document:iri,
%!   +NumberOfTriples:nonneg,
%!   +NumberOfUniqueTriples:nonneg
%! ) is det.

store_number_of_triples(
  Category,
  Document,
  NumberOfTriples,
  NumberOfUniqueTriples
):-
  % Store the number of unique triples.
  store_triple(
    Document,
    llo-triples,
    literal(type(xsd-nonNegativeInteger,NumberOfUniqueTriples))
  ),

  % Store the number of duplicate triples.
  NumberOfDuplicateTriples is NumberOfTriples - NumberOfUniqueTriples,
  store_triple(
    Document,
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
  rdf_global_id(ll:Md5, Document),
  (   datadoc_exists(Document)
  ->  true
  ;   store_triple(Document, rdf-type, llo-'URI'),
      store_triple(Document, llo-url, Uri),
      store_added(Document, Md5)
  ).



%! store_skip_clean(+Md5:atom, +Document:iri) is det.

store_skip_clean(Md5, Document):-
  store_start_clean0(Document),
  store_end_clean0(Md5, Document),
  post_rdf_triples.



%! store_start_clean(+Document:iri) is det.

store_start_clean(Document):-
  store_start_clean0(Document),
  post_rdf_triples.

store_start_clean0(Document):-
  get_dateTime_lexical(Now),
  store_triple(Document, llo-startClean, literal(type(xsd-dateTime,Now))).



%! store_start_unpack(+Document:iri) is det.

store_start_unpack(Document):-
  get_dateTime_lexical(Now),
  store_triple(Document, llo-startUnpack, literal(type(xsd-dateTime,Now))),
  post_rdf_triples.



%! store_stream(+Document:iri, +Stream:stream) is det.

store_stream(Document, Stream):-
  stream_property(Stream, position(Position)),

  stream_position_data(byte_count, Position, ByteCount),
  store_triple(
    Document,
    llo-byteCount,
    literal(type(xsd-nonNegativeInteger,ByteCount))
  ),

  stream_position_data(char_count, Position, CharCount),
  store_triple(
    Document,
    llo-charCount,
    literal(type(xsd-nonNegativeInteger,CharCount))
  ),

  stream_position_data(line_count, Position, LineCount),
  store_triple(
    Document,
    llo-lineCount,
    literal(type(xsd-nonNegativeInteger,LineCount))
  ).



%! store_warning(+Document:iri, +Warning:compound) is det.
% @tbd Should we distinguish between `warning` and `error`
%      in the second argument here?

store_warning(Document, message(Term,Kind0,_)):-
  once(rename_kind(Kind0, Kind)),
  store_lod_error(Document, Kind, Term).

rename_kind(error, warning).
rename_kind(Kind, Kind).





% HELPERS %

get_dateTime_lexical(Added0):-
  get_date(Added),
  xsd_canonical_map(xsd:dateTime, Added, Added0).

