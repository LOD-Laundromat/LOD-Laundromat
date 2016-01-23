:- module(
  lwm_store_triple,
  [
    store_archive_entry/4, % +Parent:atom
                           % +Entry:atom
                           % +EntryPath:atom
                           % +EntryProperties:list(nvpair)
    store_archive_filters/2, % +Doc
                             % +ArchiveFilters:list(atom)
    store_end_clean/2, % +Md5
                       % +Doc
    store_end_unpack/3, % +Md5
                        % +Doc
                        % +Status
    store_exception/2, % +Doc
                       % +Status:or([boolean,compound]))
    store_file_extension/2, % +Doc
                            % +FileExtension:atom
    store_http/2, % +Doc
                  % +Metadata:dict
    store_number_of_triples/3, % +Doc
                               % +NumberOfTriples:nonneg
                               % +NumberOfUniqueTriples:nonneg
    store_skip_clean/2, % +Md5
                        % +Doc
    store_seedpoint/1, % +Iri:atom
    store_start_clean/1, % +Doc
    store_start_unpack/1, % +Doc
    store_stream/2, % +Doc
                    % +Stream:stream
    store_warning/2 % +Doc
                    % +Warning:compound
  ]
).

/** <module> LOD Washing Machine: store triples

Temporarily store triples into the noRDF store.
Whenever a dirty item has been fully cleaned,
the stored triples are sent in a SPARQL Update request
(see module [noRdf_store].

@author Wouter Beek
@version 2015/11, 2016/01
*/

:- use_module(library(apply)).
:- use_module(library(lists)).
:- use_module(library(rdf11/rdf11)).
:- use_module(library(uri)).

:- use_module(lwm_debug_message).
:- use_module(noRdf_store).
:- use_module(store_lod_error).
:- use_module(query/lwm_sparql_det).





%! store_added(+Doc) is det.
% Datetime at which the seedpoint IRI was added to the LOD Basket.

store_added(Doc) :-
  get_dateTime_lexical(Added),
  store_triple(Doc, llo-added, literal(type(xsd-dateTime,Added))),
  post_rdf_triples.



%! store_archive_entry(
%!   +Parent:atom,
%!   +Entry:atom,
%!   +EntryPath:atom,
%!   +EntryProperties:list(nvpair)
%! ) is det.

store_archive_entry(Parent, Entry, EntryPath, EntryProperties0) :-
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

  store_added(Entry).



%! store_archive_filters(+Doc, +ArchiveFilters:list(atom)) is det.

store_archive_filters(_, []) :- !.
store_archive_filters(Doc, ArchiveFilters) :-
  rdf_create_bnode(B),
  store_triple(Doc, llo-archiveFilters, B),
  store_archive_filters0(B, ArchiveFilters).

store_archive_filters0(B, [H]) :- !,
  store_triple(B, rdf-first, literal(type(xsd-string,H))),
  store_triple(B, rdf-rest, rdf-nil).
store_archive_filters0(B1, [H|T]) :-
  store_triple(B1, rdf-first, literal(type(xsd-string,H))),
  rdf_create_bnode(B2),
  store_triple(B1, rdf-rest, B2),
  store_archive_filters0(B2, T).



%! store_end_clean(+Md5, +Doc) is det.

store_end_clean(Md5, Doc) :-
  store_end_clean0(Md5, Doc),
  post_rdf_triples.

store_end_clean0(Md5, Doc) :-
  get_dateTime_lexical(Now),
  store_triple(Doc, llo-endClean, literal(type(xsd-dateTime,Now))),

  % Construct the download URL for non-archive files.
  (   document_has_triples(Doc)
  ->  atom_concat('/', Md5, Path),
      uri_components(
        Datadump,
        uri_components(http,'download.lodlaundromat.org',Path,_,_)
      ),
      store_triple(Doc, void-dataDump, Datadump)
  ;   true
  ).



%! store_end_unpack(+Md5, +Doc, +Status:or([boolean,compound])) is det.

% Only unpacking actions with status `true` proceed to cleaning.
store_end_unpack(_, Doc, true) :- !,
  store_end_unpack0(Doc),
  post_rdf_triples.
% Skip cleaning if unpacking failed or throw a critical exception.
store_end_unpack(Md5, Doc, Status) :-
  store_end_unpack0(Doc),
  store_start_clean0(Doc),
  store_end_clean0(Md5, Doc),
  store_exception(Doc, Status),
  post_rdf_triples.

store_end_unpack0(Doc) :-
  get_dateTime_lexical(Now),
  store_triple(Doc, llo-endUnpack, literal(type(xsd-dateTime,Now))).



%! store_exception(+Doc, +Status:or([boolean,compound])) is det.

% Not an exception.
store_exception(_, true) :- !.
% Format exceptions.
store_exception(Doc, exception(Error)) :-
  store_lod_error(Doc, exception, Error).



%! store_file_extension(+Doc, +FileExtension:atom) is det.

store_file_extension(Doc, FileExtension) :-
  store_triple(
    Doc,
    llo-fileExtension,
    literal(type(xsd-string,FileExtension))
  ).



%! store_http(+Doc, +Metadata:dict) is det.

store_http(Doc, M) :-
  store_triple(Doc, llo-status, httpo-M.http.status_code),
  maplist(store_http_header(Doc), M.http.headers).

store_http_header(Doc, Header) :-
  rdf_global_id(llo:Header, P),
  store_triple(Doc, P, literal(type(xsd-string,Header))).



%! store_number_of_triples(
%!   +Doc,
%!   +NumberOfTriples:nonneg,
%!   +NumberOfUniqueTriples:nonneg
%! ) is det.

store_number_of_triples(Doc, NumberOfTriples, NumberOfUniqueTriples) :-
  % Store the number of unique triples.
  store_triple(
    Doc,
    llo-triples,
    literal(type(xsd-nonNegativeInteger,NumberOfUniqueTriples))
  ),

  % Store the number of duplicate triples.
  NumberOfDuplicateTriples is NumberOfTriples - NumberOfUniqueTriples,
  store_triple(
    Doc,
    llo-duplicates,
    literal(type(xsd-nonNegativeInteger,NumberOfDuplicateTriples))
  ).



%! store_seedpoint(+Uri) is det.

store_seedpoint(Uri) :-
  rdf_atom_md5(Uri, 1, Md5),
  rdf_global_id(ll:Md5, Doc),
  (   document_exists(Doc)
  ->  true
  ;   store_triple(Doc, rdf-type, llo-'URI'),
      store_triple(Doc, llo-url, Uri),
      store_added(Doc)
  ).



%! store_skip_clean(+Md5, +Doc) is det.

store_skip_clean(Md5, Doc) :-
  store_start_clean0(Doc),
  store_end_clean0(Md5, Doc),
  post_rdf_triples.



%! store_start_clean(+Doc) is det.

store_start_clean(Doc) :-
  store_start_clean0(Doc),
  post_rdf_triples.

store_start_clean0(Doc) :-
  get_dateTime_lexical(Now),
  store_triple(Doc, llo-startClean, literal(type(xsd-dateTime,Now))).



%! store_start_unpack(+Doc) is det.

store_start_unpack(Doc) :-
  get_dateTime_lexical(Now),
  store_triple(Doc, llo-startUnpack, literal(type(xsd-dateTime,Now))),
  post_rdf_triples.



%! store_stream(+Doc, +Stream) is det.

store_stream(Doc, Stream) :-
  stream_property(Stream, position(Position)),

  stream_position_data(byte_count, Position, ByteCount),
  store_triple(
    Doc,
    llo-byteCount,
    literal(type(xsd-nonNegativeInteger,ByteCount))
  ),

  stream_position_data(char_count, Position, CharCount),
  store_triple(
    Doc,
    llo-charCount,
    literal(type(xsd-nonNegativeInteger,CharCount))
  ),

  stream_position_data(line_count, Position, LineCount),
  store_triple(
    Doc,
    llo-lineCount,
    literal(type(xsd-nonNegativeInteger,LineCount))
  ).



%! store_warning(+Doc, +Warning:compound) is det.
% @tbd Should we distinguish between `warning` and `error`
%      in the second argument here?

store_warning(Doc, message(Term,Kind0,_)) :-
  once(rename_kind(Kind0, Kind)),
  store_lod_error(Doc, Kind, Term).

rename_kind(error, warning).
rename_kind(Kind, Kind).





% HELPERS %

get_dateTime_lexical(Lit) :-
  get_time(DT),
  rdf11:pre_ground_object(DT^^xsd:dateTime, Lit).
