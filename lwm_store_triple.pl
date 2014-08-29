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
    store_number_of_triples/3, % +Md5:atom
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
@version 2014/04-2014/06, 2014/08
*/

:- use_module(library(lists)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(uri)).

:- use_module(pl(pl_control)).
:- use_module(pl(pl_log)).

:- use_module(plXsd_datetime(xsd_dateTime_ext)).

:- use_module(lwm(noRdf_store)).
:- use_module(lwm_schema(tcp_schema)).

:- rdf_register_prefix(http, 'http://lodlaundromat.org/http-status/ontology/').



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
  store_triple(ll-ParentMd5, llo-contains_entry, ll-EntryMd5),

  selectchk(mtime(LastModified), EntryProperties1, EntryProperties2),
  % @tbd Store as xsd:dateTime.
  store_triple(ll-EntryMd5, llo-archive_last_modified,
      literal(type(xsd-integer,LastModified))),

  selectchk(size(ByteSize), EntryProperties2, EntryProperties3),
  store_triple(ll-EntryMd5, llo-archive_size,
      literal(type(xsd-integer,ByteSize))),

  selectchk(filetype(ArchiveFileType), EntryProperties3, []),
  store_triple(ll-EntryMd5, llo-archive_file_type,
      literal(type(xsd-string,ArchiveFileType))),

  store_added(EntryMd5).


%! store_archive_filters(+Md5:atom, +ArchiveFilters:list(atom)) is det.

store_archive_filters(_, []):- !.
store_archive_filters(Md5, ArchiveFilters):-
  forall(
    nth0(I, ArchiveFilters, ArchiveFilter),
    (
      atomic_list_concat([archive_filter,I], '_', P),
      store_triple(ll-Md5, llo-P, literal(type(xsd-string,ArchiveFilter)))
    )
  ).


%! store_end_clean(+Md5:atom) is det.

store_end_clean(Md5):-
  store_end_clean0(Md5),
  post_rdf_triples(Md5).

store_end_clean0(Md5):-
  get_dateTime(Now),
  store_triple(ll-Md5, llo-end_clean, literal(type(xsd-dateTime,Now))),
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
  store_triple(ll-Md5, llo-end_unpack, literal(type(xsd-dateTime,Now))).


%! store_exception(+Md5:atom, +Status:or([boolean,compound])) is det.

% Not an exception.
store_exception(_, true):- !.
% Format exceptions.
store_exception(Md5, exception(Error)):- !,
  store_error(Md5, Error).
% Catch-all.
store_exception(Md5, Exception):-
  with_output_to(atom(String), write_canonical_blobs(Exception)),
  store_triple(ll-Md5, llo-exception, literal(type(xsd-string,String))).

store_error(Md5, error(http_status(Status),_)):-
  (   between(400, 599, Status)
  ->  store_triple(Md5, llo-exception, http-Status)
  ;   true
  ),
  store_triple(Md5, llo-http_status, http-Status).
store_error(Md5, error(no_rdf(_))):-
  store_triple(Md5, llo-serialization_format, llo-unrecognizedFormat).
store_error(_, error(socket_error('Host not found'), _)):- !. % @tbd
store_error(_, error(socket_error('Try again'), _)):- !. % @tbd
store_error(Md5, error(socket_error(ReasonPhrase), _)):-
  tcp_error(C, ReasonPhrase), !,
  store_triple(Md5, llo-exception, tcp-C).
store_error(Md5, Error):-
  gtrace,
  store_error(Md5, Error).

%! store_file_extension(+Md5:atom, +FileExtension:atom) is det.

store_file_extension(Md5, FileExtension):-
  store_triple(ll-Md5, llo-file_extension,
      literal(type(xsd-string,FileExtension))).


%! store_http(
%!   +Md5:atom,
%!   ?ContentLength:nonneg,
%!   ?ContentType:atom,
%!   ?LastModified:nonneg
%! ) is det.

store_http(Md5, ContentLength, ContentType, LastModified):-
  unless(
    ContentLength == '',
    store_triple(ll-Md5, llo-content_length,
        literal(type(xsd-integer,ContentLength)))
  ),
  unless(
    ContentType == '',
    store_triple(ll-Md5, llo-content_type,
        literal(type(xsd-string,ContentType)))
  ),
  % @tbd Store as xsd:dateTime
  unless(
    LastModified == '',
    store_triple(ll-Md5, llo-last_modified,
        literal(type(xsd-string,LastModified)))
  ).


%! store_number_of_triples(
%!   +Md5:atom,
%!   +ReadTriples:nonneg,
%!   +WrittenTriples:nonneg
%! ) is det.

store_number_of_triples(Md5, TIn, TOut):-
  store_triple(ll-Md5, llo-triples, literal(type(xsd-integer,TOut))),
  TDup is TIn - TOut,
  store_triple(ll-Md5, llo-duplicates, literal(type(xsd-integer,TDup))),
  print_message(informational, rdf_ntriples_written(TOut,TDup)).


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
  store_triple(ll-Md5, llo-start_clean, literal(type(xsd-dateTime,Now))).


%! store_start_unpack(+Md5:atom) is det.

store_start_unpack(Md5):-
  get_dateTime(Now),
  store_triple(ll-Md5, llo-start_unpack, literal(type(xsd-dateTime,Now))),
  post_rdf_triples(Md5).


%! store_stream(+Md5:atom, +Stream:stream) is det.

store_stream(Md5, Stream):-
  stream_property(Stream, position(Position)),

  stream_position_data(byte_count, Position, ByteCount),
  store_triple(ll-Md5, llo-byte_count, literal(type(xsd-integer,ByteCount))),

  stream_position_data(char_count, Position, CharCount),
  store_triple(ll-Md5, llo-char_count, literal(type(xsd-integer,CharCount))),

  stream_position_data(line_count, Position, LineCount),
  store_triple(ll-Md5, llo-line_count, literal(type(xsd-integer,LineCount))).


%! store_warning(+Md5:atom, +Warning:compound) is det.

store_warning(Md5, Warning):-
  with_output_to(atom(String), write_canonical_blobs(Warning)),
  store_triple(ll-Md5, llo-warning, literal(type(xsd-string,String))).



% Messages

:- multifile(prolog:message//1).

prolog:message(rdf_ntriples_written(TOut,TDup)) -->
  ['  ['],
    number_of_triples_written(TOut),
    number_of_duplicates_written(TDup),
  [']'].

number_of_duplicates_written(0) --> !, [].
number_of_duplicates_written(T) --> [' (~D dups)'-[T]].

number_of_triples_written(0) --> !, [].
number_of_triples_written(T) --> ['+~D'-[T]].

