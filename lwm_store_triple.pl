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
    store_end_unpack_and_skip_clean/1, % +Md5:atom
    store_file_extensions/2, % +Md5:atom
                             % +FileExtensions:list(atom)
    store_http/4, % +Md5:atom
                  % ?ContentLength:nonneg
                  % ?ContentType:atom
                  % ?LastModified:nonneg
    store_message/2, % +Md5:atom
                     % +Message:compound
    store_number_of_triples/3, % +Md5:atom
                               % +ReadTriples:nonneg
                               % +WrittenTriples:nonneg
    store_start_clean/1, % +Md5:atom
    store_start_unpack/1, % +Md5:atom
    store_status/2, % +Md5:atom
                    % +Status:or([boolean,compound]))
    store_stream/2, % +Md5:atom
                    % +Stream:stream
    store_url/2 % +Md5:atom
                % +Url:url
  ]
).

/** <module> LOD Washing Machine: store triples

Temporarily store triples into the noRDF store.
Whenever a dirty item has been fully cleaned,
the stored triples are sent in a SPARQL Update request
(see module [noRdf_store].

@author Wouter Beek
@version 2014/04-2014/06
*/

:- use_module(library(lists)).
:- use_module(library(semweb/rdf_db)).

:- use_module(pl(pl_control)).
:- use_module(pl(pl_log)).
:- use_module(xsd(xsd_dateTime_ext)).

:- use_module(lwm(lwm_generics)).
:- use_module(lwm(noRdf_store)).



%! store_added(+Md5:atom) is det.
% Datetime at which the URL was added to the LOD Basket.

store_added(Md5):-
  get_dateTime(Added),
  store_triple(ll-Md5, ll-added, literal(type(xsd-dateTime,Added))),
  post_rdf_triples(Md5).


%! store_archive_entry(
%!   +ParentMd5:atom,
%!   +EntryPath:atom,
%!   +EntryProperties:list(nvpair)
%! ) is det.

store_archive_entry(ParentMd5, EntryPath, EntryProperties1):-
  atomic_list_concat([ParentMd5,EntryPath], ' ', Temp),
  rdf_atom_md5(Temp, 1, EntryMd5),
  store_triple(ll-EntryMd5, rdf-type, ll-'ArchiveEntry'),
  store_triple(ll-EntryMd5, ll-md5, literal(type(xsd-string,EntryMd5))),
  store_triple(ll-EntryMd5, ll-path, literal(type(xsd-string,EntryPath))),

  store_triple(ll-ParentMd5, rdf-type, ll-'Archive'),
  store_triple(ll-ParentMd5, ll-contains_entry, ll-EntryMd5),

  selectchk(mtime(LastModified), EntryProperties1, EntryProperties2),
  % @tbd Store as xsd:dateTime.
  store_triple(ll-EntryMd5, ll-archive_last_modified,
      literal(type(xsd-integer,LastModified))),

  selectchk(size(ByteSize), EntryProperties2, EntryProperties3),
  store_triple(ll-EntryMd5, ll-archive_size,
      literal(type(xsd-integer,ByteSize))),

  selectchk(filetype(ArchiveFileType), EntryProperties3, []),
  store_triple(ll-EntryMd5, ll-archive_file_type,
      literal(type(xsd-string,ArchiveFileType))),

  store_added(EntryMd5).


%! store_archive_filters(+Md5:atom, +ArchiveFilters:list(atom)) is det.

store_archive_filters(_, []):- !.
store_archive_filters(Md5, ArchiveFilters):-
  forall(
    nth0(I, ArchiveFilters, ArchiveFilter),
    (
      atomic_list_concat([archive_filter,I], '_', P),
      store_triple(ll-Md5, ll-P, literal(type(xsd-string,ArchiveFilter)))
    )
  ).


%! store_end_clean(+Md5:atom) is det.

store_end_clean(Md5):-
  store_end_clean0(Md5),
  post_rdf_triples(Md5).

store_end_clean0(Md5):-
  get_dateTime(Now),
  store_triple(ll-Md5, ll-end_clean, literal(type(xsd-dateTime,Now))).


%! store_end_unpack_and_skip_clean(+Md5:atom) is det.

store_end_unpack_and_skip_clean(Md5):-
  store_end_unpack0(Md5),
  store_start_clean0(Md5),
  store_end_clean0(Md5).

%! store_end_unpack(+Md5:atom, +Status:or([boolean,compound])) is det.

% Only unpacking actions with status `true` proceed to cleaning.
store_end_unpack(Md5, true):- !,
  store_end_unpack0(Md5),
  post_rdf_triples(Md5).
store_end_unpack(Md5, Status):-
  store_end_unpack_and_skip_clean(Md5),
  store_status(Md5, Status),
  post_rdf_triples(Md5).

store_end_unpack0(Md5):-
  get_dateTime(Now),
  store_triple(ll-Md5, ll-end_unpack, literal(type(xsd-dateTime,Now))).


%! store_file_extensions(+Md5:atom, +FileExtensions:list(atom)) is det.

store_file_extensions(_, []):- !.
store_file_extensions(_, ['']):- !.
store_file_extensions(Md5, FileExtensions):-
  atomic_list_concat(FileExtensions, '.', FileExtension),
  store_triple(ll-Md5, ll-file_extension,
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
    store_triple(ll-Md5, ll-content_length,
        literal(type(xsd-integer,ContentLength)))
  ),
  unless(
    ContentType == '',
    store_triple(ll-Md5, ll-content_type,
        literal(type(xsd-string,ContentType)))
  ),
  % @tbd Store as xsd:dateTime
  unless(
    LastModified == '',
    store_triple(ll-Md5, ll-last_modified,
        literal(type(xsd-string,LastModified)))
  ).


%! store_message(+Md5:atom, +Message:compound) is det.

store_message(Md5, Message):-
  with_output_to(atom(String), write_canonical_blobs(Message)),
  store_triple(ll-Md5, ll-message, literal(type(xsd-string,String))).


%! store_number_of_triples(
%!   +Md5:atom,
%!   +ReadTriples:nonneg,
%!   +WrittenTriples:nonneg
%! ) is det.

store_number_of_triples(Md5, TIn, TOut):-
  store_triple(ll-Md5, ll-triples, literal(type(xsd-integer,TOut))),
  TDup is TIn - TOut,
  store_triple(ll-Md5, ll-duplicates, literal(type(xsd-integer,TDup))),
  print_message(informational, rdf_ntriples_written(TOut,TDup)).


%! store_start_clean(+Md5:atom) is det.

store_start_clean(Md5):-
  store_start_clean0(Md5),
  post_rdf_triples(Md5).

store_start_clean0(Md5):-
  store_lwm_version(Md5),
  get_dateTime(Now),
  store_triple(ll-Md5, ll-start_clean, literal(type(xsd-dateTime,Now))).


%! store_start_unpack(+Md5:atom) is det.

store_start_unpack(Md5):-
  store_lwm_version(Md5),

  get_dateTime(Now),
  store_triple(ll-Md5, ll-start_unpack, literal(type(xsd-dateTime,Now))),

  post_rdf_triples(Md5).


%! store_status(+Md5:atom, +Status:or([boolean,compound])) is det.

store_status(Md5, Status):-
  with_output_to(atom(String), write_canonical_blobs(Status)),
  store_triple(ll-Md5, ll-status, literal(type(xsd-string,String))).


%! store_stream(+Md5:atom, +Stream:stream) is det.

store_stream(Md5, Stream):-
  stream_property(Stream, position(Position)),

  stream_position_data(byte_count, Position, ByteCount),
  store_triple(ll-Md5, ll-byte_count, literal(type(xsd-integer,ByteCount))),

  stream_position_data(char_count, Position, CharCount),
  store_triple(ll-Md5, ll-char_count, literal(type(xsd-integer,CharCount))),

  stream_position_data(line_count, Position, LineCount),
  store_triple(ll-Md5, ll-line_count, literal(type(xsd-integer,LineCount))).


%! store_url(+Md5:atom, +Url:url) is det.

store_url(Md5, Url):-
  store_triple(ll-Md5, rdf-type, ll-'URL'),
  store_triple(ll-Md5, ll-md5, literal(type(xsd-string,Md5))),
  store_triple(ll-Md5, ll-url, Url),
  store_added(Md5).


%! store_lwm_version(+Md5:atom) is det.

store_lwm_version(Md5):-
  lwm_version(Version),
  store_triple(ll-Md5, ll-lwm_version, literal(type(xsd-integer,Version))).



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

