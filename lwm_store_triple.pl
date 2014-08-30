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

:- use_module(plDcg(dcg_content)).
:- use_module(plDcg(dcg_generic)).

:- use_module(plXsd_datetime(xsd_dateTime_ext)).

:- use_module(lwm(noRdf_store)).

:- rdf_register_prefix(error, 'http://lodlaundromat.org/error/ontology/').
:- rdf_register_prefix(http, 'http://lodlaundromat.org/http/ontology/').
:- rdf_register_prefix(tcp, 'http://lodlaundromat.org/tcp/ontology/').



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
store_exception(Md5, exception(Error)):- !,
  store_error(Md5, Error).
% Catch-all.
store_exception(Md5, Exception):-
  with_output_to(atom(String), write_canonical_blobs(Exception)),
  store_triple(ll-Md5, llo-exception, literal(type(xsd-string,String))).

% Existence error.
store_error(Md5, error(existence_error(Kind1,Obj),context(_Pred,Msg))):- !,
  dcg_phrase(capitalize, Kind1, Kind2),
  atom_concat(Kind2, 'ExistenceError', Class),
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-exception, BNode),
  store_triple(BNode, rdf-type, error-Class),
  store_triple(BNode, error-message, literal(type(xsd-string,Msg))),
  store_triple(BNode, error-object, literal(type(xsd-string,Obj))).
% HTTP status.
store_error(Md5, error(http_status(Status),_)):-
  (   between(400, 599, Status)
  ->  store_triple(ll-Md5, llo-exception, http-Status)
  ;   true
  ),
  store_triple(ll-Md5, llo-httpStatus, http-Status).
% IO error.
store_error(Md5, error(io_error(read,_Stream),context(_Predicate,Message))):-
  tcp_error(C, Message), !,
  store_triple(ll-Md5, llo-exception, tcp-C).
% No RDF Media Type.
store_error(Md5, error(no_rdf(_))):-
  store_triple(ll-Md5, llo-serializationFormat, llo-unrecognizedFormat).
% Socket error.
store_error(Md5, error(socket_error(ReasonPhrase), _)):-
  tcp_error(C, ReasonPhrase), !,
  store_triple(ll-Md5, llo-exception, tcp-C).
% Socket error: TBD.
store_error(Md5, error(socket_error(Undefined), _)):- !,
  format(atom(LexExpr), 'socket_error(~a)', [Undefined]),
  store_triple(ll-Md5, llo-exception, literal(type(xsd-string,LexExpr))).
% SSL verification error.
store_error(Md5, error(ssl_error(ssl_verify), _)):-
  store_triple(ll-Md5, llo-exception, error-sslError).
% Read timeout error.
store_error(Md5, error(timeout_error(read,_),_)):-
  store_triple(ll-Md5, llo-exception, error-readTimeoutError).
% DEB
store_error(Md5, Error):-
  gtrace,
  store_error(Md5, Error).


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

store_warning(Md5, message(Term,warning,_)):- !,
  store_warning0(Md5, Term).
store_warning(Md5, Warning):-
  with_output_to(atom(String), write_canonical_blobs(Warning)),
  store_triple(ll-Md5, llo-warning, literal(type(xsd-string,String))).

store_warning0(Md5, sgml(sgml_parser(_),_,Line,Message)):- !,
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-warning, BNode),
  store_triple(BNode, rdf-type, error-'SgmlParserWarning'),
  store_triple(BNode, error-sourceLine, literal(type(xsd-integer,Line))),
  store_triple(BNode, error-message, literal(type(xsd-string,Message))).
store_warning0(Md5, Term):-
  gtrace,
  store_warning0(Md5, Term).



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



% Helpers.

tcp_error('EACCES', 'Permission denied').
tcp_error('EADDRINUSE', 'Address already in use').
tcp_error('EADDRNOTAVAIL', 'Cannot assign requested address').
tcp_error('EAFNOSUPPORT', 'Address family not supported by protocol').
tcp_error('EALREADY', 'Operation already in progress').
tcp_error('EBADF', 'Bad file descriptor').
tcp_error('EBADSLT', 'Invalid slot').
tcp_error('ECONNABORTED', 'Software caused connection abort').
tcp_error('ECONNREFUSED', 'Connection refused').
tcp_error('ECONNRESET', 'Connection reset by peer').
tcp_error('EDQUOT', 'Disk quota exceeded').
tcp_error('EHOSTDOWN', 'Host is down').
tcp_error('EHOSTUNREACH', 'No route to host').
tcp_error('EINPROGRESS', 'Operation now in progress').
tcp_error('EINTR', 'Interrupted system call').
tcp_error('EIO', 'Input/output error').
tcp_error('EISCONN', 'Transport endpoint is already connected').
tcp_error('EINVAL', 'Invalid argument').
tcp_error('ELOOP', 'Too many levels of symbolic links').
tcp_error('EMSGSIZE', 'Message too long').
tcp_error('ENETDOWN', 'Network is down').
tcp_error('ENETRESET', 'Network dropped connection on reset').
tcp_error('ENETUNREACH', 'Network is unreachable').
tcp_error('ENOBUFS', 'No buffer space available').
tcp_error('ENODATA', 'No data available').
tcp_error('ENOENT', 'No such file or directory').
tcp_error('ENOTCONN', 'Transport endpoint is not connected').
tcp_error('ENOTDIR', 'Not a directory').
tcp_error('ENOTSOCK', 'Socket operation on non-socket').
tcp_error('EOPNOTSUPP', 'Operation not supported').
tcp_error('EPFNOSUPPORT', 'Protocol family not supported').
tcp_error('EPIPE', 'Broken pipe').
tcp_error('EPROTO', 'Protocol error').
tcp_error('EPROTONOSUPPORT', 'Protocol not supported').
tcp_error('EPROTOTYPE', 'Protocol wrong type for socket').
tcp_error('EREMOTE', 'Object is remote').
tcp_error('EREMOTEIO', 'Remote I/O error').
tcp_error('ESHUTDOWN', 'Cannot send after transport endpoint shutdown').
tcp_error('ESTALE', 'Stale file handle').
tcp_error('ESOCKTNOSUPPORT', 'Socket type not supported').
tcp_error('ETIMEDOUT', 'Connection timed out').
tcp_error('ETOOMANYREFS', 'Too many references: cannot splice').
tcp_error('EUSERS', 'Too many users').
tcp_error('EWOULDBLOCK', 'Resource temporarily unavailable').

