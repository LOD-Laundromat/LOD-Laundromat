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

% Archive error.
store_error(Md5, error(archive_error(Code,Message),_)):- !,
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-exception, BNode),
  store_triple(BNode, rdf-type, error-'ArchiveException'),
  store_triple(BNode, rdfs-comment, literal(type(xsd-string,Message))),
  store_triple(BNode, error-code, literal(type(xsd-integer,Code))).
% Existence error.
store_error(Md5, error(existence_error(Kind1,Obj),context(_Pred,Message))):- !,
  dcg_phrase(capitalize, Kind1, Kind2),
  atom_concat(Kind2, 'ExistenceError', Class),
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-exception, BNode),
  store_triple(BNode, rdf-type, error-Class),
  store_triple(BNode, rdfs-comment, literal(type(xsd-string,Message))),
  store_triple(BNode, error-object, literal(type(xsd-string,Obj))).
% HTTP status.
store_error(Md5, error(http_status(Status),_)):- !,
  (   between(400, 599, Status)
  ->  store_triple(ll-Md5, llo-exception, http-Status)
  ;   true
  ),
  store_triple(ll-Md5, llo-httpStatus, http-Status).
store_error(Md5, error(io_error(read,_),context(_,'Inappropriate ioctl for device'))):- !,
  store_triple(ll-Md5, llo-exception, literal(type(xsd-string,'Inappropriate ioctl for device'))).
% IO error.
store_error(Md5, error(io_error(read,_Stream),context(_Predicate,Message))):-
  linux_error_code(VariableName, _, Message), !,
  store_triple(ll-Md5, llo-exception, error-VariableName).
% No RDF Media Type.
store_error(Md5, error(no_rdf(_))):- !,
  store_triple(ll-Md5, llo-serializationFormat, llo-unrecognizedFormat).
% Permission error.
store_error(Md5, error(permission_error(redirect,_,Url),_)):- !,
  % It is not allowed to perform Action on the object Term
  % that is of the given Type.
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-exception, BNode),
  store_triple(BNode, rdf-type, error-permissionError),
  store_triple(BNode, error-action, error-redirectionLoop),
  store_triple(BNode, error-object, Url).
% Socket error.
store_error(Md5, error(socket_error(Message), _)):-
  socket_error(VariableName, _, Message), !,
  store_triple(ll-Md5, llo-exception, error-VariableName).
% Socket error: TBD.
store_error(Md5, error(socket_error(Undefined), _)):- !,
  format(atom(LexExpr), 'socket_error(~a)', [Undefined]),
  store_triple(ll-Md5, llo-exception, literal(type(xsd-string,LexExpr))).
% SSL verification error.
store_error(Md5, error(ssl_error(ssl_verify), _)):- !,
  store_triple(ll-Md5, llo-exception, error-sslError).
% Read timeout error.
store_error(Md5, error(timeout_error(read,_),_)):- !,
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

linux_error_code('EPERM',           1,   'Operation not permitted').
linux_error_code('ENOENT',          2,   'No such file or directory').
linux_error_code('ESRCH',           3,   'No such process').
linux_error_code('EINTR',           4,   'Interrupted system call').
linux_error_code('EIO',             5,   'Input/output error').
linux_error_code('ENXIO',           6,   'No such device or address').
linux_error_code('E2BIG',           7,   'Argument list too long').
linux_error_code('ENOEXEC',         8,   'Exec format error').
linux_error_code('EBADF',           9,   'Bad file descriptor').
linux_error_code('ECHILD',          10,  'No child processes').
linux_error_code('EAGAIN',          11,  'Resource temporarily unavailable').
linux_error_code('ENOMEM',          12,  'Cannot allocate memory').
linux_error_code('EACCES',          13,  'Permission denied').
linux_error_code('EFAULT',          14,  'Bad address').
linux_error_code('ENOTBLK',         15,  'Block device required').
linux_error_code('EBUSY',           16,  'Device or resource busy').
linux_error_code('EXIST',           17,  'File exists').
linux_error_code('EXDEV',           18,  'Invalid cross-device link').
linux_error_code('ENODEV',          19,  'No such device').
linux_error_code('ENOTDIR',         20,  'Not a directory').
linux_error_code('EISDIR',          21,  'Is a directory').
linux_error_code('EINVAL',          22,  'Invalid argument').
linux_error_code('ENFILE',          23,  'Too many open files in system').
linux_error_code('EMFILE',          24,  'Too many open files').
linux_error_code('ENOTTY',          25,  'Inappropriate ioctl for device').
linux_error_code('ETXTBSY',         26,  'Text file busy').
linux_error_code('EFBIG',           27,  'File too large').
linux_error_code('ENOSPC',          28,  'No space left on device').
linux_error_code('ESPIPE',          29,  'Illegal seek').
linux_error_code('EROFS',           30,  'Read-only file system').
linux_error_code('EMLINK',          31,  'Too many links').
linux_error_code('EPIPE',           32,  'Broken pipe').
linux_error_code('EDOM',            33,  'Numerical argument out of domain').
linux_error_code('ERANGE',          34,  'Numerical result out of range').
linux_error_code('EDEADLK',         35,  'Resource deadlock avoided').
linux_error_code('ENAMETOOLONG',    36,  'File name too long').
linux_error_code('ENOLCK',          37,  'No locks available').
linux_error_code('ENOSYS',          38,  'Function not implemented').
linux_error_code('ENOTEMPTY',       39,  'Directory not empty').
linux_error_code('ELOOP',           40,  'Too many levels of symbolic links').
linux_error_code('EWOULDBLOCK',     41,  'Resource temporarily unavailable').
linux_error_code('ENOMSG',          42,  'No message of desired type').
linux_error_code('EIDRM',           43,  'Identifier removed').
linux_error_code('ECHRNG',          44,  'Channel number out of range').
linux_error_code('EL2NSYNC',        45,  'Level 2 not synchronized').
linux_error_code('EL3HLT',          46,  'Level 3 halted').
linux_error_code('EL3RST',          47,  'Level 3 reset').
linux_error_code('ELNRNG',          48,  'Link number out of range').
linux_error_code('EUNATCH',         49,  'Protocol driver not attached').
linux_error_code('ENOCSI',          50,  'No CSI structure available').
linux_error_code('EL2HLT',          51,  'Level 2 halted').
linux_error_code('EBADE',           52,  'Invalid exchange').
linux_error_code('EBADR',           53,  'Invalid request descriptor').
linux_error_code('EXFULL',          54,  'Exchange full').
linux_error_code('ENOANO',          55,  'No anode').
linux_error_code('EBADRQC',         56,  'Invalid request code').
linux_error_code('EBADSLT',         57,  'Invalid slot').
linux_error_code('EDEADLOCK',       58,  'Resource deadlock avoided').
linux_error_code('EBFONT',          59,  'Bad font file format').
linux_error_code('ENOSTR',          60,  'Device not a stream').
linux_error_code('ENODATA',         61,  'No data available').
linux_error_code('ETIME',           62,  'Timer expired').
linux_error_code('ENOSR',           63,  'Out of streams resources').
linux_error_code('ENONET',          64,  'Machine is not on the network').
linux_error_code('ENOPKG',          65,  'Package not installed').
linux_error_code('EREMOTE',         66,  'Object is remote').
linux_error_code('ENOLINK',         67,  'Link has been severed').
linux_error_code('EADV',            68,  'Advertise error').
linux_error_code('ESRMNT',          69,  'Srmount error').
linux_error_code('ECOMM',           70,  'Communication error on send').
linux_error_code('EPROTO',          71,  'Protocol error').
linux_error_code('EMULTIHOP',       72,  'Multihop attempted').
linux_error_code('EDOTDOT',         73,  'RFS specific error').
linux_error_code('EBADMSG',         74,  'Bad message').
linux_error_code('EOVERFLOW',       75,  'Value too large for defined data type').
linux_error_code('ENOTUNIQ',        76,  'Name not unique on network').
linux_error_code('EBADFD',          77,  'File descriptor in bad state').
linux_error_code('EREMCHG',         78,  'Remote address changed').
linux_error_code('ELIBACC',         79,  'Can not access a needed shared library').
linux_error_code('ELIBBAD',         80,  'Accessing a corrupted shared library').
linux_error_code('ELIBSCN',         81,  '.lib section in a.out corrupted').
linux_error_code('ELIBMAX',         82,  'Attempting to link in too many shared libraries').
linux_error_code('ELIBEXEC',        83,  'Cannot exec a shared library directly').
linux_error_code('EILSEQ',          84,  'Invalid or incomplete multibyte or wide character').
linux_error_code('ERESTART',        85,  'Interrupted system call should be restarted').
linux_error_code('ESTRPIPE',        86,  'Streams pipe error').
linux_error_code('EUSERS',          87,  'Too many users').
linux_error_code('ENOTSOCK',        88,  'Socket operation on non-socket').
linux_error_code('EDESTADDRREQ',    89,  'Destination address required').
linux_error_code('EMSGSIZE',        90,  'Message too long').
linux_error_code('EPROTOTYPE',      91,  'Protocol wrong type for socket').
linux_error_code('ENOPROTOOPT',     92,  'Protocol not available').
linux_error_code('EPROTONOSUPPORT', 93,  'Protocol not supported').
linux_error_code('ESOCKTNOSUPPORT', 94,  'Socket type not supported').
linux_error_code('EOPNOTSUPP',      95,  'Operation not supported').
linux_error_code('EPFNOSUPPORT',    96,  'Protocol family not supported').
linux_error_code('EAFNOSUPPORT',    97,  'Address family not supported by protocol').
linux_error_code('EADDRINUSE',      98,  'Address already in use').
linux_error_code('EADDRNOTAVAIL',   99,  'Cannot assign requested address').
linux_error_code('ENETDOWN',        100, 'Network is down').
linux_error_code('ENETUNREACH',     101, 'Network is unreachable').
linux_error_code('ENETRESET',       102, 'Network dropped connection on reset').
linux_error_code('ECONNABORTED',    103, 'Software caused connection abort').
linux_error_code('ECONNRESET',      104, 'Connection reset by peer').
linux_error_code('ENOBUFS',         105, 'No buffer space available').
linux_error_code('EISCONN',         106, 'Transport endpoint is already connected').
linux_error_code('ENOTCONN',        107, 'Transport endpoint is not connected').
linux_error_code('ESHUTDOWN',       108, 'Cannot send after transport endpoint shutdown').
linux_error_code('ETOOMANYREFS',    109, 'Too many references: cannot splice').
linux_error_code('ETIMEDOUT',       110, 'Connection timed out').
linux_error_code('ECONNREFUSED',    111, 'Connection refused').
linux_error_code('EHOSTDOWN',       112, 'Host is down').
linux_error_code('EHOSTUNREACH',    113, 'No route to host').
linux_error_code('EALREADY',        114, 'Operation already in progress').
linux_error_code('EINPROGRESS',     115, 'Operation now in progress').
linux_error_code('ESTALE',          116, 'Stale file handle').
linux_error_code('EUCLEAN',         117, 'Structure needs cleaning').
linux_error_code('ENOTNAM',         118, 'Not a XENIX named type file').
linux_error_code('ENAVAIL',         119, 'No XENIX semaphores available').
linux_error_code('EISNAM',          120, 'Is a named type file').
linux_error_code('EREMOTEIO',       121, 'Remote I/O error').
linux_error_code('EDQUOT',          122, 'Disk quota exceeded').
linux_error_code('ENOMEDIUM',       123, 'No medium found').
linux_error_code('EMEDIUMTYPE',     124, 'Wrong medium type').
linux_error_code('ECANCELED',       125, 'Operation canceled').
linux_error_code('ENOKEY',          126, 'Required key not available').
linux_error_code('EKEYEXPIRED',     127, 'Key has expired').
linux_error_code('EKEYREVOKED',     128, 'Key has been revoked').
linux_error_code('EKEYREJECTED',    129, 'Key was rejected by service').
linux_error_code('EOWNERDEAD',      130, 'Owner died').
linux_error_code('ENOTRECOVERABLE', 131, 'State not recoverable').


socket_error('EACCES',          'Permission denied').
socket_error('EAFNOSUPPORT',    'Address family not supported by protocol').
socket_error('EINVAL',          'Invalid argument').
socket_error('EMFILE',          'Too many open files').
socket_error('ENOBUFS',         'No buffer space available').
socket_error('ENOMEM',          'Cannot allocate memory').
socket_error('EPROTONOSUPPORT', 'Protocol not supported').
