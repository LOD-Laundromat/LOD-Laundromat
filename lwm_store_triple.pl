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
:- use_module(lwm(store_lod_exception)).



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
  (  store_lod_exception(Md5, Error)
  -> true
  ;  gtrace,
     store_exception(Md5, exception(Error))
  ).


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
  store_lod_warning(Md5, Term).



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

/*
error_code('EPERM',           1,   'Operation not permitted').
error_code('ENOENT',          2,   'No such file or directory').
error_code('ESRCH',           3,   'No such process').
error_code('EINTR',           4,   'Interrupted system call').
error_code('EIO',             5,   'Input/output error').
error_code('ENXIO',           6,   'No such device or address').
error_code('E2BIG',           7,   'Argument list too long').
error_code('ENOEXEC',         8,   'Exec format error').
error_code('EBADF',           9,   'Bad file descriptor').
error_code('ECHILD',          10,  'No child processes').
error_code('EAGAIN',          11,  'Resource temporarily unavailable').
error_code('ENOMEM',          12,  'Cannot allocate memory').
error_code('EACCES',          13,  'Permission denied').
error_code('EFAULT',          14,  'Bad address').
error_code('ENOTBLK',         15,  'Block device required').
error_code('EBUSY',           16,  'Device or resource busy').
error_code('EXIST',           17,  'File exists').
error_code('EXDEV',           18,  'Invalid cross-device link').
error_code('ENODEV',          19,  'No such device').
error_code('ENOTDIR',         20,  'Not a directory').
error_code('EISDIR',          21,  'Is a directory').
error_code('EINVAL',          22,  'Invalid argument').
error_code('ENFILE',          23,  'Too many open files in system').
error_code('EMFILE',          24,  'Too many open files').
error_code('ENOTTY',          25,  'Inappropriate ioctl for device').
error_code('ETXTBSY',         26,  'Text file busy').
error_code('EFBIG',           27,  'File too large').
error_code('ENOSPC',          28,  'No space left on device').
error_code('ESPIPE',          29,  'Illegal seek').
error_code('EROFS',           30,  'Read-only file system').
error_code('EMLINK',          31,  'Too many links').
error_code('EPIPE',           32,  'Broken pipe').
error_code('EDOM',            33,  'Numerical argument out of domain').
error_code('ERANGE',          34,  'Numerical result out of range').
error_code('EDEADLK',         35,  'Resource deadlock avoided').
error_code('ENAMETOOLONG',    36,  'File name too long').
error_code('ENOLCK',          37,  'No locks available').
error_code('ENOSYS',          38,  'Function not implemented').
error_code('ENOTEMPTY',       39,  'Directory not empty').
error_code('ELOOP',           40,  'Too many levels of symbolic links').
error_code('EWOULDBLOCK',     41,  'Resource temporarily unavailable').
error_code('ENOMSG',          42,  'No message of desired type').
error_code('EIDRM',           43,  'Identifier removed').
error_code('ECHRNG',          44,  'Channel number out of range').
error_code('EL2NSYNC',        45,  'Level 2 not synchronized').
error_code('EL3HLT',          46,  'Level 3 halted').
error_code('EL3RST',          47,  'Level 3 reset').
error_code('ELNRNG',          48,  'Link number out of range').
error_code('EUNATCH',         49,  'Protocol driver not attached').
error_code('ENOCSI',          50,  'No CSI structure available').
error_code('EL2HLT',          51,  'Level 2 halted').
error_code('EBADE',           52,  'Invalid exchange').
error_code('EBADR',           53,  'Invalid request descriptor').
error_code('EXFULL',          54,  'Exchange full').
error_code('ENOANO',          55,  'No anode').
error_code('EBADRQC',         56,  'Invalid request code').
error_code('EBADSLT',         57,  'Invalid slot').
error_code('EDEADLOCK',       58,  'Resource deadlock avoided').
error_code('EBFONT',          59,  'Bad font file format').
error_code('ENOSTR',          60,  'Device not a stream').
error_code('ENODATA',         61,  'No data available').
error_code('ETIME',           62,  'Timer expired').
error_code('ENOSR',           63,  'Out of streams resources').
error_code('ENONET',          64,  'Machine is not on the network').
error_code('ENOPKG',          65,  'Package not installed').
error_code('EREMOTE',         66,  'Object is remote').
error_code('ENOLINK',         67,  'Link has been severed').
error_code('EADV',            68,  'Advertise error').
error_code('ESRMNT',          69,  'Srmount error').
error_code('ECOMM',           70,  'Communication error on send').
error_code('EPROTO',          71,  'Protocol error').
error_code('EMULTIHOP',       72,  'Multihop attempted').
error_code('EDOTDOT',         73,  'RFS specific error').
error_code('EBADMSG',         74,  'Bad message').
error_code('EOVERFLOW',       75,  'Value too large for defined data type').
error_code('ENOTUNIQ',        76,  'Name not unique on network').
error_code('EBADFD',          77,  'File descriptor in bad state').
error_code('EREMCHG',         78,  'Remote address changed').
error_code('ELIBACC',         79,  'Can not access a needed shared library').
error_code('ELIBBAD',         80,  'Accessing a corrupted shared library').
error_code('ELIBSCN',         81,  '.lib section in a.out corrupted').
error_code('ELIBMAX',         82,  'Attempting to link in too many shared libraries').
error_code('ELIBEXEC',        83,  'Cannot exec a shared library directly').
error_code('EILSEQ',          84,  'Invalid or incomplete multibyte or wide character').
error_code('ERESTART',        85,  'Interrupted system call should be restarted').
error_code('ESTRPIPE',        86,  'Streams pipe error').
error_code('EUSERS',          87,  'Too many users').
error_code('ENOTSOCK',        88,  'Socket operation on non-socket').
error_code('EDESTADDRREQ',    89,  'Destination address required').
error_code('EMSGSIZE',        90,  'Message too long').
error_code('EPROTOTYPE',      91,  'Protocol wrong type for socket').
error_code('ENOPROTOOPT',     92,  'Protocol not available').
error_code('EPROTONOSUPPORT', 93,  'Protocol not supported').
error_code('ESOCKTNOSUPPORT', 94,  'Socket type not supported').
error_code('EOPNOTSUPP',      95,  'Operation not supported').
error_code('EPFNOSUPPORT',    96,  'Protocol family not supported').
error_code('EAFNOSUPPORT',    97,  'Address family not supported by protocol').
error_code('EADDRINUSE',      98,  'Address already in use').
error_code('EADDRNOTAVAIL',   99,  'Cannot assign requested address').
error_code('ENETDOWN',        100, 'Network is down').
error_code('ENETUNREACH',     101, 'Network is unreachable').
error_code('ENETRESET',       102, 'Network dropped connection on reset').
error_code('ECONNABORTED',    103, 'Software caused connection abort').
error_code('ECONNRESET',      104, 'Connection reset by peer').
error_code('ENOBUFS',         105, 'No buffer space available').
error_code('EISCONN',         106, 'Transport endpoint is already connected').
error_code('ENOTCONN',        107, 'Transport endpoint is not connected').
error_code('ESHUTDOWN',       108, 'Cannot send after transport endpoint shutdown').
error_code('ETOOMANYREFS',    109, 'Too many references: cannot splice').
error_code('ETIMEDOUT',       110, 'Connection timed out').
error_code('ECONNREFUSED',    111, 'Connection refused').
error_code('EHOSTDOWN',       112, 'Host is down').
error_code('EHOSTUNREACH',    113, 'No route to host').
error_code('EALREADY',        114, 'Operation already in progress').
error_code('EINPROGRESS',     115, 'Operation now in progress').
error_code('ESTALE',          116, 'Stale file handle').
error_code('EUCLEAN',         117, 'Structure needs cleaning').
error_code('ENOTNAM',         118, 'Not a XENIX named type file').
error_code('ENAVAIL',         119, 'No XENIX semaphores available').
error_code('EISNAM',          120, 'Is a named type file').
error_code('EREMOTEIO',       121, 'Remote I/O error').
error_code('EDQUOT',          122, 'Disk quota exceeded').
error_code('ENOMEDIUM',       123, 'No medium found').
error_code('EMEDIUMTYPE',     124, 'Wrong medium type').
error_code('ECANCELED',       125, 'Operation canceled').
error_code('ENOKEY',          126, 'Required key not available').
error_code('EKEYEXPIRED',     127, 'Key has expired').
error_code('EKEYREVOKED',     128, 'Key has been revoked').
error_code('EKEYREJECTED',    129, 'Key was rejected by service').
error_code('EOWNERDEAD',      130, 'Owner died').
error_code('ENOTRECOVERABLE', 131, 'State not recoverable').
error_code('EACCES',               'Permission denied').
error_code('EAFNOSUPPORT',         'Address family not supported by protocol').
error_code('EINVAL',               'Invalid argument').
error_code('EMFILE',               'Too many open files').
error_code('ENOBUFS',              'No buffer space available').
error_code('ENOMEM',               'Cannot allocate memory').
error_code('EPROTONOSUPPORT',      'Protocol not supported').
*/
