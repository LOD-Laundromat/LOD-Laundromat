:- module(
  tcp_socket_schema,
  [
    assert_tcp_socket_error_schema/1 % +Graph:atom
  ]
).

/** <module> TCP socket schema

Asserts the schema for TCP socket.

@author Wouter Beek
@version 2014/08
*/

:- use_module(library(semweb/rdf_db)).

:- use_module(plRdf(rdfs_build2)).
:- use_module(plRdf_term(rdf_datatype)).
:- use_module(plRdf_term(rdf_string)).

:- rdf_register_prefix(tcp, 'http://lodlaundromat.org/tcp-status/ontology/').



assert_tcp_socket_error_schema(Graph):-
  forall(
    tcp_socket_error(Code, ReasonPhrase),
    rdfs_assert_status(Code, tcp:'Status', ReasonPhrase, Graph)
  ).



% Helpers.

rdfs_assert_status(Code, Class, ReasonPhrase, Graph):-
  rdf_global_id(tcp:Code, Uri),
  rdfs_assert_instance(Uri, Class, ReasonPhrase, _, Graph),
  rdf_assert_datatype(Uri, http:statusCode, Code, xsd:int, Graph),
  rdf_assert_string(Uri, http:reasonPhrase, ReasonPhrase, Graph).
  Def = 'https://gist.github.com/gabrielfalcao/4216897',
  rdf_assert(Uri, rdfs:isDefinedBy, Def, Graph).



% Data.

tcp_socket_error(0, 'Success').
tcp_socket_error(1, 'Operation not permitted').
tcp_socket_error(2, 'No such file or directory').
tcp_socket_error(3, 'No such process').
tcp_socket_error(4, 'Interrupted system call').
tcp_socket_error(5, 'Input/output error').
tcp_socket_error(6, 'No such device or address').
tcp_socket_error(7, 'Argument list too long').
tcp_socket_error(8, 'Exec format error').
tcp_socket_error(9, 'Bad file descriptor').
tcp_socket_error(10, 'No child processes').
tcp_socket_error(11, 'Resource temporarily unavailable').
tcp_socket_error(12, 'Cannot allocate memory').
tcp_socket_error(13, 'Permission denied').
tcp_socket_error(14, 'Bad address').
tcp_socket_error(15, 'Block device required').
tcp_socket_error(16, 'Device or resource busy').
tcp_socket_error(17, 'File exists').
tcp_socket_error(18, 'Invalid cross-device link').
tcp_socket_error(19, 'No such device').
tcp_socket_error(20, 'Not a directory').
tcp_socket_error(21, 'Is a directory').
tcp_socket_error(22, 'Invalid argument').
tcp_socket_error(23, 'Too many open files in system').
tcp_socket_error(24, 'Too many open files').
tcp_socket_error(25, 'Inappropriate ioctl for device').
tcp_socket_error(26, 'Text file busy').
tcp_socket_error(27, 'File too large').
tcp_socket_error(28, 'No space left on device').
tcp_socket_error(29, 'Illegal seek').
tcp_socket_error(30, 'Read-only file system').
tcp_socket_error(31, 'Too many links').
tcp_socket_error(32, 'Broken pipe').
tcp_socket_error(33, 'Numerical argument out of domain').
tcp_socket_error(34, 'Numerical result out of range').
tcp_socket_error(35, 'Resource deadlock avoided').
tcp_socket_error(36, 'File name too long').
tcp_socket_error(37, 'No locks available').
tcp_socket_error(38, 'Function not implemented').
tcp_socket_error(39, 'Directory not empty').
tcp_socket_error(40, 'Too many levels of symbolic links').
tcp_socket_error(41, 'Unknown error 41').
tcp_socket_error(42, 'No message of desired type').
tcp_socket_error(43, 'Identifier removed').
tcp_socket_error(44, 'Channel number out of range').
tcp_socket_error(45, 'Level 2 not synchronized').
tcp_socket_error(46, 'Level 3 halted').
tcp_socket_error(47, 'Level 3 reset').
tcp_socket_error(48, 'Link number out of range').
tcp_socket_error(49, 'Protocol driver not attached').
tcp_socket_error(50, 'No CSI structure available').
tcp_socket_error(51, 'Level 2 halted').
tcp_socket_error(52, 'Invalid exchange').
tcp_socket_error(53, 'Invalid request descriptor').
tcp_socket_error(54, 'Exchange full').
tcp_socket_error(55, 'No anode').
tcp_socket_error(56, 'Invalid request code').
tcp_socket_error(57, 'Invalid slot').
tcp_socket_error(58, 'Unknown error 58').
tcp_socket_error(59, 'Bad font file format').
tcp_socket_error(60, 'Device not a stream').
tcp_socket_error(61, 'No data available').
tcp_socket_error(62, 'Timer expired').
tcp_socket_error(63, 'Out of streams resources').
tcp_socket_error(64, 'Machine is not on the network').
tcp_socket_error(65, 'Package not installed').
tcp_socket_error(66, 'Object is remote').
tcp_socket_error(67, 'Link has been severed').
tcp_socket_error(68, 'Advertise error').
tcp_socket_error(69, 'Srmount error').
tcp_socket_error(70, 'Communication error on send').
tcp_socket_error(71, 'Protocol error').
tcp_socket_error(72, 'Multihop attempted').
tcp_socket_error(73, 'RFS specific error').
tcp_socket_error(74, 'Bad message').
tcp_socket_error(75, 'Value too large for defined data type').
tcp_socket_error(76, 'Name not unique on network').
tcp_socket_error(77, 'File descriptor in bad state').
tcp_socket_error(78, 'Remote address changed').
tcp_socket_error(79, 'Can not access a needed shared library').
tcp_socket_error(80, 'Accessing a corrupted shared library').
tcp_socket_error(81, '.lib section in a.out corrupted').
tcp_socket_error(82, 'Attempting to link in too many shared libraries').
tcp_socket_error(83, 'Cannot exec a shared library directly').
tcp_socket_error(84, 'Invalid or incomplete multibyte or wide character').
tcp_socket_error(85, 'Interrupted system call should be restarted').
tcp_socket_error(86, 'Streams pipe error').
tcp_socket_error(87, 'Too many users').
tcp_socket_error(88, 'Socket operation on non-socket').
tcp_socket_error(89, 'Destination address required').
tcp_socket_error(90, 'Message too long').
tcp_socket_error(91, 'Protocol wrong type for socket').
tcp_socket_error(92, 'Protocol not available').
tcp_socket_error(93, 'Protocol not supported').
tcp_socket_error(94, 'Socket type not supported').
tcp_socket_error(95, 'Operation not supported').
tcp_socket_error(96, 'Protocol family not supported').
tcp_socket_error(97, 'Address family not supported by protocol').
tcp_socket_error(98, 'Address already in use').
tcp_socket_error(99, 'Cannot assign requested address').
tcp_socket_error(100, 'Network is down').
tcp_socket_error(101, 'Network is unreachable').
tcp_socket_error(102, 'Network dropped connection on reset').
tcp_socket_error(103, 'Software caused connection abort').
tcp_socket_error(104, 'Connection reset by peer').
tcp_socket_error(105, 'No buffer space available').
tcp_socket_error(106, 'Transport endpoint is already connected').
tcp_socket_error(107, 'Transport endpoint is not connected').
tcp_socket_error(108, 'Cannot send after transport endpoint shutdown').
tcp_socket_error(109, 'Too many references: cannot splice').
tcp_socket_error(110, 'Connection timed out').
tcp_socket_error(111, 'Connection refused').
tcp_socket_error(112, 'Host is down').
tcp_socket_error(113, 'No route to host').
tcp_socket_error(114, 'Operation already in progress').
tcp_socket_error(115, 'Operation now in progress').
tcp_socket_error(116, 'Stale NFS file handle').
tcp_socket_error(117, 'Structure needs cleaning').
tcp_socket_error(118, 'Not a XENIX named type file').
tcp_socket_error(119, 'No XENIX semaphores available').
tcp_socket_error(120, 'Is a named type file').
tcp_socket_error(121, 'Remote I/O error').
tcp_socket_error(122, 'Disk quota exceeded').
tcp_socket_error(123, 'No medium found').
tcp_socket_error(124, 'Wrong medium type').

