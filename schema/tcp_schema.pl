:- module(
  tcp_schema,
  [
    assert_tcp_schema/1, % +Graph:atom
    tcp_error/2 % -C:atom
                % +Text:atom
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

:- rdf_register_prefix(http, 'http://lodlaundromat.org/http-status/ontology/').
:- rdf_register_prefix(tcp, 'http://lodlaundromat.org/tcp-status/ontology/').



assert_tcp_schema(Graph):-
  forall(
    tcp_error(C, ReasonPhrase),
    rdfs_assert_status(C, tcp:'Status', ReasonPhrase, Graph)
  ).



% Helpers.

rdfs_assert_status(C, Class, ReasonPhrase, Graph):-
  rdf_global_id(tcp:C, Uri),
  rdfs_assert_instance(Uri, Class, C, ReasonPhrase, Graph),
  Def = 'https://gist.github.com/gabrielfalcao/4216897',
  rdf_assert(Uri, rdfs:isDefinedBy, Def, Graph).



% Data.

tcp_error('EACCES', 'Permission denied').
tcp_error('EADDRINUSE', 'Address already in use').
tcp_error('EADDRNOTAVAIL', 'Cannot assign requested address').
tcp_error('EAFNOSUPPORT', 'Address family not supported by protocol').
tcp_error('EALREADY', 'Operation already in progress').
tcp_error('EBADF', 'Bad file descriptor').
tcp_error('ECONNABORTED', 'Software caused connection abort').
tcp_error('ECONNREFUSED', 'Connection refused').
tcp_error('ECONNRESET', 'Connection reset by peer').
tcp_error('EHOSTDOWN', 'Host is down').
tcp_error('EHOSTUNREACH', 'No route to host').
tcp_error('EINPROGRESS', 'Operation now in progress').
tcp_error('EINTR', 'Interrupted system call').
tcp_error('EIO', 'Input/output error').
tcp_error('EISCONN', 'Transport endpoint is already connected').
tcp_error('ELOOP', 'Too many levels of symbolic links').
tcp_error('ENAMETOOLONG', 'File name too long').
tcp_error('ENETDOWN', 'Network is down').
tcp_error('ENETRESET', 'Network dropped connection on reset').
tcp_error('ENETUNREACH', 'Network is unreachable').
tcp_error('ENOENT', 'No such file or directory').
tcp_error('ENOTDIR', 'Not a directory').
tcp_error('ENOTSOCK', 'Socket operation on non-socket').
tcp_error('EPROTOTYPE', 'Protocol wrong type for socket').
tcp_error('ETIMEDOUT', 'Connection timed out').
tcp_error('EINVAL', 'Invalid argument').
tcp_error('ELOOP', 'Too many levels of symbolic links').
tcp_error('ENAMETOOLONG', 'File name too long').
tcp_error('ENETDOWN', 'Network is down').
tcp_error('ENOBUFS', 'No buffer space available').
tcp_error('EOPNOTSUPP', 'Operation not supported').
tcp_error('EPIPE', 'Broken pipe').
tcp_error('EREMOTEIO', 'Remote I/O error').
tcp_error('ETIMEDOUT', 'Connection timed out').

