#include <errno.h>
#include <string.h>
#include <stdio.h>

#define str(i) #i

main()
{
  // Search permission is denied for a component of the path prefix;
  // or write access to the named socket is denied.
  printf("tcp_error('%s', '%s').\n", str(EACCES), strerror(EACCES));
  
  // Attempt to establish a connection
  // that uses addresses that are already in use.
  printf("tcp_error('%s', '%s').\n", str(EADDRINUSE), strerror(EADDRINUSE));
  
  // The specified address is not available from the local machine.
  printf("tcp_error('%s', '%s').\n", str(EADDRNOTAVAIL), strerror(EADDRNOTAVAIL));
  
  // The specified address is not a valid address
  // for the address family of the specified socket.
  printf("tcp_error('%s', '%s').\n", str(EAFNOSUPPORT), strerror(EAFNOSUPPORT));
  
  // A connection request is already in progress for the specified socket.
  printf("tcp_error('%s', '%s').\n", str(EALREADY), strerror(EALREADY));
  
  // The socket argument is not a valid file descriptor.
  printf("tcp_error('%s', '%s').\n", str(EBADF), strerror(EBADF));
  
  // Software caused connection abort.
  printf("tcp_error('%s', '%s').\n", str(ECONNABORTED), strerror(ECONNABORTED));
  
  // The target address was not listening for connections or
  // refused the connection request.
  printf("tcp_error('%s', '%s').\n", str(ECONNREFUSED), strerror(ECONNREFUSED));
  
  // Remote host reset the connection request.
  printf("tcp_error('%s', '%s').\n", str(ECONNRESET), strerror(ECONNRESET));
  
  // Host is down.
  printf("tcp_error('%s', '%s').\n", str(EHOSTDOWN), strerror(EHOSTDOWN));

  // The destination host cannot be reached
  // (probably because the host is down or a remote router cannot reach it).
  printf("tcp_error('%s', '%s').\n", str(EHOSTUNREACH), strerror(EHOSTUNREACH));
  
  // O_NONBLOCK is set for the file descriptor for the socket
  // and the connection cannot be immediately established;
  // the connection shall be established asynchronously.
  printf("tcp_error('%s', '%s').\n", str(EINPROGRESS), strerror(EINPROGRESS));
  
  // The attempt to establish a connection was interrupted
  // by delivery of a signal that was caught;
  // the connection shall be established asynchronously.
  printf("tcp_error('%s', '%s').\n", str(EINTR), strerror(EINTR));
  
  // An I/O error occurred while reading from or writing to the file system.
  printf("tcp_error('%s', '%s').\n", str(EIO), strerror(EIO));
  
  // The specified socket is connection-mode and is already connected.
  printf("tcp_error('%s', '%s').\n", str(EISCONN), strerror(EISCONN));
  
  // A loop exists in symbolic links encountered during resolution
  // of the pathname in address.
  printf("tcp_error('%s', '%s').\n", str(ELOOP), strerror(ELOOP));
  
  // A component of a pathname exceeded {NAME_MAX} characters,
  // or an entire pathname exceeded {PATH_MAX} characters.
  printf("tcp_error('%s', '%s').\n", str(ENAMETOOLONG), strerror(ENAMETOOLONG));
  
  // Network is down.
  printf("tcp_error('%s', '%s').\n", str(ENETDOWN), strerror(ENETDOWN));
  
  // Network dropped connection because of reset.
  printf("tcp_error('%s', '%s').\n", str(ENETRESET), strerror(ENETRESET));
  
  // Network is unreachable.
  // No route to the network is present.
  printf("tcp_error('%s', '%s').\n", str(ENETUNREACH), strerror(ENETUNREACH));
  
  // A component of the pathname does not name an existing file or
  // the pathname is an empty string.
  printf("tcp_error('%s', '%s').\n", str(ENOENT), strerror(ENOENT));
  
  // A component of the path prefix of the pathname in address
  // is not a directory.
  printf("tcp_error('%s', '%s').\n", str(ENOTDIR), strerror(ENOTDIR));
  
  // The socket argument does not refer to a socket.
  printf("tcp_error('%s', '%s').\n", str(ENOTSOCK), strerror(ENOTSOCK));
  
  // The specified address has a different type than
  // the socket bound to the specified peer address.
  printf("tcp_error('%s', '%s').\n", str(EPROTOTYPE), strerror(EPROTOTYPE));
  
  // The attempt to connect timed out before a connection was made.
  printf("tcp_error('%s', '%s').\n", str(ETIMEDOUT), strerror(ETIMEDOUT));
  
  // The address_len argument is not a valid length for the address family;
  // or invalid address family in the sockaddr structure.
  printf("tcp_error('%s', '%s').\n", str(EINVAL), strerror(EINVAL));
  
  // More than {SYMLOOP_MAX} symbolic links were encountered
  // during resolution of the pathname in address.
  printf("tcp_error('%s', '%s').\n", str(ELOOP), strerror(ELOOP));
  
  // Pathname resolution of a symbolic link produced
  // an intermediate result whose length exceeds {PATH_MAX}.
  printf("tcp_error('%s', '%s').\n", str(ENAMETOOLONG), strerror(ENAMETOOLONG));
  
  // The local network interface used to reach the destination is down.
  printf("tcp_error('%s', '%s').\n", str(ENETDOWN), strerror(ENETDOWN));
  
  // No buffer space is available.
  printf("tcp_error('%s', '%s').\n", str(ENOBUFS), strerror(ENOBUFS));
  
  // The socket is listening and cannot be connected.
  printf("tcp_error('%s', '%s').\n", str(EOPNOTSUPP), strerror(EOPNOTSUPP));
  
  // The other end closed the socket unexpectedly
  // or a read is executed on a shut down socket. 
  printf("tcp_error('%s', '%s').\n", str(EPIPE), strerror(EPIPE));
  
  // Remote I/O error.
  printf("tcp_error('%s', '%s').\n", str(EREMOTEIO), strerror(EREMOTEIO));
  
  // The other end didn't acknowledge retransmitted data after some time.
  printf("tcp_error('%s', '%s').\n", str(ETIMEDOUT), strerror(ETIMEDOUT));
  
  return 0;
}

