#include <errno.h>
#include <stdio.h>
#include <string.h>

#define str(i) #i

main()
{
  // Search permission is denied for a component of the path prefix;
  // or write access to the named socket is denied.
  printf("tcp_error('%s', '%s').\n", str(EACCES), strerror(EACCES));
  
  // Address already in use.
  // Attempt to establish a connection
  // that uses addresses that are already in use.
  printf("tcp_error('%s', '%s').\n", str(EADDRINUSE), strerror(EADDRINUSE));
  
  // Cannot assign requested address.
  // The specified address is not available from the local machine.
  printf("tcp_error('%s', '%s').\n", str(EADDRNOTAVAIL), strerror(EADDRNOTAVAIL));
  
  // Address family not supported.
  // The specified address is not a valid address
  // for the address family of the specified socket.
  printf("tcp_error('%s', '%s').\n", str(EAFNOSUPPORT), strerror(EAFNOSUPPORT));
  
  // Operation already in progress.
  // A connection request is already in progress for the specified socket.
  printf("tcp_error('%s', '%s').\n", str(EALREADY), strerror(EALREADY));
  
  // The socket argument is not a valid file descriptor.
  printf("tcp_error('%s', '%s').\n", str(EBADF), strerror(EBADF));
  
  // Invalid slot.
  printf("tcp_error('%s', '%s').\n", str(EBADSLT), strerror(EBADSLT));
  
  // Connection aborted.
  // Software caused connection abort.
  printf("tcp_error('%s', '%s').\n", str(ECONNABORTED), strerror(ECONNABORTED));
  
  // The target address was not listening for connections or
  // refused the connection request.
  printf("tcp_error('%s', '%s').\n", str(ECONNREFUSED), strerror(ECONNREFUSED));
  
  // Connection reset by peer.
  // Remote host reset the connection request.
  printf("tcp_error('%s', '%s').\n", str(ECONNRESET), strerror(ECONNRESET));
  
  // Ran out of disk quota.
  printf("tcp_error('%s', '%s').\n", str(EDQUOT), strerror(EDQUOT));
  
  // Host is down.
  printf("tcp_error('%s', '%s').\n", str(EHOSTDOWN), strerror(EHOSTDOWN));
  
  // No route to host.
  // The destination host cannot be reached
  // (probably because the host is down or a remote router cannot reach it).
  printf("tcp_error('%s', '%s').\n", str(EHOSTUNREACH), strerror(EHOSTUNREACH));
  
  // Operation now in progress.
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
  
  // Socket is already connected.
  // The specified socket is connection-mode and is already connected.
  printf("tcp_error('%s', '%s').\n", str(EISCONN), strerror(EISCONN));
  
  // The address_len argument is not a valid length for the address family;
  // or invalid address family in the sockaddr structure.
  printf("tcp_error('%s', '%s').\n", str(EINVAL), strerror(EINVAL));
  
  // A loop exists in symbolic links encountered during resolution
  // of the pathname in address.
  // More than {SYMLOOP_MAX} symbolic links were encountered
  // during resolution of the pathname in address.
  printf("tcp_error('%s', '%s').\n", str(ELOOP), strerror(ELOOP));
  
  // Message too long.
  printf("tcp_error('%s', '%s').\n", str(EMSGSIZE), strerror(EMSGSIZE));
  
  // Network is down.
  // The local network interface used to reach the destination is down.
  printf("tcp_error('%s', '%s').\n", str(ENETDOWN), strerror(ENETDOWN));
  
  // Network dropped connection on reset.
  // Network dropped connection because of reset.
  printf("tcp_error('%s', '%s').\n", str(ENETRESET), strerror(ENETRESET));
  
  // Network is unreachable.
  // No route to the network is present.
  printf("tcp_error('%s', '%s').\n", str(ENETUNREACH), strerror(ENETUNREACH));
  
  // No buffer space is available.
  printf("tcp_error('%s', '%s').\n", str(ENOBUFS), strerror(ENOBUFS));
  
  // Valid name, no data record of requested type.
  printf("tcp_error('%s', '%s').\n", str(ENODATA), strerror(ENODATA));
  
  // A component of the pathname does not name an existing file or
  // the pathname is an empty string.
  printf("tcp_error('%s', '%s').\n", str(ENOENT), strerror(ENOENT));
  
  // Socket is not connected.
  printf("tcp_error('%s', '%s').\n", str(ENOTCONN), strerror(ENOTCONN));
  
  // A component of the path prefix of the pathname in address
  // is not a directory.
  printf("tcp_error('%s', '%s').\n", str(ENOTDIR), strerror(ENOTDIR));
  
  // Socket operation on nonsocket.
  // The socket argument does not refer to a socket.
  printf("tcp_error('%s', '%s').\n", str(ENOTSOCK), strerror(ENOTSOCK));
  
  // The socket is listening and cannot be connected.
  printf("tcp_error('%s', '%s').\n", str(EOPNOTSUPP), strerror(EOPNOTSUPP));
  
  // Protocol family not supported.
  printf("tcp_error('%s', '%s').\n", str(EPFNOSUPPORT), strerror(EPFNOSUPPORT));
  
  // The other end closed the socket unexpectedly
  // or a read is executed on a shut down socket. 
  printf("tcp_error('%s', '%s').\n", str(EPIPE), strerror(EPIPE));
  
  // Protocol error.
  printf("tcp_error('%s', '%s').\n", str(EPROTO), strerror(EPROTO));
  
  // Protocol not supported.
  printf("tcp_error('%s', '%s').\n", str(EPROTONOSUPPORT), strerror(EPROTONOSUPPORT));
  
  // Protocol wrong type for socket.
  // The specified address has a different type than
  // the socket bound to the specified peer address.
  printf("tcp_error('%s', '%s').\n", str(EPROTOTYPE), strerror(EPROTOTYPE));
  
  // Item is not available locally.
  printf("tcp_error('%s', '%s').\n", str(EREMOTE), strerror(EREMOTE));
  
  // Remote I/O error.
  printf("tcp_error('%s', '%s').\n", str(EREMOTEIO), strerror(EREMOTEIO));
  
  // Cannot send after socket shutdown.
  printf("tcp_error('%s', '%s').\n", str(ESHUTDOWN), strerror(ESHUTDOWN));
  
  // File handle reference is no longer available.
  printf("tcp_error('%s', '%s').\n", str(ESTALE), strerror(ESTALE));
  
  // Socket type not supported.
  printf("tcp_error('%s', '%s').\n", str(ESOCKTNOSUPPORT), strerror(ESOCKTNOSUPPORT));
  
  // Operation timed out.
  // The other end didn't acknowledge retransmitted data after some time.
  // The attempt to connect timed out before a connection was made.
  printf("tcp_error('%s', '%s').\n", str(ETIMEDOUT), strerror(ETIMEDOUT));
  
  // Too many references.
  printf("tcp_error('%s', '%s').\n", str(ETOOMANYREFS), strerror(ETOOMANYREFS));
  
  // Ran out of quota.
  printf("tcp_error('%s', '%s').\n", str(EUSERS), strerror(EUSERS));
  
  // Operation would block.
  printf("tcp_error('%s', '%s').\n", str(EWOULDBLOCK), strerror(EWOULDBLOCK));
  
  return 0;
}

