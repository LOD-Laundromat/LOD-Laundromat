TCP error codes
===============

The connect() function shall fail if: 
  - EADDRNOTAVAIL
  - EAFNOSUPPORT
  - EALREADY
  - EBADF
  - ECONNREFUSED
  - EINPROGRESS
  - EINTR
  - EISCONN
  - ENETUNREACH
  - ENOTSOCK
  - EPROTOTYPE
  - ETIMEDOUT

If the address family of the socket is AF_UNIX, then connect() shall fail if:
  - EIO
  - ELOOP
  - ENAMETOOLONG
  - ENOENT
  - ENOTDIR

The connect() function may fail if:
  - EACCES
  - EADDRINUSE
  - ECONNRESET
  - EHOSTUNREACH
  - EINVAL
  - ELOOP
  - ENAMETOOLONG
  - ENETDOWN
  - ENOBUFS
  - EOPNOTSUPP

