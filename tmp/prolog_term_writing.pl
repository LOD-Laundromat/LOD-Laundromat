ll_portray(Blob, Options) :-
  blob(Blob, Type),
  \+ atom(Blob),
  Type \== reserved_symbol,
  write_term('BLOB'(Type), Options).

  format(
    Out,
    "~a\t~W\n",
    [Alias,E2,[blobs(portray),portray_goal(ll_portray),quoted(true)]]
  )
