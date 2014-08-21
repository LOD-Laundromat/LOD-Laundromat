TODO
====

  - `opt_arguments/3` is not steadfast.
    The following throws an exception:
    ~~~{.pl}
    opt_arguments(
      [[default(''),opt(debug),longflags([debug]),type(atom)]],
      _,
      [Dir]
    ),
    ~~~
  - URI file extensions -> file extension
    (asserted multiple times for archive entries).

