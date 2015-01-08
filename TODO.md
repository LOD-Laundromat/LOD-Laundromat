TODO
====

  [ ] Bugs:
    [x] Archives are not closed by `ll_triple/1`.
  [ ] Debug:
    [o] Script that checks the validity of SPARQL queries in llWM.
  [ ] Swipl:
    [ ] `opt_arguments/3` is not steadfast.
        The following throws an exception:
        ```prolog
        opt_arguments(
          [[default(''),opt(debug),longflags([debug]),type(atom)]],
          _,
          [Dir]
        ),
       ```
  [ ] Lib:
    [ ] Implement HTTP headers:
      [ ] ...
      [ ] ...
      [ ] ...
  [ ] llWM:
    [o] Clean on a per-triple basis.
    [o] Disk-based sorting and deduplication.
    [ ] Store more HTTP headers per dirty document.
    [ ] Keep track of non-canonical lexical forms.
  [ ] ll:
    [o] Run llWM.
    [ ] Extend seed list.
  [ ] llPack:
    [ ] Create a Prolog Pack for LOD Laundromat.
    [ ] Demo the llPack at WAI 2015/01/12.
