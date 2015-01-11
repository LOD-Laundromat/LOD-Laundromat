TODO
====

  [ ] Bugs:
    [x] Archives are not closed by `ll_triple/1`.
    [ ] Max limit (50 errors) for XML/RDF file parsing.
    [x] Turtle family files only containing comments are guessed incorrectly.
    [o] Cannot download over FTP. @swipl-mailinglist
    [o] Cannot retrieve name of archive entry in single-entry archives.
  [ ] Debug:
    [x] Script that checks the validity of SPARQL queries in llWM.
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
    [x] Clean on a per-triple basis.
    [o] Disk-based sorting and deduplication.
    [ ] Store more HTTP headers per dirty document.
    [ ] Keep track of non-canonical lexical forms.
  [ ] ll:
    [o] Run llWM.
    [ ] Extend seed list.
  [ ] llPack:
    [ ] Create a Prolog Pack for LOD Laundromat.
    [ ] Demo the llPack at WAI 2015/01/12.
