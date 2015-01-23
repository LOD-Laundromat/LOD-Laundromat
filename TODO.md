TODO
====

  [ ] Bugs:
    [x] Archives are not closed by `ll_triple/1`.
    [x] Max limit (50 errors) for XML/RDF file parsing.
    [o] Max limit (50 errors) for XML/RDF file parsing. Reset datadocs that have this.
    [x] Turtle family files only containing comments are guessed incorrectly.
    [ ] Cannot download over FTP. @swipl-mailinglist
    [x] Vastly simplified RDF guessing: we want the parser to enumerate the syntax errors.
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
      [ ] Indicating Content-Type of archived entry.
      [ ] ...
      [ ] ...
  [ ] llWM:
    [x] Clean on a per-triple basis: Turtle-family.
    [o] Clean on a per-triple basis: XML-family.
    [o] Disk-based sorting and deduplication.
    [ ] Store more HTTP headers per dirty document.
    [ ] Keep track of non-canonical lexical forms.
  [ ] ll:
    [x] Run llWM.
    [ ] Extend seed list.
  [ ] llPack:
    [ ] Create a Prolog Pack for LOD Laundromat.
    [ ] Demo the llPack at WAI 2015/01/12.
