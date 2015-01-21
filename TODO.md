TODO
====

  [ ] Bugs:
    [x] Archives are not closed by `ll_triple/1`.
    [x] Max limit (50 errors) for XML/RDF file parsing.
    [x] Turtle family files only containing comments are guessed incorrectly.
    [o] Cannot download over FTP. @swipl-mailinglist
    [x] Backslashed in IRI are not recognized as RDF (correctly). Is this too strict? Examples: b36dde5e58e161c09b41a295b7a5599f, 63c2f001e5cf428e8a98db4aac718db4, 0d9a179fb9b080dbecd19553a48f163c.
  [x] Debug:
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
    [x] Clean on a per-triple basis.
    [x] Disk-based sorting and deduplication.
    [ ] Store more HTTP headers per dirty document.
    [ ] Keep track of non-canonical lexical forms.
  [x] ll:
    [x] Run llWM.
    [x] Extend seed list.
  [ ] llPack:
    [ ] Create a Prolog Pack for LOD Laundromat.
    [ ] Demo the llPack at WAI 2015/01/12.
