TODO
====

  [ ] Bugs:
    [x] Archives are not closed by `ll_triple/1`.
    [x] Max limit (50 errors) for XML/RDF file parsing.
    [x] Turtle family files only containing comments are guessed incorrectly.
    [x] Backslashed in IRI are not recognized as RDF (correctly). Is this too strict? Examples: b36dde5e58e161c09b41a295b7a5599f, 63c2f001e5cf428e8a98db4aac718db4, 0d9a179fb9b080dbecd19553a48f163c.
  [x] Debug:
    [x] Script that checks the validity of SPARQL queries in llWM.
  [ ] Lib:
    [ ] Implement HTTP headers:
      [ ] Indicating Content-Type of archived entry.
      [ ] ...
      [ ] ...
    [ ] Add support for downloading FTP URIs.
  [ ] llWM:
    [x] Clean on a per-triple basis.
    [x] Disk-based sorting and deduplication.
    [ ] Store more HTTP headers per cleaned document.
    [ ] Keep track of non-canonical lexical forms.
    [x] Integrate simplified ClioPatria with traditional Virtuoso version.
    [ ] Use authorization for resetting data documents on a ClioPatria
        backend. Look at SPARQL Update in ClioPatria for inspiration.
  [x] ll:
    [x] Run llWM.
    [x] Extend seed list.
  [ ] llPack:
    [ ] Create a Prolog Pack for LOD Laundromat.



Notes
=====

  - 2570ea2d41d6f3c2ce902ec233e51a68 contains Illegal UTF-8 start (183:0).
  - swipl run.pl --debug --dir=/scratch/lodlaundromat/crawls/12/ --mode=restart

