TODO
====

  [ ] Bugs:
    [x] Archives are not closed by `ll_triple/1`.
    [ ] Max limit (50 errors) for XML/RDF file parsing.
    [ ] RDF documents from the Turtle family that only contain comments would not be guessed as such. Example: data.dws.informatik.uni-mannheim.de/dbpedia/3.8/sl/geo_coordinates_sl.nt.bz2 ; http://lodlaundromat.org/resource/9e8fc4e5a8df6e3e3be5ebf8078f20cf.
  [ ] Cannot download over FTP. Example:  	

    ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/rdf/uniprot.rdf.gz ; http://lodlaundromat.org/resource/c5e884598311fff3d52018a12133de0c.
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
