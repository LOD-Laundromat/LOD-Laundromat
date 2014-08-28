Changelog LOD Laundromat 11 (changes since LL10)
================================================

Major features
--------------

  - Added support for named graphs.
    There are saved as C-Quads (a canonical subset of N-Quads).
    Cleaned files are named `clean.nq.gz` i.o. `clean.nt.gz`.
  - Triples in clean data documents are now sorted lexicographically.
    The order of triples used to be arbitrary.
  - Ability to parse more RDFa file:
    -- Improved recognition of HTML document versions, resulting in fewer parsing errors..
    -- Handle HTML documents with multiple root elements (grammatically incorrect, but appears in the wild).
  - Addition of a small ontology in which all properties that are used in LOD Laundromat scrape metadata are defined in terms of RDFS.
  - Inclusion of data documents that are serialized in the N-triples format (these were not included in LL10).
  - IRIs that denote data (resources) and vocabulary (ontology) now belong to different namespaces.

Deployment
----------

  - LL is now a collection of Linux services that automatically restart upon server crashes/restarts.
  - The LL codebase was split into multiple parts: llWashingMachine for collecting+cleaning data, llEndpoint for debugging purposes, and a backend repository which implements the LL Web services.
  - The ability to store files of old LL scrapes next to each other (used in debugging, turned off in production).
  - The ability to retain the dirty/original data files in a compressed format (useful for debugging, but turned off in production).

Website
-------

  - Easy-to-(re)use code samples for using the LL service in some of the most popular programming languages (Python, Java, JavaScript).
  - Multiple analytics widgets that show statistics about the cleaned data.

Bugfixes
--------

  - Archives with directory paths in them would not be unpacked.
  - Archives with entries in different compression formats would not be processed correctly.
  - Archives with multiple enties would receive two end_unpack timestamps.
  - Unpacking would stop whenever the triple store / backend went offline (correct behavior: wait for the backend to auto-restart and come back online again).
  - The HTTP Content-Type property was not used for guessing the RDF serialization format of downloaded documents.
  - HTTP redirects that require the setting of cookies were not followed.
  - Archives with no contents at all would not enter the cleaning phase (correct behavior: they should trivially pass through the cleaning phase).
