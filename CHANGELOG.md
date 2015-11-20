Changelog LOD Laundromat 13 (changes since LOD Laundromat 12)
=============================================================

This is the version on (2015/11/20).

Major features
--------------

  - **LOD-Laundromat-CLI**: Allows the LOD Washing Machine to be run
    locally, from your command prompt.  This requires installing
    SWI-Prolog.
  - **New stains**: The following data stains are now recorded in
    the metadata.  (1) invalidCentralDirectorySignature (archive error),
    (2) HTTP status codes in the 4xx- and 5xx-range.


Deployment
----------

  - The scrape version number is now set as part of the settings
    configuration.
  - Make it less easy to reset all crawled data documents at once.
  - LIFO processing of seed list and intermediary (e.g., unpacked) results.


Bugfixes
--------

  - Slowdown when very many (>100K) seedpoints are stored based on
    metadata encountered while cleaning a data file.
  - VoID seedpoints were incorrectly typed as archive entries.



Changelog LOD Laundromat 12 (changes since LOD Laundromat 10)
=============================================================

This is the version on (2015/02/12).

Major features
--------------

  - **Streamed cleaning**: data documents are now cleaned inside a stream,
    relying on very little memory.
  - **Multi-threaded cleaning**: the ability to start multiple cleaning
    and unpacking threads that work on the pool of pending data documents.
  - **Multiple scrapes**: the ability to run multiple scraping processes
    (which can themselves be multi-thread) in parallel.
  - **Named graph support**:  if at least one named graph occurs
    in an original data document, then the cleaned document is stored
    in C-Quads (a canonical subset of N-Quads).
    These files are named `clean.nq.gz` i.o. `clean.nt.gz`.
  - **Lexicographic sorting**: triples and quadruples in clean documents
    are now sorted. (The order of statements used to be arbitrary.)
  - **Vocabulary**: an RDFS description of all the metadata properties
    that are added by the scraping and cleaning process, and an RDFS
    description of all the errors that may occur while
    downloading (e.g., TCP/IP), unpacking (e.g., libarchive),
    and cleaning (e.g., Turtle parser) data documents.
  - **Namespaces**: distinction between resource and ontology IRIs.

Deployment
----------

  - **Linux sevices**: the LOD Laundromat now consists of a collection of
    services that automatically restart upon server crashes/restarts.
  - **Modular code**: the LOD Laundromat codebase is split into modular
    projects and parts (e.g., _llWashingMachine_ for collecting+cleaning
    data, _llEndpoint_ for debugging purposes, and a backend repository
    implementing the LOD Laundromat Web services and Web site.
  - **Settings architecture**: settings and services can now be set in
    external settings files.
  - The ability to retain the dirty/original data files next to the cleaned
    data files.

Website
-------

  - **Code samples**: easy-to-(re)use code samples for using the
    LOD Laundromat service in various popular programming languages:
    Python, Java, JavaScript, SWI-Prolog.
  - **Analytics widgets**: statistics widgets that illustrate some of the
    properies of cleaned LOD Cloud data.
  - **'About' page**: explaining what the LOD Laundromat project is about
    and how its various parts are constructed and evaluated.

Bugfixes
--------

  - Archives:
    - Archives with directory paths in them would not be unpacked.
    - Archives with entries in different compression formats
      would not be processed correctly.
    - Archives with multiple enties would receive two `end_unpack` timestamps.
    - Archives with no contents at all would not enter the cleaning phase.
      (The correct behavior is to trivially pass through the cleaning phase.)
    - The void:dataDump property should not occur for archives
      (only for archive entries).
  - HTTP:
    - The HTTP Content-Type property was not used for guessing
      the RDF serialization format of downloaded documents.
    - HTTP redirects that require the setting of cookies were not followed.
    - Unpacking would stop whenever the triple store/backend went offline.
      (The correct behavior is to wait for the backend to auto-restart
      and come back online again.)
  - Serialization format guessing:
    - N-Quads files with graph terms were guessed incorrectly.
    - Turtle-family files with a blank node terms were guessed incorrectly.
    - Improved recognition of HTML document versions, resulting in fewer
      false negative parsing errors for RDFa.
  - Serialization format processing:
    - N-Triples files were never cleaned.
    - Handle HTML documents with multiple root elements
      (grammatically incorrect, but appears in the wild).
    - The parser would illegitimately break on Processing Instructions (PIs)
      appearing in an XML document.
    - HTML and XML documents with more than 50 errors would not be parsed.
  - POSIX/Linux/Bash:
    - Files with braces in their file name would be subject to
      Bash brace expansion and would not be found on the disk.

Known limitations
=================

    - Blank nodes that occur in the metadata describing the scraping process
      are formatted `_:x<NUMBER>` i.o. `_:<NUMBER>` to be Virtuoso-compliant.
    - The messages for warnings and exceptions are truncated at
      1000 characters and only the first 100 warnings are stored because
      of the bytesize bounds that Virtuoso enforces on SPARQL Updates.
    - Virtuoso cannot handle very many near-simultaneaus SPARQL Update
      requests (HTTP 413).
      New seed URIs from VoID descriptions can therefore not
      be added to the datastore directly but are written to a text file
      which has to be manually imported.
    - Some servers would block multiple nearly-simultaneous requests
      for downloading different documents.  The unpacking threads now only
      pick documents whose authority is not currently being downloaded from
      by another thread.
    - Files are not downloaded over (S)FTP.

