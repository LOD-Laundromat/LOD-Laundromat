Changelog LOD Laundromat 11 (changes since LOD Laundromat 10)
=============================================================

This is the version on (2015/01/11).

Major features
--------------

  - Added support for **named graphs**.  If at least one named graph occurs
    in the original data document, then the cleaned document is stored
    in C-Quads (a canonical subset of N-Quads).
    These files are named `clean.nq.gz` i.o. `clean.nt.gz`.
  - Triples and Quadruples in clean documents are now **sorted
    lexicographically**.  (The order of triples used to be arbitrary.)
  - Addition of an **ontology** which describes all the metadata properties
    that are added by the LOD Laundromat in the process of cleaning LOD.
  - Addition of an **ontology** which describes errors that may occur
    in downloading (e.g., TCP/IP), unpacking (e.g., libarchive),
    and cleaning (e.g., Turtle parser) data documents.
  - IRIs that denote data (resources) and the newly introduced
    vocabulary (ontology) now belong to different **namespaces**.

Deployment
----------

  - The LOD Laundromat now consists of a collection of **Linux services**
    that automatically restart upon server crashes/restarts.
  - The ability to store files of multiple LOD Laundromat versions
    next to each other (used in debugging, turned off in production).
  - The ability to retain the dirty/original data files in a compressed
    format (useful for comparisons; used in debugging, turned off
    in production).
  - The ability to start multiple cleaning threads that work on
    dirty data documents of varying size.  This allows multi-threading
    without exhausting *memory* resources too quickly.
    Threads can be created for small, medium, or large cleaning jobs.
  - Restriction of the downloaded+unpacked-but-not-yet-cleaned document pool
    to (currently) 100 instances.  This prevents the *hard disk* from getting
    filled with dirty data in case the cleaning threads lag behind too much.
  - The LOD Laundromat codebase is split into modular parts:
    llWashingMachine for collecting+cleaning data,
    llEndpoint for debugging purposes, and a backend repository which
    implements the LOD Laundromat Web services and Web site.

Website
-------

  - Easy-to-(re)use **code samples** for using the LOD Laundromat service
    in some of the most popular programming languages (Python, Java,
    JavaScript).
  - Multiple **analytics widgets** that show statistics about
    the cleaned data.
  - Addition of the **'About' page** which explains what the LOD Laundromat
    project is about.

Bugfixes
--------

  - Archives:
    - Archives with directory paths in them would not be unpacked.
    - Archives with entries in different compression formats
      would not be processed correctly.
    - Archives with multiple enties would receive two `end_unpack` timestamps.
    - Archives with no contents at all would not enter the cleaning phase.
      (The correct behavior is to trivially pass through the cleaning phase.)
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
  - POSIX/Linux/Bash:
    - Files with braces in their file name would be subject to
      Bash brace expansion and would not be found on the disk.

Known limitations
=================

    - Blank nodes that occur in the metadata describing the scraping process
      are formatted `_:x<NUMBER>` i.o. `_:<NUMBER>` to be Virtuoso-compliant.
    - The messages for warnings and exceptions are truncated at 1000 characters
      and only the first 100 warnings are stored because of the bytesize bounds
      that Virtuoso enforces on SPARQL Updates.
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
