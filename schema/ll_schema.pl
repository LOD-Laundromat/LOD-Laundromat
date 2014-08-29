:- module(
  ll_schema,
  [
    init_schema/1 % +Graph:atom
  ]
).

/** <module> LOD Laundromat: schema

Generates the schema file for the LOD Washing Machine.

@author Wouter Beek
@author Laurens Rietveld
@version 2014/06, 2014/08
*/

:- use_module(library(semweb/rdf_db)).

:- use_module(plRdf(rdf_prefixes)). % RDF prefix registrations.
:- use_module(plRdf(rdfs_build2)).

:- use_module(lle(lle_settings)). % RDF prefix registration.



assert_schema(Graph):-
  % ArchiveEntry and URL partition the space of data documents.
  % Some data documents are in Archive.

  % Some archives are archiveEntries.
  % Some archiveEntries are archives
  % No URL is an archiveEntry.
  % Every data document is represented by either an archiveEntry or a URL.

  % Archive.
  rdfs_assert_class(
    llo:'Archive',
    dcat:'Distribution',
    'file archive',
    'The class of resources that denote \c
     a data document that can be unpacked in order to reveal one or more \c
     data document entries. These entries are of type ArchiveEntry.\n\c
     Since archives can contain archives, there may be resources that are \c
     both Archive and ArchiveEntry.',
    Graph
  ),

  % ArchiveEntry.
  rdfs_assert_class(
    llo:'ArchiveEntry',
    dcat:'Distribution',
    'file archive entry',
    'The class of resource that denote \c
     data documents that are not directly downloaded over the Internet, \c
     but that are extracted from another data document. \c
     The data document from which the archive entry is extracted is always \c
     of type Archive, and can either be of type URL or of type ArchiveEntry.',
    Graph
  ),

  % URL.
  rdfs_assert_class(
    llo:'URL',
    dcat:'Distribution',
    'URL',
    'The class of resources denoting \c
     data documents that are directly downloaded over the Internet.\n\c
     Such URLs are always added as seed points to the LOD Basket, \c
     via an HTTP SEND request to the LOD Basket endpoint.\n\c
     These requests can be performed either by \c
     (1) a bash script we use to initialize the LOD Laundromat, \c
     (2) the procedure the LOD Washing Machine cleaning process uses \c
     to extract VoID datadump locations, and \c
     (3) human input, delivered through the HTML form at \c
     the LOD Laundromat dissemination Website.',
    Graph
  ),

  % Added.
  rdfs_assert_property(
    llo:added,
    dcat:'Distribution',
    xsd:dateTime,
    added,
    'The date and time at which the dirty data document was added \c
     to the LOD Basket.',
    Graph
  ),

  % Archive file type
  rdfs_assert_property(
    llo:archive_file_type,
    llo:'Archive',
    xsd:string,
    'archive file type',
    'The high-level file type of an archive file.\n\c
     Possible values: `file` and `dir`.\n\c
     An archive of type `dir` can be unpacked into archive entries.',
    Graph
  ),

  % Archive format.
  rdfs_assert_property(
    llo:archive_format,
    llo:'Archive',
    xsd:string,
    'archive format',
    'TODO',
    Graph
  ),

  % Archive last modified.
  rdfs_assert_property(
    llo:archive_last_modified,
    llo:'Archive',
    xsd:dateTime,
    'archive last modified',
    'TODO',
    Graph
  ),

  % Archive size.
  rdfs_assert_property(
    llo:archive_size,
    llo:'Archive',
    xsd:integer,
    'archive size',
    'TODO',
    Graph
  ),

  % Byte count.
  rdfs_assert_property(
    llo:byte_count,
    dcat:'Distribution',
    xsd:integer,
    'byte count',
    'The number of bytes that were processed in the stream of \c
     the dirty data document.',
    Graph
  ),

  % Character count.
  rdfs_assert_property(
    llo:character_count,
    dcat:'Distribution',
    xsd:integer,
    'character count',
    'The number of characters that were processed in the stream of \c
     the dirty data document.',
    Graph
  ),

  % Contains entry.
  rdfs_assert_property(
    llo:contains_entry,
    llo:'Archive',
    llo:'ArchiveEntry',
    'contains entry',
    'A link between a parent archive and one of its direct archive entries.',
    Graph
  ),

  % Content length.
  rdfs_assert_property(
    llo:content_length,
    llo:'URL',
    xsd:integer,
    'content length',
    'The number of bytes denoted in the Content-Length header \c
     of the HTTP reply message, received upon downloading a single \c
     dirty data document of type URL. \c
     Availability of this information depends on whether \c
     the disseminating host can be accessed and the HTTP reply contains \c
     the factum.',
    Graph
  ),

  % Content type.
  rdfs_assert_property(
    llo:content_type,
    llo:'URL',
    xsd:string,
    'content type',
    'The value of the Content-Type header of the HTTP reply message, \c
     received upon downloading a single data document of type URL. \c
     Availability of this information depends on whether \c
     the disseminating host can be accessed and the HTTP reply contains \c
     the factum.',
    Graph
  ),

  % Duplicates.
  rdfs_assert_property(
    llo:duplicates,
    dcat:'Distribution',
    xsd:integer,
    'number of duplicate triples',
    'The number of triples that are duplicates of other triples \c
     in the same dirty data document.',
    Graph
  ),

  % End cleaning.
  rdfs_assert_property(
    llo:end_clean,
    dcat:'Distribution',
    xsd:dateTime,
    'end cleaning a data document',
    'The date and time at which the process of cleaning the data document \c
     ended.',
    Graph
  ),

  % End unpacking.
  rdfs_assert_property(
    llo:end_unpack,
    dcat:'Distribution',
    xsd:dateTime,
    'end unpacking a data document',
    'The date and time at which the process of downloading and unpacking \c
     the data document ended.',
    Graph
  ),

  % Exception.
  rdfs_assert_property(
    llo:exception,
    dcat:'Distribution',
    xsd:string,
    status,
    'A blockig exception that occured somewhere during the unpacking \c
     and/or cleaning process. Possible values: \c
     (1) fail, failed to unpack/clean due to an unanticipated reason. \c
     (2) true, successfully unpacked and cleaned data document. \c
     (3) exception, failed to unpack/clean due to an anticipated reason.',
    Graph
  ),

  % File extension.
  rdfs_assert_property(
    llo:file_extension,
    dcat:'Distribution',
    xsd:string,
    'file extension',
    'The file extension of a the dirty data document.\n\c
     This is only set for data documents that can be downloading and \c
     extracted, and that have a file extension.',
    Graph
  ),

  % Last modified.
  rdfs_assert_property(
    llo:last_modified,
    llo:'URL',
    xsd:dateTime,
    'last modified',
    'The date and time denoted by the Last-Modified header of \c
     the HTTP reply message, \c
     received upon downloading a single data document of type URL. \c
     Availability of this information depends on whether \c
     the disseminating host can be accessed and the HTTP reply contains \c
     the factum.',
    Graph
  ),

  % Line count.
  rdfs_assert_property(
    llo:line_count,
    dcat:'Distribution',
    xsd:integer,
    'line count',
    'The number of lines that were processed in the stream of \c
     the data document.',
    Graph
  ),

  % MD5.
  rdfs_assert_property(
    llo:md5,
    dcat:'Distribution',
    xsd:string,
    'MD5',
    'The unique identifier of the data document, \c
     derived by taking the MD5 hash of the source
     of the data document. The source of a data document is either its URL,
     or the pair of (1) the source of the archive from which it was derived,
     and (2) its entry path within that archive.',
    Graph
  ),

  % Path.
  rdfs_assert_property(
    llo:path,
    dcat:'ArchiveEntry',
    xsd:string,
    'file archive path',
    'For data documents that are entries in a file archive, \c
     the path of the data document in that file archive.',
    Graph
  ),

  % Serialization format
  rdfs_assert_property(
    llo:serialization_format,
    dcat:'Distribution',
    rdfs:'Resource',
    'serialization format',
    'The RDF serialization format that the dirty data document \c
     was parsed in.\n\c
     This format is determined based on a parse of an initial portion of \c
     the file contents, the file extension (if any) \c
     and the value of the HTTP Content-Type header (if any).\n\c
     The possible values are: JSON-LD, N-Quads, N-Triples, RDF/XML, \c
     RDFa, Turtle, TriG.',
    Graph
  ),

  % Size.
  rdfs_assert_property(
    llo:size,
    dcat:'Distribution',
    xsd:integer,
    'byte size',
    'The size of the downloaded dirty data document, \c
     represented in bytes on disk.\n\c
     Availability: Any data document that can be downloaded/unpacked.',
    Graph
  ),

  % Start cleaning.
  rdfs_assert_property(
    llo:start_clean,
    dcat:'Distribution',
    xsd:dateTime,
    'start cleaning a data document',
    'The date and time at which the process of cleaning the data document \c
     started.',
    Graph
  ),

  % Start unpacking.
  rdfs_assert_property(
    llo:start_unpack,
    dcat:'Distribution',
    xsd:dateTime,
    'start unpacking a data document',
    'The date and time at which the process of downloading and unpacking \c
     the data document started.',
    Graph
  ),

  % Triples.
  rdfs_assert_property(
    llo:triples,
    dcat:'Distribution',
    xsd:integer,
    triples,
    'The number of triples that could be read from the dirty data document. \c
     This is also the number of triples that is stored in \c
     the cleaned data document.
     This is after triple deduplication.',
    Graph
  ),

  % URL.
  rdfs_assert_property(
    llo:url,
    llo:'URL',
    rdfs:'Resource',
    'URL',
    'The URL from which the original version of the data document \c
     was downloaded.',
    Graph
  ),
  
  % Warning.
  rdfs_assert_property(
    llo:warning,
    dcat:'Distribution',
    xsd:string,
    message,
    'A non-blocking warning message that is either emitted \c
     while downloading, unpacking, or cleaning a dirty data document.\n\c
     Possible values: (1) TODO \c
       (1) Syntax error while parsing RDF file. \c
       (2) No RDF in file.',
    Graph
  ),
  
  % unrecognizedFormat
  rdfs_assert_instance(
    llo:unrecognizedFormat,
    rdfs:'Resource',
    'unrecognized format',
    'A syntactic format that is not recognized as any of \c
     the RDF serialization formats.',
    Graph
  ).


init_schema(Graph):-
  rdf_graph(Graph), !.
init_schema(Graph):-
  assert_schema(Graph).

