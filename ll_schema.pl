:- module(
  ll_schema,
  [
    assert_ll_schema/1 % +Graph:atom
  ]
).

/** <module> LOD Washing Machine: schema

Generates the schema file for the LOD Washing Machine.

## TODO

### ll:file_extension
If present, the file extension of a single data document.
Availability: any data document that can be downloaded/unpacked.

### ll:message
A non-blocking warning message emitted during the unpacking and/or cleaning process.
Availability: Zero or more per data document.
Possible values:

    Syntax error while parsing RDF file.
    No RDF in file.
    ...

### ll:size
The size of a single data document on disk.
Availability: Any data document that can be downloaded/unpacked.

ll:archive_file_type: type van archive entry. mogelijke waardes (?): 'file' en 'dir'. Een dir zou opnieuw uitgepakt worden in meerdere archive entries
ll:archive_last_modified: hmm, geen idee! Is dit metadata die je uit een archive kan halen?
ll:archive_size: de -compressed- size van een archive? Wat is de relatie met ll:size?
ll:duplicates: aantal duplicates
ll:serialization_format: XML/JSON/RDFa/turtle/etc
ll:triples: total aantal triples  
ll:contains_entry: link tussen parent archive, en archive contents

@author Wouter Beek
@author Laurens Rietveld
@version 2014/06, 2014/08
*/

:- use_module(plRdf(rdf_prefixes)). % Registrations.
:- use_module(plRdf(rdfs_build2)).

:- use_module(lwm(lwm_generics)).



assert_ll_schema(Graph):-
  % ArchiveEntry and URL partition the space of data documents.
  % Some data documents are in Archive.
  
  % Some archives are archiveEntries.
  % Some archiveEntries are archives
  % No URL is an archiveEntry.
  % Every data document is represented by either an archiveEntry or a URL.
  
  % Archive.
  rdfs_assert_class(
    ll:'Archive',
    dcat:'Distribution',
    'file archive',
    'The class of resources that denote\c
     a data document that can be unpacked in order to reveal one or more\c
     data document entries. These entries are of type ArchiveEntry.\c\c
     Since archives can contain archives, there may be resources that are\c
     both Archive and ArchiveEntry.',
    Graph
  ),
  
  % ArchiveEntry.
  rdfs_assert_class(
    ll:'ArchiveEntry',
    dcat:'Distribution',
    'file archive entry',
    'The class of resource that denote\c
     data documents that are not directly downloaded over the Internet,\c
     but that are extracted from another data document.\c
     The data document from which the archive entry is extracted is always\c
     of type Archive, and can either be of type URL or of type ArchiveEntry.',
    Graph
  ),
  
  % URL.
  rdfs_assert_class(
    ll:'URL',
    dcat:'Distribution',
    'URL',
    'The class of resources denoting\c
     data documents that are directly downloaded over the Internet.\c\c
     Such URLs are always added as seed points to the LOD Basket,\c
     via an HTTP SEND request to the LOD Basket endpoint.\c
     These requests can be performed either by\c
     (1) a bash script we use to initialize the LOD Laundromat,\c
     (2) the procedure the LOD Washing Machine cleaning process uses\c
     to extract VoID datadump locations, and\c
     (3) human input, delivered through the HTML form at\c
     the LOD Laundromat dissemination Website.',
    Graph
  ),
  
  
  % Added.
  rdfs_assert_property(
    ll:added,
    dcat:'Distribution',
    xsd:dateTime,
    added,
    'The date and time at which the dirty data document was added\c
     to the LOD Basket.',
    Graph
  ),
  
  % Archive format.
  rdfs_assert_property(
    ll:archive_format,
    ll:'Archive',
    xsd:string,
    'archive format',
    'TODO',
    Graph
  ),
  
  % Byte count.
  rdfs_assert_property(
    ll:byte_count,
    dcat:'Distribution',
    xsd:integer,
    'byte count',
    'The number of bytes that were processed in the stream of\c
     the dirty data document.',
    Graph
  ),
  
  % Character count.
  rdfs_assert_property(
    ll:character_count,
    dcat:'Distribution',
    xsd:integer,
    'character count',
    'The number of characters that were processed in the stream of\c
     the dirty data document.',
    Graph
  ),
  
  % Content length.
  rdfs_assert_property(
    ll:content_length,
    ll:'URL',
    xsd:integer,
    'content length',
    'The number of bytes denoted in the Content-Length header\c
     of the HTTP reply message, received upon downloading a single\c
     dirty data document of type URL.\c
     Availability of this information depends on whether\c
     the disseminating host can be accessed and the HTTP reply contains\c
     the factum.',
    Graph
  ),

  % Content type.
  rdfs_assert_property(
    ll:content_type,
    ll:'URL',
    xsd:string,
    'content type',
    'The value of the Content-Type header of the HTTP reply message,\c
     received upon downloading a single data document of type URL.\c
     Availability of this information depends on whether\c
     the disseminating host can be accessed and the HTTP reply contains\c
     the factum.',
    Graph
  ),

  % End cleaning.
  rdfs_assert_property(
    ll:end_clean,
    dcat:'Distribution',
    xsd:dateTime,
    'end cleaning',
    'The date and time at which the process of cleaning the data document\c
     ended.',
    Graph
  ),
  
  % End unpacking.
  rdfs_assert_property(
    ll:end_unpack,
    dcat:'Distribution',
    xsd:dateTime,
    'end unpacking',
    'The date and time at which the process of downloading and unpacking\c
     the data document ended.',
    Graph
  ),
  
  % Last modified.
  rdfs_assert_property(
    ll:last_modified,
    ll:'URL',
    xsd:dateTime,
    'last modified',
    'The date and time denoted by the Last-Modified header of\c
     the HTTP reply message,\c
     received upon downloading a single data document of type URL.\c
     Availability of this information depends on whether\c
     the disseminating host can be accessed and the HTTP reply contains\c
     the factum.',
    Graph
  ),
  
  % Line count.
  rdfs_assert_property(
    ll:line_count,
    dcat:'Distribution',
    xsd:integer,
    'line count',
    'The number of lines that were processed in the stream of\c
     the data document.',
    Graph
  ),
  
  % MD5.
  rdfs_assert_property(
    ll:md5,
    dcat:'Distribution',
    xsd:string,
    'MD5',
    'The unique identifier of the data document,\c
     derived by taking the MD5 hash of the source
     of the data document. The source of a data document is either its URL,
     or the pair of (1) the source of the archive from which it was derived,
     and (2) its entry path within that archive.',
    Graph
  ),
  
  % Path.
  rdfs_assert_property(
    ll:path,
    dcat:'ArchiveEntry',
    xsd:string,
    'file archive path',
    'For data documnts that are entries in a file archive,\c
     the path of the data document in that file archive.',
    Graph
  ),
  
  % Start cleaning.
  rdfs_assert_property(
    ll:start_clean,
    dcat:'Distribution',
    xsd:dateTime,
    'start cleaning',
    'The date and time at which the process of cleaning the data document\c
     started.',
    Graph
  ),
  
  % Start unpacking.
  rdfs_assert_property(
    ll:start_unpack,
    dcat:'Distribution',
    xsd:dateTime,
    'start unpacking',
    'The date and time at which the process of downloading and unpacking\c
     the data document started.',
    Graph
  ),
  
  % Status.
  rdfs_assert_property(
    ll:status,
    dcat:'Distribution',
    xsd:string,
    status,
    'The status of the entire unpacking and/or cleaning process.\c
     Possible values:\c
     (1) fail, failed to unpack/clean due to an unanticipated reason.\c
     (2) true, successfully unpacked and cleaned data document.\c
     (3) exception, failed to unpack/clean due to an anticipated reason.',
    Graph
  ),
  
  % URL.
  rdfs_assert_property(
    ll:url,
    ll:'URL',
    rdfs:'Resource',
    'URL',
    'The URL from which the original version of the data document\c
     was downloaded.',
    Graph
  ),
  
  % Version.
  rdfs_assert_property(
    ll:version,
    dcat:'Distribution',
    xsd:integer,
    version,
    'The version of the LOD Washing Machine that was used for cleaning\c
     the data document.',
    Graph
  ).

