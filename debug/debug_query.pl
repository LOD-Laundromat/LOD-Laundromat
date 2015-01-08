:- module(
  debug_query,
  [
    debug_cleaning/0,
    debug_pending/0,
    debug_unpacked/0,
    debug_unpacking/0
  ]
).

/** <module> Debug LOD Washing Machine queries

@author Wouter Beek
@version 2015/01
*/

:- use_module(library(apply)).

:- use_module(lwm(lwm_sparql_query)).





debug_cleaning:-
  datadoc_cleaning(Datadoc),
  datadoc_queries(Datadoc).

debug_pending:-
  datadoc_pending(Datadoc),
  datadoc_queries(Datadoc).

debug_unpacked:-
  datadoc_unpacked(_, _, Datadoc, Size),
  format('Size:\t~D\n', [Size]),
  datadoc_queries(Datadoc).

debug_unpacking:-
  datadoc_unpacking(Datadoc),
  datadoc_queries(Datadoc).



%! datadoc_queries(+Datadoc:uri) is det.

datadoc_queries(Datadoc):-
  datadoc_archive_entry(Datadoc, ParentMd5, EntryPath),
  format('Parent MD5:\t~a\n', [ParentMd5]),
  format('Entry path:\t~a\n', [EntryPath]),
  
  datadoc_content_type(Datadoc, ContentType),
  format('Content-Type:\t~a\n', [ContentType]),
  
  datadoc_describe(Datadoc, Triples),
  format('Triples:\n', []),
  maplist(writeln, Triples),
  
  datadoc_file_extension(Datadoc, FileExtension),
  format('File extension:\t~a\n', [FileExtension]),
  
  datadoc_source(Datadoc, Source),
  format('Source:\t~a\n', [Source]).