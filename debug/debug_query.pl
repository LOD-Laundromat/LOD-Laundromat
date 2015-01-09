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
:- use_module(library(lists), except([delete/3,subset/2])).

:- use_module(lwm(query/lwm_sparql_query)).





debug_cleaning:-
  datadoc_enum_cleaning(Datadoc),
  datadoc_queries(Datadoc).

debug_pending:-
  datadoc_enum_pending(Datadoc, Dirty),
  format('Dirty:\t~a\n', [Dirty]),
  datadoc_queries(Datadoc).

debug_unpacked:-
  datadoc_enum_unpacked(_, _, Datadoc, Size),
  format('Size:\t~D\n', [Size]),
  datadoc_queries(Datadoc).

debug_unpacking:-
  datadoc_enum_unpacking(Datadoc),
  datadoc_queries(Datadoc).



%! datadoc_queries(+Datadoc:uri) is det.

datadoc_queries(Datadoc):-
  forall(
    datadoc_archive_entry(Datadoc, ParentMd5, EntryPath),
    (
      format('Parent MD5:\t~a\n', [ParentMd5]),
      format('Entry path:\t~a\n', [EntryPath])
    )
  ),
  
  (   datadoc_content_type(Datadoc, ContentType)
  ->  format('Content-Type:\t~a\n', [ContentType])
  ;   true
  ),
  
  datadoc_describe(Datadoc, Triples),
  (   Triples \== []
  ->  format('Triples:\n', []),
      forall(
        member(Triple, Triples),
        format('\t~w\n', Triple)
      )
  ;   true
  ),
  
  datadoc_file_extension(Datadoc, FileExtension),
  format('File extension:\t~a\n', [FileExtension]),
  
  datadoc_source(Datadoc, Source),
  format('Source:\t~a\n', [Source]).
