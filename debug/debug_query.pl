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
@version 2015/11
*/

:- use_module(library(lists)).
:- use_module(library(rdf/rdf_print)).

:- use_module('LOD-Laundromat'(query/lwm_sparql_det)).
:- use_module('LOD-Laundromat'(query/lwm_sparql_nondet)).





debug_cleaning:-
  cleaning(Document),
  print_document_overview(Document).

debug_pending:-
  pending(Document, Download),
  format("Dirty:\t~a\n", [Download]),
  print_document_overview(Document).

debug_unpacked:-
  unpacked(_, _, Document, Size),
  format("Size:\t~D~n", [Size]),
  print_document_overview(Document).

debug_unpacking:-
  unpacking(Document),
  print_document_overview(Document).


print_document_overview(Document):-
  forall(
    document_archive_entry(Document, EntryPath),
    format("Entry path:\t~a~n", [EntryPath])
  ),

  (   document_content_type(Document, ContentType)
  ->  format("Content-Type:\t~a~n", [ContentType])
  ;   true
  ),

  rdf_print_describe(Document),

  (   document_file_extension(Document, Ext)
  ->  format("File extension:\t~a~n", [Ext])
  ;   true
  ),

  document_source(Document, Source),
  format("Source:\t~a~n", [Source]).
