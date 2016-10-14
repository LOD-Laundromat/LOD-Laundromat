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
:- use_module(library(q/q_print)).

:- use_module('LOD-Laundromat'(query/lwm_sparql_det)).
:- use_module('LOD-Laundromat'(query/lwm_sparql_nondet)).





debug_cleaning:-
  cleaning(Doc),
  print_document_overview(Doc).

debug_pending:-
  pending(Doc, Download),
  format("Dirty:\t~a\n", [Download]),
  print_document_overview(Doc).

debug_unpacked:-
  unpacked(_, _, Doc, Size),
  format("Size:\t~D~n", [Size]),
  print_document_overview(Doc).

debug_unpacking:-
  unpacking(Doc),
  print_document_overview(Doc).


print_document_overview(Doc):-
  forall(
    document_archive_entry(Doc, EntryPath),
    format("Entry path:\t~a~n", [EntryPath])
  ),

  (   document_content_type(Doc, ContentType)
  ->  format("Content-Type:\t~a~n", [ContentType])
  ;   true
  ),

  q_print_triples(_, _, _, Doc),

  (   document_file_extension(Doc, Ext)
  ->  format("File extension:\t~a~n", [Ext])
  ;   true
  ),

  document_source(Doc, Source),
  format("Source:\t~a~n", [Source]).
