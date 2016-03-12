:- module(
  lwm_sparql_det,
  [
    archive_to_entries/2, % +Archive:iri
                          % -Entries:list(iri)
    document_archive_entry/2, % +Document:iri
                              % -EntryPath:atom
    document_archive_parent/2, % +Document:iri
                               % -Parent:iri
    document_cleaning/1, % -Documents:list(iri))
    document_content_type/2, % +Document:iri
                             % -ContentType:atom
    document_describe/2, % +Md5:atom
                         % -Triples:list(compound)
    document_exists/1, % +Document:iri
    document_file_extension/2, % +Document:iri
                               % -FileExtension:atom
    document_has_triples/1, % +Document:iri
    document_original_location/2, % +Document:iri
                                  % -Location:iri
    document_source/2, % +Document:iri
                       % -Source:atom
    document_unpacked_size/2, % +Document:iri
                              % -UnpackedSize:nonneg
    document_unpacking/1, % -Documents:list(iri))
    document_p_os/3, % +Document:iri
                     % +Property:iri
                     % -Objects:list(rdf_term)
    entry_to_archive/2  % +Entry:iri
                        % -Archive:iri
  ]
).

/** <module> LOD Laundromat: Deterministic SPARQL queries

SPARQL queries for retrieving specific properties of a data document
in the LOD Washing Machine.

@author Wouter Beek
@version 2016/01
*/

:- use_module(library(apply)).
:- use_module(library(lists)).
:- use_module(library(rdf/rdf_term)).
:- use_module(library(semweb/rdf11)).

:- use_module(query/lwm_sparql_generics).

:- rdf_meta
   document_p_os(r, r, -).





%! archive_to_entries(+Archive:iri, -Entries:list(iri)) is det.

archive_to_entries(Archive, Entries):-
  % This query has to be performed iteratively
  % since there may be many results.
  findall(
    Rows,
    lwm_sparql_select_iteratively(
      [llo],
      [entry],
      [rdf(Archive, llo:containsEntry, var(entry))],
      10000,
      Rows,
      []
    ),
    Entries0
  ),
  flatten(Entries0, Entries).



%! document_archive_entry(+Document:iri, -EntryPath:atom) is det.

document_archive_entry(Doc, EntryPath):-
  lwm_sparql_select(
    [llo],
    [entryPath],
    [rdf(Doc, llo:path, var(entryPath))],
    [EntryPathLit],
    [limit(1)]
  ),
  rdf_literal_value(EntryPathLit, EntryPath).



%! document_archive_parent(+Document:iri, -Parent:iri) is det.

document_archive_parent(Doc, Parent):-
  lwm_sparql_select(
    [llo],
    [parent],
    [rdf(var(parent), llo:containsEntry, Doc)],
    [Parent],
    [limit(1)]
  ).



document_cleaning(L):-
  lwm_sparql_select(
    [llo],
    [datadoc],
    [
      rdf(var(datadoc), llo:startClean, var(startClean)),
      not([rdf(var(datadoc), llo:endClean, var(endClean))])
    ],
    L0,
    []
  ),
  flatten(L0, L).



%! document_content_type(+Document:iri, -ContentType:atom) is semidet.
% Returns a variable if the content type is not known.

document_content_type(Doc, ContentType):-
  lwm_sparql_select(
    [llo],
    [contentType],
    [rdf(Doc, llo:contentType, var(contentType))],
    [[ContentTypeLit]],
    [limit(1)]
  ),
  rdf_literal_value(ContentTypeLit, ContentType).



%! document_describe(+Document:iri, -Triples:list(compound)) is det.

document_describe(Doc, Ts):-
  lwm_sparql_select(
    [llo],
    [p,o],
    [rdf(Doc, var(p), var(o))],
    Rows,
    [distinct(true)]
  ),
  maplist(pair_to_triple(Doc), Rows, Ts).

pair_to_triple(S, [P,O], rdf(S,P,O)).



%! document_exists(+Document:iri) is semidet.

document_exists(Doc):-
  lwm_sparql_ask([llo], [rdf(Doc,llo:md5,var(md5))], []).



%! document_file_extension(+Document:iri, -Extension:atom) is det.

document_file_extension(Doc, Ext):-
  lwm_sparql_select(
    [llo],
    [ext],
    [rdf(Doc, llo:fileExtension, var(ext))],
    [[ExtLit]],
    [limit(1)]
  ),
  rdf_literal_value(ExtLit, Ext).



%! document_has_triples(+Document:iri) is semidet.

document_has_triples(Doc):-
  lwm_sparql_ask([llo], [rdf(Doc,llo:triples,var(triples))], []).



%! document_original_location(+Document:iri, -Download:iri) is det.

document_original_location(Doc, Download):-
  lwm_sparql_select(
    [llo],
    [download],
    [rdf(Doc, llo:url, var(download))],
    [[Download]],
    [limit(1)]
  ).



%! document_source(+Document:iri, -Source:atom) is det.
% Returns the original source of the given datadocument.
%
% This is either an IRI simpliciter,
% or an IRI suffixed by an archive entry path.

% The data document derives from an IRI.
document_source(Doc, Source):-
  document_original_location(Doc, Source), !.
% The data document derives from an archive entry.
document_source(Doc, Source):-
  lwm_sparql_select(
    [llo],
    [parent,path],
    [rdf(Doc, llo:path, var(path)), rdf(var(parent), llo:containsEntry, Doc)],
    [[Parent,PathLiteral]],
    [limit(1)]
  ),
  rdf_literal_value(PathLiteral, Path),
  document_source(Parent, ParentSource),
  atomic_list_concat([ParentSource,Path], ' ', Source).



%! document_unpacked_size(+Document:iri, -UnpackedSize:nonneg) is det.

document_unpacked_size(Doc, UnpackedSize):-
  lwm_sparql_select(
    [llo],
    [unpackedSize],
    [rdf(Doc, llo:unpackedSize, var(unpackedSize))],
    [[UnpackedSizeLiteral]],
    [limit(1)]
  ),
  rdf_literal_value(UnpackedSizeLiteral, UnpackedSize).



%! document_unpacking(-Documents:list(iri)) is nondet.

document_unpacking(Docs):-
  lwm_sparql_select(
    [llo],
    [datadoc],
    [
      rdf(var(datadoc), llo:startUnpack, var(startUnpack)),
      not([rdf(var(datadoc), llo:endUnpack, var(endUnpack))])
    ],
    Docs0,
    []
  ),
  flatten(Docs0, Docs).



%! document_p_os(+Document:iri, +Property:iri, -Objects:list(rdf_term)) is det.

document_p_os(Doc, P, Os):-
  lwm_sparql_select([llo], [o], [rdf(Doc,P,var(o))], Os0, []),
  flatten(Os0, Os).



%! entry_to_archive(+Entry:iri, -Archive:iri) is semidet.

entry_to_archive(Entry, Archive):-
  lwm_sparql_select(
    [llo],
    [archive],
    [rdf(var(archive), llo:containsEntry, Entry)],
    [[Archive]],
    []
  ).
