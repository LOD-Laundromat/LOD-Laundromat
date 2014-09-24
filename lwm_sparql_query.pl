:- module(
  lwm_sparql_query,
  [
    lwm_sparql_ask/3, % +Prefixes:list(atom)
                      % +Bgps:list(compound)
                      % +Options:list(nvpair)
    lwm_sparql_select/5, % +Prefixes:list(atom)
                         % +Variables:list(atom)
                         % +Bgps:list(compound)
                         % -Result:list(list)
                         % +Options:list(nvpair)
    datadoc_archive_entry/3, % +Datadoc:url
                             % -ParentMd5:atom
                             % -EntryPath:atom
    datadoc_cleaning/1, % -Datadoc:url
    datadoc_content_type/2, % +Datadoc:url
                            % -ContentType:atom
    datadoc_describe/2, % +Md5:atom
                    % -Triples:list(compound)
    datadoc_file_extension/2, % +Datadoc:url
                              % -FileExtension:atom
    datadoc_source/2, % +Datadoc:url
                      % -Source:atom
    datadoc_unpacking/1, % -Datadoc:url
    get_one_pending_datadoc/2, % -Datadoc:url
                               % -Dirty:url
    get_one_unpacked_datadoc/2 % -Datadoc:url
                               % -Size:nonneg
  ]
).

/** <module> LOD Washing Machine (LWM): SPARQL queries

SPARQL queries for the LOD Washing Machine.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09
*/

:- use_module(library(apply)).
:- use_module(library(lists)).
:- use_module(library(option)).

:- use_module(generics(meta_ext)).

:- use_module(plRdf_term(rdf_datatype)).

:- use_module(plSparql_query(sparql_query_api)).

:- use_module(lwm(lwm_settings)).



% GENERICS

lwm_sparql_ask(Prefixes, Bgps, Options1):-
  lwm_version_graph(Graph),
  merge_options([named_graph(Graph),sparql_errors(fail)], Options1, Options2),
  loop_until_true(
    sparql_ask(virtuoso_query, Prefixes, Bgps, Options2)
  ).


lwm_sparql_select(Prefixes, Variables, Bgps, Result, Options1):-
  % Set the RDF Dataset over which SPARQL Queries are executed.
  lod_basket_graph(BasketGraph),
  lwm_version_graph(LwmGraph),
  merge_options(
    [default_graph(BasketGraph),default_graph(LwmGraph),sparql_errors(fail)],
    Options1,
    Options2
  ),

  loop_until_true(
    sparql_select(virtuoso_query, Prefixes, Variables, Bgps, Result, Options2)
  ).



% QUERIES

%! datadoc_archive_entry(+Datadoc:url, -ParentMd5:atom, -EntryPath:atom) is det.

datadoc_archive_entry(Datadoc, ParentMd5, EntryPath):-
  lwm_sparql_select(
    [llo],
    [parentMd5,entryPath],
    [
      rdf(Datadoc, llo:path, var(entryPath)),
      rdf(var(md5parent), llo:containsEntry, Datadoc),
      rdf(var(md5parent), llo:md5, var(parentMd5))
    ],
    [Row],
    [limit(1)]
  ),
  maplist(rdf_literal, Row, [ParentMd5,EntryPath]).


%! datadoc_cleaning(-Datadoc:url) is nondet.

datadoc_cleaning(Datadoc):-
  lwm_sparql_select(
    [llo],
    [datadoc],
    [
      rdf(var(datadoc), llo:startClean, var(startClean)),
      not([
        rdf(var(datadoc), llo:endClean, var(endClean))
      ])
    ],
    Rows,
    []
  ),
  member([Datadoc], Rows).


%! datadoc_content_type(+Datadoc:url, -ContentType:atom) is det.
% Returns a variable if the content type is not known.

datadoc_content_type(Datadoc, ContentType):-
  lwm_sparql_select(
    [llo],
    [contentType],
    [rdf(Datadoc, llo:contentType, var(contentType))],
    [ContentTypeLiteral],
    [limit(1)]
  ),
  rdf_literal_value(ContentTypeLiteral, ContentType).
datadoc_content_type(_, _VAR).


%! datadoc_describe(+Datadoc:url, -Triples:list(compound)) is det.

datadoc_describe(Datadoc, Triples):-
  lwm_sparql_select(
    [llo],
    [p,o],
    [rdf(Datadoc, var(p), var(o))],
    Rows,
    [distinct(true)]
  ),
  maplist(pair_to_triple(Datadoc), Rows, Triples).


%! datadoc_file_extension(+Datadoc:url, -FileExtension:atom) is det.

datadoc_file_extension(Datadoc, FileExtension):-
  lwm_sparql_select(
    [llo],
    [fileExtension],
    [rdf(Datadoc, llo:fileExtension, var(fileExtension))],
    [FileExtensionLiteral],
    [limit(1)]
  ),
  rdf_literal_value(FileExtensionLiteral, [FileExtension]).


%! datadoc_source(+Datadoc:url, -Source:atom) is det.
% Returns the original source of the given datadocument.
%
% This is either a URL simpliciter,
% or a URL suffixed by an archive entry path.

% The data document derives from a URL.
datadoc_source(Datadoc, Url):-
  lwm_sparql_select(
    [llo],
    [url],
    [rdf(Datadoc, llo:url, var(url))],
    [[Url]],
    [limit(1)]
  ), !.
% The data document derives from an archive entry.
datadoc_source(Datadoc, Source):-
  lwm_sparql_select(
    [llo],
    [parent,path],
    [
      rdf(Datadoc, llo:path, var(path)),
      rdf(var(parent), llo:containsEntry, Datadoc)
    ],
    [[Parent,PathLiteral]],
    [limit(1)]
  ),
  rdf_literal_value(PathLiteral, Path),
  datadoc_source(Parent, ParentSource),
  atomic_concat(ParentSource, Path, Source).


%! datadoc_unpacking(-Datadoc:url) is nondet.

datadoc_unpacking(Datadoc):-
  lwm_sparql_select(
    [llo],
    [datadoc],
    [
      rdf(var(datadoc), llo:startUnpack, var(startUnpack)),
      not([
        rdf(var(datadoc), llo:endUnpack, var(endUnpack))
      ])
    ],
    Rows,
    []
  ),
  member([Datadoc], Rows).


%! get_one_pending_datadoc(-Datadoc:url, -Dirty:url) is semidet.

get_one_pending_datadoc(Datadoc, Dirty):-
  lwm_sparql_select(
    [llo],
    [datadoc,dirty],
    [
      rdf(var(datadoc), llo:added, var(added)),
      not([
        rdf(var(datadoc), llo:startUnpack, var(startUnpack))
      ]),
      optional([
        rdf(var(datadoc), llo:url, var(dirty))
      ])
    ],
    [[Datadoc,Dirty]],
    [limit(1)]
  ).


%! get_one_unpacked_datadoc(-Datadoc:url, -Size:nonneg) is semidet.
% Size is expressed as the number of bytes.

get_one_unpacked_datadoc(Datadoc, Size):-
  lwm_sparql_select(
    [llo],
    [datadoc,size],
    [
      rdf(var(datadoc), llo:endUnpack, var(endUnpack)),
      not([
        rdf(var(datadoc), llo:startClean, var(startClean))
      ]),
      rdf(var(datadoc), llo:size, var(size))
    ],
    [[Datadoc,SizeLiteral]],
    [limit(1)]
  ),
  rdf_literal_value(SizeLiteral, Size).



% Helpers.

pair_to_triple(S, [P,O], rdf(S,P,O)).

