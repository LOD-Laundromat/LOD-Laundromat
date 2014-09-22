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
    datadoc_size/2, % +Datadoc:url
                    % -Size:nonneg
    datadoc_source/2, % +Datadoc:url
                      % -Source:atom
    datadoc_unpacking/1, % -Datadoc:url
    datadoc_url/2 % +Datadoc:url
                  % -Url:url
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

:- use_module(plRdf_term(rdf_literal)).

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
      rdf(var(md5parent),llo:md5,var(parentMd5))
    ],
    [[literal(ParentMd5),literal(EntryPath)]],
    [limit(1)]
  ).


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
    [[literal(type(_,ContentType))]],
    [limit(1)]
  ), !.
datadoc_content_type(_, _VAR).


%! datadoc_describe(+Datadoc:url, -Triples:list(compound)) is det.

datadoc_describe(Datadoc, Triples):-
  lwm_sparql_select(
    [llo],
    [p,o],
    [rdf(Datadoc,var(p),var(o))],
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
    [[literal(type(_,FileExtension))]],
    [limit(1)]
  ).


%! datadoc_size(+Datadoc:url, -NumberOfGigabytes:between(0.0,inf)) is det.

datadoc_size(Datadoc, NumberOfGigabytes):-
  lwm_sparql_select(
    [llo],
    [size],
    [rdf(Datadoc, llo:size, var(size))],
    [[literal(type(_,NumberOfBytes1))]],
    [limit(1)]
  ), !,
  atom_number(NumberOfBytes1, NumberOfBytes2),
  NumberOfGigabytes is NumberOfBytes2 / (1024 ** 3).


%! datadoc_source(+Datadoc:url, -Source:atom) is det.
% Returns the original source of the given datadocument.
%
% This is either a URL simpliciter,
% or a URL suffixed by an archive entry path.

datadoc_source(Datadoc, Url):-
  datadoc_url(Datadoc, Url), !.
datadoc_source(Datadoc, Source):-
  lwm_sparql_select(
    [llo],
    [parent,path],
    [
      rdf(Datadoc, llo:path, var(path)),
      rdf(var(parent), llo:containsEntry, Datadoc)
    ],
    [[Parent,literal(type(_,Path))]],
    [limit(1)]
  ), !,
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


%! datadoc_url(+Datadoc:url, -Url:url) is det.

datadoc_url(Datadoc, Url):-
  lwm_sparql_select(
    [llo],
    [url],
    [rdf(Datadoc, llo:url, var(url))],
    [[Url]],
    [limit(1)]
  ).



% Helpers.

pair_to_triple(S, [P,O], rdf(S,P,O)).
