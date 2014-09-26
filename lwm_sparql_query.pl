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
    datadoc_pending/2, % -Datadoc:url
                       % -Dirty:url
    datadoc_source/2, % +Datadoc:url
                      % -Source:atom
    datadoc_unpacked/4, % ?Min:nonneg
                        % ?Max:nonneg
                        % -Datadoc:url
                        % -Size:nonneg
    datadoc_unpacking/1 % -Datadoc:url
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
  (   lwm:lwm_server(virtuoso)
  ->  Endpoint = virtuoso_query
  ;   lwm:lwm_server(cliopatria)
  ->  Endpoint = cliopatria_localhost
  ),
  loop_until_true(
    sparql_ask(Endpoint, Prefixes, Bgps, Options2)
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
  (   lwm:lwm_server(virtuoso)
  ->  Endpoint = virtuoso_query
  ;   lwm:lwm_server(cliopatria)
  ->  Endpoint = cliopatria_localhost
  ),
  loop_until_true(
    sparql_select(Endpoint, Prefixes, Variables, Bgps, Result, Options2)
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
  maplist(rdf_literal_value2, Row, [ParentMd5,EntryPath]).


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
    [[ContentTypeLiteral]],
    [limit(1)]
  ),
  rdf_literal_value2(ContentTypeLiteral, ContentType).
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
    [[FileExtensionLiteral]],
    [limit(1)]
  ),
  rdf_literal_value2(FileExtensionLiteral, FileExtension).


%! datadoc_pending(-Datadoc:url, -Dirty:url) is nondet.
% @tbd Make sure that at no time two data documents are
%      being downloaded from the same host.
%      This avoids being blocked by servers that do not allow
%      multiple simultaneous requests.
%      ~~~{.pl}
%      (   nonvar(DirtyUrl)
%      ->  uri_component(DirtyUrl, host, Host),
%          \+ lwm:current_host(Host),
%          % Set a lock on this host for other unpacking threads.
%          assertz(lwm:current_host(Host))
%      ;   true
%      ), !,
%      ~~~
%      Add argument `Host` for releasing the lock in [lwm_unpack].

datadoc_pending(Datadoc, Dirty):-
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
  rdf_literal_value2(PathLiteral, Path),
  datadoc_source(Parent, ParentSource),
  atomic_concat(ParentSource, Path, Source).


%! datadoc_unpacked(
%!   ?Min:nonneg,
%!   ?Max:nonneg,
%!   -Datadoc:url,
%!   -Size:nonneg
%! ) is semidet.
% Size is expressed as the number of bytes.

datadoc_unpacked(Min, Max, Datadoc, Size):-
  Query1 = [
    rdf(var(datadoc), llo:endUnpack, var(endUnpack)),
    not([
      rdf(var(datadoc), llo:startClean, var(startClean))
    ]),
    rdf(var(datadoc), llo:size, var(size))
  ],

  % Insert the range restriction on size as a filter.
  (   nonvar(Min)
  ->  MinFilter = >(var(size),Min)
  ;   true
  ),
  (   nonvar(Max)
  ->  MaxFilter = <(var(size),Max)
  ;   true
  ),
  exclude(var, [MinFilter,MaxFilter], FilterComponents),
  (   conjunctive_filter(FilterComponents, FilterContent)
  ->  append(Query1, [filter(FilterContent)], Query2)
  ;   Query2 = Query1
  ),

  lwm_sparql_select(
    [llo],
    [datadoc,size],
    Query2,
    [[Datadoc,SizeLiteral]],
    [limit(1)]
  ),
  rdf_literal_value2(SizeLiteral, Size).
conjunctive_filter([H], H):- !.
conjunctive_filter([H|T1], and(H,T2)):-
  conjunctive_filter(T1, T2).


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



% Helpers.

pair_to_triple(S, [P,O], rdf(S,P,O)).

