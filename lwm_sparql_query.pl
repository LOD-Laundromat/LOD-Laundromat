:- module(
  lwm_sparql_query,
  [
    md5_archive_entry/3, % +Md5:atom
                         % -ParentMd5:atom
                         % -EntryPath:atom
    md5_cleaned/1, % ?Md5:atom
    md5_cleaning/1, % ?Md5:atom
    md5_content_type/2, % +Md5:atom
                        % -ContentType:atom
    md5_describe/2, % +Md5:atom
                    % -Triples:list(compound)
    md5_file_extension/2, % +Md5:atom
                          % -FileExtension:atom
    md5_pending/1, % ?Md5:atom
    md5_size/2, % +Md5:atom
                % -Size:nonneg
    md5_source/2, % +Md5:atom
                  % -Source:atom
    md5_unpacked/1, % ?Md5:atom
    md5_unpacking/1, % ?Md5:atom
    md5_url/2 % +Md5:atom
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



%! md5_archive_entry(+Md5:atom, -ParentMd5:atom, -EntryPath:atom) is det.

md5_archive_entry(Md5, ParentMd5, EntryPath):-
  lwm_sparql_select([llo], [md5,path],
      [rdf(var(md5ent),llo:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5ent),llo:path,var(path)),
       rdf(var(md5parent),llo:containsEntry,var(md5ent)),
       rdf(var(md5parent),llo:md5,var(md5))],
      [[ParentMd50,EntryPath0]], [limit(1)]),
  maplist(rdf_literal, [ParentMd50,EntryPath0], [ParentMd5,EntryPath], _).


%! md5_cleaned(+Md5:atom) is semidet.
%! md5_cleaned(-Md5:atom) is nondet.

md5_cleaned(Md5):-
  var(Md5), !,
  with_mutex(lwm_endpoint, (
    lwm_sparql_select([llo], [md5],
        [rdf(var(md5res),llo:endClean,var(end_clean)),
         rdf(var(md5res),llo:md5,var(md5))],
        Rows, [sparql_errors(fail)])
  )),
  member([Literal], Rows),
  rdf_literal(Literal, Md5, _).
md5_cleaned(Md5):-
  with_mutex(lwm_endpoint, (
    lwm_sparql_ask([llo],
        [rdf(var(md5),llo:md5,literal(type(xsd:string,Md5))),
         rdf(var(md5),llo:endClean,var(end))],
        [sparql_errors(fail)])
  )).


%! md5_cleaning(+Md5:atom) is semidet.
%! md5_cleaning(-Md5:atom) is nondet.

md5_cleaning(Md5):-
  var(Md5), !,
  with_mutex(lwm_endpoint, (
    lwm_sparql_select([llo], [md5],
        [rdf(var(md5res),llo:startClean,var(start_clean)),
         not([rdf(var(md5res),llo:endClean,var(end_clean))]),
         rdf(var(md5res),llo:md5,var(md5))],
        Rows, [sparql_errors(fail)])
  )),
  member([Literal], Rows),
  rdf_literal(Literal, Md5, _).
md5_cleaning(Md5):-
  with_mutex(lwm_endpoint, (
    lwm_sparql_ask([llo],
        [rdf(var(md5),llo:md5,literal(type(xsd:string,Md5))),
         rdf(var(md5),llo:startClean,var(end)),
         not([rdf(var(md5),llo:endClean,var(end))])],
        [sparql_errors(fail)])
  )).


%! md5_content_type(+Md5:atom, -ContentType:atom) is det.

md5_content_type(Md5, ContentType):-
  lwm_sparql_select([llo], [content_type],
      [rdf(var(md5res),llo:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5res),llo:contentType,var(content_type))],
      [[Literal]], [limit(1)]), !,
  rdf_literal(Literal, ContentType, _).
md5_content_type(_, _).


%! md5_describe(+Md5:atom, -Triples:list(compound)) is det.

md5_describe(Md5, Triples):-
  lwm_sparql_select([llo], [s,p,o],
      [rdf(var(s),llo:md5,literal(type(xsd:string,Md5))),
       rdf(var(s),var(p),var(o))],
      TripleRows, [distinct(true)]),
  maplist(triple_row_to_compound, TripleRows, Triples).


%! md5_file_extension(+Md5:atom, -FileExtension:atom) is det.

md5_file_extension(Md5, FileExtension):-
  lwm_sparql_select([llo], [file_extension],
      [rdf(var(md5res),llo:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5res),llo:fileExtension,var(file_extension))],
      [[Literal]], [limit(1)]),
  rdf_literal(Literal, FileExtension, _).


%! md5_pending(+Md5:atom) is semidet.
%! md5_pending(-Md5:atom) is nondet.

md5_pending(Md5):-
  var(Md5), !,
  with_mutex(lwm_endpoint, (
    lwm_sparql_select(
      [llo],
      [md5],
      [rdf(var(md5res),llo:added,var(added)),
       rdf(var(md5res),llo:md5,var(md5)),
       not([rdf(var(md5res),llo:startUnpack,var(start))])],
      Rows, [sparql_errors(fail)]
    )
  )),
  member([Literal], Rows),
  rdf_literal(Literal, Md5, _).
md5_pending(Md5):-
  with_mutex(lwm_endpoint, (
    lwm_sparql_ask(
      [llo],
      [rdf(var(md5),llo:md5,literal(type(xsd:string,Md5))),
       not([rdf(var(md5),llo:startUnpack,var(start))])],
      [sparql_errors(fail)]
    )
  )).


%! md5_size(+Md5:atom, -NumberOfGigabytes:between(0.0,inf)) is det.

md5_size(Md5, NumberOfGigabytes):-
  lwm_sparql_select([llo], [size],
      [rdf(var(datadoc),llo:md5,literal(type(xsd:string,Md5))),
       rdf(var(datadoc),llo:size,var(size))],
      [[Literal]], [limit(1)]), !,
  rdf_literal(Literal, NumberOfBytes1, _),
  atom_number(NumberOfBytes1, NumberOfBytes2),
  NumberOfGigabytes is NumberOfBytes2 / (1024 ** 3).


%! md5_source(+Md5:atom, -Source:atom) is det.
% Returns the original source of the given datadocument.
%
% This is either a URL simpliciter,
% or a URL suffixed by an archive entry path.

md5_source(Md5, Url):-
  md5_url(Md5, Url), !.
md5_source(Md5, Source):-
  lwm_sparql_select([llo], [md5parent,path],
      [rdf(var(ent),llo:md5,literal(type(xsd:string,Md5))),
       rdf(var(ent),llo:path,var(path)),
       rdf(var(parent),llo:containsEntry,var(md5ent)),
       rdf(var(parent),llo:md5,var(md5parent))],
      [[Literal1,Literal2]], [limit(1)]), !,
  maplist(rdf_literal, [Literal1,Literal2], [ParentMd5,Path], _),
  md5_source(ParentMd5, ParentSource),
  atomic_concat(ParentSource, Path, Source).


%! md5_unpacked(+Md5:atom) is semidet.
%! md5_unpacked(-Md5:atom) is nondet.

md5_unpacked(Md5):-
  var(Md5), !,
  with_mutex(lwm_endpoint, (
    lwm_sparql_select([llo], [md5],
        [rdf(var(md5res),llo:endUnpack,var(start)),
         not([rdf(var(md5res),llo:startClean,var(clean))]),
         rdf(var(md5res),llo:md5,var(md5))],
        Rows, [sparql_errors(fail)])
  )),
  member([Literal], Rows),
  rdf_literal(Literal, Md5, _).
md5_unpacked(Md5):-
  with_mutex(lwm_endpoint, (
    lwm_sparql_ask([llo],
        [rdf(var(md5),llo:md5,literal(type(xsd:string,Md5))),
         rdf(var(md5),llo:endUnpack,var(start)),
         not([rdf(var(md5res),ll:startClean,var(clean))])],
        [sparql_errors(fail)])
  )).


%! md5_unpacking(+Md5:atom) is semidet.
%! md5_unpacking(-Md5:atom) is nondet.

md5_unpacking(Md5):-
  var(Md5), !,
  % Notice that choicepoints are closed within a mutex.
  with_mutex(lwm_endpoint, (
    lwm_sparql_select([llo], [md5],
        [rdf(var(md5res),llo:startUnpack,var(start)),
         not([rdf(var(md5res),llo:endUnpack,var(clean))]),
         rdf(var(md5res),llo:md5,var(md5))],
        Rows, [sparql_errors(fail)])
  )),
  member([Literal], Rows),
  rdf_literal(Literal, Md5, _).
md5_unpacking(Md5):-
  with_mutex(lwm_endpoint, (
    lwm_sparql_ask([llo],
        [rdf(var(md5),llo:md5,literal(type(xsd:string,Md5))),
         rdf(var(md5),llo:startUnpack,var(start)),
         not([rdf(var(md5res),llo:endUnpack,var(clean))])],
        [sparql_errors(fail)])
  )).


%! md5_url(+Md5:atom, -Url:url) is det.

md5_url(Md5, Url):-
  lwm_sparql_select([llo], [url],
      [rdf(var(md5res),llo:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5res),llo:url,var(url))],
      [[Url]], [limit(1)]).



% Helpers.

lwm_sparql_ask(Prefixes, Bgps, Options1):-
  lwm_version_graph(Graph),
  merge_options([named_graph(Graph)], Options1, Options2),
  loop_until_true(
    sparql_ask(virtuoso_query, Prefixes, Bgps, Options2)
  ).


lwm_sparql_select(Prefixes, Variables, Bgps, Result, Options1):-
  % Set the RDF Dataset over which SPARQL Queries are executed.
  lod_basket_graph(BasketGraph),
  lwm_version_graph(LwmGraph),
  merge_options(
    [default_graph(BasketGraph),default_graph(LwmGraph)],
    Options1,
    Options2
  ),

  loop_until_true(
    sparql_select(virtuoso_query, Prefixes, Variables, Bgps, Result, Options2)
  ).


triple_row_to_compound([S,P,O], rdf(S,P,O)).
