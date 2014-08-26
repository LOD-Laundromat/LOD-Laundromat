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
@version 2014/06, 2014/08
*/

:- use_module(library(apply)).
:- use_module(library(option)).

:- use_module(generics(meta_ext)).

:- use_module(plRdf_term(rdf_literal)).

:- use_module(plSparql_query(sparql_query_api)).

:- use_module(lwm(lwm_settings)).



%! md5_archive_entry(+Md5:atom, -ParentMd5:atom, -EntryPath:atom) is det.

md5_archive_entry(Md5, ParentMd5, EntryPath):-
  lwm_sparql_select([ll], [md5,path],
      [rdf(var(md5ent),ll:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5ent),ll:path,var(path)),
       rdf(var(md5parent),ll:contains_entry,var(md5ent)),
       rdf(var(md5parent),ll:md5,var(md5))],
      [[ParentMd50,EntryPath0]], [limit(1)]),
  maplist(rdf_literal, [ParentMd50,EntryPath0], [ParentMd5,EntryPath], _).


%! md5_cleaned(+Md5:atom) is semidet.
%! md5_cleaned(-Md5:atom) is nondet.

md5_cleaned(Md5):-
  var(Md5), !,
  with_mutex(lwm_basket, (
    lwm_sparql_select([ll], [md5],
        [rdf(var(md5res),ll:end_clean,var(end_clean)),
         rdf(var(md5res),ll:md5,var(md5))],
        [[Literal]], [limit(1),sparql_errors(fail)]),
    rdf_literal(Literal, Md5, _)
  )).
md5_cleaned(Md5):-
  with_mutex(lwm_basket, (
    lwm_sparql_ask([ll],
        [rdf(var(md5),ll:md5,literal(type(xsd:string,Md5))),
         rdf(var(md5),ll:end_clean,var(end))],
        [sparql_errors(fail)])
  )).


%! md5_cleaning(+Md5:atom) is semidet.
%! md5_cleaning(-Md5:atom) is nondet.

md5_cleaning(Md5):-
  var(Md5), !,
  with_mutex(lwm_basket, (
    lwm_sparql_select([ll], [md5],
        [rdf(var(md5res),ll:start_clean,var(start_clean)),
         not([rdf(var(md5res),ll:end_clean,var(end_clean))]),
         rdf(var(md5res),ll:md5,var(md5))],
        [[Literal]], [limit(1),sparql_errors(fail)]),
    rdf_literal(Literal, Md5, _)
  )).
md5_cleaning(Md5):-
  with_mutex(lwm_basket, (
    lwm_sparql_ask([ll],
        [rdf(var(md5),ll:md5,literal(type(xsd:string,Md5))),
         rdf(var(md5),ll:start_clean,var(end)),
         not([rdf(var(md5),ll:end_clean,var(end))])],
        [sparql_errors(fail)])
  )).


%! md5_content_type(+Md5:atom, -ContentType:atom) is det.

md5_content_type(Md5, ContentType):-
  lwm_sparql_select([ll], [content_type],
      [rdf(var(md5res),ll:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5res),ll:content_type,var(content_type))],
      [[Literal]], [limit(1)]), !,
  rdf_literal(Literal, ContentType, _).
md5_content_type(_, _).


%! md5_file_extension(+Md5:atom, -FileExtension:atom) is det.

md5_file_extension(Md5, FileExtension):-
  lwm_sparql_select([ll], [file_extension],
      [rdf(var(md5res),ll:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5res),ll:file_extension,var(file_extension))],
      [[Literal]], [limit(1)]),
  rdf_literal(Literal, FileExtension, _).


%! md5_pending(+Md5:atom) is semidet.
%! md5_pending(-Md5:atom) is nondet.

md5_pending(Md5):-
  var(Md5), !,
  with_mutex(lwm_basket, (
    lwm_sparql_select([ll], [md5],
        [rdf(var(md5res),ll:added,var(added)),
         not([rdf(var(md5res),ll:start_unpack,var(start))]),
         rdf(var(md5res),ll:md5,var(md5))],
        [[Literal]], [limit(1),sparql_errors(fail)]),
    rdf_literal(Literal, Md5, _)
  )).
md5_pending(Md5):-
  with_mutex(lwm_basket, (
    lwm_sparql_ask([ll],
        [rdf(var(md5),ll:md5,literal(type(xsd:string,Md5))),
         rdf(var(md5),ll:added,var(added)),
         not([rdf(var(md5),ll:start_unpack,var(start))])],
        [sparql_errors(fail)])
  )).


%! md5_size(+Md5:atom, -NumberOfGigabytes:between(0.0,inf)) is det.

md5_size(Md5, NumberOfGigabytes):-
  lwm_sparql_select([ll], [size],
      [rdf(var(datadoc),ll:md5,literal(type(xsd:string,Md5))),
       rdf(var(datadoc),ll:size,var(size))],
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
  lwm_sparql_select([ll], [md5parent,path],
      [rdf(var(ent),ll:md5,literal(type(xsd:string,Md5))),
       rdf(var(ent),ll:path,var(path)),
       rdf(var(parent),ll:contains_entry,var(md5ent)),
       rdf(var(parent),ll:md5,var(md5parent))],
      [[Literal1,Literal2]], [limit(1)]), !,
  maplist(rdf_literal, [Literal1,Literal2], [ParentMd5,Path], _),
  md5_source(ParentMd5, ParentSource),
  atomic_concat(ParentSource, Path, Source).


%! md5_unpacked(+Md5:atom) is semidet.
%! md5_unpacked(-Md5:atom) is nondet.

md5_unpacked(Md5):-
  var(Md5), !,
  with_mutex(lwm_basket, (
    lwm_sparql_select([ll], [md5],
        [rdf(var(md5res),ll:end_unpack,var(start)),
         not([rdf(var(md5res),ll:start_clean,var(clean))]),
         rdf(var(md5res),ll:md5,var(md5))],
        [[Literal]], [limit(1),sparql_errors(fail)]),
    rdf_literal(Literal, Md5, _)
  )).
md5_unpacked(Md5):-
  with_mutex(lwm_basket, (
    lwm_sparql_ask([ll],
        [rdf(var(md5),ll:md5,literal(type(xsd:string,Md5))),
         rdf(var(md5),ll:end_unpack,var(start)),
         not([rdf(var(md5res),ll:start_clean,var(clean))])],
        [sparql_errors(fail)])
  )).


%! md5_unpacking(+Md5:atom) is semidet.
%! md5_unpacking(-Md5:atom) is nondet.

md5_unpacking(Md5):-
  var(Md5), !,
  with_mutex(lwm_basket, (
    lwm_sparql_select([ll], [md5],
        [rdf(var(md5res),ll:start_unpack,var(start)),
         not([rdf(var(md5res),ll:end_unpack,var(clean))]),
         rdf(var(md5res),ll:md5,var(md5))],
        [[Literal]], [limit(1),sparql_errors(fail)]),
    rdf_literal(Literal, Md5, _)
  )).
md5_unpacking(Md5):-
  with_mutex(lwm_basket, (
    lwm_sparql_ask([ll],
        [rdf(var(md5),ll:md5,literal(type(xsd:string,Md5))),
         rdf(var(md5),ll:start_unpack,var(start)),
         not([rdf(var(md5res),ll:end_unpack,var(clean))])],
        [sparql_errors(fail)])
  )).


%! md5_url(+Md5:atom, -Url:url) is det.

md5_url(Md5, Url):-
  lwm_sparql_select([ll], [url],
      [rdf(var(md5res),ll:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5res),ll:url,var(url))],
      [[Url]], [limit(1)]).



% Helpers.

lwm_sparql_ask(Prefixes, Bgps, Options1):-
  lwm_version_graph(Graph),
  merge_options([named_graph(Graph)], Options1, Options2),
  loop_until_true(
    sparql_ask(virtuoso, Prefixes, Bgps, Options2)
  ).


lwm_sparql_select(Prefixes, Variables, Bgps, Result, Options1):-
  lwm_version_graph(Graph),
  merge_options([named_graph(Graph)], Options1, Options2),
  loop_until_true(
    sparql_select(virtuoso, Prefixes, Variables, Bgps, Result, Options2)
  ).
