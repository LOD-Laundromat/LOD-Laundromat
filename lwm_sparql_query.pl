:- module(
  lwm_sparql_query,
  [
    lwm_file_extension/2, % +Md5:atom
                          % -FileExtension:atom
    lwm_size/2, % +Md5:atom
                % -Size:nonneg
    lwm_source/2, % +Md5:atom
                  % -Source:atom
    lwm_url/2 % +Md5:atom
              % -Url:url
  ]
).

/** <module> LOD Washing Machine (LWM): SPARQL queries

SPARQL queries for the LOD Washing Machine.

@author Wouter Beek
@version 2014/08
*/

:- use_module(library(apply)).
:- use_module(library(semweb/rdf_db)).

:- use_module(lwm(lwm_sparql_api)).

:- use_module(plRdf_term(rdf_literal)).

:- rdf_register_prefix(ll, 'http://lodlaundromat.org/vocab#').



%! lwm_file_extension(+Md5:atom, -FileExtension:atom) is det.

lwm_file_extension(Md5, FileExtension):-
  lwm_sparql_select([ll], [file_extension],
      [rdf(var(md5res),ll:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5res),ll:file_extension,var(file_extension))],
      [[Literal]], [limit(1)]),
  rdf_literal(Literal, FileExtension, _).


%! lwm_size(+Md5:atom, -NumberOfGigabytes:between(0.0,inf)) is det.

lwm_size(Md5, NumberOfGigabytes):-
  lwm_sparql_select([ll], [size],
      [rdf(var(datadoc),ll:md5,literal(type(xsd:string,Md5))),
       rdf(var(datadoc),ll:size,var(size))],
      [[Literal]], [limit(1)]), !,
  rdf_literal(Literal, NumberOfBytes1, _),
  atom_number(NumberOfBytes1, NumberOfBytes2),
  NumberOfGigabytes is NumberOfBytes2 / (1024 ** 3).


%! lwm_source(+Md5:atom, -Source:atom) is det.
% Returns the original source of the given datadocument.
%
% This is either a URL simpliciter,
% or a URL suffixed by an archive entry path.

lwm_source(Md5, Url):-
  lwm_url(Md5, Url), !.
lwm_source(Md5, Source):-
  lwm_sparql_select([ll], [md5parent,path],
      [rdf(var(ent),ll:md5,literal(type(xsd:string,Md5))),
       rdf(var(ent),ll:path,var(path)),
       rdf(var(parent),ll:contains_entry,var(md5ent)),
       rdf(var(parent),ll:md5,var(md5parent))],
      [[Literal1,Literal2]], [limit(1)]), !,
  maplist(rdf_literal, [Literal1,Literal2], [ParentMd5,Path], _),
  lwm_source(ParentMd5, ParentSource),
  atomic_concat(ParentSource, Path, Source).


%! lwm_url(+Md5:atom, -Url:url) is det.

lwm_url(Md5, Url):-
  lwm_sparql_select([ll], [url],
      [rdf(var(md5res),ll:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5res),ll:url,var(url))],
      [[Url]], [limit(1)]).
