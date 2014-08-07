:- module(
  lwm_settings,
  [
    lwm_authority/1, % ?Authority:atom
    lwm_scheme/1, % ?Scheme:atom
    lwm_version/1, % ?Version:positive_integer
    lwm_versioned_graph/2 % +Graph:atom
                          % -VersionedGraph:atom
  ]
).

/** <module> LOD Washing Machine: generics

Generic predicates for the LOD Washing Machine.

@author Wouter Beek
@version 2014/06, 2014/08
*/

:- use_module(library(uri)).

:- use_module(lwm(md5)).



%! lwm_authority(+Authortity:atom) is semidet.
%! lwm_authority(-Authortity:atom) is det.

lwm_authority('lodlaundromat.org').


%! lwm_scheme(+Scheme:atom) is semidet.
%! lwm_scheme(-Scheme:oneof([http])) is det.

lwm_scheme(http).


%! lwm_version(+Version:positive_integer) is semidet.
%! lwm_version(-Version:positive_integer) is det.

lwm_version(11).


%! lwm_versioned_graph(+Graph:atom, -VersionedGraph:atom) is det.

lwm_versioned_graph(Graph, VersionedGraph):-
  md5_bnode_base(Graph, Scheme-Authority-Hash1),
  lwm_version(Version),
  atomic_list_concat([Hash1,Version], '#', Path1),
  atomic_list_concat(['',Path1], '/', Path2),
  uri_components(VersionedGraph, uri_components(Scheme,Authority,Path2,_,_)).

