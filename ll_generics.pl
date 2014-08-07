:- module(
  ll_generics,
  [
    ll_authority/1, % ?Authority:atom
    ll_scheme/1, % ?Scheme:atom
    ll_version/2, % +Mode:oneof([collection,dissemination])
                  % ?Version:positive_integer
    ll_versioned_graph/3 % +Graph:atom
                         % +Mode:oneof([collection,dissemination])
                         % -VersionedGraph:atom
  ]
).

/** <module> LOD Laundromat: generics

Generic predicates for the LOD Laundromat.

@author Wouter Beek
@version 2014/06, 2014/08
*/

:- use_module(library(uri)).

:- use_module(ll(md5)).



%! ll_authority(+Authortity:atom) is semidet.
%! ll_authority(-Authortity:atom) is det.

ll_authority('lodlaundromat.org').


%! ll_scheme(+Scheme:atom) is semidet.
%! ll_scheme(-Scheme:oneof([http])) is det.

ll_scheme(http).


%! ll_version(
%!   +Mode:oneof([collection,dissemination]),
%!   +Version:positive_integer
%! ) is semidet.
%! ll_version(
%!   +Mode:oneof([collection,dissemination]),
%!   -Version:positive_integer
%! ) is det.

ll_version(collection, 11).
ll_version(dissemination, 10).


%! ll_versioned_graph(
%!   +Graph:atom,
%!   +Mode:oneof([collection,dissemination]),
%!   -VersionedGraph:atom
%! ) is det.

ll_versioned_graph(Graph, Mode, VersionedGraph):-
  md5_bnode_base(Graph, Scheme-Authority-Hash1),
  ll_version(Mode, Version),
  atomic_list_concat([Hash1,Version], '#', Path1),
  atomic_list_concat(['',Path1], '/', Path2),
  uri_components(VersionedGraph, uri_components(Scheme,Authority,Path2,_,_)).

