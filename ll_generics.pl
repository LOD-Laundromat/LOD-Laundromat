:- module(
  ll_generics,
  [
    ll_authority/1, % ?Authority:atom
    ll_scheme/1, % ?Scheme:atom
    ll_version/1, % ?Version:positive_integer
    ll_versioned_graph/2 % +Graph:atom
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


%! ll_version(+Version:positive_integer) is semidet.
%! ll_version(-Version:positive_integer) is det.

ll_version(11).


%! ll_versioned_graph(+Graph:atom, -VersionedGraph:atom) is det.

ll_versioned_graph(G1, G2):-
  md5_bnode_base(G1, Scheme-Authority-Hash1),
  ll_version(Version),
  atomic_list_concat([Hash1,Version], '#', Path1),
  atomic_list_concat(['',Path1], '/', Path2),
  uri_components(G2, uri_components(Scheme,Authority,Path2,_,_)).

