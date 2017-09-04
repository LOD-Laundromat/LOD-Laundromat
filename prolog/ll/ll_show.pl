:- module(
  ll_show,
  [
    show_uri/1 % +Uri
  ]
).

/** <module> LOD Laundromat: Show

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_seedlist)).
:- use_module(library(pp)).



%! show_uri(+Uri:atom) is det.

show_uri(Uri) :-
  uri_hash(Uri, Hash),
  seed(Hash, Dict),
  pp_term(Dict).
