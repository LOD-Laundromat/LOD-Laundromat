:- module(
  ll_seedlist,
  [
    add_uri/1,    % +Uri
    clear_all/0,
    clear_hash/1, % +Hash
    clear_seed/1, % +Uri
    reset_seed/1, % +Uri
    seed_merge/1  % +Dict
  ]
).

/** <module> LOD Laundromat: Seedlist

@author Wouter Beek
@version 2017/09-2017/11
*/

:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(filesex)).
:- use_module(library(settings)).
:- use_module(library(yall)).

:- use_module(library(dict)).
:- use_module(library(http/http_client2)).
:- use_module(library(uri_ext)).

:- use_module(ll_generics).
:- use_module(sitemap).





%! add_uri(+Uri:atom) is det.
%
% Add a URI to the seedlist.

add_uri(Uri1) :-
  is_sitemap_uri(Uri1), !,
  forall(
    sitemap_uri_interval(Uri1, Uri2, Interval),
    add_uri(Uri2, Interval)
  ).



%! clear_all is det.
%
% Deletes the entire seedlist.

clear_all :-
  repeat,
  (   seed(Seed)
  ->  Hash{} :< Seed,
      clear_hash(Hash),
      fail
  ;   !, true
  ),
  delete_empty_directories,
  rocks_clear(seedlist).



%! clear_hash(+Hash:atom) is det.

clear_hash(Hash) :-
  seed(Hash, Seed),
  dict_get(children, Seed, [], Children),
  maplist([Child]>>ignore(clear_hash(Child)), Children),
  hash_directory(Hash, Dir),
  (exists_directory(Dir) -> delete_directory_and_contents(Dir) ; true),
  rocks_delete(seedlist, Hash).



%! clear_seed(+Uri:atom) is det.
%
% Removed all information about the given seed URI.

clear_seed(Uri) :-
  uri_hash(Uri, Hash),
  clear_hash(Hash).



%! reset_seed(+Uri:atom) is det.

reset_seed(Uri) :-
  clear_seed(Uri),
  add_uri(Uri).
  


%! seed_merge(+Dict:dict) is det.

seed_merge(Dict) :-
  dict_tag(Dict, Hash),
  with_mutex(ll_seedlist, rocks_merge(seedlist, Hash, Dict)).
