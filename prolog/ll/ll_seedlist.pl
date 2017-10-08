:- module(
  ll_seedlist,
  [
    add_uri/1,    % +Uri
    clear_all/0,
    clear_hash/1, % +Hash
    clear_seed/1, % +Uri
    reset_seed/1, % +Uri
    seed/1,       % -Seed
    seed/2,       % +Hash, -Seed
    seed_merge/1, % +Dict
    seed_store/1, % +Dict
    stale_seed/2  % -Uri, -Hash
  ]
).

/** <module> LOD Laundromat: Seedlist

@author Wouter Beek
@version 2017/09-2017/10
*/

:- use_module(library(apply)).
:- use_module(library(dict_ext)).
:- use_module(library(error)).
:- use_module(library(filesex)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(rocks_ext)).
:- use_module(library(settings)).
:- use_module(library(sitemap)).
:- use_module(library(uri)).
:- use_module(library(yall)).

:- at_halt(maplist(rocks_close, [seedlist])).

:- initialization
   rocks_init(seedlist, [key(atom),merge(ll_seedlist:merge_dicts),value(term)]).

:- setting(default_interval, float, 86400.0,
           "The default interval for recrawling.").

merge_dicts(partial, _, New, In, Out) :-
  merge_dicts(New, In, Out).
merge_dicts(full, _, Initial, Additions, Out) :-
  merge_dicts([Initial|Additions], Out).





%! add_uri(+Uri:atom) is det.
%
% Add a URI to the seedlist.

add_uri(Uri1) :-
  is_sitemap_uri(Uri1), !,
  forall(
    sitemap_uri_interval(Uri1, Uri2, Interval),
    add_uri(Uri2, Interval)
  ).
add_uri(Uri) :-
  uri_is_global(Uri), !,
  (   catch(uri_last_modified(Uri, LastModified), _, fail)
  ->  get_time(Now),
      Interval is Now - LastModified
  ;   setting(default_interval, Interval)
  ),
  add_uri(Uri, Interval).
add_uri(Uri) :-
  type_error(absolute_uri, Uri).

add_uri(Uri, Interval) :-
  uri_hash(Uri, Hash),
  (   % The URI has already been added to the seedlist.
      rocks_key(seedlist, Hash)
  ->  print_message(informational, existing_seed(Uri,Hash))
  ;   get_time(Now),
      Dict = Hash{
        added: Now,
        interval: Interval,
        processed: 0.0,
        uri: Uri
      },
      seed_store(Dict)
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
  


%! seed(-Seed:dict) is nondet.
%! seed(+Hash:atom, -Seed:dict) is nondet.

seed(Seed) :-
  seed(_, Seed).


seed(Hash, Seed) :-
  rocks(seedlist, Hash, Seed).



%! seed_merge(+Dict:dict) is det.

seed_merge(Dict) :-
  dict_tag(Dict, Hash),
  with_mutex(ll_seedlist, rocks_merge(seedlist, Hash, Dict)).



%! seed_store(+Dict:dict) is det.

seed_store(Dict) :-
  dict_tag(Dict, Hash),
  rocks_put(seedlist, Hash, Dict).



%! stale_seed(-Uri:atom, -Hash) is nondet.
%
% Pop a stale seed for processing (i.e., downloading).

stale_seed(Uri, Hash) :-
  get_time(Now),
  with_mutex(ll_seedlist, (
    seed(Seed),
    Hash{
      interval: Interval,
      processed: Processed,
      uri: Uri
    } :< Seed,
    Processed + Interval < Now,
    rocks_merge(seedlist, Hash, Hash{processed: Now})
  )).
