:- module(
  ll_seedlist,
  [
    add_seed/1,   % +Uri
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
:- use_module(library(filesex)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(rocks_ext)).
:- use_module(library(settings)).
:- use_module(library(uri)).

:- at_halt(maplist(rocks_close, [seedlist])).

:- initialization
   rocks_init(seedlist, [key(atom),merge(ll_seedlist:merge_dicts),value(term)]).

:- setting(default_interval, float, 86400.0,
           "The default interval for recrawling.").

merge_dicts(partial, _, New, In, Out) :-
  merge_dicts(New, In, Out).
merge_dicts(full, _, Initial, Additions, Out) :-
  merge_dicts([Initial|Additions], Out).





%! add_seed(+Uri:atom) is det.
%
% Add a URI to the seedlist.

add_seed(Uri) :-
  (uri_is_global(Uri) -> Relative = false ; Relative = true),
  uri_hash(Uri, Hash),
  (   % The URI has already been added to the seedlist.
      rocks_key(seedlist, Hash)
  ->  print_message(informational, existing_seed(Uri,Hash))
  ;   get_time(Now),
      (   uri_last_modified(Uri, LastModified)
      ->  Interval is Now - LastModified
      ;   setting(default_interval, Interval)
      ),
      seed_store(
        Hash{
          added: Now,
          interval: Interval,
          processed: 0.0,
          relative: Relative,
          uri: Uri
        }
      )
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
  (Hash{children: Children} :< Seed -> maplist(clear_hash, Children) ; true),
  hash_directory(Hash, Dir),
  delete_directory_and_contents(Dir),
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
  add_seed(Uri).
  


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
      relative: false,
      interval: Interval,
      processed: Processed,
      uri: Uri
    } :< Seed,
    Processed + Interval < Now,
    rocks_merge(seedlist, Hash, Hash{processed: Now})
  )).
