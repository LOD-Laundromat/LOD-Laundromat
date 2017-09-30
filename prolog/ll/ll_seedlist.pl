:- module(
  ll_seedlist,
  [
    add_uri/1,     % +Uri
    clear_all/0,
    clear_hash/1,  % +Hash
    clear_uri/1,   % +Uri
    reset_uri/1,   % +Uri
    seed/1,        % -Seed
    seed/2,        % +Hash, -Seed
    seed_create/1, % +Dict
    seed_merge/1   % +Dict
  ]
).

/** <module> LOD Laundromat: Seedlist

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(apply)).
:- use_module(library(date_time)).
:- use_module(library(dict_ext)).
:- use_module(library(filesex)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(rocks_ext)).
:- use_module(library(uri)).

:- at_halt(maplist(rocks_close, [seedlist])).

:- initialization
   rocks_init(seedlist, [key(atom),merge(ll_seedlist:merge_dicts),value(term)]).

merge_dicts(partial, _, New, In, Out) :-
  merge_dicts(New, In, Out).
merge_dicts(full, _, Initial, Additions, Out) :-
  merge_dicts([Initial|Additions], Out).




%! add_uri(+Uri:atom) is det.
%
% Add a URI to the seedlist.

add_uri(Uri) :-
  (uri_is_global(Uri) -> Relative = false ; Relative = true),
  uri_hash(Uri, Hash),
  (   % The URI has already been added to the seedlist.
      rocks_key(seedlist, Hash)
  ->  print_message(informational, existing_seed(Uri,Hash))
  ;   seed_create(Hash{relative: Relative, status: added, uri: Uri})
  ).



%! clear_all is det.
%
% Deletes the entire seedlist.

clear_all :-
  forall(
    seed(Seed),
    (
      Hash{} :< Seed,
      clear_hash(Hash)
    )
  ),
  rocks_clear(seedlist).



%! clear_hash(+Hash:atom) is det.

clear_hash(Hash) :-
  seed(Hash, Seed),
  (Hash{children: Children} :< Seed -> maplist(clear_hash, Children) ; true),
  hash_directory(Hash, Dir),
  delete_directory_and_contents(Dir),
  rocks_delete(seedlist, Hash).



%! clear_uri(+Uri:atom) is det.
%
% Removed all information about the given URI.

clear_uri(Uri) :-
  uri_hash(Uri, Hash),
  clear_hash(Hash).



%! reset_uri(+Uri:atom) is det.

reset_uri(Uri) :-
  clear_uri(Uri),
  add_uri(Uri).
  


%! seed(-Seed:dict) is nondet.
%! seed(+Hash:atom, -Seed:dict) is nondet.

seed(Seed) :-
  seed(_, Seed).


seed(Hash, Seed) :-
  rocks(seedlist, Hash, Seed).



%! seed_create(+Dict:dict) is det.

seed_create(Dict1) :-
  dict_tag(Dict1, Hash),
  now(Now),
  merge_dicts(Hash{added: Now}, Dict1, Dict2),
  rocks_put(seedlist, Hash, Dict2).



%! seed_merge(+Dict:dict) is det.

seed_merge(Dict) :-
  dict_tag(Dict, Hash),
  rocks_merge(seedlist, Hash, Dict).
