:- module(
  ll_seedlist,
  [
    add_uri/1,        % +Uri
    clear_seedlist/0,
    seed/1,           % -Seed:dict
    seed_add/1,       % +Dict
    seed_merge/1      % +Dict
  ]
).

/** <module> LOD Laundromat: Seedlist

entry:
  - added(dt)
  - children(list(atom))
  - parent(atom)
  - status(oneof([filed,unarchiving]))

uri:
  - added(dt)
  - children(list(atom))
  - parent(atom)
  - relative(boolean)
  - status(oneof([added,downloading,filed,unarchiving]))
  - uri(atom)

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(apply)).
:- use_module(library(date_time)).
:- use_module(library(dict_ext)).
:- use_module(library(hash_ext)).
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

add_uri(Uri1) :-
  uri_normalized(Uri1, Uri2),
  (uri_is_global(Uri2) -> Relative = false ; Relative = true),
  md5(Uri2, Hash),
  seed_add(Hash{relative: Relative, status: added, uri: Uri2}).



%! clear_seedlist is det.

clear_seedlist :-
  rocks_clear(seedlist).



%! seed(-Seed:dict) is nondet.

seed(Seed) :-
  rocks(seedlist, _, Seed).



%! seed_add(+Dict:dict) is det.

seed_add(Dict1) :-
  dict_tag(Dict1, Hash),
  now(Now),
  merge_dicts(Hash{added: Now}, Dict1, Dict2),
  rocks_put(seedlist, Hash, Dict2).



%! seed_merge(+Dict:dict) is det.

seed_merge(Dict) :-
  dict_tag(Dict, Hash),
  rocks_merge(seedlist, Hash, Dict).
