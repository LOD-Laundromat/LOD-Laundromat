:- module(
  ll_seedlist,
  [
    add_seed/3,      % +Hash, +Status, +Dict
    add_uri/1,       % +Uri
    clear_seedlist/0
  ]
).
:- reexport(library(rocks_ext)).

/** <module> LOD Laundromat: Seedlist

Status is either of the following:
  - added
  - downloading
  - downloaded

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(apply)).
:- use_module(library(date_time)).
:- use_module(library(dict_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(lists)).
:- use_module(library(uri)).

:- at_halt(maplist(rocks_close, [seedlist])).

:- initialization
   rocks_init(seedlist, [key(atom),merge(ll_seedlist:merge_dicts),value(term)]).

merge_dicts(partial, _, New, In, Out) :-
  merge_dicts(New, In, Out).
merge_dicts(full, _, New, Ins, Out) :-
  merge_dicts([New|Ins], Out).




%! add_seed(+Hash:atom, +Status:atom, +Dict:dict) is det.

add_seed(Hash, Status, Dict1) :-
  now(Now),
  merge_dicts(_{added: Now, status: Status}, Dict1, Dict2),
  rocks_put(seedlist, Hash, Dict2).



%! add_uri(+Uri:atom) is det.

add_uri(Uri1) :-
  uri_normalized(Uri1, Uri2),
  (uri_is_global(Uri2) -> Relative = false ; Relative = true),
  md5(Uri2, Hash),
  add_seed(Hash, added, _{relative: Relative, uri: Uri2}).



%! clear_seedlist is det.

clear_seedlist :-
  rocks_clear(seedlist).
