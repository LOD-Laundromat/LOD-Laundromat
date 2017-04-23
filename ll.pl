:- module(
  ll,
  [
    add_wm/0,
    add_wms/1,  % +NumWMs
    debug_wm/0,
    debug_wm/1, % +Hash
    start_ll/0
  ]
).

/** <module> LOD Laundromat

@author Wouter Beek
@version 2017/04
*/

:- use_module(library(aggregate)).
:- use_module(library(debug)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(thread_ext)).
:- use_module(library(zlib)).

:- use_module(library(hdt/hdt_api)).

:- use_module(clean).
:- use_module(seedlist).

:- debug(ll).





%! add_wm is det.

add_wm :-
  max_wm(N0),
  N is N0 + 1,
  atomic_list_concat([m,N], :, Alias),
  thread_create(loop(0), _, [alias(Alias),detached(true)]).

loop(Idle) :-
  start_seed(Hash, Uri), !,
  clean_uri(Uri),
  end_seed(Hash),
  loop(Idle).
loop(Idle) :-
  sleep(10),
  thread_name(Alias),
  debug(ll, "💤 machine ~a idle ~D", [Alias,Idle]),
  loop(Idle).



%! add_wms(+NumWMs) is det.


add_wms(0) :- !.
add_wms(N1) :-
  N2 is N1 - 1,
  add_wm,
  add_wms(N2).



%! debug_wm is det.
%! debug_wm(+Hash) is det.

debug_wm :-
  loop(0).

% dbe8e9e8dc412990d5a13289a3613bd6
debug_wm(Hash) :-
  seed(Hash, Uri, _, _, _),
  clean_uri(Uri).



%! max_wm(-N) is det.
%
% The highest washing machine identifier.

max_wm(N) :-
  aggregate_all(
    max(N),
    (
      wm_thread_alias(m, Alias),
      atomic_list_concat([m,N0], :, Alias),
      atom_number(N0, N)
    ),
    N
  ), !.
max_wm(0).



%! number_of_wms(-NumWMs) is det.

number_of_wms(NumWMs) :-
  aggregate_all(count, wm_thread_alias(m, _), NumWMs).



%! start_ll is det.

start_ll :-
  add_wms(5).



%! wm_thread_alias(+Prefix:oneof([a,e,m]), -Alias) is nondet.
%
% @arg Prefix Either `a` (archive), `e` (entry) or `m` (machine).

wm_thread_alias(Prefix, Alias) :-
  thread_property(Id, status(running)),
  thread_property(Id, alias(Alias)),
  atomic_list_concat([Prefix|_], :, Alias).
