:- module(
  seedlist,
  [
    add_iri/1,         % +Iri
    add_iri/2,         % +Iri, -Hash
    begin_seed/2,      % -Hash, -Iri
    current_seed/1,    % -Seed
    current_seed/2,    % +Hash, -Seed
    end_seed/1,        % +Hash
    remove_seed/1,     % +Hash
    reset_seed/1,      % +Hash
    seed/1             % ?Hash
  ]
).

/** <module> Seedlist

@author Wouter Beek
@version 2016/01-2016/02
*/

:- use_module(library(apply)).
:- use_module(library(debug_ext)).
:- use_module(library(error)).
:- use_module(library(hash_ext)).
:- use_module(library(list_ext)).
:- use_module(library(pair_ext)).
:- use_module(library(persistency)).
:- use_module(library(thread)).

:- persistent
   seed(hash:atom, from:atom, added:float, started:float, ended:float).

:- initialization((
     absolute_file_name(
       cpack('LOD-Laundromat/seedlist.db'),
       File,
       [access(read)]
     ),
     db_attach(File, [sync(flush)])
   )).





%! add_iri(+Iri) is det.
%! add_iri(+Iri, -Hash) is det.
% Adds an IRI to the seedlist.
%
% @throws existence_error if IRI is already in the seedlist.

add_iri(I) :-
  add_iri(I, _).

add_iri(I1, H) :-
  iri_normalized(I1, I2),
  with_mutex(seedlist, add_iri_with_check0(I2, H)),
  debug(seedlist, "Added to seedlist: ~a (~a)", [I1,H]).

add_iri_with_check0(I, _) :-
  seed(_, I, _, _, _), !,
  existence_error(seed, I).
add_iri_with_check0(I, H) :-
  md5(I, H),
  add_iri_without_check0(I, H).

add_iri_without_check0(I, H) :-
  get_time(A),
  assert_seed(H, I, A, 0.0, 0.0).



%! begin_seed(+Hash, -Iri) is det.
%! begin_seed(-Hash, -Iri) is det.
% Pop a dirty seed off the seedlist.
%
% @throws existence_error If the seed is not in the seedlist.

begin_seed(H, I) :-
  with_mutex(seedlist, begin_seed0(H, I)),
  debug(seedlist(begin), "Started cleaning seed ~a (~a)", [H,I]).

begin_seed0(H, _) :-
  nonvar(H),
  \+ seed(H, _, _, _, _), !,
  existence_error(seed, H).
begin_seed0(H, I) :-
  retract_seed(H, I, A, 0.0, 0.0),
  get_time(S),
  assert_seed(H, I, A, S, 0.0).



%! current_seed(-Seed) is nondet.
% Enumerates the seeds in the currently loaded seedlist.

current_seed(seed(H,I,A,S,E)) :-
  seed(H,I,A,S,E).


%! current_seed(+Hash, -Seed) is det.

current_seed(H, seed(H,I,A,S,E)) :-
  seed(H, I, A, S, E).



%! end_seed(+Hash) is det.

end_seed(H) :-
  get_time(E),
  with_mutex(seedlist, (
    retract_seed(H, I, A, S, 0.0),
    assert_seed(H, I, A, S, E)
  )),
  debug(seedlist(end), "Ended cleaning seed ~a (~a)", [H,I]).



%! remove_seed(+Hash) is det.

remove_seed(H) :-
  with_mutex(seedlist, retract_seed(H, I, _, _, _)),
  debug(seedlist(remove), "Removed seed ~a (~a)", [H,I]).



%! reset_seed(+Hash) is det.

reset_seed(H) :-
  with_mutex(seedlist, (
    retract_seed(H, I, _, _, _),
    add_iri_without_check0(I, H)
  )),
  debug(seedlist(reset), "Reset seed ~a (~a)", [H,I]).



%! seed(+Hash) is semidet.
%! seed(-Hash) is nondet.

seed(H) :-
  ground(H), !,
  once(current_seed(H, _)).
seed(H) :-
  current_seed(seed(H,_,_,_,_)).
