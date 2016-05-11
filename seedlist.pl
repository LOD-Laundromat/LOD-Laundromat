:- module(
  seedlist,
  [
    add_iri/1,               % +Iri
    add_iri/2,               % +Iri, -Hash
    begin_processing_seed/2, % -Hash, -Iri
    current_seed/1,          %        -Seed
    end_processing_seed/1,   % +Hash
    print_seed/1,            % +Hash
    print_seeds/0,
    remove_seed/1,           % +Hash
    reset_seed/1,            % +Hash
    seed_dict/2,             % +Seed, -D
    seeds_dict/1             %        -D
  ]
).

/** <module> Seedlist

@author Wouter Beek
@version 2016/01-2016/02, 2016/05
*/

:- use_module(library(apply)).
:- use_module(library(debug_ext)).
:- use_module(library(error)).
:- use_module(library(hash_ext)).
:- use_module(library(json_ext)).
:- use_module(library(list_ext)).
:- use_module(library(os/file_ext)).
:- use_module(library(os/gnu_sort)).
:- use_module(library(pair_ext)).
:- use_module(library(persistency)).
:- use_module(library(print_ext)).
:- use_module(library(sparql/sparql_query)).
:- use_module(library(thread)).

:- persistent
   seed(hash:atom, from:atom, added:float, started:float, ended:float).

:- initialization(init_seedlist).

init_seedlist :-
  init_seedlist('seedlist.db').

init_seedlist(File) :-
  access_file(File, read), !,
  db_attach(File, [sync(flush)]).
init_seedlist(File) :-
  touch(File),
  db_attach(File, [sync(flush)]),
  % Extract all seeds from the old LOD Laundromat server and store them locally
  % as a seedlist.  This is intended for debugging purposes only.
  Q = '\c
PREFIX llo: <http://lodlaundromat.org/ontology/>\n\c
SELECT ?url\n\c
WHERE {\n\c
  ?doc llo:url ?url\n\c
}\n',
  setup_call_cleanup(
    open(File, write, Write),
    forall(
      sparql_select('http://sparql.backend.lodlaundromat.org', Q, Rows),
      forall(member([Iri], Rows), add_iri(Iri))
    ),
    close(Write)
  ),
  sort_file(File).



%! add_iri(+Iri) is det.
%! add_iri(+Iri, -Hash) is det.
% Adds an IRI to the seedlist.
%
% @throws existence_error if IRI is already in the seedlist.

add_iri(Iri) :-
  add_iri(Iri, _).


add_iri(Iri1, Hash) :-
  iri_normalized(Iri1, Iri2),
  with_mutex(seedlist, add_iri_with_check0(Iri2, Hash)),
  debug(seedlist, "Added to seedlist: ~a (~a)", [Iri1,Hash]).

add_iri_with_check0(Iri, _) :-
  seed(_, Iri, _, _, _), !,
  existence_error(seed, Iri).
add_iri_with_check0(Iri, Hash) :-
  md5(Iri, Hash),
  add_iri_without_check0(Iri, Hash).

add_iri_without_check0(Iri, Hash) :-
  get_time(Now),
  assert_seed(Hash, Iri, Now, 0.0, 0.0).



%! begin_processing_seed(+Hash, -Iri) is det.
%! begin_processing_seed(-Hash, -Iri) is det.
% Pop a dirty seed off the seedlist.
%
% @throws existence_error If the seed is not in the seedlist.

begin_processing_seed(H, I) :-
  with_mutex(seedlist, begin_processing_seed0(H, I)),
  debug(seedlist(begin), "Started cleaning seed ~a (~a)", [H,I]).

begin_processing_seed0(H, _) :-
  nonvar(H),
  \+ seed(H, _, _, _, _), !,
  existence_error(seed, H).
begin_processing_seed0(H, I) :-
  retract_seed(H, I, A, 0.0, 0.0),
  get_time(S),
  assert_seed(H, I, A, S, 0.0).



%! current_seed(-Seed) is nondet.
% Enumerates the seeds in the currently loaded seedlist.

current_seed(seed(H,I,A,S,E)) :-
  seed(H, I, A, S, E).



%! end_processing_seed(+Hash) is det.

end_processing_seed(H) :-
  get_time(E),
  with_mutex(seedlist, (
    retract_seed(H, I, A, S, 0.0),
    assert_seed(H, I, A, S, E)
  )),
  debug(seedlist(end), "Ended cleaning seed ~a (~a)", [H,I]).



%! print_seed(+Hash) is det.

print_seed(Hash) :-
  seed_dict(Hash, D),
  print_dict(D).



%! print_seeds is det.

print_seeds :-
  forall(seed_dict(_, D), print_dict(D)).



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



%! seed_dict(+Seed, -D) is det.
%! seed_dict(-Seed, -D) is nondet.
% Prolog term conversion that make formulating a JSON response very easy.

seed_dict(Hash, D) :-
  current_seed(seed(Hash,Iri,Added,Started1,Ended1)),
  maplist(json_var_to_null, [Started1,Ended1], [Started2,Ended2]),
  D = _{added:Added,ended:Ended2,hash:Hash,seed:Iri,started:Started2}.



%! seeds_dict(-D) is det.

seeds_dict(D) :-
  findall(D, (current_seed(seed(Hash,_,_,_,_)), seed_dict(Hash, D)), Ds),
  length(Ds, N),
  D = _{seeds:Ds,size:N}.
