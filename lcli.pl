:- module(
  lcli,
  [
    ll/0,
    ll/1, % +Search
    ll/2, % +Search, +Name
    ll/5  % +Search, +Name, ?S, ?P, ?O
  ]
).

/** <module> LOD Laundromat CLI

@author Wouter Beek
@version 2016/03
*/

:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(gen/gen_ntuples)).
:- use_module(library(lists)).
:- use_module(library(os/dir_ext)).
:- use_module(library(print_ext)).
:- use_module(library(rdf/rdf_print)).
:- use_module(library(semweb/rdf11)).

:- use_module(cpack('LOD-Laundromat'/lfs)).
:- use_module(cpack('LOD-Laundromat'/lhdt)).

:- meta_predicate
    lcli_call(1, +).

:- rdf_meta
   ll(+, +, r, r, o).





%! ll is det.
%! ll(+Search) is det.
%! ll(+Search, +Name) is det.
%! ll(+Search, +Name, ?S, ?P, ?O) is nondet.

ll :-
  lroot(Dir),
  lls(Dir).


ll(Search) :-
  lcli_call(lls, Search).


ll(Search, Name) :-
  ll(Search, Name, _, _, _).


ll(Search, Name, S, P, O) :-
  must_be(lname, Name),
  lcli_call(lq(Name, S, P, O), Search).


%! lls(+Dir) is det.

lls(Dir) :-
  ldir_lhash(Dir, Hash),
  lcli_print("The contents for '~a':~n", [Hash]),
  ls(Dir).


%! lq(+Name, ?S, ?P, ?O, +Dir) is nondet.

lq(Name, S, P, O, Dir) :-
  ldir_ldoc(Dir, Name, Doc),
  % NONDET
  findnsols(10, rdf(S,P,O), lhdt(S, P, O, Doc), Triples),
  rdf_print_triples(Triples).





% HELPERS %

%! lcli_call(:Goal_1, +Search) is det.

lcli_call(Goal_1, Search) :-
  atom_length(Search, N),
  (   N =:= 2
  ->  Dir1 = Search,
      Dirs = [Dir1],
      Prefix = ''
  ;   N > 2
  ->  atom_codes(Search, [H1,H2|T1]),
      atom_codes(Dir1, [H1,H2]),
      append(T1, [0'*], T2),
      atom_codes(Dir2, T2),
      Dirs = [Dir1,Dir2],
      atom_codes(Prefix, T1)
  ),
  lroot(Root),
  append_dirs([Root|Dirs], Wildcard),
  expand_file_name(Wildcard, L),
  (L = [H] -> call(Goal_1, H) ; lcli_match(Dir1, Prefix, L)).



% lcli_match(+Dir1, +Dir2, +Matches) is det.
% Print a list of LOD Laundromat directoy matches.

lcli_match(Dir1, Dir2, []) :- !,
  lcli_print("% Nothing matches '~a/~a'~n", [Dir1,Dir2]).
lcli_match(Dir1, Dir2, L) :-
  lcli_print("% Multiple candidates match '~a/~a':~n", [Dir1,Dir2]),
  lcli_match0(Dir1, Dir2, L).

lcli_match0(_, _, []) :- !.
lcli_match0(Dir1, Dir2, [H]) :- !,
  lcli_print("%  - "),
  lcli_print_hash(Dir1, Dir2, H),
  nl.
lcli_match0(Dir1, Dir2, [H1,H2|T]) :-
  lcli_print("%  - "),
  lcli_print_hash(Dir1, Dir2, H1),
  lcli_print("  - "),
  lcli_print_hash(Dir1, Dir2, H2),
  nl,
  lcli_match0(Dir1, Dir2, T).



%! lcli_print(+Format) is det.
%! lcli_print(+Format, +Args) is det.

lcli_print(Format) :-
  lcli_print(Format, []).


lcli_print(Format, Args) :-
  ansi_format(user_output, [fg(green)], Format, Args).



%! lcli_print_hash(+Dir1, +Dir2, +Path) is det.

lcli_print_hash(Dir1, Dir2, Path) :-
  directory_file_path(_, File, Path),
  atom_concat(Dir2, Rest, File),
  lcli_print("~a/~a|~a", [Dir1,Dir2,Rest]).
