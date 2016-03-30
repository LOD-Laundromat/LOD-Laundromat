:- module(
  lcli,
  [
    lf/0,
    lf/1, % +HashPrefix
    ld/1, % +HashPrefix
    ld/2, % +HashPrefix, +Opts
    ld/3, % ?S, ?P, ?O
    ld/4, % ?S, ?P, ?O, ?HashPrefix
    ld/5, % ?S, ?P, ?O, ?HashPrefix, +Opts
    lm/1, % +HashPrefix
    lm/2, % +HashPrefix, +Opts
    lm/3, % ?S, ?P, ?O
    lm/4, % ?S, ?P, ?O, ?HashPrefix
    lm/5, % ?S, ?P, ?O, ?HashPrefix, +Opts
    lw/1, % +HashPrefix
    lw/2, % +HashPrefix, +Opts
    lw/3, % ?S, ?P, ?O
    lw/4, % ?S, ?P, ?O, ?HashPrefix
    lw/5  % ?S, ?P, ?O, ?HashPrefix, +Opts
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
    call_on_dir(+, 1),
    call_on_dirs(?, 1).

:- rdf_meta
   ld(r, r, o),
   ld(r, r, o, r),
   ld(r, r, o, r, +),
   lm(r, r, o),
   lm(r, r, o, r),
   lm(r, r, o, r, +),
   lw(r, r, o),
   lw(r, r, o, r),
   lw(r, r, o, r, +).





%! lf is det.
%! lf(+HashPrefix) is det.

lf :-
  lroot(Dir),
  ls(Dir).


lf(HashPrefix) :-
  call_on_dir(HashPrefix, lf0(HashPrefix)).


lf0(HashPrefix, Dir) :-
  lcli_print("The contents for "),
  lcli_print_hash(HashPrefix, Dir),
  nl,
  ls(Dir).



%! ld(+HashPrefix) is det.
%! ld(+HashPrefix, +Opts) is det.
%! ld(?S, ?P, ?O) is det.
%! ld(?S, ?P, ?O, +HashPrefix) is det.
%! ld(?S, ?P, ?O, +HashPrefix, +Opts) is det.

ld(HashPrefix) :-
  ld(HashPrefix, _{}).


ld(HashPrefix, Opts) :-
  ld(_, _, _, HashPrefix, Opts).


ld(S, P, O) :-
  ld(S, P, O, _).


ld(S, P, O, HashPrefix) :-
  ld(S, P, O, HashPrefix, _{}).


ld(S, P, O, HashPrefix, Opts) :-
  call_on_dirs(HashPrefix, ldmw0(data, S, P, O, Opts)).


ldmw0(Name, S, P, O, Opts, Dir) :-
  ldir_ldoc(Dir, Name, Doc),
  lhdt_page(S, P, O, Doc, Opts, Result),
  lcli_print_result(S, P, O, Result, Opts).



%! lm(+HashPrefix) is det.
%! lm(+HashPrefix, +Opts) is det.
%! lm(?S, ?P, ?O) is det.
%! lm(?S, ?P, ?O, +HashPrefix) is det.
%! lm(?S, ?P, ?O, +HashPrefix, +Opts) is det.

lm(HashPrefix) :-
  lm(HashPrefix, _{}).


lm(HashPrefix, Opts) :-
  lm(_, _, _, HashPrefix, Opts).


lm(S, P, O) :-
  lm(S, P, O, _).


lm(S, P, O, HashPrefix) :-
  lm(S, P, O, HashPrefix, _{}).


lm(S, P, O, HashPrefix, Opts) :-
  call_on_dirs(HashPrefix, ldmw0(meta, S, P, O, Opts)).



%! lw(+HashPrefix) is det.
%! lw(+HashPrefix, +Opts) is det.
%! lw(?S, ?P, ?O) is det.
%! lw(?S, ?P, ?O, +HashPrefix) is det.
%! lw(?S, ?P, ?O, +HashPrefix, +Opts) is det.

lw(HashPrefix) :-
  lw(HashPrefix, _{}).


lw(HashPrefix, Opts) :-
  lw(_, _, _, HashPrefix, Opts).


lw(S, P, O) :-
  lw(S, P, O, _).


lw(S, P, O, HashPrefix) :-
  lw(S, P, O, HashPrefix, _{}).


lw(S, P, O, HashPrefix, Opts) :-
  call_on_dirs(HashPrefix, ldmw0(warn, S, P, O, Opts)).





% HELPERS %

%! call_on_dir(+Prefix, :Goal_1) is det.
% Call Goal_1 once for the single directory determined by Prefix.
% If no single directory is determined by Prefix, then show an overview
% of matches.

call_on_dir(Prefix, Goal_1) :-
  prefix_to_dirs(Prefix, Dirs),
  (Dirs = [Dir] -> call(Goal_1, Dir) ; lfind_results(Prefix, Dirs)).



%! call_on_dirs(?Prefix, :Goal_1) is nondet.
% Consecutively call Goal_1 for each directory matching Prefix.

call_on_dirs(Prefix, Goal_1) :-
  prefix_to_dirs(Prefix, Dirs),
  Dirs \== [], !,
  member(Dir, Dirs),
  call(Goal_1, Dir).
call_on_dirs(_, Goal_1) :-
  ldir(Dir),
  call(Goal_1, Dir).



%! lcli_print(+Format) is det.
%! lcli_print(+Format, +Args) is det.

lcli_print(Format) :-
  lcli_print(Format, []).


lcli_print(Format, Args) :-
  ansi_format(user_output, [fg(green)], Format, Args).



%! lcli_print_hash(+Prefix, +Path) is det.

lcli_print_hash(Prefix, Path) :-
  lhash_prefix_parts(Prefix, Dir1, Dir2),
  directory_file_path(_, File, Path),
  atom_concat(Dir2, Rest, File),
  lcli_print("~a/~a|~a", [Dir1,Dir2,Rest]).



%! lcli_print_hash_prefix(+Prefix) is det.

lcli_print_hash_prefix(Prefix) :-
  lhash_prefix_parts(Prefix, Dir1, Dir2),
  lcli_print("'~a/~a'", [Dir1,Dir2]).



%! lcli_print_result(?S, ?P, ?O, +Result, +Opts) is det.

lcli_print_result(S, P, O, Result, Opts) :-
  rdf_print_triples(Result.triples, Opts),
  nl,
  (   maplist(var, [S,P,O])
  ->  I2 is Result.page * Result.number_of_triples_per_page,
      I1 is I2 - Result.number_of_triples_per_page + 1,
      I0 is Result.number_of_triples,
      lcli_print("Showing triples ~D--~D (total ~D)~n", [I1,I2,I0])
  ;   true
  ).



% lfind_results(+Prefix, +Dirs) is det.
% Print a list of LOD Laundromat directoy matches.

lfind_results(Prefix, []) :- !,
  lcli_print("% Nothing matches "),
  lcli_print_hash_prefix(Prefix),
  lcli_print(":"),
  nl.
lfind_results(Prefix, L) :-
  lcli_print("% Multiple candidates match "),
  lcli_print_hash_prefix(Prefix),
  lcli_print(":"),
  nl,
  lfind_results0(Prefix, L).

lfind_results0(_, []) :- !.
lfind_results0(Prefix, [H]) :- !,
  lcli_print("%  - "),
  lcli_print_hash(Prefix, H),
  nl.
lfind_results0(Prefix, [H1,H2|T]) :-
  lcli_print("%  - "),
  lcli_print_hash(Prefix, H1),
  lcli_print("  - "),
  lcli_print_hash(Prefix, H2),
  nl,
  lfind_results0(Prefix, T).



%! lhash_prefix_parts(+Prefix, -Dir1, -Dir2) is det.

lhash_prefix_parts(Prefix, Dir1, Dir2) :-
  sub_atom(Prefix, 0, 2, _, Dir1),
  sub_atom(Prefix, 2, _, 0, Dir2).



%! prefix_to_dirs(+Prefix, -Dirs) is det.

prefix_to_dirs(Prefix, Dirs2) :-
  atom(Prefix),
  atom_length(Prefix, N),
  (   N =:= 2
  ->  Dir1 = Prefix,
      Dirs1 = [Dir1]
  ;   N > 2
  ->  atom_codes(Prefix, [H1,H2|T1]),
      atom_codes(Dir1, [H1,H2]),
      append(T1, [0'*], T2),
      atom_codes(Dir2, T2),
      Dirs1 = [Dir1,Dir2]
  ), !,
  lroot(Root),
  append_dirs([Root|Dirs1], Wildcard),
  expand_file_name(Wildcard, Dirs2).
