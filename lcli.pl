:- module(
  lcli,
  [
    ll/0,
    ll/1, % +Prefix
    ll/2, % +Prefix, +Name
    ll/3, % +Prefix, +Name,                   +Opts
    ll/5, % +Prefix, +Name, ?S, ?P, ?O
    ll/6, % +Prefix, +Name, ?S, ?P, ?O,       +Opts
    lm/3, %                 ?S, ?P, ?O
    lm/4, %                 ?S, ?P, ?O, ?Doc
    number_of_warnings/2 % ?Hash, ?N
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
:- use_module(library(os/gnu_wc)).
:- use_module(library(print_ext)).
:- use_module(library(rdf/rdf_print)).
:- use_module(library(semweb/rdf11)).

:- use_module(cpack('LOD-Laundromat'/lfs)).
:- use_module(cpack('LOD-Laundromat'/lhdt)).

:- meta_predicate
    ll_call(+, 1).

:- rdf_meta
   ll(+, +, r, r, o),
   ll(+, +, r, r, o, +),
   lm(r, r, o),
   lm(r, r, o, r).





%! ll is det.
%! ll(+Prefix) is det.
%! ll(+Prefix, +Name) is det.
%! ll(+Prefix, +Name, ?S, ?P, ?O) is nondet.
%! ll(+Prefix, +Name, ?S, ?P, ?O, +Opts) is nondet.

ll :-
  lroot(Dir),
  ls(Dir).


ll(Prefix) :-
  ll_call(Prefix, ll1(Prefix)).


ll(Prefix, Name) :-
  ll(Prefix, Name, _{}).


ll(Prefix, Name, Opts) :-
  ll(Prefix, Name, _, _, _, Opts).


ll(Prefix, Name, S, P, O) :-
  ll(Prefix, Name, S, P, O, _{}).


ll(Prefix, Name, S, P, O, Opts) :-
  ll_call(Prefix, ll2(Name, S, P, O, Opts)).


ll1(Prefix, Dir) :-
  lcli_print("The contents for "),
  lcli_print_hash(Prefix, Dir),
  nl,
  ls(Dir).


ll2(Name, S, P, O, Opts, Dir) :-
  ldir_ldoc(Dir, Name, Doc),
  lhdt_page(S, P, O, Doc, Opts, Result),
  lcli_print_result(S, P, O, Result, Opts).



%! lm(?S, ?P, ?O) is nondet.
%! lm(?S, ?P, ?O, ?Doc) is nondet.

lm(S, P, O) :-
  lm(S, P, O, _).


lm(S, P, O, Doc) :-
  ldoc(meta, Doc),
  lhdt(S, P, O, Doc).





% HELPERS %

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



%! ll_call(+Prefix, :Goal_1) is det.

ll_call(Prefix, Goal_1) :-
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
  ),
  lroot(Root),
  append_dirs([Root|Dirs1], Wildcard),
  expand_file_name(Wildcard, Dirs2),
  (Dirs2 = [Dir] -> call(Goal_1, Dir) ; lfind_results(Prefix, Dirs2)).



%! number_of_warnings(?Hash, ?N) is nondet.

number_of_warnings(Hash, N) :-
  lfile(warn, _, File),
  lfile_lhash(File, Hash),
  file_lines(File, N).
