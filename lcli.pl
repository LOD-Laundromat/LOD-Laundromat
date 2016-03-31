:- module(
  lcli,
  [
    lf/0,
    lf/1, %             +HashPrefix
    lf/2, %             +HashPrefix, +Opts
    ld/0,
    ld/1, %             ?HashPrefix
    ld/2, %             ?HashPrefix, +Opts
    ld/3, % ?S, ?P, ?O
    ld/4, % ?S, ?P, ?O, ?HashPrefix
    lm/0,
    lm/1, %             ?HashPrefix
    lm/2, %             ?HashPrefix, +Opts
    lm/3, % ?S, ?P, ?O
    lm/4, % ?S, ?P, ?O, ?HashPrefix
    lw/0,
    lw/1, %             ?HashPrefix
    lw/2, %             ?HashPrefix, +Opts
    lw/3, % ?S, ?P, ?O
    lw/4  % ?S, ?P, ?O, ?HashPrefix
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
:- use_module(library(pagination)).
:- use_module(library(print_ext)).
:- use_module(library(rdf/rdf_print)).
:- use_module(library(semweb/rdf11)).

:- use_module(cpack('LOD-Laundromat'/lfs)).
:- use_module(cpack('LOD-Laundromat'/lhdt)).

:- rdf_meta
   ld(r, r, o),
   ld(r, r, o, +),
   lm(r, r, o),
   lm(r, r, o, +),
   lw(r, r, o),
   lw(r, r, o, +).





%! lf                     is det.
%! lf(+HashPrefix       ) is det.
%! lf(+HashPrefix, +Opts) is det.

lf :-
  lf('').


lf(HashPrefix) :-
  lf(HashPrefix, _{}).


lf(HashPrefix, Opts) :-
  pagination(Dir, ldir(HashPrefix, Dir), Opts, Result),
  pagination_result(HashPrefix, Result, pp_hash_paths(HashPrefix)).



%! ld                                 is det.
%! ld(            +HashPrefix       ) is det.
%! ld(            +HashPrefix, +Opts) is det.
%! ld(?S, ?P, ?O                    ) is det.
%! ld(?S, ?P, ?O, +HashPrefix       ) is det.

ld :-
  ld('').

ld(HashPrefix) :-
  ld(HashPrefix, _{}).


ld(HashPrefix, Opts) :-
  ldmw_pagination0(HashPrefix, Opts, data).


ld(S, P, O) :-
  ld(S, P, O, _).


ld(S, P, O, HashPrefix) :-
  ldmw0(S, P, O, HashPrefix, data).



%! lm                          is det.
%! lm(            +HashPrefix) is det.
%! lm(?S, ?P, ?O             ) is det.
%! lm(?S, ?P, ?O, +HashPrefix) is det.

lm :-
  lm('').


lm(HashPrefix) :-
  lm(HashPrefix, _{}).


lm(HashPrefix, Opts) :-
  ldmw_pagination0(HashPrefix, Opts, meta).


lm(S, P, O) :-
  lm(S, P, O, _).


lm(S, P, O, HashPrefix) :-
  ldmw0(S, P, O, HashPrefix, meta).



%! lw                          is det.
%! lw(            +HashPrefix) is det.
%! lw(?S, ?P, ?O             ) is det.
%! lw(?S, ?P, ?O, +HashPrefix) is det.

lw :-
  lw('').


lw(HashPrefix) :-
  lw(HashPrefix, _{}).


lw(HashPrefix, Opts) :-
  ldmw_pagination0(HashPrefix, Opts, warn).


lw(S, P, O) :-
  lw(S, P, O, _).


lw(S, P, O, HashPrefix) :-
  ldmw0(S, P, O, HashPrefix, warn).





% HELPERS %

ldmw0(S, P, O, HashPrefix, Name) :-
  ldmw0(S, P, O, _, HashPrefix, Name).


ldmw0(S, P, O, Hash, HashPrefix, Name) :-
  ldir(HashPrefix, Dir),
  ldir_lhash(Dir, Hash),
  ldir_lfile(Dir, Name, hdt, File),
  catch(
    lhdt(S, P, O, File),
    E,
    (
      print_message(informational, E),
      fail
    )
  ).



ldmw_pagination0(HashPrefix, Opts, Name) :-
  pagination(
    rdf(S,P,O,Hash),
    ldmw0(S, P, O, Hash, HashPrefix, Name),
    Opts,
    Result
  ),
  pagination_result(HashPrefix, Result, rdf_print_quads0(Opts)).
rdf_print_quads0(Opts, Results) :- rdf_print_quads(Results, Opts).



%! lhash_prefix_parts(+HashPrefix, -Dir1, -Dir2) is det.

lhash_prefix_parts(HashPrefix, Dir1, Dir2) :-
  sub_atom(HashPrefix, 0, 2, _, Dir1), !,
  sub_atom(HashPrefix, 2, _, 0, Dir2).
lhash_prefix_parts(HashPrefix, HashPrefix, '').



%! pp(+Format) is det.
%! pp(+Format, +Args) is det.

pp(Format) :-
  pp(Format, []).


pp(Format, Args) :-
  ansi_format(user_output, [fg(green)], Format, Args).



%! pp_files(+Dir) is det.
% Print the files in Dir.

pp_files(Dir) :-
  forall(
    dir_file(Dir, Path),
    (
      directory_file_path(_, File, Path),
      pp("    ~a", [File])
    )
  ).



%! pp_hash_path(+HashPrefix, +Path) is det.
% Print the 

pp_hash_path(HashPrefix, Path) :-
  pp("  "),
  ldir_lhash(Path, Hash),
  atom_concat(HashPrefix, Rest, Hash),
  pp("~a|~a:  ", [HashPrefix,Rest]),
  pp_files(Path),
  nl.


pp_hash_paths(HashPrefix, Paths) :-
  maplist(pp_hash_path(HashPrefix), Paths).
