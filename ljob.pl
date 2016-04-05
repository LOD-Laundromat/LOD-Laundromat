:- module(
  ljob,
  [
    ctuples/1, % -N
    lf_warm/0,
    lf_warm/1, % +HashPrefix
    ld_warm/0,
    ld_warm/1, % +HashPrefix
    lm_warm/0,
    lm_warm/1, % +HashPrefix
    lw_warm/0,
    lw_warm/1  % +HashPrefix    
  ]
).

/** <module> LOD Laundromat jobs

@author Wouter Beek
@version 2016/03-2016/04
*/

:- use_module(library(aggregate)).

:- use_module(cpack('LOD-Laundromat'/lcli)).
:- use_module(cpack('LOD-Laundromat'/lfs)).





ctuples(N) :-
  aggregate_all(sum(N), lm(_, llo:unique_tuples, ^^(N,_)), N).



lf_warm :-
  lf_warm('').


lf_warm(HashPrefix) :-
  ld_warm(HashPrefix),
  lm_warm(HashPrefix),
  lw_warm(HashPrefix).



ld_warm :-
  ld_warm('').


ld_warm(HashPrefix) :-
  ldmw_warm0(HashPrefix, data).



lm_warm :-
  lm_warm('').


lm_warm(HashPrefix) :-
  ldmw_warm0(HashPrefix, meta).



lw_warm :-
  lw_warm('').


lw_warm(HashPrefix) :-
  ldmw_warm0(HashPrefix, warn).





% HELPERS %

ldmw_warm0(HashPrefix, Name) :-
  ldir(HashPrefix, Dir),
  ldir_lhash(Dir, Hash),
  lhdt_build(Hash, Name).
