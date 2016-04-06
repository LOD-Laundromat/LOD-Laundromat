:- module(
  ljob,
  [
    ctuples/1, % -N
    lf_warm/0,
    lf_warm/1, % ?Hash
    lf_warm/2, % ?Hash, +HashPrefix
    ld_warm/0,
    ld_warm/1, % ?Hash
    ld_warm/2, % ?Hash, +HashPrefix
    lm_warm/0,
    lm_warm/1, % ?Hash
    lm_warm/2, % ?Hash, +HashPrefix
    lw_warm/0,
    lw_warm/1, % ?Hash
    lw_warm/2  % ?Hash, +HashPrefix    
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
  lf_warm(_).


lf_warm(Hash) :-
  lf_warm(Hash, '').


lf_warm(Hash, HashPrefix) :-
  ldmw_warm0(Hash, HashPrefix, _).



ld_warm :-
  ld_warm(_).


ld_warm(Hash) :-
  ld_warm(Hash, '').


ld_warm(Hash, HashPrefix) :-
  ldmw_warm0(Hash, HashPrefix, data).



lm_warm :-
  lm_warm(_).


lm_warm(Hash) :-
  lm_warm(Hash, '').


lm_warm(Hash, HashPrefix) :-
  ldmw_warm0(Hash, HashPrefix, meta).



lw_warm :-
  lw_warm(_).


lw_warm(Hash) :-
  lw_warm(Hash, '').


lw_warm(Hash, HashPrefix) :-
  ldmw_warm0(Hash, HashPrefix, warn).





% HELPERS %

%! lmdw_warn0(?Hash, +HashPrefix, ?Name) is nondet.

ldmw_warm0(Hash, HashPrefix, Name) :-
  lready_hash(HashPrefix, Hash),
  lname(Name),
  lhdt_build(Hash, Name).
