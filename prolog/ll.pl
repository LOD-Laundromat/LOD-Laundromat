:- module(ll, []).
:- reexport(ll_analysis).
:- reexport(ll_download).
:- reexport(ll_guess).
:- reexport(ll_index).
:- reexport(ll_parse).
:- reexport(ll_seedlist).
:- reexport(ll_show).
:- reexport(ll_unarchive).

/** <module> LOD Laundromat

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(apply)).
:- use_module(library(debug)).

:- use_module(ll_generics).

:- debug(ll).

:- initialization
   %(debugging(ll) -> tmon ; true),
   maplist(call_loop, [ll_download,ll_unarchive,ll_guess,ll_parse,ll_index]).
