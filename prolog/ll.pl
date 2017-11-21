:- module(ll, []).

/** <module> LOD Laundromat

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(apply)).
:- use_module(library(debug)).

:- use_module(ll_analysis).
:- use_module(ll_cloud).
:- use_module(ll_download).
:- use_module(ll_generics).
:- use_module(ll_guess).
:- use_module(ll_parse).
:- use_module(ll_seedlist).
:- use_module(ll_show).
:- use_module(ll_unarchive).

:- debug(ll).

:- initialization
   %(debugging(ll) -> tmon ; true),
   maplist(call_loop, [ll_download,ll_unarchive,ll_guess,ll_parse,ll_store]).
