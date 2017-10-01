:- module(ll, []).
:- reexport(library(ll/ll_analysis)).
:- reexport(library(ll/ll_download)).
:- reexport(library(ll/ll_guess)).
%:- reexport(library(ll/ll_index)).
:- reexport(library(ll/ll_parse)).
:- reexport(library(ll/ll_seedlist)).
:- reexport(library(ll/ll_show)).
:- reexport(library(ll/ll_unarchive)).

/** <module> LOD Laundromat

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- reexport(library(ll/ll_generics)).

:- debug(ll).

:- initialization
   %(debugging(ll) -> tmon ; true),
   maplist(call_loop, [ll_download,ll_unarchive,ll_guess,ll_parse]).
