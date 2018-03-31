:- module(ll_loop, []).

/** <module> LOD Laundromat: Main loop

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).

:- use_module(library(ll/ll_decompress)).
:- use_module(library(ll/ll_download)).
:- use_module(library(ll/ll_generics)).
%:- use_module(library(ll/ll_guess)).
%:- use_module(library(ll/ll_parse)).

:- initialization
   maplist(call_loop, [ll_decompress]).%,ll_download]).
