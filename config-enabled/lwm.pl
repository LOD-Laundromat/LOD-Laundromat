:- module(conf_lwm, []).

:- use_module(library(ansi_term)).
:- use_module(library(portray_text)).
:- portray_text(true).
:- set_prolog_flag(backquoted_string, true).
:- set_prolog_flag(
    toplevel_print_options,
    [backquoted_string(true),
     max_depth(9999),
     portray(true),
     spacing(next_argument)]
  ).
:- set_prolog_flag(
    debugger_print_options,
    [backquoted_string(true),
     max_depth(9999),
     portray(true),
     spacing(next_argument)]
  ).

:- use_module(library(debug)).
:- use_module(library(swi_ide)).
:- debug(sparql_api).
:- prolog_ide(debug_monitor).

:- use_module(library(http/html_head)).
:- use_module(library(http/http_dispatch)).


:- use_module(cliopatria(hooks)).

cliopatria:menu_item(500=places/lwm, 'LOD Washing Machine').
cliopatria:menu_item(600=places/plTabular, plTabular).


:- ensure_loaded('../load').


:- http_handler(cliopatria(plTabular), rdf_tabular, [id(plTabular)]).
:- http_handler(cliopatria(lwm), lwm, []).
:- http_handler(cliopatria(lwm/basket), lwm_basket, []).


:- dynamic(user:file_search_path/2).
:- multifile(user:file_search_path/2).
   user:file_search_path(css, lwm('web/css')).



% PL_TABULAR

:- use_module(plTabular(rdf_tabular)).
rdf_tabular(Request):-
  rdf_tabular(Request, plTabular).

:- html_resource(plTabular, [requires([css('plTabular.css')]),virtual(true)]).

:- multifile(user:body//2).
user:body(plTabular, Body) -->
  html_requires(plTabular),
  user:body(cliopatria(default), Body).



% LOD-WASHING-MACHINE

:- use_module(lwm(lwm)).
lwm(Request):-
  lwm(Request, lwm).

:- multifile(user:body//2).
user:body(lwm, Body) -->
  html_requires(plTabular),
  user:body(cliopatria(default), Body).

:- use_module(lwm(lwm_start)).
:- lwm_start:init_washing_machine.

