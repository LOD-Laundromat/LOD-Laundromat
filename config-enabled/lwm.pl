:- module(conf_lwm, []).

:- use_module(library(http/html_head)).
:- use_module(library(http/http_dispatch)).


:- use_module(cliopatria(hooks)).

cliopatria:menu_item(500=places/lwm, 'LOD Washing Machine').
cliopatria:menu_item(600=places/plTabular, plTabular).


:- ensure_loaded('../load').


:- http_handler(cliopatria(plTabular), rdf_tabular, []).
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

