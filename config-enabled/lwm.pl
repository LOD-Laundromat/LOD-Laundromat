:- module(conf_lwm, []).

:- use_module(library(http/html_head)).
:- use_module(library(http/http_dispatch)).

:- use_module(cliopatria(hooks)).
   cliopatria:menu_item(500=places/lwm, 'LOD Washing Machine').
   cliopatria:menu_item(600=places/plTabular, plTabular).

:- if(\+ current_module(load_project)).
  :- ensure_loaded('../debug').
  :- ensure_loaded('../load').
:- endif.

:- http_handler(cliopatria(basket), lwm_basket, []).

:- ensure_loaded(plServer(style)).


% plTabular

:- http_handler(cliopatria(plTabular), rdf_tabular, [id(plTabular)]).

:- use_module(plTabular(rdf_tabular)).
rdf_tabular(Request):-
  rdf_tabular(Request, plTabular).

:- multifile(user:body//2).
user:body(plTabular, Body) -->
  html_requires(css('plServer.css')),
  user:body(cliopatria(default), Body).



% LOD InfoBox

:- http_handler(cliopatria(infobox), ll_infobox, [prefix]).

:- use_module(lwm(ll_infobox)).



% LOD-Washing-Machine

:- http_handler(cliopatria(lwm), lwm, [prefix]).

:- use_module(lwm(lwm)).
lwm(Request):-
  lwm(Request, lwm).

:- multifile(user:body//2).
user:body(lwm, Body) -->
  html_requires(css('plServer.css')),
  user:body(cliopatria(default), Body).

