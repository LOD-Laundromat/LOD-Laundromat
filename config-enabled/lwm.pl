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



% LOD-Washing-Machingif_to_gv_filee

:- http_handler(cliopatria(lwm), lwm, [prefix]).

:- use_module(lwm(lwm)).
lwm(Request):-
  lwm(Request, lwm).

:- multifile(user:body//2).
user:body(lwm, Body) -->
  html_requires(css('plServer.css')),
  user:body(cliopatria(default), Body).



% Export LOD Laundromat schema.

:- use_module(library(apply)).
:- use_module(os(pdf)).
:- use_module(plRdf(rdf_export)).
:- use_module(plGraphViz(gv_file)).
:- use_module(lwm(ll_schema)).
export_ll_schema:-
  Graph = ll,
  assert_ll_schema(Graph),
  
  % GIF
  maplist(
    rdf_register_prefix_color(Graph),
    [dcat, ll,   rdf,rdfs],
    [black,green,red,blue]
  ),
  rdf_graph_to_gif(
    Graph,
    Gif,
    [
      directed(true),
      iri_description(with_all_literals),
      language_preferences([nl,en])
    ]
  ),
  
  % DOT
  absolute_file_name(data(ll), DotFile, [access(write),file_type(dot)]),
  gif_to_gv_file(Gif, DotFile, [method(sfdp),to_file_type(dot)]),
  
  % PDF
  absolute_file_name(data(ll), PdfFile, [access(write),file_type(pdf)]),
  gif_to_gv_file(Gif, PdfFile, [method(sfdp),to_file_type(pdf)]),
  open_pdf(PdfFile).

