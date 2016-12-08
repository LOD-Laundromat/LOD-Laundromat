:- module(meta, []).

/** <module> LOD Laundromat: Metadata API

@author Wouter Beek
@version 2016/09-2016/10, 2016/12
*/

:- use_module(library(apply)).
:- use_module(library(html/html_doc)).
:- use_module(library(html/html_ext)).
:- use_module(library(html/qh)).
:- use_module(library(html/qh_ui)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_parameters)).
:- use_module(library(http/rest)).
:- use_module(library(json_ext)).
:- use_module(library(pagination)).
:- use_module(library(q/q_container)).
:- use_module(library(q/q_fs)).
:- use_module(library(q/q_rdf)).
:- use_module(library(q/q_rdfs)).
:- use_module(library(q/q_term)).
:- use_module(library(settings)).

:- use_module(ll(style/ll_style)).

:- http_handler(ll(meta), meta_handler, [prefix]).

:- multifile
    http_param/1,
    media_type/1.

http_param(page).
http_param(page_size).

media_type(application/json).
media_type(text/html).

:- setting(
     default_page_size,
     positive_integer,
     15,
     "The default number of tuples that is retreived in one request."
   ).
:- setting(
     max_page_size,
     positive_integer,
     100,
     "The maximum number of tuples that can be retrieved in one request."
   ).





meta_handler(Req) :-
  rest_method(
    Req,
    [get],
    meta_plural_method,
    meta_handler,
    meta_singular_method
  ).


meta_plural_method(Req, Method, MTs) :-
  http_is_get(Method),
  http_parameters(
    Req,
    [page(Page),page_size(PageSize)],
    [attribute_declarations(http_param(meta_endpoint))]
  ),
  http_location_iri(Req, Iri),
  PageOpts = _{iri: Iri, page: Page, page_size: PageSize},
  pagination([Hash], q_hash(Hash), PageOpts, Result),  
  rest_media_type(Req, Method, MTs, meta_plural_media_type(Result)).


meta_singular_method(Res, Req, Method, MTs) :-
  rdf_global_id(meta:Hash, Res),
  (   q_graph(Hash, meta, G)
  ->  rest_media_type(Req, Method, MTs, meta_singular_media_type(G, Hash))
  ;   rest_exception(MTs, 404)
  ).


meta_plural_media_type(Result, Method, text/html) :-
  reply_html_page(
    Method,
    ll([]),
    \q_title(["Metadata browser"]),
    \pagination_result(Result, meta_table)
  ).
meta_plural_media_type(Result, Method, MT) :-
  MT = application/json,
  rest_reply(
    Method,
    reply_content_type(MT),
    json_write_dict(Result.results)
  ).


meta_singular_media_type(G, Hash, Method, text/html) :-
  once(q_container(hdt, G, Path, G)),
  reply_html_page(
    Method,
    ll([]),
    \q_title(["Metadata browser",Hash]),
    \panels([\overall_panel(G),\entry_panels(G, Path)])
  ).

overall_panel(G) -->
  {
    q(hdt, _, nsdef:numberOfWarnings, NumWarns^^xsd:nonNegativeInteger, G),
    (NumWarns =:= 0 -> Alert = success ; Alert = danger)
  },
  panel(
    true,
    0,
    "Overall",
    html([
      div([
        \ll_badge(success, nsdef:numberOfTuples, G),
        " composed of ",
        \ll_badge(success, nsdef:numberOfTriples, G),
        " and ",
        \ll_badge(success, nsdef:numberOfQuads, G)
      ]),
      \ll_badge(Alert, nsdef:numberOfWarnings, G)
    ])
  ).

ll_badge(Alert, P0, G) -->
  {
    atomic_list_concat([alert,Alert], -, Class),
    % @tbd RDF prefix expansion does not work.
    rdf_global_id(P0, P),
    once(q_pref_label_lex(trp, P, Lbl)),
    once(q(hdt, _, P, Lit, G))
  },
  html(
    button([
      class=[alert,Class,btn,'btn-primary'],
      role=alert,
      type=button
    ], [
      span(class=badge, \qh_literal(Lit)),
      " ",
      Lbl
    ])
  ).

entry_panels(G, Path) -->
  entry_panels(G, 1, Path).

entry_panels(_, _, []) --> !, [].
entry_panels(G, N1, [H|T]) -->
  {format(string(Lbl), "Entry ~D", [N1])},
  panel(N1, Lbl, \entry_panel(G, H)),
  {N2 is N1 + 1},
  entry_panels(G, N2, T).

entry_panel(G, Entry) -->
  qh_describe(hdt, Entry, G).

meta_table([])   --> !, [].
meta_table(Rows) --> table(tr(th("Hash")), \html_maplist(meta_row, Rows)).

meta_row(Cells) --> html(tr(\html_maplist(meta_cell, Cells))).

meta_cell(Hash) -->
  {http_link_to_id(meta_handler, path_postfix(Hash), Iri)},
  html(td(\internal_link(Iri, Hash))).
