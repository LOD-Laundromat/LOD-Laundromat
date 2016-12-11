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
:- use_module(library(q/q_print)).
:- use_module(library(q/q_rdf)).
:- use_module(library(q/q_rdfs)).
:- use_module(library(q/q_term)).
:- use_module(library(settings)).
:- use_module(library(yall)).

:- use_module(cp(http_param)).

:- use_module(ll(style/ll_style)).

:- http_handler(ll(data), data_handler, [prefix]).
:- http_handler(ll(meta), meta_handler, [prefix]).

:- multifile
    http_param/1,
    media_type/1.

http_param(hash).
http_param(object).
http_param(page).
http_param(page_size).
http_param(predicate).
http_param(subject).

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
     50,
     "The maximum number of tuples that can be retrieved in one request."
   ).




data_handler(Req) :-
  handler0(data, Req).


meta_handler(Req) :-
  handler0(meta, Req).


handler0(Mode, Req) :-
  rest_method(Req, [get], method0(Mode)).


method0(Mode, Req, Method, MTs) :-
  http_is_get(Method),
  http_parameters(
    Req,
    [
      hash(Hash, [atom,optional(true)]),
      object(O0),
      page(Page),
      page_size(PageSize),
      predicate(P0),
      subject(S0)
    ],
    [attribute_declarations(http_param(meta))]
  ),
  maplist(q_term_expansion, [O0,P0,S0], [O,P,S]),
  http_location_iri(Req, Iri),
  include(ground, [hash(Hash),object(O),predicate(P),subject(S)], Query0),
  maplist(q_query_term, Query0, Query),
  PageOpts = _{iri: Iri, page: Page, page_size: PageSize, query: Query},
  pagination(
    rdf(S,P,O,G),
    (
      q_hash(Hash),
      q_graph_hash(G, Mode, Hash),
      q(hdt, S, P, O, G)
    ),
    PageOpts,
    Result
  ),
  rest_media_type(Req, Method, MTs, media_type0(Mode, Result)).


media_type0(Mode, Result, Method, text/html) :-
  mode_label(Mode, Lbl),
  reply_html_page(
    Method,
    cp([]),
    \cp_title([Lbl,"browser"]),
    \pagination_result(Result, {Mode}/[Quads]>>qh_quad_table(Mode, Quads))
  ).

qh_quad_table(Mode, Quads) -->
  {HeaderRow = ["Subject","Predicate","Object","Graph"]},
  table(
    \table_header_row(HeaderRow),
    \html_maplist({Mode}/[Quad]>>qh_quad_row0(Mode, Quad), Quads)
  ).

qh_quad_row0(Mode, rdf(S,P,O,G)) -->
  {
    mode_handle(Mode, HandleId),
    Opts = _{max_iri_length: 25, max_lit_len: 25}
  },
  html(
    tr([
      td(class='col-md-3', \internal_link(link_to_id(HandleId,[subject(S)]),\qh_subject(S, Opts))),
      td(class='col-md-3', \internal_link(link_to_id(HandleId,[predicate(P)]),\qh_predicate(P, Opts))),
      td(class='col-md-3', \internal_link(link_to_id(HandleId,[object(O)]),\qh_object(O, Opts))),
      td(class='col-md-3', \graph_cell(HandleId, G))
    ])
  ).

graph_cell(HandleId, G) -->
  {q_graph_hash(G, Hash)},
  internal_link(link_to_id(HandleId,[hash(Hash)]),\qh_graph_term(G)).

mode_handle(data, data_handler).
mode_handle(meta, meta_handler).

mode_label(data, "Data").
mode_label(meta, "Meta").

/*
meta_media_type(G, Method, text/html) :-
  once(q_container(hdt, _, Path, G)),
  reply_html_page(
    Method,
    ll([]),
    \cp_title_call(("Metadata browser",dcg_q_print_graph_term(G))),
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

meta_cell(G) -->
  {http_link_to_id(meta_handler, [graph(G)], Link)},
  html(td(a(href=Link, \qh_graph_term(G)))).
*/
