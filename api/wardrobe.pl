:- module(wardrobe, []).

/** <module> LOD Laundromat: Wardrobe API

The page where cleaned data documents are displayed.

@author Wouter Beek
@version 2016/09-2016/12
*/

:- use_module(library(apply)).
:- use_module(library(dict_ext)).
:- use_module(library(html/html_date_time)).
:- use_module(library(html/html_doc)).
:- use_module(library(html/html_ext)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/http_parameters)).
:- use_module(library(http/rest)).
:- use_module(library(pagination)).
:- use_module(library(q/q_fs)).
:- use_module(library(q/q_io)).
:- use_module(library(q/q_rdf)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(service/es_api)).
:- use_module(library(settings)).

:- use_module(cp(http_param)).
:- use_module(cp(style/cp_style)).

:- use_module(ll(api/seedlist)).
:- use_module(ll(style/ll_style)).

:- http_handler(ll(wardrobe), wardrobe_handler, [prefix]).

:- multifile
    http_param/1,
    media_type/1.

http_param(page).
http_param(page_size).
http_param(pattern).

media_type(text/html).

:- setting(
     default_page_size,
     positive_integer,
     5,
     "The default number of documents that is retreived in one request."
   ).
:- setting(
     max_page_size,
     positive_integer,
     10,
     "The maximum number of documents that can be retrieved in one request."
   ).





wardrobe_handler(Req) :-
  rest_method(Req, [get], wardrobe_method).


wardrobe_method(Req, Method, MTs) :-
  http_is_get(Method),
  http_parameters(
    Req,
    [page(Page),page_size(PageSize),pattern(Pattern)],
    [attribute_declarations(http_param(wardrobe))]
  ),
  http_location_iri(Req, Iri),
  include(ground, [pattern(Pattern)], Query),
  PageOpts = _{
    iri: Iri,
    page: Page,
    page_size: PageSize,
    pattern: Pattern,
    query: Query
  },
  Filter = _{range: _{ended: _{gt: 0.0}}},
  (   var(Pattern)
  ->  Must = _{}
  ;   atomics_to_string(["*",Pattern,"*"], Wildcard),
      Must = _{wildcard: _{from: _{value: Wildcard}}}
  ),
  once(
    es_search(
      [ll,seedlist],
      _{
        query: _{bool: _{filter: Filter, must: Must}},
        sort: [_{number_of_tuples: _{order: "desc"}}]
      },
      PageOpts,
      Result
    )
  ),
  rest_media_type(Req, Method, MTs, wardrobe_media_type(Result)).


wardrobe_media_type(Result, Method, text/html) :-
  reply_html_page(
    Method,
    ll([]),
    [
      \pagination_links(Result),
      \cp_title(["Wardrobe","Overview"])
    ],
    [
      \wardrobe_header,
      \search_box(
        [autocomplete=off,class='search-box'],
        link_to_id(wardrobe_handler)
      ),
      \pagination_result(Result, documents)
    ]
  ).


wardrobe_header -->
  html(
    header(
      \ll_image_content(
        wardrobe,
        [
          h1("Wardrobe"),
          p("This is where the cleaned data is stored.  You can
          download both clean and dirty (i.e. the original) data.
          Each data document contains a meta-data description that
          includes all the stains that were detected.  The LOD Laundry
          Basket contains the URLs of dirty datasets that are waiting
          to be cleaned by the LOD Laundromat.  You can also add your
          own URLs to the basket (see below).")
        ]
      )
    )
  ).


documents(Dicts) -->
  grid(700, 650, document, Dicts).

document(Dict) -->
  {
    dict_tag(Dict, Hash),
    q_graph_hash(DataG, data, Hash),
    q_graph_hash(MetaG, meta, Hash),
    http_link_to_id(sgp_handler, [graph=DataG], DataIri),
    http_link_to_id(sgp_handler, [graph=MetaG], MetaIri),
    q(hdt, _, nsdef:numberOfTuples, NumTuples^^xsd:nonNegativeInteger, MetaG),
    q(hdt, _, nsdef:numberOfWarnings, NumWarnings^^xsd:nonNegativeInteger, MetaG)
  },
  row_4(
    9,
    [
      div([
        \internal_link(DataIri, Dict.from),
        " ",
        \external_link_icon(Dict.from)
      ]),
      div([
        "Added on ",
        \html_date_time(Dict.ended, _{masks: [second,offset]}),
        \pipe,
        \html_thousands(NumTuples),
        " statements",
        \pipe,
        \html_thousands(NumWarnings),
        " warnings"
      ])
    ],
    1,
    div(\link_button(DataIri, "Data")),
    1,
    div(\link_button(MetaIri, "Meta")),
    1,
    []
  ).
