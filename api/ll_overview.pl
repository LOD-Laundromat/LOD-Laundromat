:- module(ll_overview, []).

/** <module> LOD Laundromat: Overview page

@author Wouter Beek
@version 2016/02-2016/03, 2016/08-2016/10, 2016/12
*/

:- use_module(library(html/html_ext)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/js_write)).
:- use_module(library(http/rest)).

:- use_module(ll(api/basket)).
:- use_module(ll(api/ll_about)).
:- use_module(ll(api/wardrobe)).
:- use_module(ll(style/ll_style)).

:- http_handler(ll(overview), ll_overview_handler,  [methods([get]),prefix,priority(1)]).





ll_overview_handler(Req) :-
  rest_method(Req, ll_overview_handler).


ll_overview_handler(get, MTs) :-
  rest_media_type(MTs, ll_overview_get).


ll_overview_get(text/html) :-
  reply_html_page(
    ll([]),
    \cp_title(["Overview"]),
    [
      \laundromat_intro,
      \basket_intro,
      \wardrobe_intro,
      \analytics_intro,
      %\metadata_intro,
      \about_intro,
      \js_script({|javascript(_)||
$(".devLink").click(function() { window.location = "/about"; });
$(".wardrobeLink").click(function() { window.location = "/wardrobe"; });
$(".laundryBasketLink").click(function() { window.location = "/basket"; });
$(".analysisLink").click(function() { window.location = "/visualizations"; });
$(".labelsLink").click(function() { window.location = "/sparql"; });
      |})
    ]
  ).

laundromat_intro -->
  ll_image_content(
    logo,
    [
      h1("LOD Laundromat 2"),
      p("The LOD Laundromat provides access to all Linked Open Data
        (LOD) in the world.  It does so by crawling the LOD Cloud, and
        converting all its contents in a standards-compliant way
        (gzipped N-Quads), removing all data stains such as syntax
        errors, duplicate occurrences, and file-local blank nodes.")
    ]
  ).

basket_intro -->
  ll_content_image(
    [
      h1(["LOD Laundry Basket", \endpoint_link(basket_handler)]),
      p("The LOD Laundry Basket contains the URLs of dirty datasets
         that are waiting to be cleaned by the LOD Washing Machine.
         You can add URLs to the LOD Laundry Basket for cleaning.")
    ],
    laundry_basket
  ).

wardrobe_intro -->
  ll_image_content(
    wardrobe,
    [
      h1(["Wardrobe", \endpoint_link(wardrobe_handler)]),
      p("The LOD Wardrobe is where the cleaned data is stored.  You
         can download both the cleaned and the original version.  Each
         dataset contains a metadata describing the crawling and
         cleaning process.  This enumerates all the stains that were
         detected, including wrong HTTP headers, syntax errors, etc.")
    ]
  ).

analytics_intro -->
  ll_content_image(
    [
      h1(["Analytics", \endpoint_link(wardrobe_handler)]),
      p([
        "How much data did we clean?  How many ",
        del("socks"),
        " triples did we lose in the LOD Washing Machine?  Which RDF
         serialization formats did we come across?  This is where we
         show such LOD Analytics."
      ])
    ],
    analytics
  ).

%metadata_intro -->
%  ll_image_content(labels, [
%    h1(["SPARQL endpoint", \endpoint_link(metadata_handler)]),
%    p("For an in-depth overview of the data cleaned by the LOD Laundromat, we provide a live SPARQL endpoint in which all metadata can be queried.")
%  ], false).

about_intro -->
  ll_image_content(
    laundry_line,
    [
      h1(["About", \endpoint_link(ll_about_handler)]),
      p("This is great!  I think...  But what do you exactly do?  How
         can I use it?")
    ]
  ).
