:- module(overview_endpoint, []).

/** <module> LOD Laundromat: Overview page

@author Wouter Beek
@version 2016/02-2016/03, 2016/08-2016/10
*/

:- use_module(library(html/html_ext)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/js_write)).

:- use_module(q(endpoint/about_endpoint)).
:- use_module(q(endpoint/basket_endpoint)).
:- use_module(q(endpoint/wardrobe_endpoint)).
:- use_module(q(html/llw_html)).

:- http_handler(llw(overview), overview_handler,  [prefix,priority(1)]).





overview_handler(_) :-
  reply_html_page(
    llw([]),
    \q_title(["Overview"]),
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
  llw_image_content(
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
  llw_content_image(
    [
      h1(["LOD Laundry Basket", \endpoint_link(basket_handler)]),
      p("The LOD Laundry Basket contains the URLs of dirty datasets
         that are waiting to be cleaned by the LOD Washing Machine.
         You can add URLs to the LOD Laundry Basket for cleaning.")
    ],
    laundry_basket
  ).

wardrobe_intro -->
  llw_image_content(
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
  llw_content_image(
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
%  llw_image_content(labels, [
%    h1(["SPARQL endpoint", \endpoint_link(metadata_handler)]),
%    p("For an in-depth overview of the data cleaned by the LOD Laundromat, we provide a live SPARQL endpoint in which all metadata can be queried.")
%  ], false).

about_intro -->
  llw_image_content(
    laundry_line,
    [
      h1(["About", \endpoint_link(about_handler)]),
      p("This is great!  I think...  But what do you exactly do?  How
         can I use it?")
    ]
  ).
