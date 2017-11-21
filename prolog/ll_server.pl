:- module(ll_server, []).

:- use_module(library(html/html_ext)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_server)).
:- use_module(library(rocks_ext)).
:- use_module(library(settings)).

:- use_module(ll_cloud).
:- use_module(ll_seedlist).

:- html_meta
   content_image(html, +, ?, ?),
   image_content(+, html, ?, ?).

html:author("Wouter Beek").
html:author("Laurens Rietveld").
html:author("Jan Wielemaker").
html:author("Filip Ilievski").
html:author("Stefan Schlobach").
html:author("Frank van Harmelen").

:- http_handler(/, ll_handler, [methods([get,head,options])]).

:- set_setting(html:google_analytics_id, 'UA-51130014-1').

% /
ll_handler(Request) :-
  rest_method(Request, ll_method).

% /: GET,HEAD
ll_method(Method, MediaTypes) :-
  http_is_get(Method),
  rest_media_type(MediaTypes, ll_media_type).

% /: GET,HEAD: text/html
ll_media_type(media(text/html,_)) :-
  reply_html_page(
    ll(_,["Overview"]),
    [],
    [
      \laundromat_intro%,
      %\basket_intro,
      %\wardrobe_intro,
      %\analytics_intro,
      %\metadata_intro,
      %\about_intro
    ]
  ).

laundromat_intro -->
  image_content(
    logo,
    [
      h1("LOD Laundromat 2"),
      p("The LOD Laundromat provides access to all Linked Open Data (LOD) in the world.  It does so by crawling the LOD Cloud, and converting all its contents in a standards-compliant way (gzipped N-Quads), removing all data stains such as syntax errors, duplicate occurrences, and file-local blank nodes.")
    ]
  ).

basket_intro -->
  {http_link_to_id(basket_handler, Uri)},
  content_image(
    [
      h1(["LOD Laundry Basket",a(href=Uri,code(Uri))]),
      p("The LOD Laundry Basket contains the URLs of dirty datasets that are waiting to be cleaned by the LOD Washing Machine.  You can add URLs to the LOD Laundry Basket for cleaning.")
    ],
    laundry_basket
  ).

wardrobe_intro -->
  {http_link_to_id(wardrobe_handler, Uri)},
  image_content(
    wardrobe,
    [
      h1(["Wardrobe",a(href=Uri,code(Uri))]),
      p("The LOD Wardrobe is where the cleaned data is stored.  You can download both the cleaned and the original version.  Each dataset contains a metadata describing the crawling and cleaning process.  This enumerates all the stains that were detected, including wrong HTTP headers, syntax errors, etc.")
    ]
  ).

analytics_intro -->
  {http_link_to_id(analytics_handler, Uri)},
  content_image(
    [
      h1(["Analytics",a(href=Uri,code(Uri))]),
      p([
        "How much data did we clean?  How many ",
        del("socks"),
        " triples did we lose in the LOD Washing Machine?  Which RDF serialization formats did we come across?  This is where we show such LOD Analytics."
      ])
    ],
    analytics
  ).

metadata_intro -->
  {http_link_to_id(metadata_handler, Uri)},
  image_content(
    labels,
    [
      h1(["SPARQL endpoint",a(href=Uri,code(Uri))]),
      p("For an in-depth overview of the data cleaned by the LOD Laundromat, we provide a live SPARQL endpoint in which all metadata can be queried.")
    ]
  ).

about_intro -->
  {http_link_to_id(about_handler, Uri)},
  image_content(
    laundry_line,
    [
      h1(["About",a(href=Uri,code(Uri))]),
      p("This is great!  I think...  But what do you exactly do?  How can I use it?")
    ]
  ).





% HELPERS %

%! content_image(:Content_0, +Img:atom)// is det.

content_image(Content_0, Img) -->
  {maplist(html_ext:widths0, [9,3], [ClassesA,ClassesB])},
  html(
    div([class=[container,'img-container']],
      div(class=row, [
        div(class=ClassesA, Content_0),
        div(class=ClassesB, \image(Img))
      ])
    )
  ).


image(Name) -->
  {
    image_resource(Name, Alt, Ext),
    file_name_extension(Name, Ext, Local),
    http_absolute_location(img(Local), Path)
  },
  html(img([alt=Alt,src=Path], [])).



%! image_content(+Img:atom, :Content_0)// is det.

image_content(Img, Content_0) -->
  {maplist(html_ext:widths0, [3,9], [ClassesA,ClassesB])},
  html(
    div(class=[container,'img-container'],
      div(class=row, [
        div(class=ClassesA, \image(Img)),
        div([class=ClassesB,style='vertical-align: middle;'], Content_0)
      ])
    )
  ).





% CONTENT %

image_resource(analytics, "Various diagrams that show analytics", svg).
image_resource(filip_ilievski, "Picture of Filip Ilievski", jpg).
image_resource(frank, "The logo of the Frank command-line tool", svg).
image_resource(hdt, "The Header Dictionary Triples (HDT) logo", png).
image_resource(javier_fernandez, "Picture of Javier Fernandez", jpg).
image_resource(labels, "Various icons familiar from descriptions of how to wash clothes", svg).
image_resource(laundry_basket, "The LOD Laundry Basket logo", svg).
image_resource(laundry_line, "Picture of a laundry line", svg).
image_resource(logo, "The LOD Laundromat logo", svg).
image_resource(lodlab, "The LOD Lab logo", png).
image_resource(lotus, "The LOTUS logo", svg).
image_resource(paul_groth, "Picture of Paul Groth", jpg).
image_resource(rdf_w3c, "RDF W3C logo", svg).
image_resource(wardrobe, "The LOD Wardrobe logo", svg).
image_resource(wouter_beek, "Picture of Wouter Beek", jpg).





% HTML STYLE %

user:head(ll(Page,Subtitles), Content_0) -->
  {atomics_to_string(["LOD Laundromat"|Subtitles], " ― ", Title)},
  html(
    head([
      \html_root_attribute(lang, en),
      meta(charset='utf-8', []),
      \meta_ie_latest,
      \meta_viewport,
      \meta_authors,
      \favicon,
      \google_analytics,
      \html_if_then(ground(Page), html_pagination_links(Page)),
      title(Title),
      \html_requires(html_ext)
    | Content_0
    ])
  ).

user:body(ll(_,_), Content_0) -->
  html(body([\navbar("LOD Laundromat", \menu, \counter)|Content_0])).

counter -->
  {
    number_of_datasets(NumDatasets),
    number_of_triples(NumTriples)
  },
  html(
    p(class='navbar-text', [
      "(",
      \html_thousands(NumDatasets),
      " datasets with ",
      \html_thousands(NumTriples),
      " triples)"
    ])
  ).
