:- module(lodlab, []).

/** <module> LOD Laundromat: LOD Lab page

@author Wouter Beek
@version 2016/02-2016/03, 2016/08-2016/10, 2016/12
*/

:- use_module(library(html/html_ext)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/rest)).
:- use_module(library(rdfa/rdfa_ext)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(settings)).

:- use_module(ll(style/ll_style)).

:- http_handler(ll(lodlab), lodlab_handler, [prefix]).

:- setting(
     backend,
     oneof([hdt,trp]),
     hdt,
     "The default backend that powers the LOD Lab page."
   ).





lodlab_handler(Req) :-
  setting(backend, M),
  rest_method(Req, [get], lodlab_handler(M)).


lodlab_handler(M, Req, Method, MTs) :-
  http_is_get(Method),
  rest_media_type(Method, MTs, lodlab_media_type(M)).


lodlab_media_type(M, Method, text/html) :-
  rdf_default_graph(G),
  reply_html_page(
    Method,
    ll(false),
    [
      \meta_description("SPARQL endpoint of the LOD Laundromat"),
      \q_title(["LOD Lab"])
    ],
    article([
      \lodlab_header,
      \lodlab_desc,
      \lodlab_program,
      \lodlab_technologies,
      \lodlab_organizers(M, G)
    ])
  ).

lodlab_header -->
  html(
    header([
      \ll_image(lodlab),
      h1("ESWC Tutorial"),
      p("Learn how to scale your Linked Data evaluations and applications to hundreds of thousands of datasets.")
    ])
  ).

lodlab_desc -->
  html(
    section([
      h2("Overview"),
      p([
        "This tutorial will focus on obtaining hands-on experience with LOD Lab: a new evaluation paradigm that makes algorithmic evaluation against hundreds of thousands of datasets the new norm.  The LOD Lab approach builds on the award-winning ",
        \ll_link(lod_laundromat),
        " architecture, ",
        \ll_link(hdt),
        " technology and ",
        \ll_link(lotus),
        " text index.  The intended audience for this tutorial includes all Semantic Web practitioners that need to run evaluations on Linked Open Data or that would otherwise benefit from being able to easily process large volumes of Linked Data."
      ])
    ])
  ).

lodlab_program -->
  html(
    section([
      h2("Program"),
      dl(class='dl-horizontal', [
        dt("Introduction"),
        dd("The importance of LOD evaluations."),
        dt(\ll_link(lotus)),
        dd("From natural language text to resources."),
        dt("Index"),
        dd("From resource to all documents about that resource."),
        dt([
          \ll_link(hdt),
          "+",
          \ll_link(ldf),
          "+",
          \ll_link(frank)
        ]),
        dd("From a resource to all triples about that resource."),
        dt("Metadata"),
        dd("Filter data with specific (graph) properties."),
        dt("Discussion"),
        dd("The future of (tooling for) LOD evaluations.")
      ])
    ])
  ).

lodlab_technologies -->
  {findall(T, technology(T), L)},
  html(
    section([
      h2("Covered technologies"),
      \deck(technology, L)
    ])
  ).

deck(Card_3, L) -->
  html(ul(class=deck, \html_maplist(Card_3, L))).

technology(technology(Img,Link,Desc)) :-
  technology0(Img, Link, Desc).
technology(technology(X,X,Desc)) :-
  technology0(X, Desc).

technology0(lod_laundromat, "Cleaning and republishing service for Linked Open Data.").
technology0(frank, "Command-line interface to the LOD Cloud.").
technology0(lotus, "Full text index over the LOD Cloud.").

technology0(hdt, header_dictionary_triples, "Queryable compression format for Linked Data.").

technology(technology(Img, Link, Desc)) -->
  card(\ll_image(Img), [h2(\ll_link(Link)),p(Desc)]).

lodlab_organizers(M, G) -->
  html(
    section([
      h2("Organizers"),
      ul(class=deck, [
        \wouter_beek(M, G),
        \javier_fernandez(M, G),
        \paul_groth(M, G),
        \filip_ilievski(M, G)
      ])
    ])
  ).

wouter_beek(M, G) -->
  {rdf_equal(llr:wouter_beek, Res)},
  card(
    \ll_image(wouter_beek),
    [
      h1(\agent_name(M, Res, G)),
      p(["Developer of the ",\ll_link(lod_laundromat)]),
      \'org:memberOf'(M, Res, G), %'
      div(\'foaf:mbox'(M, Res, G)), %'
      div(\'foaf:homepage'(M, Res, G)) %'
    ]
  ).

javier_fernandez(M, G) -->
  {rdf_equal(llr:javier_fernandez, Res)},
  card(
    \ll_image(javier_fernandez),
    [
      h1(\agent_name(M, Res, G)),
      p(["Developer of ",\ll_link(header_dictionary_triples)]),
      \'org:memberOf'(M, Res, G), %'
      div(\'foaf:mbox'(M, Res, G)), %'
      div(\'foaf:homepage'(M, Res, G)) %'
    ]
  ).

paul_groth(M, G) -->
  {rdf_equal(llr:paul_groth, Res)},
  card(
    \ll_image(paul_groth),
    [
      h1(\agent_name(M, Res, G)),
      \'org:memberOf'(M, Res, G), %'
      div(\'foaf:mbox'(M, Res, G)), %'
      div(\'foaf:homepage'(M, Res, G)) %'
    ]
  ).

filip_ilievski(M, G) -->
  {rdf_equal(llr:filip_ilievski, Res)},
  card(
    \ll_image(filip_ilievski),
    [
      h1(\agent_name(M, Res, G)),
      p(["Developer of ",\ll_link(lotus)]),
      \'org:memberOf'(M, Res, G), %'
      div(\'foaf:mbox'(M, Res, G)), %'
      div(\'foaf:homepage'(M, Res, G)) %'
    ]
  ).
