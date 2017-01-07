:- module(
  ll_style,
  [
    ll_content_image//2, % +Content_0, +Img
    ll_image//1,         % +Name
    ll_image_content//2, % +Img, :Content_0
    ll_link//1           % +Name
  ]
).

/** <module> LOD Laundromat: HTML generics

@author Wouter Beek
@author Laurens Rietveld
@version 2016/02-2016/04, 2016/08-2016/10, 2016/12
*/

:- reexport(cp(style/cp_style)).

:- use_module(library(html/html_ext)).
:- use_module(library(http/html_head)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(pair_ext)).
:- use_module(library(service/rocks_api)).
:- use_module(library(settings)).

:- use_module(ll(api/ll_overview)).

:- html_meta
   ll_content_image(html, +, ?, ?),
   ll_image_content(+, html, ?, ?).

:- html_resource(
     css(ll),
     [
       ordered(true),
       requires([css('ll.css')]),
       virtual(true)
     ]
   ).

:- set_setting(html:google_analytics_id, 'UA-51130014-1').

:- discontiguous
    ll_image_alt0/2,
    ll_image_alt0/3.    

cp:head(ll(_), Content_0) -->
  html(
    head([
      \cp_head_generics,
      \meta_authors([
        "Wouter Beek",
        "Laurens Rietveld",
        "Jan Wielemaker",
        "Filip Ilievski",
        "Stefan Schlobach",
        "Frank van Harmelen"
      ]),
      \html_requires(css(ll)),
      \google_analytics
    | Content_0
    ])
  ).

cp:body(ll(_), Content_0) -->
  html(
    body([
      \cp_navbar,
      \row_1([class=main], 12, Content_0),
      \cp_footer,
      \html_post(cp_navbar_right, \(ll_style:data_counter))
    ])
  ).





ll_content_image(Content_0, Img) -->
  {maplist(widths, [9,3], [ClassesA,ClassesB])},
  html(
    div([class=[container,'img-container']],
      div(class=row, [
        div(class=ClassesA, Content_0),
        div(class=ClassesB, \ll_image(Img))
      ])
    )
  ).



ll_image(Name) -->
  {
    ll_image_alt(Name, Alt, Ext),
    file_name_extension(Name, Ext, Local),
    http_absolute_location(img(Local), Path)
  },
  html(img([alt=Alt,src=Path], [])).



ll_image_alt(Name, Alt, Ext) :-
  ll_image_alt0(Name, Alt, Ext), !.
ll_image_alt(Name, Alt, svg) :-
  ll_image_alt0(Name, Alt).


ll_image_alt0(analytics,        "Various diagrams that show analytics").
ll_image_alt0(filip_ilievski,   "Picture of Filip Ilievski", jpg).
ll_image_alt0(frank,            "The logo of the Frank command-line tool").
ll_image_alt0(hdt,              "The Header Dictionary Triples (HDT) logo", png).
ll_image_alt0(javier_fernandez, "Picture of Javier Fernandez", jpg).
ll_image_alt0(labels,           "Various icons familiar from descriptions of how to wash clothes").
ll_image_alt0(laundry_basket,   "The LOD Laundry Basket logo").
ll_image_alt0(laundry_line,     "Picture of a laundry line").
ll_image_alt0(logo,             "The LOD Laundromat logo").
ll_image_alt0(lodlab,           "The LOD Lab logo", png).
ll_image_alt0(lotus,            "The LOTUS logo").
ll_image_alt0(paul_groth,       "Picture of Paul Groth", jpg).
ll_image_alt0(rdf_w3c,          "RDF W3C logo").
ll_image_alt0(wardrobe,         "The LOD Wardrobe logo").
ll_image_alt0(wouter_beek,      "Picture of Wouter Beek", jpg).



ll_image_content(Img, Content_0) -->
  {maplist(widths, [3,9], [ClassesA,ClassesB])},
  html(
    div(class=[container,'img-container'],
      div(class=row, [
        div(class=ClassesA, \ll_image(Img)),
        div([class=ClassesB,style='vertical-align: middle;'], Content_0)
      ])
    )
  ).



ll_link(Name) -->
  {ll_link0(Name, Lbl)}, !,
  internal_link(root(Name), Lbl).
ll_link(Name) -->
  {ll_link0(Name, Lbl, Iri)},
  external_link(Iri, Lbl).


ll_link0(about,          "about page").
ll_link0(lod_laundromat, "LOD Laundromat").
ll_link0(lodlab,         "LOD Lab").
ll_link0(lotus,          "LOTUS").
ll_link0(sparql,         "SPARQL endpoint").
ll_link0(wardrobe,       "LOD Wardrobe").

ll_link0(frank,       "Frank",       'https://github.com/LOD-Laundromat/Frank.git').
ll_link0(hdt,         "HDT",         'http://www.rdfhdt.org').
ll_link0(header_dictionary_triples, "Header Dictionary Triples", 'http://www.rdfhdt.org').
ll_link0(java,        "Java",        'https://github.com/LOD-Laundromat/GettingStartedJava.git').
ll_link0(ldf,         "LDF",         'https://github.com/LinkedDataFragments/Server.js').
ll_link0(javascript,  "JavaScript",  'https://github.com/LOD-Laundromat/GettingStarted.git').
ll_link0(python,      "Python",      'https://github.com/LOD-Laundromat/GettingStarted.git').
ll_link0(nodejs,      "Node.js",     'https://github.com/LOD-Laundromat/GettingStarted.git').
ll_link0(prov,        "PROV",        'https://www.w3.org/TR/prov-overview/').
ll_link0(swipl,       "SWI-Prolog",  'https://github.com/wouterbeek/LOD-Laundromat-API.git').
ll_link0(void,        "VoID",        'https://www.w3.org/TR/void/').
ll_link0(wouter_beek, "Wouter Beek", 'http://www.wouterbeek.com').



data_counter -->
  {
    rocks_get(llw, number_of_documents, NumDocs),
    rocks_get(llw, number_of_tuples, NumTuples)
  },
  html(
    p(class='navbar-text', [
      "(",
      \html_thousands(NumDocs),
      " docs with ",
      \html_thousands(NumTuples),
      " stmts)"
    ])
  ).
