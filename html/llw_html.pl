:- module(
  llw_html,
  [
    llw_content_image//2, % +Content_0, +Img
    llw_image//1,         % +Name
    llw_image_content//2, % +Img, :Content_0
    llw_link//1           % +Name
  ]
).

/** <module> LOD Laundromat Web site: Style

@author Wouter Beek
@author Laurens Rietveld
@version 2016/02-2016/04, 2016/08-2016/10
*/

:- use_module(library(html/html_ext)).
:- use_module(library(http/html_head)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(pair_ext)).
:- use_module(library(service/rocksdb_ext)).
:- use_module(library(settings)).

:- use_module(q(html/q_html)).

:- use_module(llw(page/llw_overview)).

:- html_meta
   llw_content_image(html, +, ?, ?),
   llw_image_content(+, html, ?, ?).

:- html_resource(
     css(llw),
     [
       ordered(true),
       requires([css('llw.css')]),
       virtual(true)
     ]
   ).

:- http_handler(
     llw(.),
     http_redirect(moved, location_by_id(overview_handler)),
     [prefix,priority(-1)]
   ).

:- set_setting(html:google_analytics_id, 'UA-51130014-1').

:- discontiguous
    llw_image_alt0/2,
    llw_image_alt0/3.    

q_html:page_body(llw(_), Content_0) -->
  html(
    body([
      \q_navbar,
      \row_1([class=main], 12, Content_0),
      \q_footer,
      \html_post(q_navbar_right, \(llw_skin:data_counter))
    ])
  ).

q_html:page_head(llw(_), Content_0) -->
  html(
    head([
      \meta_authors([
        "Wouter Beek",
        "Laurens Rietveld",
        "Jan Wielemaker",
        "Filip Ilievski",
        "Stefan Schlobach",
        "Frank van Harmelen"
      ]),
      \html_requires(css(llw)),
      \google_analytics,
      \q_head_generics
    | Content_0
    ])
  ).





llw_content_image(Content_0, Img) -->
  {maplist(widths, [9,3], [ClassesA,ClassesB])},
  html(
    div([class=[container,'img-container']],
      div(class=row, [
        div(class=ClassesA, Content_0),
        div(class=ClassesB, \llw_image(Img))
      ])
    )
  ).



llw_image(Name) -->
  {
    llw_image_alt(Name, Alt, Ext),
    file_name_extension(Name, Ext, Local),
    http_absolute_location(img(Local), Path)
  },
  html(img([alt=Alt,src=Path], [])).



llw_image_alt(Name, Alt, Ext) :-
  llw_image_alt0(Name, Alt, Ext), !.
llw_image_alt(Name, Alt, svg) :-
  llw_image_alt0(Name, Alt).


llw_image_alt0(analytics,        "Various diagrams that show analytics").
llw_image_alt0(filip_ilievski,   "Picture of Filip Ilievski", jpg).
llw_image_alt0(frank,            "The logo of the Frank command-line tool").
llw_image_alt0(hdt,              "The Header Dictionary Triples (HDT) logo", png).
llw_image_alt0(javier_fernandez, "Picture of Javier Fernandez", jpg).
llw_image_alt0(labels,           "Various icons familiar from descriptions of how to wash clothes").
llw_image_alt0(laundry_basket,   "The LOD Laundry Basket logo").
llw_image_alt0(laundry_line,     "Picture of a laundry line").
llw_image_alt0(logo,             "The LOD Laundromat logo").
llw_image_alt0(lodlab,           "The LOD Lab logo", png).
llw_image_alt0(lotus,            "The LOTUS logo").
llw_image_alt0(paul_groth,       "Picture of Paul Groth", jpg).
llw_image_alt0(rdf_w3c,          "RDF W3C logo").
llw_image_alt0(wardrobe,         "The LOD Wardrobe logo").
llw_image_alt0(wouter_beek,      "Picture of Wouter Beek", jpg).



llw_image_content(Img, Content_0) -->
  {maplist(widths, [3,9], [ClassesA,ClassesB])},
  html(
    div(class=[container,'img-container'],
      div(class=row, [
        div(class=ClassesA, \llw_image(Img)),
        div([class=ClassesB,style='vertical-align: middle;'], Content_0)
      ])
    )
  ).



llw_link(Name) -->
  {llw_link0(Name, Lbl)}, !,
  internal_link(root(Name), Lbl).
llw_link(Name) -->
  {llw_link0(Name, Lbl, Iri)},
  external_link(Iri, Lbl).


llw_link0(about,          "about page").
llw_link0(lod_laundromat, "LOD Laundromat").
llw_link0(lodlab,         "LOD Lab").
llw_link0(lotus,          "LOTUS").
llw_link0(sparql,         "SPARQL endpoint").
llw_link0(wardrobe,       "LOD Wardrobe").

llw_link0(frank,       "Frank",       'https://github.com/LOD-Laundromat/Frank.git').
llw_link0(hdt,         "HDT",         'http://www.rdfhdt.org').
llw_link0(header_dictionary_triples, "Header Dictionary Triples", 'http://www.rdfhdt.org').
llw_link0(java,        "Java",        'https://github.com/LOD-Laundromat/GettingStartedJava.git').
llw_link0(ldf,         "LDF",         'https://github.com/LinkedDataFragments/Server.js').
llw_link0(javascript,  "JavaScript",  'https://github.com/LOD-Laundromat/GettingStarted.git').
llw_link0(python,      "Python",      'https://github.com/LOD-Laundromat/GettingStarted.git').
llw_link0(nodejs,      "Node.js",     'https://github.com/LOD-Laundromat/GettingStarted.git').
llw_link0(prov,        "PROV",        'https://www.w3.org/TR/prov-overview/').
llw_link0(swipl,       "SWI-Prolog",  'https://github.com/wouterbeek/LOD-Laundromat-API.git').
llw_link0(void,        "VoID",        'https://www.w3.org/TR/void/').
llw_link0(wouter_beek, "Wouter Beek", 'http://www.wouterbeek.com').



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
