:- module(
  ll_infobox,
  [
    ll_infobox/1 % +Request:list(nvpair)
  ]
).

/** <module> LOD Laundromat Infobox

Serves responses for the contents of a metadata infobox in HTML,
for use in LOD Laundromat.

@author Wouter Beek
@version 2014/07
*/

:- use_module(library(aggregate)).
:- use_module(library(http/html_write)).
:- use_module(library(http/term_html)).
:- use_module(library(semweb/rdf_db)).

:- use_module(dcg(dcg_generic)).
:- use_module(generics(uri_search)).

:- use_module(plRdf(rdf_name)).
:- use_module(plRdf_term(rdf_string)).

:- use_module(plTabular(rdf_html_table)).

:- use_module(lwm(lwm_generics)).



ll_infobox(Request):-
  request_search_read(Request, md5, Md5), !,
  lwm_default_graph(Graph),
  aggregate_all(
    set([PName,OName]),
    (
      rdf_string(Datadoc, ll:md5, Md5, Graph),
      rdf(Datadoc, P, O, Graph),
      dcg_with_output_to(atom(PName), rdf_term_name(P)),
      dcg_with_output_to(atom(OName), rdf_term_name(O))
    ),
    Rows
  ),
  lwm_source(Md5, Source),
  dcg_with_output_to(
    string(String),
    html(
      \rdf_html_table(
        html(['Infobox for ',Source]),
        Rows,
        [graph(Graph),header_row(po)]
      )
    )
  ),
  write(String).

