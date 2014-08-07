:- module(
  ll_schema_export,
  [
    ll_schema_export/0
  ]
).

/** <module> LOD Laundromat: schema export

Exports the LOD Laundromat schema.

@author Wouter Beek
@version 2014/08
*/

:- use_module(library(apply)).

:- use_module(os(pdf)).

:- use_module(plRdf(rdf_export)).

:- use_module(plGraphViz(gv_file)).

:- use_module(ll_schema(ll_schema)).



%! export_ll_schema is det.

export_ll_schema:-
  export_ll_schema(ll).

%! export_ll_schema(+Graph:atom) is det.

export_ll_schema(Graph):-
  assert_ll_schema(Graph),
  
  % GIF
  maplist(
    rdf_register_prefix_color(Graph),
    [dcat, ll,   rdf,rdfs],
    [black,green,red,blue]
  ),
  rdf_graph_to_gif(
    Graph,
    Gif,
    [
      directed(true),
      iri_description(with_all_literals),
      language_preferences([nl,en])
    ]
  ),
  
  % DOT
  absolute_file_name(data(ll), DotFile, [access(write),file_type(dot)]),
  gif_to_gv_file(Gif, DotFile, [method(sfdp),to_file_type(dot)]),
  
  % PDF
  absolute_file_name(data(ll), PdfFile, [access(write),file_type(pdf)]),
  gif_to_gv_file(Gif, PdfFile, [method(sfdp),to_file_type(pdf)]),
  open_pdf(PdfFile).

