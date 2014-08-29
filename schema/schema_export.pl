:- module(
  schema_export,
  [
    schema_export/0,
    schema_export/1 % +Graph:atom
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
:- use_module(plRdf_ser(rdf_serial)).

:- use_module(plGraphViz(gv_file)).

:- use_module(lwm_schema(schema_error)).
:- use_module(lwm_schema(schema_http)).
:- use_module(lwm_schema(schema_llo)).
:- use_module(lwm_schema(schema_tcp)).



%! schema_export is det.

schema_export:-
  schema_error(error),
  schema_http(http),
  schema_llo(llo),
  schema_tcp(tcp),

  % GIF
  maplist(
    rdf_register_prefix_color(_),
    [dcat,  error, http, ll,    llo,   rdf,  rdfs, tcp],
    [black, red,   red,  green, green, blue, blue, red]
  ),

  forall(
    member(Graph, [error,http,llo,tcp]),
    schema_export(Graph)
  ).

schema_export(Graph):-
  absolute_file_name(data(Graph), RdfFile, [access(write),file_type(turtle)]),
  rdf_save_any(RdfFile, [format(turtle),graph(Graph)]),
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
  absolute_file_name(data(Graph), DotFile, [access(write),file_type(dot)]),
  gif_to_gv_file(Gif, DotFile, [method(sfdp),to_file_type(dot)]),

  % PDF
  absolute_file_name(data(Graph), PdfFile, [access(write),file_type(pdf)]),
  gif_to_gv_file(Gif, PdfFile, [method(sfdp),to_file_type(pdf)]),
  
  % DEB
  %%%%open_pdf(PdfFile),
  true.

