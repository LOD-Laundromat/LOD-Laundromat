:- module(
  ll_sources,
  [
    ll_source/1, % +Source
    ll_source/2  % +Source, -Uri
  ]
).

/** <module> LOD Laundromat: sources for seedpoints

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(debug)).
:- use_module(library(http/ckan_api)).
:- use_module(library(zlib)).

:- discontiguous
    rdf_format/1,
    rdf_format/2.




%! ll_source(+Source:oneof([datahub])) is det.

ll_source(Source) :-
  file_name_extension(Source, 'tsv.gz', File),
  setup_call_cleanup(
    gzopen(File, write, Out),
    forall(
      (
        ckan_resource_loop('https://datahub.io', Dict),
        _{format: Format, url: Uri} :< Dict
      ),
      format(Out, "~a\t~a\n", [Uri,Format])
    ),
    close(Out)
  ).

ckan_resource_loop(Uri, Dict) :-
  catch(ckan_resource(Uri, Dict), E, true),
  (var(E) -> true ; print_message(warning, E), ckan_resource_loop(Uri, Dict)).



%! ll_source(+Source:atom, -Uri:atom) is nondet.

ll_source(Local, Uri) :-
  absolute_file_name(Local, File, [access(read)]),
  setup_call_cleanup(
    gzopen(File, read, In),
    (
      read_line_to_string(In, Line),
      split_string(Line, "\t", "", [Uri,Format]),
      rdf_format(Format)
    ),
    close(In)
  ).
ll_source(datahub, Uri) :-
  ckan_resource('https://datahub.io', Dict),
  _{format: Format, url: Uri} :< Dict,
  debug(ll(sources), "~a\t~a", [Format,Uri]),
  rdf_format(Format).

rdf_format(Format) :-
  rdf_format(Format, _).

rdf_format("api/sparql").
rdf_format("application/n-triples",  application/'n-triples').
rdf_format("application/rdf xml",    application/'rdf+xml').
rdf_format("application/rdf+xml",    application/'rdf+xml').
rdf_format("application/trig",       application/trig).
rdf_format("application/x-nquads",   application/'n-quads').
rdf_format("application/x-ntriples", application/'n-triples').
rdf_format("application/x-trig",     application/trig).
rdf_format("example/n3",             text/n3).
rdf_format("example/ntriples",       application/'n-triples').
rdf_format("example/rdf xml",        application/'rdf+xml').
rdf_format("example/rdf+json",       application/'ld+json').
rdf_format("example/rdf+ttl",        text/turtle).
rdf_format("example/rdf+xml",        application/'rdf+xml').
rdf_format("example/rdfa",           application/'xhtml+xml').
rdf_format("example/turtle",         text/turtle).
rdf_format("example/x-turtle",       text/turtle).
rdf_format("gzip::nquads",           application/'n-quads').
rdf_format("html+rdfa",              application/'xhtml+xml').
rdf_format("linked data").
rdf_format("mapping/owl").
rdf_format("mapping/rdfs").
rdf_format("meta/owl").
rdf_format("meta/rdf-schema").
rdf_format("meta/void").
rdf_format("n-quads",                application/'n-quads').
rdf_format("n-triples",              application/'n-triples').
rdf_format("owl").
rdf_format("owl, ontology, meta/owl").
rdf_format("rdf").
rdf_format("rdf-n3",                 text/n3).
rdf_format("rdf-turtle",             text/turtle).
rdf_format("rdf-xml",                application/'rdf-xml').
rdf_format("rdf/n3",                 text/n3).
rdf_format("rdf/turtle",             text/turtle).
rdf_format("rdf/xml, html, json").
rdf_format("rdfa",                   application/'xhtml+xml').
rdf_format("sparql").
rdf_format("sparql-query").
rdf_format("sparql web form").
rdf_format("text/n3",                text/n3).
rdf_format("text/rdf+n3",            text/n3).
rdf_format("text/rdf+ttl",           text/turtle).
rdf_format("text/turtle",            text/turtle).
rdf_format("ttl",                    text/turtle).
