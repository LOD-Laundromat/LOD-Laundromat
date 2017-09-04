:- module(test_seeds, [test_seed/1]).

:- use_module(library(ll/ll_seedlist)).
:- use_module(library(http/ckan_api)).

:- discontiguous
    rdf_format/1,
    rdf_format/2.

test_seed(Uri) :-
  ckan_resource('https://old.datahub.io', Dict),
  _{format: Format, url: Uri} :< Dict,
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
