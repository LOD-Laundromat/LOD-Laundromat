:- module(
  ll_sources,
  [
    ll_source/1, % +Source
    ll_source/2  % +Source, -Uri
  ]
).

/** <module> LOD Laundromat: sources for seedpoints

@author Wouter Beek
@version 2017/09-2017/12
*/

:- use_module(library(debug)).
:- use_module(library(http/ckan_api)).
:- use_module(library(http/json)).
:- use_module(library(http/http_client2)).
:- use_module(library(string_ext)).
:- use_module(library(zlib)).

:- discontiguous
    other_format/1,
    other_format/2,
    rdf_format/1,
    rdf_format/2,
    sparql_format/1,
    sparql_format/2.




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
  absolute_file_name(Local, File, [access(read),file_errors(fail)]), !,
  setup_call_cleanup(
    gzopen(File, read, In),
    (
      read_line_to_string(In, Line),
      split_string(Line, "\t", "", [Uri,Format]),
      rdf_format0(Format)
    ),
    close(In)
  ).
ll_source(datahub, Uri) :-
  ckan_resource('https://datahub.io', Dict),
  _{format: Format, url: Uri} :< Dict,
  debug(ll(sources), "~a\t~a", [Format,Uri]),
  rdf_format0(Format).
ll_source(lov, Uri) :-
  setup_call_cleanup(
    http_open2('http://lov.okfn.org/dataset/lov/api/v2/vocabulary/list', In),
    json_read_dict(In, Dicts, [value_string_as(atom)]),
    close(In)
  ),
  member(Dict, Dicts),
  _{uri: Uri} :< Dict.

rdf_format0(Format1) :-
  string_lower(Format1, Format2),
  string_strip(Format2, " ", Format),
  Format \== "",
  (   rdf_format(Format)
  ->  true
  ;   sparql_format(Format)
  ->  fail
  ;   other_format(Format)
  ->  fail
  ;   format(user_output, "~s\n", [Format])
  ->  fail
  ).

other_format(Format) :-
  other_format(Format, _).
other_format(Format) :-
  sub_string(Format, _, _, 0, ".doc").
other_format(Format) :-
  sub_string(Format, _, _, 0, ".e00").
other_format(Format, application/'geo+json') :-
  sub_string(Format, _, _, 0, ".geojson").
other_format(Format, application/'gml+xml') :-
  sub_string(Format, _, _, 0, ".gml").
other_format(Format, text/html) :-
  sub_string(Format, _, _, 0, ".html").
other_format(Format, application/json) :-
  sub_string(Format, _, _, 0, ".json").
other_format(Format) :-
  sub_string(Format, _, _, 0, ".mid").
other_format(Format, image/png) :-
  sub_string(Format, _, _, 0, ".png").
other_format(Format) :-
  sub_string(Format, _, _, 0, ".shp").
other_format(Format) :-
  sub_string(Format, _, _, 0, ".tab").
other_format(Format) :-
  sub_string(Format, _, _, 0, ".topojson").
other_format(Format, application/'x-tar') :-
  sub_string(Format, _, _, 0, ".tgz").
other_format(Format) :-
  sub_string(Format, _, _, 0, ".txt").
other_format(Format, application/zip) :-
  sub_string(Format, _, _, 0, ".zip").
other_format("api").
other_format("api/dcat").
other_format("api/git").
other_format("api/search").
other_format("application/csv").
other_format("application/download").
other_format("application/octet-stream").
other_format("application/sql+gzip").
other_format("application/vnd.ms-excel").
other_format("application/xhtml+xml", application/'xhtml+xml').
other_format("application/x-bzip", application/'x-bzip2').
other_format("application/x-bzip2", application/'x-bzip2').
other_format("application/x-pdf", application/pdf).
other_format("application/x-tgz", application/'x-tar').
other_format("application/x-vnd.oasis.opendocument.spreadsheet",
             application/'x-vnd.oasis.opendocument.spreadsheet').
other_format("application/x-zip-compressed").
other_format("application/xml+atom", application/'atom+x').
other_format("application/zip+vnd.ms-excel").
other_format("arcgis").
other_format("ascii").
other_format("asp").
other_format("aspx").
other_format("atom",                 application/'atom+x').
other_format("atom feed",            application/'atom+x').
other_format("bin").
other_format("biopax").
other_format("bz2",                  application/'x-bzip2').
other_format("bz2:xml",              application/xml).
other_format("csv",                  text/csv).
other_format("creole").
other_format("data file in asc").
other_format("data file in csv").
other_format("data file in csv, excel").
other_format("data file in excel",   application/'vnd.ms-excel').
other_format("data file in shp and csv").
other_format("data file in sol").
other_format("data file in spss").
other_format("data file in stata").
other_format("doc").
other_format("doc:04").
other_format("docx",                 application/msword).
other_format("epub",                 application/'epub+zip').
other_format("esri arc export").
other_format("esri grid").
other_format("esri shape").
other_format("esri shape file").
other_format("esri shape files").
other_format("example/*").
other_format("example/html").
other_format("gdocs").
other_format("gdocs/spreadsheet").
other_format("geojson",              application/'geo+json').
other_format("geopdf").
other_format("georss",               application/'rss+xml').
other_format("geotiff",              image/tiff).
other_format("gif",                  image/gif).
other_format("gis").
other_format("git").
other_format("gml",                  application/'gml+xml').
other_format("google doc").
other_format("google spreadsheet").
other_format("gtfs").
other_format("gtfs-realtime").
other_format("gz",                   application/gzip).
other_format("gzip",                 application/gzip).
other_format("gzip:text/sql").
other_format("html",                 text/html).
other_format("html5",                text/html).
other_format("html/doc").
other_format("img").
other_format("index/ftp").
other_format("iso").
other_format("jpeg",                 image/jpeg).
other_format("json",                 application/json).
other_format("js",                   application/javascript).
% Java Server Pages
other_format("jsp",                  application/jsp).
other_format("kml",                  application/'vnd.google-earth.kml+xml').
other_format("kml/kmz",              application/'vnd.google-earth.kml+xml').
other_format("kmz",                  application/'vnd.google-earth.kmz').
other_format("labpal").
other_format("mabxml").
other_format("map").
other_format("marc/xml",             text/xml).
other_format("marc21").
% Microsoft Access
other_format("mdb").
other_format("meta/sitemap").
other_format("microsoft excel",      application/'vnd.ms-excel').
other_format("mobi",                 application/'vnd.amazon.mobi8-ebook').
other_format("mol").
other_format("ms acess mdb").
other_format("ms excel csv").
other_format("nc:55").
other_format("netcdf").
other_format("ods",                  application/'vnd.oasis.opendocument.spreadsheet').
other_format("ods format",           application/'vnd.oasis.opendocument.spreadsheet').
other_format("odt",                  application/'vnd.oasis.opendocument.text').
other_format("osm").
other_format("pbf").
other_format("pdf",                  application/pdf).
other_format("php").
other_format("plain text").
other_format("png",                  image/png).
other_format("png jpg").
other_format("rss",                  application/'rss+xml').
other_format("sdf").
other_format("shape").
other_format("shp").
other_format("sisis export format").
other_format("solr").
other_format("search").
other_format("spreadsheet").
other_format("sql").
other_format("sqlite").
other_format("svg",                  image/'svg+xml').
other_format("tab").
other_format("tar",                  application/'x-tar').
other_format("tar.gz",               application/'x-tar').
other_format("text/sql",             application/sql).
other_format("text/x-csv").
other_format("tgz",                  application/'x-tar').
other_format("tiff",                 image/tiff).
% Translation Memory eXchange (TMX) is an XML specification for the
% exchange of translation memory data between computer-aided
% translation and localization tools with little or no loss of
% critical data.
other_format("tmx").
other_format("tomtom").
other_format("topojson").
other_format("torrent",              application/'x-bittorrent').
other_format("tsv",                  text/'tab-separated-values').
other_format("txt").
other_format("wfs/gml",              application/'gml+xml').
other_format("wms").
other_format("wsdl").
other_format("xhtml",                application/'xhtml+xml').
other_format("xls",                  application/'vnd.ms-excel').
other_format("xls (zip)",            application/'vnd.ms-excel').
other_format("xlsx",                 application/'vnd.ms-excel').
other_format("xml",                  text/xml).
other_format("xml (zipped)",         text/xml).
other_format("xsd").
other_format("xslx",                 application/'vnd.ms-excel').
other_format("yml").
other_format("zip",                  application/zip).
other_format("zip archive",          application/zip).
other_format("zip:csv").
other_format("zip/json",             application/json).
other_format("zip/tsv",              text/'tab-separated-values').

rdf_format(Format) :-
  rdf_format(Format, _).
rdf_format(Format) :-
  (Format == "_mapping.owl" -> gtrace ; true),
  sub_string(Format, _, _, 0, ".owl").
rdf_format(Format) :-
  sub_string(Format, _, _, 0, ".rdf").
rdf_format("api/linked-data").
rdf_format("apping.owl").
rdf_format("application/ld+json",    application/'ld+json').
rdf_format("application/n-triples",  application/'n-triples').
rdf_format("application/rdf xml",    application/'rdf+xml').
rdf_format("application/rdf+xml",    application/'rdf+xml').
rdf_format("application/trig",       application/trig).
rdf_format("application/trix").
rdf_format("application/turtle",     text/turtle).
rdf_format("application/x-nquads",   application/'n-quads').
rdf_format("application/x-ntriples", application/'n-triples').
rdf_format("application/x-trig",     application/trig).
rdf_format("application/x-turtle",   text/turtle).
rdf_format("biopax",                 application/'vnd.biopax.rdf+xml').
rdf_format("bz2:nt",                 application/'n-triples').
rdf_format("data file in excel and rdf").
rdf_format("example/application/rdf+xml", application/'xhtml+xml').
rdf_format("example/n3",             text/n3).
rdf_format("example/ntriples",       application/'n-triples').
rdf_format("example/html+rdfa",      application/'xhtml+xml').
rdf_format("example/rdf").
rdf_format("example/rdf xml",        application/'rdf+xml').
rdf_format("example/rdf+json",       application/'ld+json').
rdf_format("example/rdf+ttl",        text/turtle).
rdf_format("example/rdf+xml",        application/'rdf+xml').
rdf_format("example/rdfa",           application/'xhtml+xml').
rdf_format("example/turtle",         text/turtle).
rdf_format("example/x-turtle",       text/turtle).
rdf_format("gzip::nquads",           application/'n-quads').
rdf_format("gzip:ntriples",          application/'n-triples').
rdf_format("html+rdfa",              application/'xhtml+xml').
rdf_format("html/rdf").
rdf_format("json-ld",                application/'ld+json').
rdf_format("linked data").
rdf_format("mapping/owl").
rdf_format("mapping/rdfs").
rdf_format("meta/owl").
rdf_format("meta/rdf-schema").
rdf_format("meta/rdf+schema").
rdf_format("meta/void").
rdf_format("n-quads",                application/'n-quads').
rdf_format("n-triples",              application/'n-triples').
rdf_format("n3",                     text/n3).
rdf_format("owl").
rdf_format("owl, ontology, meta/owl").
rdf_format("rdf").
rdf_format("rdf endpoint").
rdf_format("rdf trig",               application/trig).
rdf_format("rdf-n3",                 text/n3).
rdf_format("rdf-turtle",             text/turtle).
rdf_format("rdf-xml",                application/'rdf+xml').
rdf_format("rdf+xml",                application/'rdf+xml').
rdf_format("rdf/n3",                 text/n3).
rdf_format("rdf/n-triples",          application/'n-triples').
rdf_format("rdf/nt",                 application/'n-triples').
rdf_format("rdf/turtle",             text/turtle).
rdf_format("rdf/void").
rdf_format("rdf/xml example",        application/'rdf+xml').
rdf_format("rdf/xml, html, json").
rdf_format("rdfa",                   application/'xhtml+xml').
rdf_format("rdfxml",                 application/'rdf+xml').
rdf_format("text/n3",                text/n3).
rdf_format("text/rdf+n3",            text/n3).
rdf_format("text/rdf+ttl",           text/turtle).
rdf_format("text/turtle",            text/turtle).
rdf_format("trig gzip",              application/trig).
rdf_format("ttl",                    text/turtle).
rdf_format("ttl.bz2",                text/turtle).
rdf_format("ttl.bzip2",              text/turtle).
rdf_format("turtle",                 text/turtle).
rdf_format("void").

sparql_format(Format) :-
  sparql_format(Format, _).
sparql_format("api/sparql").
sparql_format("sparql").
sparql_format("sparql-json",            application/'sparql-results+json').
sparql_format("sparql/json",            application/'sparql-results+json').
sparql_format("sparql-query").
sparql_format("sparql-xml",             application/'sparql-results+xml').
sparql_format("sparql/xml",             application/'sparql-results+xml').
sparql_format("sparql web form").
