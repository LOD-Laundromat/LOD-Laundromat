:- module(
  ckan_scrape,
  [
    ckan_scrape_init/1 % +Source
  ]
).

/** <module> LOD Laundromat: RDF datasets from CKAN

@author Wouter Beek
@version 2018
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(http/json)).
:- use_module(library(lists)).

:- use_module(library(atom_ext)).
:- use_module(library(dcg)).
:- use_module(library(http/ckan_api), []).
:- use_module(library(media_type)).

:- use_module(library(ll/ll_seedlist)).





%! ckan_add_seed(+Source:atom, +Package:dict) is det.

ckan_add_seed(Source, Package) :-
  Uri{name: DName, resources: Resources} :< Package,
  maplist(ckan_resource_url, Resources, Urls),
  % organization
  ckan_organization(Source, Package, [documents-Urls,name-DName], L1),
  % description
  ckan_description(Package, L1, L2),
  % license
  ckan_license(Package, L2, L3),
  dict_pairs(Seed, Uri, L3),
  add_seed(Source, Seed).

ckan_description(Package, T, [description-Desc|T]) :-
  _{notes: Desc0} :< Package,
  Desc0 \== null, !,
  atom_string(Desc0, Desc).
ckan_description(_, T, T).

ckan_license(Package, T, [license-License|T]) :-
  _{license_url: License} :< Package, !.
ckan_license(_, T, T).

ckan_organization(_, Package, T, [organization-Org|T]) :-
  is_dict(Package.organization), !,
  % name
  _{name: Name} :< Package.organization,
  % image
  (   _{image_url: Url} :< Package.organization
  ->  Org = _{image: Url, name: Name}
  ;   Org = _{name: Name}
  ).
ckan_organization(Source, _, T, [organization-_{name: Source}|T]).

ckan_resource_url(Resource, Url) :-
  _{url: Url} :< Resource.



%! ckan_package(+Source:atom, -Package:dict) is multi.

ckan_package(File, Package) :-
  exists_file(File), !,
  setup_call_cleanup(
    gzopen(File, read, In),
    (
      json_read_dict(In, Packages, [value_string_as(atom)]),
      member(Package, Packages)
    ),
    close(In)
  ).
ckan_package(Site, Package) :-
  ckan_api:ckan_package(Site, Package).



%! ckan_package_resource(+Package:dict, -Resource:dict,
%!                       -MediaTypes:list(compound)) is multi.

ckan_package_resource(Package, Resource, MediaTypes) :-
  _{resources: Resources} :< Package,
  member(Resource, Resources),
  _{format: Format1, mimetype: Format2} :< Resource,
  aggregate_all(
    set(MediaType),
    (
      member(Format, [Format1,Format2]),
      clean_media_type(Format, MediaType)
    ),
    MediaTypes
  ).



%! ckan_rdf_package(+Source:atom, -Package:dict) is nondet.

ckan_rdf_package(Source, Package) :-
  ckan_package(Source, Package),
  % The dataset contains at least one RDF document.
  once((
    ckan_package_resource(Package, _, MediaTypes),
    member(MediaType, MediaTypes),
    rdf_media_type_(MediaType)
  )).



%! ckan_scrape_init(+Source:atom) is det.

ckan_scrape_init(Source) :-
  forall(
    ckan_rdf_package(Source, Package),
    ckan_add_seed(Source, Package)
  ).

rdf_media_type_(media(application/'n-quads',[])).
rdf_media_type_(media(application/'n-triples',[])).
rdf_media_type_(media(application/'rdf+xml',[])).
rdf_media_type_(media(application/trig,[])).
rdf_media_type_(media(text/turtle,[])).





% CLEANUP CODE %

clean_media_type(Format1, MediaType) :-
  downcase_atom(Format1, Format2),
  atom_strip(Format2, Format3),
  \+ memberchk(Format3, ['',null]),
  (   atom_phrase(media_type(MediaType0), Format3)
  ->  (   % known known Media Type
          media_type_media_type_(MediaType0, MediaType)
      ->  true
      ;   % known unknown Media Type
          media_type_(MediaType0)
      ->  fail
      ;   % unknown unknown Media Type
          print_message(warning, unknown_media_type(MediaType0)),
          fail
      )
  ;   (   % knwon known format
          format_media_type_(Format3, MediaType)
      ->  true
      ;   % known unknown format
          format_(Format3)
      ->  fail
      ;   % unknown unknown format
          print_message(warning, unknown_format(Format3)),
          fail
      )
  ).

format_(Format) :-
  sub_atom(Format, _, _, 0, '.e00').
format_(Format) :-
  sub_atom(Format, _, _, 0, '.mid').
format_(Format) :-
  sub_atom(Format, _, _, 0, '.owl').
format_(Format) :-
  sub_atom(Format, _, _, 0, '.rdf').
format_(Format) :-
  sub_atom(Format, _, _, 0, '.shp').
format_(Format) :-
  sub_atom(Format, _, _, 0, '.topojson').
format_(Format) :-
  sub_atom(Format, _, _, 0, '.txt').
format_('apping.owl').
format_('data file in asc').
format_('data file in csv').
format_('data file in csv, excel').
format_('data file in excel and rdf').
format_('data file in shp and csv').
format_('data file in sol').
format_('data file in spss').
format_('data file in stata').
format_('data file in tifgis').
format_('doc:04').
format_('esri arc export').
format_('esri grid').
format_('esri shape file').
format_('esri shape files').
format_('esri shape').
format_('google doc').
format_('google spreadsheet').
format_('gtfs-realtime').
format_('gzip:text/sql').
format_('linked data').
format_('marc21').
format_('ms acess mdb').
format_('ms excel csv').
format_('nc:55').
format_('owl, ontology, meta/owl').
format_('plain text').
format_('png jpg').
format_('rdf endpoint').
format_('sisis export format').
format_('sparql-query').
format_(api).
format_(arcgis).
format_(ascii).
format_(asp).
format_(aspx).
format_(bin).
format_(biopax).
format_(creole).
format_('conll-u').
format_(gdocs).
format_(geopdf).
format_(gis).
format_(git).
format_(googlespreadsheet).
format_(gtfs).
format_(hydra).
format_(img).
format_(index).
format_(iso).
format_(labpal).
format_(link).
format_(mabxml).
format_(map).
format_(mol).
format_(netcdf).
format_(osm).
format_(owl).
format_(pbf).
format_(php).
format_(rdf).
format_(scraper).
format_(sdf).
format_(search).
format_(shape).
format_(shp).
format_(solr).
format_(sparql).
format_(spreadsheet).
format_(sql).
format_(sqlite).
format_(sru).
% Translation Memory eXchange (TMX) is an XML specification for the
% exchange of translation memory data between computer-aided
% translation and localization tools with little or no loss of
% critical data.
format_(tmx).
format_(tomtom).
format_(topojson).
format_(txt).
format_(void).
format_(wms).
format_(wsdl).
format_('xml, json, rdf').
format_(xsd).
format_(yml).

format_media_type_(Format, MediaType) :-
  media_type_extension(MediaType, Format).
format_media_type_(Format, media(application/msword,[])) :-
  sub_atom(Format, _, _, 0, '.doc').
format_media_type_(Format, media(application/'vnd.geo+json',[])) :-
  sub_atom(Format, _, _, 0, '.geojson').
format_media_type_(Format, media(application/'gml+xml',[])) :-
  sub_atom(Format, _, _, 0, '.gml').
format_media_type_(Format, text/html) :-
  sub_atom(Format, _, _, 0, '.html').
format_media_type_(Format, media(application/json,[])) :-
  sub_atom(Format, _, _, 0, '.json').
format_media_type_(Format, media(image/png,[])) :-
  sub_atom(Format, _, _, 0, '.png').
format_media_type_(Format, media(text/'tab-separated-values',[])) :-
  sub_atom(Format, _, _, 0, '.tab').
format_media_type_(Format, media(application/'x-tar',[])) :-
  sub_atom(Format, _, _, 0, '.tgz').
format_media_type_(Format, media(application/zip,[])) :-
  sub_atom(Format, _, _, 0, '.zip').
format_media_type_('atom feed',                 media(application/'atom+x',[])).
format_media_type_('bz2:nt',                    media(application/'n-triples',[])).
format_media_type_('bz2:xml',                   media(application/xml,[])).
format_media_type_('data file in excel',        media(application/'vnd.ms-excel',[])).
format_media_type_('gzip::nquads',              media(application/'n-quads',[])).
format_media_type_('gzip:ntriples',             media(application/'n-triples',[])).
format_media_type_('html+rdfa',                 media(application/'xhtml+xml',[])).
format_media_type_('json-ld',                   media(application/'ld+json',[])).
format_media_type_('microsoft access database', media(application/'vnd.ms-access',[])).
format_media_type_('microsoft excel',           media(application/'vnd.ms-excel',[])).
format_media_type_('ms access',                 media(application/'vnd.ms-access',[])).
format_media_type_('ms access mdb',             media(application/'vnd.ms-access',[])).
format_media_type_('n-quads',                   media(application/'n-quads',[])).
format_media_type_('n-triples',                 media(application/'n-triples',[])).
format_media_type_('ods format',                media(application/'vnd.oasis.opendocument.spreadsheet',[])).
format_media_type_('rdf trig',                  media(application/trig,[])).
format_media_type_('rdf+xml',                   media(application/'rdf+xml',[])).
format_media_type_('rdf-n3',                    media(text/n3,[])).
format_media_type_('rdf-turtle',                media(text/turtle,[])).
format_media_type_('rdf-xml',                   media(application/'rdf+xml',[])).
format_media_type_('sparql-json',               media(application/'sparql-results+json',[])).
format_media_type_('tar.gz',                    media(application/'x-tar',[])).
format_media_type_('trig gzip',                 media(application/trig,[])).
format_media_type_('ttl.bz2',                   media(text/turtle,[])).
format_media_type_('ttl.bzip2',                 media(text/turtle,[])).
format_media_type_('xls (zip)',                 media(application/'vnd.ms-excel',[])).
format_media_type_('xml (zipped)',              media(text/xml,[])).
format_media_type_('zip archive',               media(application/zip,[])).
format_media_type_('zip:csv',                   media(text/csv,[])).
format_media_type_(biopax,                      media(application/'vnd.biopax.rdf+xml',[])).
format_media_type_(georss,                      media(application/'rss+xml',[])).
format_media_type_(geotiff,                     media(image/tiff,[])).
format_media_type_(gzip,                        media(application/gzip,[])).
format_media_type_(html5,                       media(text/html,[])).
format_media_type_(rdfa,                        media(application/'xhtml+xml',[])).
format_media_type_(rdfxml,                      media(application/'rdf+xml',[])).
format_media_type_('sparql-xml',                media(application/'sparql-results+xml',[])).
format_media_type_(tab,                         media(text/'tab-separated-values',[])).
format_media_type_(tar,                         media(application/'x-tar',[])).
format_media_type_(tgz,                         media(application/'x-tar',[])).
format_media_type_('tsv.gz',                    media(text/'tab-separated-values',[])).
format_media_type_(turtle,                      media(text/turtle,[])).
format_media_type_(xsl,                         media(application/'vnd.ms-excel',[])).
format_media_type_(xslx,                        media(application/'vnd.openxmlformats-officedocument.spreadsheetml.sheet',[])).

media_type_(media(api/'linked-data',_)).
media_type_(media(api/dcat,_)).
media_type_(media(api/git,_)).
media_type_(media(api/search,_)).
media_type_(media(api/sparql,_)).
media_type_(media(application/'octet-stream',_)).
media_type_(media(application/'sql+gzip',_)).
media_type_(media(application/'x-zip-compressed',_)).
media_type_(media(application/'zip+vnd.ms-excel',_)).
media_type_(media(application/download,_)).
media_type_(media(example/'*',_)).
media_type_(media(example/html,_)).
media_type_(media(example/rdf,_)).
media_type_(media(gdocs/spreadsheet,_)).
media_type_(media(index/ftp,_)).
media_type_(media(mapping/owl,_)).
media_type_(media(mapping/rdfs,_)).
media_type_(media(meta/'rdf+schema',_)).
media_type_(media(meta/'rdf-schema',_)).
media_type_(media(meta/owl,_)).
media_type_(media(meta/sitemap,_)).
media_type_(media(meta/void,_)).
media_type_(media(multipart/'form-data',_)).
media_type_(media(rdf/'xml, html, json',_)).
media_type_(media(rdf/void,_)).
media_type_(media(text/plain,_)).

media_type_media_type_(MediaType, MediaType) :-
  media_type(MediaType).
media_type_media_type_(media(application/csv,L),               media(text/csv,L)).
media_type_media_type_(media(application/xml,L),               media(text/xml,L)).
media_type_media_type_(media(application/'x-bzip',L),          media(application/'x-bzip2',L)).
media_type_media_type_(media(application/'x-bzip2',L),         media(application/'x-bzip2',L)).
media_type_media_type_(media(application/'x-pdf',L),           media(application/pdf,L)).
media_type_media_type_(media(application/'x-tgz',L),           media(application/'x-tar',L)).
media_type_media_type_(media(application/'xml+atom',L),        media(application/'atom+x',L)).
media_type_media_type_(media(html/doc,L),                      media(text/html,L)).
media_type_media_type_(media(kml/kmz,L),                       media(application/'vnd.google-earth.kml+xml',L)).
media_type_media_type_(media(marc/xml,L),                      media(text/xml,L)).
media_type_media_type_(media(text/sql,L),                      media(application/sql,L)).
media_type_media_type_(media(text/'x-csv',L),                  media(text/csv,L)).
media_type_media_type_(media(wfs/gml,L),                       media(application/'gml+xml',L)).
media_type_media_type_(media(zip/json,L),                      media(application/json,L)).
media_type_media_type_(media(zip/tsv,L),                       media(text/'tab-separated-values',L)).
media_type_media_type_(media(application/'rdf xml',L),         media(application/'rdf+xml',L)).
media_type_media_type_(media(application/'x-gzip',L),          media(application/gzip,L)).
media_type_media_type_(media(application/'x-nquads',L),        media(application/'n-quads',L)).
media_type_media_type_(media(application/'x-ntriples',L),      media(application/'n-triples',L)).
media_type_media_type_(media(application/'x-trig',L),          media(application/trig,L)).
media_type_media_type_(media(application/'x-turtle',L),        media(text/turtle,L)).
media_type_media_type_(media(application/'x-vnd.oasis.opendocument.spreadsheet',[]), media(application/'vnd.oasis.opendocument.spreadsheet',[])).
media_type_media_type_(media(application/'xml+rdf',L),         media(application/'rdf+xml',L)).
media_type_media_type_(media(example/'application/rdf+xml',L), media(application/'rdf+xml',L)).
media_type_media_type_(media(example/n3,L),                    media(text/n3,L)).
media_type_media_type_(media(example/ntriples,L),              media(application/'n-triples',L)).
media_type_media_type_(media(example/'html+rdfa',L),           media(application/'xhtml+xml',L)).
media_type_media_type_(media(example/'rdf xml',L),             media(application/'rdf+xml',L)).
media_type_media_type_(media(example/'rdf+json',L),            media(application/'ld+json',L)).
media_type_media_type_(media(example/'rdf+ttl',L),             media(text/turtle,L)).
media_type_media_type_(media(example/'rdf+xml',L),             media(application/'rdf+xml',L)).
media_type_media_type_(media(example/rdfa,L),                  media(application/'xhtml+xml',L)).
media_type_media_type_(media(example/turtle,L),                media(text/turtle,L)).
media_type_media_type_(media(example/'x-turtle',L),            media(text/turtle,L)).
media_type_media_type_(media(html/rdf,L),                      media(application/'xhtml_xml',L)).
media_type_media_type_(media(rdf/n3,L),                        media(text/n3,L)).
media_type_media_type_(media(rdf/'n-triples',L),               media(application/'n-triples',L)).
media_type_media_type_(media(rdf/nt,L),                        media(application/'n-triples',L)).
media_type_media_type_(media(rdf/turtle,L),                    media(text/turtle,L)).
media_type_media_type_(media(rdf/'xml example',L),             media(application/'rdf+xml',L)).
media_type_media_type_(media(text/'comma-separated-values',L), media(text/csv,L)).
media_type_media_type_(media(text/javascript,L),               media(application/javascript,L)).
media_type_media_type_(media(text/n3,L),                       media(text/n3,L)).
media_type_media_type_(media(text/'rdf+n3',L),                 media(text/n3,L)).
media_type_media_type_(media(text/'rdf+ttl',L),                media(text/turtle,L)).
media_type_media_type_(media(sparql/json,L),                   media(application/'sparql-results+json',L)).
media_type_media_type_(media('sparql-xml',L),                  media(application/'sparql-results+xml',L)).
media_type_media_type_(media(sparql/xml,L),                    media(application/'sparql-results+xml',L)).
media_type_media_type_(media(xml/rdf,L),                       media(application/'rdf+xml',L)).
