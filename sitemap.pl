:- use_module(library(http/http_client2)).
:- use_module(library(sgml)).
:- use_module(library(xpath)).

test(Frequency, DumpUri) :-
  Uri = 'http://wifo5-03.informatik.uni-mannheim.de/drugbank/sitemap.xml',
  Sitemap = 'http://www.sitemaps.org/schemas/sitemap/0.9',
  SemanticSitemap = 'http://sw.deri.org/2007/07/sitemapextension/scschema.xsd',
  http_open2(Uri, In),
  call_cleanup(
    load_structure(In, Dom, [dialect(xmlns),space(remove)]),
    close(In)
  ),
  xpath(Dom, //(SemanticSitemap:dataset), Dataset),
  xpath(Dataset, //(Sitemap:changefreq(normalize_space)), Frequency),
  xpath(
    Dataset,
    //(SemanticSitemap:dataDumpLocation(normalize_space)),
    DumpUri
  ).
