:- module(
  sitemap,
  [
    is_sitemap_uri/1,      % @Term
    sitemap_uri_interval/3 % +SitemapUri, -LocUri, -Interval
  ]
).

/** <module> Sitemap

@author Wouter Beek
@version 2017/10
*/

:- use_module(library(apply)).
:- use_module(library(http/http_client2)).
:- use_module(library(sgml)).
:- use_module(library(uri/uri_ext)).
:- use_module(library(xpath)).




%! is_sitemap_uri(@Term) is semidet.

is_sitemap_uri(Uri) :-
  uri_comps(Uri, uri(Scheme,Authority,Segments,_,_)),
  maplist(ground, [Scheme,Authority]),
  last(Segments, 'sitemap.xml').



%! sitemap_uri_interval(+SitemapUri:atom, -LocUri:atom,
%!                      -Interval:atom) is nondet.

sitemap_uri_interval(Uri0, Uri, Interval) :-
  Uri0 = 'http://wifo5-03.informatik.uni-mannheim.de/drugbank/sitemap.xml',
  Sitemap = 'http://www.sitemaps.org/schemas/sitemap/0.9',
  SemSitemap = 'http://sw.deri.org/2007/07/sitemapextension/scschema.xsd',
  http_open2(Uri0, In),
  call_cleanup(
    load_structure(In, Dom, [dialect(xmlns),space(remove)]),
    close(In)
  ),
  (   xpath(Dom, //(Sitemap:loc(normalize_space)), Uri),
      xpath(Dom, //(Sitemap:changefreq(normalize_space)), Interval)
  ;   xpath(Dom, //(SemSitemap:dataset), Dataset),
      xpath(Dataset, //(Sitemap:changefreq(normalize_space)), Interval),
      xpath(Dataset, //(SemSitemap:dataDumpLocation(normalize_space)), Uri)
  ).
