:- use_module(library(aggregate)).
:- use_module(library(thread)).

:- use_module(library(http/ckan_api)).
:- use_module(library(pp)).

%:- use_module(library(ll/ll_debug)).
:- use_module(library(ll/ll_init)).
:- use_module(library(ll/ll_workers)).

run :-
  %aggregate_all(set(Uri), ckan_site_uri(Uri), Uris),
  %concurrent_maplist(ckan_scrape_init, Uris),
  %member(Uri, Uris),
  %writeln(Uri),
  Uri = 'http://africaopendata.org/',
  ckan_scrape_init(Uri).
