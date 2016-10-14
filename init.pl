:- module('conf_LOD-Laundromat-Web', []).

/** <module> LOD Laundromat configuration
*/

:- use_module(library(html/qh), []).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_host), []).
:- use_module(library(http/http_write)).
:- use_module(library(iri/iri_ext), []).
:- use_module(library(nlp/nlp_lang), []).
:- use_module(library(q/q_io)).
:- use_module(library(q/q_iri)).
:- use_module(library(q/qb)).
:- use_module(library(rdf/rdf__io)).
:- use_module(library(semweb/rdf_litindex)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(service/rocksdb_ext)).
:- use_module(library(settings)).
:- use_module(library(vocab/dbpedia)).

:- set_setting(q:custom_brand, true).
:- set_setting(q:subtitle, "Cleaning Other People's Dirty Data").
:- set_setting(q:title, "LOD Laundromat").
:- set_setting(http:public_host, 'cliopatria.lod.labs.vu.nl:3020').
:- set_setting(http:public_scheme, http).
:- set_setting(http_io:user_agent, "LOD-Laundromat/2.0.0").
:- set_setting(iri:data_auth, 'lodlaundromat.org').
:- set_setting(iri:data_scheme, http).
:- set_setting(nlp:lrange, [nl,'en-US']).
:- set_setting(qh:handle_id, sgp_handler).

:- setting(
     llw_index,
     atom,
     '/scratch/wbeek/crawls/13/llw/',
     "Directory for the LOD Laundromat RocksDB index."
   ).

http:location(llw, /, []).

:- http_handler(llw(.), root, []).

root(Req) :-
  http_redirect(moved_temporary, location_by_id(overview_handler), Req).

:- dynamic
    user:file_search_path/2.

:- multifile
    user:file_search_path/2.

user:file_search_path(llw, cpack('LOD-Laundromat-Web')).
  user:file_search_path(resource, llw(resource)).

:- initialization(set_data_dirs).

set_data_dirs :-
  set_setting(q_io:source_dir, '/scratch/wbeek/source/'),
  set_setting(q_io:store_dir, '/scratch/wbeek/crawls/13/store/'),
  setting(llw_index, Dir),
  rocks_open(
    Dir,
    _,
    [alias(llw),key(atom),merge(rocks_merge_sum),value(int64)]
  ),
  rocks_merge(llw, number_of_documents, 0),
  rocks_merge(llw, number_of_tuples, 0).

:- [llw(debug)].

:- q_init_ns.

% Pages
:- use_module(q(page/sgp)).
:- use_module(llw(page/llw_about)).
:- use_module(llw(page/llw_basket)).
:- use_module(llw(page/llw_meta)).
:- use_module(llw(page/llw_overview)).
:- use_module(llw(page/llw_seedlist)).
:- use_module(llw(page/llw_wardrobe)).

html:menu_item(110, wardrobe_handler, "Wardrobe").
html:menu_item(120, basket_handler, "Basket").
html:menu_item(130, about_handler, "About").
html:menu_item(210, sgp_handler, "Graph").
%html:menu_item(195, stat_handler, "Stats").
%html:menu_item(196, lodlab_handler, "LOD Lab").
%html:menu_item(197, rdf11_handler, "RDF 1.1").
%html:menu_item(198, lotus_handler, "LOTUS").
%html:menu_item(199, article, "Articles") :-
%html:menu_item(210, seed_handler, "Seedlist").

% Datasets
:- rdf_load_file(ttl('llw.ttl')).

:- at_halt(rocks_close(llw)).

:- use_module(llw(seedlist)).
:- use_module(llw(wm)).
