:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_host), []).
:- use_module(library(nlp/nlp_lang), []).
:- use_module(library(q/q_io)).
:- use_module(library(q/q_iri)).
:- use_module(library(settings)).

:- dynamic
    http:location/3,
    user:file_search_path/2.

:- multifile
    http:location/3,
    user:file_search_path/2.

user:file_search_path(q, cpack('LOD-Laundromat')).

http:location(llw, q, []).

:- set_setting(http:public_host, 'cliopatria.lod.labs.vu.nl:3020').
:- set_setting(http:public_scheme, http).
:- set_setting(http_io:user_agent, "LOD-Laundromat/2.0.0").
:- set_setting(iri:data_auth, 'lodlaundromat.org').
:- set_setting(iri:data_scheme, http).
:- set_setting(nlp:lrange, [nl,'en-US']).
:- set_setting(q:custom_brand, true).
:- set_setting(q:subtitle, "Cleaning Other People's Dirty Data").
:- set_setting(q:title, "LOD Laundromat").
:- set_setting(q_io:source_dir, '/scratch/wbeek/source/'),
:- set_setting(q_io:store_dir, '/scratch/wbeek/crawls/13/store/'),
:- set_setting(qh:handle_id, sgp_handler).

:- use_module(q(endpoint/graph/sgp_endpoint)).
:- use_module(q(endpoint/llw_about)).
:- use_module(q(endpoint/llw_basket)).
:- use_module(q(endpoint/llw_meta)).
:- use_module(q(endpoint/llw_overview)).
:- use_module(q(endpoint/llw_seedlist)).
:- use_module(q(endpoint/llw_wardrobe)).

:- http_handler(/, http_redirect(moved, location_by_id(overview_handler)), []).

:- setting(
     llw_index,
     atom,
     '/scratch/wbeek/crawls/13/llw/',
     "Directory for the LOD Laundromat RocksDB index."
   ).

:- initialization(init_rocksdb).

init_rocksdb :-
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
