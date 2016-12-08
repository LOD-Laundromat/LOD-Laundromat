:- use_module(library(iri/iri_ext)).
:- set_setting(iri:data_auth, 'lodlaundromat.org').
:- set_setting(iri:data_scheme, http).

:- use_module(library(q/q_iri)).
:- q_init_ns.

:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_host), []).
:- use_module(library(q/q_io)).
:- use_module(library(settings)).

:- use_module(cp(applications/admin)).
:- use_module(cp(hooks)).

:- dynamic
    user:file_search_path/2.

:- multifile
    user:file_search_path/2.

user:file_search_path(ll, cpack('LOD-Laundromat')).
  user:file_search_path(resource, ll(resource)).

cp:logo(washing_machine_round).

:- use_module(cp(api/alias)).
:- use_module(cp(api/class)).
:- use_module(cp(api/dataset)).
:- use_module(cp(api/documentation)).
:- use_module(cp(api/download)).
:- use_module(cp(api/geo_proximity)).
:- use_module(cp(api/graph)).
:- use_module(cp(api/predicate)).
:- use_module(cp(api/simple_graph_pattern)).
:- use_module(cp(api/sparql_query)).
:- use_module(cp(api/term)).
:- use_module(cp(api/triple)).

:- use_module(ll(api/ll_overview)).

%:- set_setting(q_io:source_dir, '/scratch/wbeek/crawls/source/').
%:- set_setting(q_io:store_dir,  '/scratch/wbeek/crawls/store/' ).

:- http_handler(/, root, []).

root(Req) :-
  redirect_create_admin(Req),
  http_redirect(moved, location_by_id(ll_overview_handler), Req).

html:menu_item(10, dataset_handler, "Dataset").
html:menu_item(20, browse, "Browse").
  html:menu_item(browse, 10, alias_handler, "Alias").
  html:menu_item(browse, 20, graph_handler, "Graph").
  html:menu_item(browse, 30, class_handler, "Class").
  html:menu_item(browse, 40, predicate_handler, "Predicate").
  html:menu_item(browse, 50, triple_handler, "Triple").
html:menu_item(30, geo_proximity_handler, "Geo").
html:menu_item(40, query, "Query").
  html:menu_item(query, 10, sgp_handler, "SGP").
  html:menu_item(query, 20, sparql_query_handler, "SPARQL").
html:menu_item(50, doc_handler, "Docs").

:- [ll(debug)].
:- [ll(local)].
