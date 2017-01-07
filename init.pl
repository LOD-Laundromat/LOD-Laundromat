:- use_module(library(iri/iri_ext)).
:- set_setting(iri:data_auth, 'lodlaundromat.org').
:- set_setting(iri:data_scheme, http).

:- use_module(library(q/q_iri)).
:- q_init_ns.

:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_host), []).
:- use_module(library(q/q_io)).
:- use_module(library(rdf/rdf__io)).
:- use_module(library(service/rocks_api)).
:- use_module(library(settings)).

:- use_module(cp(applications/admin)).
:- use_module(cp(hooks)).

:- dynamic
    user:file_search_path/2.

:- multifile
    http:location/3,
    user:file_search_path/2.

http:location(ll, cp(.), [priority(-1)]).

user:file_search_path(ll, cpack('LOD-Laundromat')).
  user:file_search_path(resource, ll(resource)).

cp:logo(washing_machine_round).

:- use_module(cp(api/alias)).
:- use_module(cp(api/class)).
:- use_module(cp(api/graph)).
:- use_module(cp(api/predicate)).
:- use_module(cp(api/simple_graph_pattern)).
:- use_module(cp(api/term)).
:- use_module(cp(api/triple)).

:- set_setting(simple_graph_pattern:backend, hdt).

:- initialization(set_data_dirs).
set_data_dirs :-
  rdf_load_file(ttl('ll.ttl')),
  set_setting(q_io:source_dir, '/scratch/wbeek/crawls/13/source/'),
  set_setting(q_io:store_dir,  '/scratch/wbeek/crawls/13/store/' ),
  set_setting(rocks_api:index_dir, '/scratch/wbeek/crawls/13/index/'),
  rocks_open(llw, int),
  rocks_merge(llw, number_of_documents, 0),
  rocks_merge(llw, number_of_tuples, 0),
  ll_start.
:- at_halt(rocks_close(llw)).

:- http_handler(/, root, []).

root(Req) :-
  redirect_create_admin(Req),
  http_redirect(moved, location_by_id(ll_overview_handler), Req).

:- use_module(ll(api/basket)).
:- use_module(ll(api/ll_about)).
:- use_module(ll(api/ll_overview)).
:- use_module(ll(api/meta)).
:- use_module(ll(api/seedlist)).
:- use_module(ll(api/wardrobe)).
:- use_module(ll(ll)).
:- use_module(ll(wm)).

html:menu_item(10, seedlist_handler, "Seeds").
html:menu_item(20, meta_handler, "Meta").
html:menu_item(30, data_handler, "Data").
html:menu_item(40, wardrobe_handler, "Wardrobe").

:- [ll(debug)].
:- [ll(local)].
