:- use_module(library(iri/iri_ext)).
:- set_setting(iri:data_auth, 'lodlaundromat.org').
:- set_setting(iri:data_scheme, http).

:- use_module(library(q/q_iri)).
:- q_init_ns.

:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_host), []).
:- use_module(library(q/q_io)).
:- use_module(library(rdf/rdf__io)).
:- use_module(library(service/rocks_ext)).
:- use_module(library(settings)).

:- dynamic
    user:file_search_path/2.

:- multifile
    http:location/3,
    user:file_search_path/2.

init :-
  set_setting(q_io:source_dir, '/scratch/wbeek/crawls/13/source/'),
  set_setting(q_io:store_dir,  '/scratch/wbeek/crawls/13/store/' ),
  set_setting(rocks_ext:index_dir, '/scratch/wbeek/crawls/13/index/'),
  rocks_open(llw, int),
  rocks_merge(llw, number_of_documents, 0),
  rocks_merge(llw, number_of_tuples, 0),
  ll_start.
:- initialization(init).
:- at_halt(rocks_close(llw)).

:- use_module(ll).
:- use_module(wm).

:- [debug].
