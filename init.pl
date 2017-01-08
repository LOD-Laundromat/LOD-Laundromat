:- use_module(library(iri/iri_ext), []).
:- use_module(library(q/q_io), []).
:- use_module(library(q/q_iri)).
:- use_module(library(service/rocks_api)).
:- use_module(library(settings)).

:- set_setting(iri:data_auth, 'lodlaundromat.org').
:- set_setting(iri:data_scheme, http).
:- q_init_ns.

:- use_module(ll).
:- use_module(wm).

:- dynamic
    user:file_search_path/2.

:- multifile
    user:file_search_path/2.

:- at_halt(rocks_close(llw)).

:- initialization(init).

init :-
  set_setting(q_io:source_dir, '/scratch/wbeek/crawls/13/source/'),
  set_setting(q_io:store_dir,  '/scratch/wbeek/crawls/13/store/' ),
  set_setting(rocks_api:index_dir, '/scratch/wbeek/crawls/13/index/'),
  rocks_open(llw, int),
  rocks_merge(llw, number_of_documents, 0),
  rocks_merge(llw, number_of_tuples, 0),
  ll_start.

:- [debug].
