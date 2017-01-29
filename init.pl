:- [library(init_rdf_aliases)].

:- use_module(library(debug)).
:- use_module(library(print_ext)).
:- use_module(library(q/q_fs), []).
:- use_module(library(service/es_api)).
:- use_module(library(service/rocks_api), []).
:- use_module(library(settings)).

:- set_setting(rocks_api:index_dir, '/scratch/wbeek/crawls/13/index/').
:- set_setting(q_fs:store_dir, '/scratch/wbeek/crawls/13/store/').

:- use_module(ll).
:- use_module(seedlist).

:- debug(es_api).
:- debug(http(reply)).
:- debug(http(send_request)).
:- debug(http_io).
:- debug(io(close)).
:- debug(io(open)).
% @tbd Document that ‘ll(idle)’ overrules ‘ll(_)’.
:- debug(ll(_)).
:- debug(seedlist(_)).
