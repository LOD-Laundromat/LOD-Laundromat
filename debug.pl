% Debug file for the llWashingMachine project.


:- use_module(library(debug)).

%:- debug(ac).
:- debug(lwm_restart).
:- debug(lwm_unpack).
%:- debug(sparql_graph_store).
%:- debug(sparql_update).

% Show idle looping on threads.
%:- debug(lwm_idle_loop(clean_large)).
%:- debug(lwm_idle_loop(clean_medium)).
%:- debug(lwm_idle_loop(clean_small)).
%:- debug(lwm_idle_loop(unpack)).

% Show progress.
:- debug(lwm_progress(clean_large)).
:- debug(lwm_progress(clean_medium)).
:- debug(lwm_progress(clean_small)).
:- debug(lwm_progress(unpack)).


:- [load].


:- use_module(debug_project).
%:- debug_all_files.


% Debugging specific data documents, based on their MD5.

:- dynamic(debug:debug_md5/2).
:- multifile(debug:debug_md5/2).

debug:debug_md5('bc2901f9ff1756b162e5a17e47b6a26e', unpack).
debug:debug_md5('b8357b16eadfcc792d47d057fc86ce9e', unpack).
debug:debug_md5('d5e05249adef425a9554cb4b1b5622f2', unpack).
debug:debug_md5('f0ef0f94bdb8383b3badb90751b7adaf', unpack).

show_idle:-
  flag(number_of_idle_loops_clean_small, Small, Small),
  flag(number_of_idle_loops_clean_medium, Medium, Medium),
  flag(number_of_idle_loops_clean_large, Large, Large),
  format(
    user_output,
    'Idle loops:\n  - Small: ~D\n  - Medium: ~D\n  - Large: ~D\n',
    [Small,Medium,Large]
  ).


:- use_module(lwm(lwm_reset)).
:- use_module(lwm(debug/debug_datadoc)).
:- use_module(lwm(debug/debug_query)).

