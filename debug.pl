% Debug file for the llWashingMachine project.


:- use_module(library(debug)).

% PLC
:- debug(list_script).

% plSparql
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
%:- debug(lwm_progress(clean_small)).
%:- debug(lwm_progress(unpack)).

:- debug(lwm_restart).
:- debug(lwm_seedpoint).
:- debug(lwm_unpack).

:- [load].


:- use_module(debug_project).
%:- debug_all_files.


% Debugging specific data documents, based on their MD5.

:- dynamic(debug:debug_md5/2).
:- multifile(debug:debug_md5/2).

debug:debug_md5('d5e05249adef425a9554cb4b1b5622f2', unpack).

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

