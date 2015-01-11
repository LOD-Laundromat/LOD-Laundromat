% Debug file for the llWashingMachine project.


:- use_module(library(debug)).

:- debug(ac).
:- debug(lwm_restart).
%:- debug(sparql_graph_store).
%:- debug(sparql_update).
:- debug(store_new_url).

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


:- set_prolog_flag(
     answer_write_options,
     [max_depth(10),portrayed(true),spacing(next_argument)]
   ).
:- set_prolog_flag(
     debugger_write_options,
     [max_depth(10),portrayed(true),spacing(next_argument)]
   ).


% Debugging specific data documents, based on their MD5.

:- dynamic(debug:debug_md5/2).
:- multifile(debug:debug_md5/2).

debug:debug_md5('3022a019c87979c18ce75cf950fcf678', clean).
debug:debug_md5('3e04988fd36a2d96aa67690034b6d397', clean).
debug:debug_md5('45e0c8bb39ec87fb351241a750ae306e', clean).
debug:debug_md5('8fae0111994668e2c060a13e6ace0d91', clean).
debug:debug_md5('9a37588bcfafa3100dc31211f9725266', clean).
debug:debug_md5('9e8fc4e5a8df6e3e3be5ebf8078f20cf', clean).
debug:debug_md5('b7d40ead79bb991f86a6e61309e78656', clean).
debug:debug_md5('c4052f658d9c32b7bd91c171411ef7c9', clean).


show_idle:-
  flag(number_of_idle_loops_clean_small, Small, Small),
  flag(number_of_idle_loops_clean_medium, Medium, Medium),
  flag(number_of_idle_loops_clean_large, Large, Large),
  format(
    user_output,
    'Idle loops:\n  - Small: ~D\n  - Medium: ~D\n  - Large: ~D\n',
    [Small,Medium,Large]
  ).


:- use_module(lwm(debug/debug_query)).
