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

debug:debug_md5('17dc0dda4ed11c7799330c97ce335ea9', clean).
debug:debug_md5('26ec4ec6b3c3aecb409c5f483b84026e', clean).
debug:debug_md5('2d2a92c4a908d8fb915cac1ea47eb1a8', clean).
debug:debug_md5('37ba3563c407ba3fdd418c2d0ca0ed93', clean).
debug:debug_md5('55de79d4ff3f8c53fbee37d1a9d323aa', clean).
debug:debug_md5('61a265b90ab0175858b98157b2986346', clean).
debug:debug_md5('7a870b44551a1a2d19c2cad346c468b8', clean).
debug:debug_md5('8188f82397af5dead860c091bce14d4d', clean).
debug:debug_md5('c4052f658d9c32b7bd91c171411ef7c9', clean).
debug:debug_md5('cc7443ad33c1a5c37df68fe22c545e7c', clean).
debug:debug_md5('d0ca92f8fa6c00b96bf673039c8da5c2', clean).

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
