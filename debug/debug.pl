:- use_module(library(debug)).

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
:- debug(lwm(progress(clean(large)))).
:- debug(lwm(progress(clean(medium)))).
:- debug(lwm(progress(clean(small)))).
:- debug(lwm(progress(unpack))).
:- debug(lwm(restart)).
:- debug(lwm(seedpoint)).
:- debug(lwm(status)).
:- debug(lwm(unpack)).

% Debugging specific data documents, based on their MD5.

:- dynamic(debug:debug_md5/2).
:- multifile(debug:debug_md5/2).

debug:debug_md5('12336fc95b072ce38550a293ba4e9114', unpack).
debug:debug_md5('6e1b5b1c576b6f1a5368a642bef4ecd5', clean).
debug:debug_md5('c57f2c71a56955607aa3d99cd0559630', unpack).
debug:debug_md5('e3df124b12ec0d4a9005ab7132838dda', unpack).

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
