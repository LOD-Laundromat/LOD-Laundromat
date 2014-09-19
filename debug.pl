% Debug tools for the llWashingMachine project.



% Avoid errors when using gtrace/0 in threads.
:- initialization(guitracer).



% Customize the way in which terminal and GUI tracer appear.

:- use_module(library(ansi_term)).

:- set_prolog_flag(
     answer_write_options,
     [max_depth(10),portrayed(true),spacing(next_argument)]
   ).
:- set_prolog_flag(
     debugger_write_options,
     [max_depth(10),portrayed(true),spacing(next_argument)]
   ).



% Show/hide debug messages per category.

:- use_module(library(debug)).

% ClioPatria debug tools.

% This requires a running CP instance.
:- debug(lwm_cp).

% LOD Washing Machine-specific debug messages that do not fit anywhere else.
:- debug(lwm_generic).

% Show idle looping on threads.
:- debug(lwm_idle_loop(clean_large)).
:- debug(lwm_idle_loop(clean_medium)).
:- debug(lwm_idle_loop(clean_small)).
:- debug(lwm_idle_loop(unpack)).

% Show progress.
:- debug(lwm_progress(clean_large)).
:- debug(lwm_progress(clean_medium)).
%:- debug(lwm_progress(clean_small)).
%:- debug(lwm_progress(unpack)).



% Debugging specific data documents, based on their MD5.

:- dynamic(debug:debug_md5/2).
:- multifile(debug:debug_md5/2).

debug:debug_md5('000395afb9a9c7923171213733bf4a06', _).
debug:debug_md5('7e0d80c3fb3a10c96ea998373027ecbf', _).
debug:debug_md5('7fcd76754bb711160d4f661ca4bb5d65', _).
debug:debug_md5('9642958d99a44d41e62943989e5d20f5', _).
debug:debug_md5('d3c9349c6bdd8d4d477ace2e616b482e', _).
debug:debug_md5('e12babad5e313f46734086426e475faa', _).

