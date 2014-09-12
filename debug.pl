% Debug tools for the llWashingMachine project.



% Avoid errors when using gtrace/0 in threads.
:- initialization guitracer.



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



% Debugging specific data documents, based on their MD5.

:- dynamic(debug:debug_md5/1).
:- multifile(debug:debug_md5/1).

debug:debug_md5('3019899320737bfc88e28f323983139c'). % DirectoryExistenceError



% Show/hide debug messages per category.

:- use_module(library(debug)).

% ClioPatria debug tools.

% This requires a running CP instance.
:- debug(lwm_cp).

% LOD Washing Machine-specific debug messages that do not fit anywhere else.
:- debug(lwm_generic).

% Show idle looping on threads.
:- debug(lwm_idle_loop(clean)).
:- debug(lwm_idle_loop(unpack)).

% Show progress.
:- debug(lwm_progress(clean)).
%:- debug(lwm_progress(unpack)).
