% Debug tools for the llWashingMachine project.

% Avoid errors when using gtrace/0 in threads.
:- initialization guitracer.

:- set_prolog_flag(
     answer_write_options,
     [max_depth(10),portrayed(true),spacing(next_argument)]
   ).
:- set_prolog_flag(
     debugger_write_options,
     [max_depth(10),portrayed(true),spacing(next_argument)]
   ).

:- use_module(library(ansi_term)).

%:- use_module(library(portray_text)).
%:- portray_text(true).
%:- set_portray_text(ellipsis, 100).

:- use_module(library(debug)).
:- debug(loop_until_true).
%:- debug(sparql_request).
%:- debug(sparql_reply).


:- dynamic(debug:debug_md5/1).
:- multifile(debug:debug_md5/1).

debug:debug_md5('3019899320737bfc88e28f323983139c'). % DirectoryExistenceError
