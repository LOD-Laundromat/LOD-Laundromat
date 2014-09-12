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
:- debug(lwm_generic).
:- debug(lwm_idle_loop(clean)).
:- debug(lwm_idle_loop(unpack)).
:- debug(lwm_progress(clean)).
:- debug(lwm_progress(unpack)).
%:- debug(sparql_request).
%:- debug(sparql_reply).


:- dynamic(debug:debug_md5/1).
:- multifile(debug:debug_md5/1).

%debug:debug_md5('0101ae619a8468f8b48ab9b010d95bca'). % existence_error(file)
%debug:debug_md5('194a342e95cf5b1376e61491edc4cd50'). % existence_error(source_sink)
%debug:debug_md5('4e736d4f4a44593ce56c32b2efdd41f3'). % existence_error(file)
%debug:debug_md5('939bc206a6da52511ce497786deebc31'). % existence_error(file)
%debug:debug_md5('9d3b5a7ecddc9b4d8ddab2ec7a1e90f0'). % existence_error(file)
debug:debug_md5('cfee1f887364ff5c4311a5ba7b416a96'). % false
%debug:debug_md5('f4f0beffd8c21e195f9b41d3056c3b10'). % ssl_error(ssl_verify) GITHUB

