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

debug:debug_md5('0af4cf76187202f4f3a051b9542f3b46'). % existence_error(source_sink)
debug:debug_md5('28665808584a3425631a805afbd72336'). % existence_error(source_sink)
debug:debug_md5('3019899320737bfc88e28f323983139c'). % existence_error(source_sink)
debug:debug_md5('7645e27b568b8d2e1798f7eda7057975'). % existence_error(source_sink)
debug:debug_md5('baa7651083fc7e429fb6f1f98fe15856'). % false
debug:debug_md5('cfee1f887364ff5c4311a5ba7b416a96'). % false
debug:debug_md5('f0ef0f94bdb8383b3badb90751b7adaf'). % existence_error(source_sink)
debug:debug_md5('f4f0beffd8c21e195f9b41d3056c3b10'). % ssl_error(ssl_verify) GITHUB
