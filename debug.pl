% Debug tools for the LOD-Washing-Machine project.

:- use_module(library(ansi_term)).
:- use_module(library(portray_text)).
:- portray_text(true).
:- set_prolog_flag(backquoted_string, true).
:- set_prolog_flag(
    toplevel_print_options,
    [backquoted_string(true),
     max_depth(9999),
     portray(true),
     spacing(next_argument)]
  ).
:- set_prolog_flag(
    debugger_print_options,
    [backquoted_string(true),
     max_depth(9999),
     portray(true),
     spacing(next_argument)]
  ).

