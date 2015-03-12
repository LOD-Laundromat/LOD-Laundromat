:- module(
  debug_project,
  [
    debug_all_files/0,
    start_pldoc_server/0
  ]
).

/** <module> Debug project

Generic code for debugging a project:
  * Load all subdirectories and Prolog files contained in those directories.

@author Wouter Beek
@version 2015/02/26
*/

:- use_module(library(ansi_term)).
:- use_module(library(apply)).
:- use_module(library(pldoc)).
:- use_module(library(portray_text)).

:- set_prolog_flag(
  answer_write_options,
  [max_depth(100),portrayed(true),spacing(next_argument)]
).
:- set_prolog_flag(
  debugger_write_options,
  [max_depth(100),portrayed(true),spacing(next_argument)]
).
:- set_portray_text(ellipsis, 1000).

:- dynamic(user:debug_mode).
:- multifile(user:debug_mode).

:- initialization(init_debug_mode).





debug_all_files:-
  ensure_loaded(plc(io/dir_ext)),
  absolute_file_name(project(.), Dir, [access(read),file_type(directory)]),
  directory_files(
    Dir,
    Files1,
    [
      file_types([prolog]),
      include_directories(false),
      include_self(false),
      recursive(true)
    ]
  ),
  exclude(do_not_load, Files1, Files2),
  maplist(use_module0, Files2).
use_module0(File):-
  print_message(informational, loading_module(File)),
  use_module(File).

do_not_load(File1):-
  file_base_name(File1, File2),
  file_name_extension(File3, pl, File2),
  do_not_load0(File3).

do_not_load0(dcg_ascii).
do_not_load0(dcg_unicode).
do_not_load0(debug).
do_not_load0(debug_project).
do_not_load0(index).
do_not_load0(load).
do_not_load0(load_project).
do_not_load0(run).
do_not_load0(style).



%! start_pldoc_server is det.
% The plDoc server should be started *before* documented modules are loaded.

start_pldoc_server:-
  doc_server(9999).





% INITIALIZATION

init_debug_mode:-
  % Set the debug mode flag.
  (   user:debug_mode
  ->  true
  ;   assert(user:debug_mode)
  ),
  % Avoid errors when using gtrace/0 in threads.
  guitracer.



% MESSAGES

:- multifile(prolog:message//1).

prolog:message(loading_module(File)) -->
  ['[M] ',File].
