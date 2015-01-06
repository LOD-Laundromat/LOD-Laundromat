:- module(
  debug_project,
  [
    debug_all_files/0
  ]
).

/** <module> Debug project

Generic code for debugging a project:
  * Load all subdirectories and Prolog files contained in those directories.

@author Wouter Beek
@version 2014/11/19
*/

:- use_module(library(ansi_term)).
:- use_module(library(apply)).

:- use_module(os(dir_ext)).

% Avoid errors when using gtrace/0 in threads.
:- initialization(guitracer).



debug_all_files:-
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



% MESSAGES

:- multifile(prolog:message//1).

prolog:message(loading_module(File)) -->
  ['[M] ',File].
