:- module(
  load_project,
  [
    load_project/1, % +ChildProjects:list(or([atom,pair(atom)]))
    load_subproject/2, % +ParentFileSearchPath:atom
                       % +Child:or([atom,pair(atom)])
    set_data_subdirectory/1 % +ParentDirectory:atom
  ]
).

/** <module> Load project

Generic code for loading a project:
  * Create a subdirectory for data.
  * Load the root of subprojects onto the file search path.
  * Load the index of subprojects onto the file search path.

@author Wouter Beek
@version 2014/05/27
*/

:- use_module(library(ansi_term)).
:- use_module(library(apply)).



load_project(ChildProjects):-
  user:project(_, _, ParentFsp),

  % Entry point.
  source_file(load_project(_), ThisFile),
  file_directory_name(ThisFile, ThisDir),
  assert(user:file_search_path(ParentFsp, ThisDir)),
  assert(user:file_search_path(project, ThisDir)),

  % Set the data subdirectory.
  set_data_subdirectory(ThisDir),

  % Load the root of submodules onto the file search path.
  maplist(load_subproject(ParentFsp), ChildProjects),
  
  % Load the index into the file search path.
  load_project_index(ParentFsp).



%! load_subproject(
%!   +ParentFileSearchPath:atom,
%!   +Child:or([atom,pair(atom)])
%! ) is det.

load_subproject(ParentFsp, ChildFsp-ChildDir):- !,
  load_subproject_file_search_path(ParentFsp, ChildFsp, ChildDir),
  load_project_index(ChildFsp).
load_subproject(ParentFsp, Child):-
  load_subproject(ParentFsp, Child-Child).


%! load_subproject_file_search_path(
%!   +ParentFileSearchPath:atom,
%!   +ChildFileSearchPath:atom,
%!   +ChildDirectory:atom
%! ) is det.

% The file search path for the subproject has already been set.
load_subproject_file_search_path(_, ChildFsp, _):-
  user:file_search_path(ChildFsp, _).
load_subproject_file_search_path(ParentFsp, ChildFsp, ChildDir):-
  Spec =.. [ParentFsp,ChildDir],
  absolute_file_name(Spec, _, [access(read),file_type(directory)]), !,
  assert(user:file_search_path(ChildFsp, Spec)).
load_subproject_file_search_path(_, ChildFsp, ChildDir):-
  print_message(warning, missing_subproject_directory(ChildFsp,ChildDir)).


%! load_project_index(+FileSearchPath:atom) is det.

load_project_index(Fsp):-
  Spec =.. [Fsp,index],
  absolute_file_name(Spec, File, [access(read),file_type(prolog)]),
  ensure_loaded(File).


%! set_data_subdirectory(+ParentDirectory:atom) is det.

set_data_subdirectory(ParentDir):-
  directory_file_path(ParentDir, data, DataDir),
  make_directory_path(DataDir),
  assert(user:file_search_path(data, DataDir)).



:- multifile(prolog:message//1).

prolog:message(missing_subproject_directory(ChildFsp,ChildDir)) -->
  [
    'The ~a submodule is not present.'-[ChildFsp], nl,
    'Check whether subdirectory ~a is present in your project directory:'-[ChildDir], nl,
    '    git submodule init', nl,
    '    git submodule update'
  ].

