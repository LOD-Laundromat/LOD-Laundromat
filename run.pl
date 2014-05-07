% The run file for the LOD-Washing-Machine project.

:- initialization(run_lwm).

run_lwm:-
  % Entry point.
  source_file(run_lwm, ThisFile),
  file_directory_name(ThisFile, ThisDir),
  assert(user:file_search_path(project, ThisDir)),
  
  % PGC
  load_plc(project),
  
  % Set the data subdirectory.
  set_data_directory(ThisDir),
  
  % LOD-Washing-Machine load file.
  ensure_loaded(load).

load_plc(_Project):-
  user:file_search_path(plc, _Spec), !.
load_plc(Project):-
  Spec =.. [Project,'Prolog-Library-Collection'],
  assert(user:file_search_path(plc, Spec)),
  load_or_debug(plc).

load_or_debug(Project):-
  predicate_property(user:debug_mode, visible), !,
  Spec =.. [Project,debug],
  ensure_loaded(Spec).
load_or_debug(Project):-
  Spec =.. [Project,load],
  ensure_loaded(Spec).

set_data_directory(ParentDir):-
  directory_file_path(ParentDir, data, DataDir),
  ensure_directory_exists(DataDir),
  user:file_search_path(data, DataDir).

ensure_directory_exists(Dir):-
  exists_directory(Dir), !.
ensure_directory_exists(Dir):-
  make_directory(Dir).

