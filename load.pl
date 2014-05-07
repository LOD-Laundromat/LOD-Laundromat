% The load file for the LOD-Washing-Machine project.

:- multifile(user:project/2).
user:project('LOD-Washing-Machine', '').

:- initialization(load_lwm).

load_lwm:-
  % Entry point.
  source_file(load_lwm, ThisFile),
  file_directory_name(ThisFile, ThisDir),

  % LWM
  assert(user:file_search_path(lwm, ThisDir)),
  use_module(lwm(lod_laundry)).

