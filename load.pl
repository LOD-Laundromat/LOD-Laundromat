% The load file for the LOD-Washing-Machine project.

:- use_module(library(ansi_term)).

:- initialization(load_lwm).

load_lwm:-
  % Entry point.
  source_file(load_lwm, ThisFile),
  file_directory_name(ThisFile, ThisDir),
  assert(user:file_search_path(lwm, ThisDir)),
  
  % Data subdirectory.
  directory_file_path(ThisDir, data, DataDir),
  make_directory_path(DataDir),
  
  % File search paths.
  ensure_loaded(lwm(index)),
  
  % Load submodules.
  load_plc(lwm),
  load_plHtml(lwm),
  load_plServer(lwm),
  load_plDev(lwm),
  load_plRdf(lwm),
  load_plRdfDev(lwm),
  
  % Load the Web-based development environment.
  use_module(plDev(plDev)).


load_plc(_):-
  user:file_search_path(plc, _), !.
load_plc(Project):-
  Spec =.. [Project,'Prolog-Library-Collection'],
  absolute_file_name(Spec, _, [access(read),file_type(directory)]), !,
  assert(user:file_search_path(plc, Spec)),
  ensure_loaded(plc(index)).
load_plc(_):-
  print_message(warning, no_plc).

load_plHtml(_):-
  user:file_search_path(plHtml, _), !.
load_plHtml(Project):-
  Spec =.. [Project,plHtml],
  absolute_file_name(Spec, _, [access(read),file_type(directory)]), !,
  assert(user:file_search_path(plHtml, Spec)).
load_plHtml(_):-
  print_message(warning, no_plHtml).

load_plServer(_):-
  user:file_search_path(plServer, _), !.
load_plServer(Project):-
  Spec =.. [Project,plServer],
  absolute_file_name(Spec, _, [access(read),file_type(directory)]), !,
  assert(user:file_search_path(plServer, Spec)).
load_plServer(_):-
  print_message(warning, no_plServer).

load_plDev(_):-
  user:file_search_path(plDev, _), !.
load_plDev(Project):-
  Spec =.. [Project,plDev],
  absolute_file_name(Spec, _, [access(read),file_type(directory)]), !,
  assert(user:file_search_path(plDev, Spec)).
load_plDev(_):-
  print_message(warning, no_plDev).

load_plRdfDev(_):-
  user:file_search_path(plRdfDev, _), !.
load_plRdfDev(Project):-
  Spec =.. [Project,'plRdf-Dev'],
  absolute_file_name(Spec, _, [access(read),file_type(directory)]), !,
  assert(user:file_search_path(plRdfDev, Spec)).
load_plRdfDev(_):-
  print_message(warning, no_plRdfDev).

load_plRdf(_):-
  user:file_search_path(plRdf, _), !.
load_plRdf(Project):-
  Spec =.. [Project,plRdf],
  absolute_file_name(Spec, _, [access(read),file_type(directory)]), !,
  assert(user:file_search_path(plRdf, Spec)).
load_plRdf(_):-
  print_message(warning, no_plRdf).


:- multifile(prolog:message//1).

prolog:message(no_plc) -->
  [
    'The Prolog-Library-Collection submodule is not present.', nl,
    'Consider running the following from within the LOD-Washing-Machine directory:', nl,
    '    git submodule init', nl,
    '    git submodule update'
  ].
prolog:message(no_plHtml) -->
  [
    'The plHtml submodule is not present.', nl,
    'Consider running the following from within the LOD-Washing-Machine directory:', nl,
    '    git submodule init', nl,
    '    git submodule update'
  ].
prolog:message(no_plServer) -->
  [
    'The plServer submodule is not present.', nl,
    'Consider running the following from within the LOD-Washing-Machine directory:', nl,
    '    git submodule init', nl,
    '    git submodule update'
  ].
prolog:message(no_plDev) -->
  [
    'The plDev submodule is not present.', nl,
    'Consider running the following from within the LOD-Washing-Machine directory:', nl,
    '    git submodule init', nl,
    '    git submodule update'
  ].
prolog:message(no_plRdf) -->
  [
    'The plRdf submodule is not present.', nl,
    'Consider running the following from within the LOD-Washing-Machine directory:', nl,
    '    git submodule init', nl,
    '    git submodule update'
  ].
prolog:message(no_plRdfDev) -->
  [
    'The plRdfDev submodule is not present.', nl,
    'Consider running the following from within the LOD-Washing-Machine directory:', nl,
    '    git submodule init', nl,
    '    git submodule update'
  ].

