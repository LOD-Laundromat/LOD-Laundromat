% The load file for the LOD-Washing-Machine project.

:- use_module(library(ansi_term)).

:- multifile(user:project/3).
:- dynamic(user:project/3).
   user:project('LOD-Washing-Machine',
       'Where we clean other people\'s dirty data', lwm).

:- use_module(load_project).
:- load_project([
    plc-'Prolog-Library-Collection',
    plDev,
    plHtml,
    plRdf,
    plServer,
    plTabular
]).

% Load the Web-based development environment.
:- ensure_loaded(plTabular(set_default_http_handler)).
:- use_module(lwm(run_download_lod)).

