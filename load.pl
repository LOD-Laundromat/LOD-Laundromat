% The load file for the LOD-Laundromat project.

:- use_module(library(ansi_term)).

:- multifile(user:project/3).
:- dynamic(user:project/3).
   user:project('LOD-Laundromat',
       'Where we clean other people\'s dirty data', ll).

:- use_module(load_project).
:- load_project([
    plc-'Prolog-Library-Collection',
    plDev,
    plGraph,
    plGraphViz,
    plHtml,
    plRdf,
    plServer,
    plSparql,
    plTabular
]).

