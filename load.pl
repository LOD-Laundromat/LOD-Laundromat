% The load file for the LOD Washing Machine,
% part of the LOD Laundromat project.

:- use_module(library(ansi_term)).

:- dynamic(user:project/3).
:- multifile(user:project/3).
   user:project(llWashingMachine,
       'Where we clean other people\'s dirty data', lwm).

:- use_module(load_project).
:- load_project([
     plc-'Prolog-Library-Collection',
     plDcg,
     plGraph,
     plHtml,
     plHttp,
     plLangTag,
     plRdf,
     plServer,
     plSet,
     plSparql,
     plTabular,
     plTms,
     plTree,
     plUri,
     plXml,
     plXsd
]).

