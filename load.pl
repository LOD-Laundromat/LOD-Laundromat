% The load file for the LOD Washing Machine.
% This is part of the LOD Laundromat.

:- dynamic(user:project/3).
:- multifile(user:project/3).
user:project(
  llWashingMachine,
  'Where we clean other people\'s dirty data',
  lwm
).


:- use_module(load_project).
:- load_project([
  plc-'Prolog-Library-Collection',
  plGraph,
  plHtml,
  plHttp,
  plLangTag,
  plRdf,
  plSet,
  plSparql,
  plTree,
  plUri,
  plXml,
  plXsd
]).


:- use_module(library(semweb/rdf_db), except([rdf_node/1])).

:- rdf_register_prefix(error, 'http://lodlaundromat.org/error/ontology/').
:- rdf_register_prefix(httpo, 'http://lodlaundromat.org/http/ontology/').
:- rdf_register_prefix(ll, 'http://lodlaundromat.org/resource/').
:- rdf_register_prefix(llo, 'http://lodlaundromat.org/ontology/').

