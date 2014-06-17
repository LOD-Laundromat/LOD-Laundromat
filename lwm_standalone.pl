% This is the script for running the LOD Washing Machine.
% This requires an accessible LOD Laundromat server that serves
% the cleaned files and an accessible SPARQL endpoint
% for storing the metadata.

:- ensure_loaded(debug).
:- ensure_loaded(load).

%%%%:- use_module(library(swi_ide)).
%%%%:- prolog_ide(debug_monitor).
%%%%:- debug(sparql_request).

:- use_module(lwm(lwm_start)).
:- run_washing_machine.
