% Standalone startup file for the LOD Washing Machine.

% The LOD Washing Machine requires an accessible LOD Laundromat server
% that serves the cleaned files and an accessible SPARQL endpoint
% for storing the metadata.

:- ensure_loaded(debug).
:- ensure_loaded(load).

:- use_module(lwm(run_singlethread)).
:- run_washing_machine.

