:- module(
  lwm_start,
  [
    run_washing_machine/0
  ]
).

/** <module> LOD Washing Machine: start

Initializes the downloading and cleaning of LOD in a single-threaded process.

See module [lwm_start_threaded] for the threaded version of this module.

@author Wouter Beek
@version 2014/03-2014/06
*/

:- use_module(library(semweb/rdf_db)).

:- use_module(lwm(lod_basket)).
:- use_module(lwm(lwm_cleaning)).
:- use_module(lwm(lwm_generics)).



run_washing_machine:-
  init_washing_machine,
  thread_create(washing_machine_loop, _, []).

washing_machine_loop:-
  % Debug.
  flag(loop, X, X + 1),
  writeln(X),

  % Pick a new source to process.
  catch(remove_from_basket(Md5), E, writeln(E)),

  % Process the URL we picked.
  clean(Md5),

  % Intermittent loop.
  washing_machine_loop.
% Done for now. Check whether there are new jobs in one minute.
washing_machine_loop:-
  sleep(1),
  washing_machine_loop.



% Initialization

init_washing_machine:-
  flag(number_of_processed_files, _, 0),
  flag(number_of_skipped_files, _, 0),
  flag(number_of_triples_written, _, 0),

  % Set the directory where the data is stored.
  absolute_file_name(data(.), DataDir, [access(write),file_type(directory)]),
  set_data_directory(DataDir),

  % Each file is loaded in an RDF serialization + snapshot.
  % These inherit the triples that are not in an RDF serialization.
  % We therefore have to clear all such triples before we begin.
  forall(
    rdf_graph(G),
    rdf_unload_graph(G)
  ),
  rdf_retractall(_, _, _, _).

