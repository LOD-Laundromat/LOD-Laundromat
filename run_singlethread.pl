:- module(
  run_singlethread,
  [
    run_singlethread/0
  ]
).

/** <module> Run single-threaded

Initializes the downloading and cleaning of LOD in a single-threaded process.

See module [run_multithread] for the threaded version of this module.

@author Wouter Beek
@version 2014/03-2014/06
*/

:- use_module(library(semweb/rdf_db)).

:- use_module(lwm(lwm_clean)).
:- use_module(lwm(lwm_generics)).
:- use_module(lwm(lwm_unpack)).



run_singlethread:-
  init_washing_machine,
  forall(
    between(1, 5, _),
    thread_create(lwm_unpack_loop, _, [detached(true)])
  ),
  forall(
    between(1, 1, _),
    thread_create(lwm_clean_loop, _, [detached(true)])
  ).

init_washing_machine:-
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

