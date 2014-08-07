:- module(lwm_init, []).

/** <module> LOD Washing Machine (LWM): initialization

Initializes the LOD Washing Machine.

@author Wouter Beek
@version 2014/06, 2014/08
*/

:- use_module(library(semweb/rdf_db)).

:- use_module(generics(db_ext)).
:- use_module(os(dir_ext)).

:- initialization(lwm_init).



lwm_init:-
  % Set the directory where the data is stored.
  absolute_file_name(data(.), DataDir, [access(write),file_type(directory)]),
  create_directory(DataDir),

  % Each file is loaded in an RDF serialization + snapshot.
  % These inherit the triples that are not in an RDF serialization.
  % We therefore have to clear all such triples before we begin.
  forall(
    rdf_graph(G),
    rdf_unload_graph(G)
  ),
  rdf_retractall(_, _, _, _).

