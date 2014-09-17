:- module(lwm_init, []).

/** <module> LOD Washing Machine (LWM): initialization

Initializes the LOD Washing Machine.

@author Wouter Beek
@version 2014/06, 2014/08
*/

:- use_module(library(debug)).
:- use_module(library(filesex)).
:- use_module(library(optparse)).
:- use_module(library(semweb/rdf_db)).

:- use_module(os(dir_ext)).

:- use_module(lwm(lwm_restart)).

:- initialization(lwm_init).



lwm_init:-
  % Read the command-line arguments.
  absolute_file_name(data(.), DefaultDir, [file_type(directory)]),
  opt_arguments(
    [
      [default(false),opt(debug),longflags([debug]),type(boolean)],
      [default(DefaultDir),opt(directory),longflags([dir]),type(atom)],
      [default(false),opt(restart),longflags([restart]),type(boolean)],
      [default(false),opt(continue),longflags([continue]),type(boolean)]
    ],
    Opts,
    _
  ),

  % Process the directory option.
  memberchk(directory(Dir), Opts),
  make_directory_path(Dir),
  retractall(user:file_search_path(data, _)),
  assert(user:file_search_path(data, Dir)),

  % Process the restart option.
  (   memberchk(restart(true), Opts)
  ->  lwm_restart
  ;   debugging(lwm),
      memberchk(continue(true), Opts)
  ->  lwm_continue
  ;   true
  ),

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

