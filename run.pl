/* LOD Washing Machine: Stand-alone startup

Initializes the LOD Washing Machine.

The LOD Washing Machine requires an accessible LOD Laundromat server
that serves the cleaned files and an accessible SPARQL endpoint
for storing the metadata. See module [lwm_settings] for this.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09
*/

:- [debug].
:- [load].

:- use_module(library(optparse)).
:- use_module(library(semweb/rdf_db)).

:- use_module(math(float_ext)).
:- use_module(os(dir_ext)).

:- use_module(lwm(lwm_clean)).
:- use_module(lwm(lwm_continue)).
:- use_module(lwm(lwm_restart)).
:- use_module(lwm(lwm_unpack)).

:- dynamic(lwm:current_authority/1).
:- multifile(lwm:current_authority/1).

:- initialization(init).



init:-
  clean_lwm_state,
  process_command_line_arguments,
  NumberOfUnpackThreads = 30,
  NumberOfSmallCleanThreads = 7,
  NumberOfMediumCleanThreads = 2,
  NumberOfLargeCleanThreads = 1,
  
  % Start the downloading+unpacking threads.
  forall(
    between(1, NumberOfUnpackThreads, UnpackId),
    start_unpack_thread(UnpackId)
  ),
  
  % Start the cleaning threads:
  %   1. Clean small files.
  forall(
    between(1, NumberOfSmallCleanThreads, SmallCleanId),
    start_small_thread(SmallCleanId)
  ),
  %   2. Clean medium files.
  forall(
    between(1, NumberOfMediumCleanThreads, MediumCleanId),
    start_medium_thread(MediumCleanId)
  ),
  %   3. Clean large files.
  forall(
    between(1, NumberOfLargeCleanThreads, LargeCleanId),
    start_large_thread(LargeCleanId)
  ).


clean_lwm_state:-
  % Reset the authorities from which
  % data documents are currently being downloaded.
  retractall(current_authority(_)),

  %%%%% Reset the count of pending MD5s.
  %%%%flag(number_of_pending_md5s, _, 0),

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


process_command_line_arguments:-
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

  % Process the restart or continue option.
  (   memberchk(restart(true), Opts)
  ->  lwm_restart
  ;   memberchk(continue(true), Opts)
  ->  lwm_continue
  ;   true
  ).



% Helpers

start_large_thread(Id):-
  format(atom(Alias), 'clean_large_~d', [Id]),
  thread_create(
    lwm_clean_loop(clean_large, float_between(0.75,_)),
    _,
    [alias(Alias),detached(true)]
  ).


start_medium_thread(Id):-
  format(atom(Alias), 'clean_medium_~d', [Id]),
  thread_create(
    lwm_clean_loop(clean_medium, float_between(0.25,0.75)),
    _,
    [alias(Alias),detached(true)]
  ).


start_small_thread(Id):-
  format(atom(Alias), 'clean_small_~d', [Id]),
  thread_create(
    lwm_clean_loop(clean_small, float_between(_,0.25)),
    _,
    [alias(Alias),detached(true)]
  ).


start_unpack_thread(Id):-
  format(atom(Alias), 'unpack_~d', [Id]),
  thread_create(lwm_unpack_loop, _, [alias(Alias),detached(true)]).

