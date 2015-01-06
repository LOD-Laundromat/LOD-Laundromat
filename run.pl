/* LOD Washing Machine: Stand-alone startup

Initializes the LOD Washing Machine.

The LOD Washing Machine requires an accessible LOD Laundromat server
that serves the cleaned files and an accessible SPARQL endpoint
for storing the metadata. See module [lwm_settings] for this.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09, 2014/11
*/

:- set_prolog_stack(global, limit(125*10**9)).


:- if(current_prolog_flag(argv, ['--debug'])).
  :- ensure_loaded(debug).
:- else.
  :- ensure_loaded(load).
:- endif.


:- use_module(library(optparse)).
:- use_module(library(semweb/rdf_db), except([rdf_node/1])).

:- use_module(plServer(app_server)).
:- use_module(plServer(web_modules)). % Web module registration.

:- use_module(lwm(lwm_clean)).
:- use_module(lwm(lwm_continue)).
:- use_module(lwm(lwm_restart)).
:- use_module(lwm(lwm_unpack)).
:- use_module(lwm(debug/lwm_progress)).

:- http_handler(root(progress), lwm_progress, [id(lwm_progress)]).

:- dynamic(user:web_module/2).
:- multifile(user:web_module/2).

user:current_html_style(menu_page).

user:web_module('LWM Progress', lwm_progress).

:- initialization(init).



init:-
  start_app_server([workers(2)]),

  clean_lwm_state,
  process_command_line_arguments,
  NumberOfUnpackThreads = 10,
  NumberOfSmallCleanThreads = 12,
  NumberOfMediumCleanThreads = 1,
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
  %%%%flag(number_of_pending_md5s, _, 0),
  flag(store_new_url, _, 0),

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
  GlobalStack is 100 * (1024 ** 3), % 100 GB
  Min is 2.5 * (1024 ** 3), % 2.5 GB
  Max is 20 * (1024 ** 3), % 20 GB
  thread_create(
    lwm_clean_loop(clean_large, Min, Max),
    _,
    [alias(Alias),detached(true),global(GlobalStack)]
  ).


start_medium_thread(Id):-
  format(atom(Alias), 'clean_medium_~d', [Id]),
  GlobalStack is 12 * (1024 ** 3), % 12 GB
  Min is 0.5 * (1024 ** 3), % 0.5 GB
  Max is 2.5 * (1024 ** 3), % 2.5 GB
  thread_create(
    lwm_clean_loop(clean_medium, Min, Max),
    _,
    [alias(Alias),detached(true),global(GlobalStack)]
  ).


start_small_thread(Id):-
  format(atom(Alias), 'clean_small_~d', [Id]),
  GlobalStack is 2 * (1024 ** 3), % 2 GB
  Max is 0.5 * (1024 ** 3), % 0.5 GB
  thread_create(
    lwm_clean_loop(clean_small, _, Max),
    _,
    [alias(Alias),detached(true),global(GlobalStack)]
  ).


start_unpack_thread(Id):-
  format(atom(Alias), 'unpack_~d', [Id]),
  thread_create(lwm_unpack_loop, _, [alias(Alias),detached(true)]).

