/* LOD Washing Machine: Stand-alone startup

Initializes the LOD Washing Machine.

The LOD Washing Machine requires an accessible LOD Laundromat server
that serves the cleaned files and an accessible SPARQL endpoint
for storing the metadata. See module [lwm_settings] for this.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09, 2014/11, 2015/01
*/

:- set_prolog_stack(global, limit(125*10**9)).


:- if(current_prolog_flag(argv, ['--debug'|_])).
  :- ensure_loaded(debug).
:- else.
  :- ensure_loaded(load).
:- endif.

:- use_module(library(option)).
:- use_module(library(optparse)).
:- use_module(library(semweb/rdf_db), except([rdf_node/1])).

:- use_module(generics(typecheck)).

:- use_module(lwm(lwm_clean)).
:- use_module(lwm(lwm_continue)).
:- use_module(lwm(lwm_restart)).
:- use_module(lwm(lwm_settings)).
:- use_module(lwm(lwm_unpack)).
:- use_module(lwm(debug/debug_datadoc)).

:- initialization(init).



init:-
  % Read the command-line arguments.
  absolute_file_name(data(.), DefaultDir, [file_type(directory)]),
  OptSpec= [
    [
      help('Debug a specific data document based on its MD5.'),
      longflags([datadoc]),
      opt(datadoc),
      type(atom)
    ],
    [
      default(false),
      help('Whether debug messages are displayed or not.'),
      longflags([debug]),
      opt(debug),
      type(boolean)
    ],
    [
      default(DefaultDir),
      help('The directory where the cleaned data is stored.'),
      longflags([dir,directory]),
      opt(directory),
      type(atom)
    ],
    [
      default(false),
      help('Enumerate the supported command-line options.'),
      longflags([help]),
      opt(help),
      shortflags([h]),
      type(boolean)
    ],
    [
      default(4001),
      help('The port at which the triple store for the scrape metadata \c
            can be reached.'),
      longflags([port]),
      opt(port),
      shortflags([p]),
      type(integer)
    ],
    [
      default(default),
      help('The mode in which the LOD Washing Machine runs.\c
            Possible values are `default` (which is the default),\c
            `continue`, and `restart`.'),
      longflags([mode]),
      opt(mode),
      shortflags([m]),
      type(atom)
    ]
  ],
  opt_arguments(OptSpec, Options, _),

  % Process help.
  (   option(help(true), Options)
  ->  opt_help(OptSpec, Help),
      format(user_output, '~a\n', [Help]),
      halt
  ;   init(Options)
  ).

init(Options):-
  % Process the directory option.
  option(directory(Dir), Options),
  make_directory_path(Dir),
  retractall(user:file_search_path(data, _)),
  assert(user:file_search_path(data, Dir)),

  % Initialization phase.
  clean_lwm_state,

  % Set the port of the LOD Laundromat Endpoint.
  option(port(Port), Options),
  init_lwm_settings(Port),

  % Process the restart or continue option.
  option(mode(Mode), Options),
  (   Mode == restart
  ->  lwm_restart
  ;   Mode == continue
  ->  lwm_continue
  ;   true
  ),

  % Either process a specific data documents in a single thread (debug)
  % or start a couple of continuous threads (production).
  (   option(debug(true), Options),
      option(datadoc(Datadoc0), Options),
      ground(Datadoc0)
  ->  ensure_datadoc(Datadoc0, Datadoc),
      gtrace, %DEB
      debug_datadoc(Datadoc)
  ;   init_production
  ).

ensure_datadoc(Datadoc, Datadoc):-
  is_uri(Datadoc), !.
ensure_datadoc(Md5, Datadoc):-
  rdf_global_id(ll:Md5, Datadoc).

init_production:-
  lwm_settings:setting(
    number_of_small_cleaning_threads,
    NumberOfSmallCleaningThreads
  ),
  lwm_settings:setting(
    number_of_medium_cleaning_threads,
    NumberOfMediumCleaningThreads
  ),
  lwm_settings:setting(
    number_of_large_cleaning_threads,
    NumberOfLargeCleaningThreads
  ),
  lwm_settings:setting(
    number_of_unpacking_threads,
    NumberOfUnpackingThreads
  ),
  init_production(
    NumberOfUnpackingThreads,
    NumberOfSmallCleaningThreads,
    NumberOfMediumCleaningThreads,
    NumberOfLargeCleaningThreads
  ).

%! init_production(
%!   +NumberOfUnpackingThreads:nonneg,
%!   +NumberOfSmallCleaningThreads:nonneg,
%!   +NumberOfMediumCleaningThreads:nonneg,
%!   +NumberOfLargeCleaningThreads:nonneg
%! ) is det.

init_production(
  NumberOfUnpackingThreads,
  NumberOfSmallCleaningThreads,
  NumberOfMediumCleaningThreads,
  NumberOfLargeCleaningThreads
):-
  % Start the downloading+unpacking threads.
  forall(
    between(1, NumberOfUnpackingThreads, UnpackId),
    start_unpack_thread(UnpackId)
  ),

  % Start the cleaning threads:
  %   1. Clean small files.
  forall(
    between(1, NumberOfSmallCleaningThreads, SmallCleanId),
    start_small_thread(SmallCleanId)
  ),
  %   2. Clean medium files.
  forall(
    between(1, NumberOfMediumCleaningThreads, MediumCleanId),
    start_medium_thread(MediumCleanId)
  ),
  %   3. Clean large files.
  forall(
    between(1, NumberOfLargeCleaningThreads, LargeCleanId),
    start_large_thread(LargeCleanId)
  ).


clean_lwm_state:-
  % Each file is loaded in an RDF serialization + snapshot.
  % These inherit the triples that are not in an RDF serialization.
  % We therefore have to clear all such triples before we begin.
  forall(
    rdf_graph(G),
    rdf_unload_graph(G)
  ),
  rdf_retractall(_, _, _, _).





% HELPERS %

start_large_thread(Id):-
  format(atom(Alias), 'clean_large_~d', [Id]),
  GlobalStack is 125 * (1024 ** 3), % 125 GB
  Min is 2.5 * (1024 ** 3), % 2.5 GB
  thread_create(
    lwm_clean_loop(clean_large, Min, _),
    _,
    [alias(Alias),detached(true),global(GlobalStack)]
  ).


start_medium_thread(Id):-
  format(atom(Alias), 'clean_medium_~d', [Id]),
  GlobalStack is 10 * (1024 ** 3), % 10 GB
  Min is 0.5 * (1024 ** 3), % 0.5 GB
  Max is 2.5 * (1024 ** 3), % 2.5 GB
  thread_create(
    lwm_clean_loop(clean_medium, Min, Max),
    _,
    [alias(Alias),detached(true),global(GlobalStack)]
  ).


start_small_thread(Id):-
  format(atom(Alias), 'clean_small_~d', [Id]),
  GlobalStack is 1.5 * (1024 ** 3), % 1.5 GB
  Max is 0.5 * (1024 ** 3), % 0.5 GB
  thread_create(
    lwm_clean_loop(clean_small, _, Max),
    _,
    [alias(Alias),detached(true),global(GlobalStack)]
  ).


start_unpack_thread(Id):-
  format(atom(Alias), 'unpack_~d', [Id]),
  thread_create(lwm_unpack_loop, _, [alias(Alias),detached(true)]).

