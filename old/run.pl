/* LOD Washing Machine: Stand-alone startup

Initializes the LOD Washing Machine.

The LOD Washing Machine requires an accessible LOD Laundromat server
that serves the cleaned files and an accessible SPARQL endpoint
for storing the metadata. See module [lwm_settings] for this.

@author Wouter Beek
@version 2015/11
*/

%:- set_prolog_stack(global, limit(125*10**9)).

:- ensure_loaded(config).

:- use_module(library(option_ext)).
:- use_module(library(optparse)).
:- use_module(library(os/thread_ext)).
:- use_module(library(semweb/rdf_db)).

:- qb_alias(error, 'http://lodlaundromat.org/error/ontology/').
:- qb_alias(httpo, 'http://lodlaundromat.org/http/ontology/').
:- qb_alias(ll, 'http://lodlaundromat.org/resource/').
:- qb_alias(llo, 'http://lodlaundromat.org/ontology/').

:- use_module('LOD-Laundromat'(debug/lwm_debug)).
:- use_module('LOD-Laundromat'(lwm_clean)).
:- use_module('LOD-Laundromat'(lwm_continue)).
:- use_module('LOD-Laundromat'(lwm_restart)).
:- use_module('LOD-Laundromat'(lwm_settings)).
:- use_module('LOD-Laundromat'(lwm_unpack)).
:- use_module('LOD-Laundromat'(debug/debug_datadoc)).

:- initialization(init).

init:-
  default_number_of_threads(DefaultNumberOfThreads),
  OptSpec= [
    [ % Datadoc
      help('Debug a specific data document based on its MD5.'),
      longflags([datadoc]),
      opt(datadoc),
      type(atom)
    ],
    [ % Debug
      default(false),
      help('Whether debug messages are displayed or not.'),
      longflags([debug]),
      opt(debug),
      type(boolean)
    ],
    [ % Directory
      help('The directory where the cleaned data is stored.'),
      longflags([dir,directory]),
      opt(directory),
      type(atom)
    ],
    [ % Help
      default(false),
      help('Enumerate the supported command-line options.'),
      longflags([help]),
      opt(help),
      shortflags([h]),
      type(boolean)
    ],
    [ % Port
      help('The port at which the triple store for the scrape metadata \c
            can be reached.'),
      longflags([port]),
      opt(port),
      shortflags([p]),
      type(integer)
    ]
  ],
  opt_arguments(OptSpec, Opts, _),

  % Process debug option.
  if_option(debug(true), Opts, set_debug_flags),

  % Process help.
  if_option(help(true), Opts, show_help(OptSpec)),

  init(Opts).

init(Opts):-
  % Process the directory option.
  option(directory(Dir), Opts),
  make_directory_path(Dir),
  retractall(user:file_search_path(data, _)),
  assert(user:file_search_path(data, Dir)),

  % Initialization phase.
  clean_lwm_state,

  % Set the port of the LOD Laundromat Endpoint.
  option(port(Port), Opts),
  init_lwm_settings(Port),

  % Either process a specific data documents in a single thread (debug)
  % or start a couple of continuous threads (production).
  (   option(debug(true), Opts),
      option(datadoc(Datadoc0), Opts),
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




% HELPERS %

start_large_thread(Id):-
  format(atom(Alias), 'clean_large_~d', [Id]),
  GlobalStack is 125 * (1024 ** 3), % 125 GB
  Min is 3.5 * (1024 ** 3), % 3.5 GB
  thread_create(
    lwm_clean_loop(clean_large, Min, _),
    _,
    [alias(Alias),detached(true),global(GlobalStack)]
  ).


start_medium_thread(Id):-
  format(atom(Alias), 'clean_medium_~d', [Id]),
  GlobalStack is 10.5 * (1024 ** 3), % 10.5 GB
  Min is 1 * (1024 ** 3), % 1 GB
  Max is 3.5 * (1024 ** 3), % 3.5 GB
  thread_create(
    lwm_clean_loop(clean_medium, Min, Max),
    _,
    [alias(Alias),detached(true),global(GlobalStack)]
  ).


start_small_thread(Id):-
  format(atom(Alias), 'clean_small_~d', [Id]),
  GlobalStack is 3 * (1024 ** 3), % 3 GB
  Max is 1 * (1024 ** 3), % 1 GB
  thread_create(
    lwm_clean_loop(clean_small, _, Max),
    _,
    [alias(Alias),detached(true),global(GlobalStack)]
  ).


start_unpack_thread(Id):-
  format(atom(Alias), 'unpack_~d', [Id]),
  thread_create(lwm_unpack_loop, _, [alias(Alias),detached(true)]).
