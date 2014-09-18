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

  % Start the downloading+unpacking threads.
  forall(
    between(1, 5, UnpackId),
    (
      format(atom(UnpackAlias), 'unpack_~d', [UnpackId]),
      thread_create(lwm_unpack_loop, _, [alias(UnpackAlias),detached(true)])
    )
  ),

  % Start the cleaning threads:
  %   1. Clean small files.
  forall(
    between(1, 5, SmallCleanId),
    (
      format(atom(SmallCleanAlias), 'clean_small_~d', [SmallCleanId]),
      thread_create(
        lwm_clean_loop(float_between(_,0.25)),
        _,
        [alias(SmallCleanAlias),detached(true)]
      )
    )
  ),
  %   2. Clean medium files.
  thread_create(
    lwm_clean_loop(float_between(0.25,0.75)),
    _,
    [alias(clean_medium),detached(true)]
  ),
  %   3. Clean large files.
  thread_create(
    lwm_clean_loop(float_between(0.75,_)),
    _,
    [alias(clean_large),detached(true)]
  ).



clean_lwm_state:-
  % Reset the authorities from which
  % data documents are currently being downloaded.
  retractall(current_authority(_)),

  % Reset the count of pending MD5s.
  flag(number_of_pending_md5s, _, 0),

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

