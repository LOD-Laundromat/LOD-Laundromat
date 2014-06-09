:- module(lwm_start, []).

/** <module> LOD Washing Machine: start

Initializes the downloading and cleaning of LOD in a single-threaded process.

See module [lwm_start_threaded] for the threaded version of this module.

@author Wouter Beek
@version 2014/03-2014/06
*/

:- use_module(library(filesex)).
:- use_module(library(semweb/rdf_db)).

:- use_module(generics(thread_ext)).

:- use_module(lwm(download_lod_file)).
:- use_module(lwm(lwm_basket)).
:- use_module(lwm(lwm_generics)).
:- use_module(lwm(lwm_history)).

:- initialization(run_washing_machine).
:- initialization(refresh_lod_basket).



%! new_dirty_item(-Md5:atom) is nondet.
% Pick a new dirty item from the LOD Basket.
% We make sure that this item has not been washed before.

new_dirty_item(Md5):-
  pending_source(Md5),
  
  % Exclude URIs that were unsuccesfully processed in the past.
  \+ has_failed(Md5),
  
  % Exclude URIs that were succesfully processed in the past.
  \+ has_finished(Md5).


run_washing_machine:-
  init_washing_machine,
  thread_create(washing_machine_loop, _, []).

washing_machine_loop:-
  % Pick one new URL to process.
  once(new_dirty_item(Md5)),

  % Process the URL we picked.
  clean_datadoc(Md5),

  % Intermittent loop.
  washing_machine_loop.
% Done for now. Check whether there are new jobs in one minute.
washing_machine_loop:-
  sleep(60),
  washing_machine_loop.


%! refresh_lod_basket is det.
% Starts the intermittent thread that keeps the contents
% of the LOD Basket up-to-date.

refresh_lod_basket:-
  % Run every minute.
  intermittent_thread(use_module(lod_basket), fail, 60, _, []).



% INITIALIZATION

init_washing_machine:-
  flag(number_of_processed_files, _, 0),
  flag(number_of_skipped_files, _, 0),
  flag(number_of_triples_written, _, 0),

  % Set the directory where the data is stored.
  absolute_file_name(data(.), DataDir, [access(write),file_type(directory)]),
  set_data_directory(DataDir),

  % Make sure the output directory is there.
  directory_file_path(DataDir, 'Output', OutputDir),
  make_directory_path(OutputDir),

  % Each file is loaded in an RDF serialization + snapshot.
  % These inherit the triples that are not in an RDF serialization.
  % We therefore have to clear all such triples before we begin.
  rdf_retractall(_, _, _).

