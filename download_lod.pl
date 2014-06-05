:- module(download_lod, []).

/** <module> Run download LOD

Initializes the downloading of LOD.

@author Wouter Beek
@version 2014/03-2014/06
*/

:- use_module(library(filesex)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(uri)).

:- use_module(generics(thread_ext)).

:- use_module(lwm(download_lod_file)).
:- use_module(lwm(download_lod_generics)).
:- use_module(lwm(lod_urls)).

%! failed(?Url:url) is nondet.

:- dynamic(failed/1).

%! finished(?Url:url) is nondet.

:- dynamic(finished/1).

:- initialization(process_lod_urls).
:- initialization(update_lod_urls).



init_process_lod_urls:-
  flag(number_of_processed_files, _, 0),
  flag(number_of_skipped_files, _, 0),
  flag(number_of_triples_written, _, 0),

  % Set the directory where the data is stored.
  absolute_file_name(data(.), DataDir, [access(write),file_type(directory)]),
  set_data_directory(DataDir),

  % Make sure the output directory is there.
  directory_file_path(DataDir, 'Output', OutputDir),
  make_directory_path(OutputDir),

  % Read which URLs were loaded and have failed earlier.
  read_lod_urls(DataDir),

  % Each file is loaded in an RDF serialization + snapshot.
  % These inherit the triples that are not in an RDF serialization.
  % We therefore have to clear all such triples before we begin.
  rdf_retractall(_, _, _).


%! new_lod_url(-Url:url) is nondet.

% Exclude URIs that were succesfully processed in the past.
new_lod_url(Url):-
  finished(Url), !,
  fail.
% Exclude URIs that were unsuccesfully processed in the past.
new_lod_url(Url):-
  failed(Url), !,
  fail.
new_lod_url(Url2):-
  lod_url(Url1),
  % Make sure that it is a URL.
  uri_iri(Url2, Url1).


process_lod_urls:-
  init_process_lod_urls,

  % Pick one new URL to process.
  once(new_lod_url(Url)),

  download_lod_file(Url).
% Done for now. Check whether there are new jobs in one minute.
process_lod_urls:-
  sleep(60),
  process_lod_urls.


%! read_lod_urls(+DataDirectory:atom) is det.

read_lod_urls(DataDir):-
  read_lod_urls(failed, DataDir),
  read_lod_urls(finished, DataDir).


%! read_lod_urls(+Kind:oneof([failed,finished]), +DataDirectory:atom) is det.

read_lod_urls(Kind, DataDir):-
  absolute_file_name(
    Kind,
    File,
    [access(read),file_errors(fail),file_type(logging),relative_to(DataDir)]
  ), !,
  ensure_loaded(File).
read_lod_urls(_, _).


%! update_lod_urls is det.
% Starts the intermittent thread that keeps the list of LOD URLs up-to-date.

update_lod_urls:-
  % Run every minute.
  intermittent_thread(use_module(lod_urls), fail, 60, _, []).

