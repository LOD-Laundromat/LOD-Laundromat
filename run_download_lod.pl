:- module(
  run_download_lod,
  [
    run_download_lod/0,
    run_download_lod/1 % +NumberOfThreads:nonneg
  ]
).

/** <module> Run download LOD

Initializes the downloading of LOD.

@author Wouter Beek
@version 2014/03-2014/05
*/

:- use_module(library(filesex)).

:- use_module(lod(download_lod)).
:- use_module(lod(lod_urls)).



run_download_lod:-
  run_download_lod(1).

run_download_lod(N):-
  % Make sure the output directory is there.
  absolute_file_name(data(.), DataDir, [access(write),file_type(directory)]),
  directory_file_path(DataDir, 'Output', OutputDir),
  make_directory_path(OutputDir),
  
  % Collect URLs.
  findall(
    Url,
    lod_url(Url),
    Urls
  ),

  download_lod(OutputDir, Urls, N).

