:- module(
  download_lod_generics,
  [
    data_directory/1, % -DataDirectory:atom
    has_failed/1, % +Url:url
    has_finished/1, % +Url:url
    report_failed/1, % +Url:url
    report_finished/1, % +Url:url
    set_data_directory/1, % +DataDirectory:atom
    scrape_version/1 % -Version:positive_integer
  ]
).

/** <module> Download LOD generics

Generic predicates that are used in the LOD download process.

@author Wouter Beek
@version 2014/04-2014/06
*/

:- use_module(library(persistency)).

:- use_module(generics(db_ext)).
:- use_module(os(file_ext)).

:- db_add_novel(user:prolog_file_type(log, logging)).

:- dynamic(data_directory/1).

%! failed(?Url:url) is nondet.

:- persistent(failed(url:atom)).

%! finished(?Url:url) is nondet.

:- persistent(finished(url:atom)).

:- initialization(init_lod_history).



has_failed(Url):-
  failed(Url).

has_finished(Url):-
  finished(Url).

report_failed(Url):-
  assert_failed(Url).

report_finished(Url):-
  assert_finished(Url).


%! set_data_directory(+DataDirectory:atom) is det.

set_data_directory(DataDir):-
  % Assert the data directory.
  db_replace_novel(data_directory(DataDir), [e]).


scrape_version(7).



% INITIALIZATION

%! init_lod_history is det.
% Read which URLs were loaded and have failed earlier.

init_lod_history:-
  lod_history_file(File),
  safe_db_attach(File).


%! lod_history_file(-File:atom) is det.

lod_history_file(File):-
  absolute_file_name(data(history), File, [access(write),file_type(logging)]).


%! safe_db_attach(+File:atom) is det.

safe_db_attach(File):-
  exists_file(File), !,
  db_attach(File, []).
safe_db_attach(File):-
  touch_file(File),
  safe_db_attach(File).

