:- module(
  lwm_history,
  [
    has_failed/1, % +Md5:atom
    has_finished/1, % +Md5:atom
    report_failed/1, % +Md5:atom
    report_finished/1 % +Md5:atom
  ]
).

/** <module> LOD Washing Machine: history

@author Wouter Beek
@version 2014/04-2014/06
*/

:- use_module(library(persistency)).

:- use_module(generics(db_ext)).
:- use_module(os(file_ext)).

:- db_add_novel(user:prolog_file_type(log, logging)).

%! failed(?Md5:atom) is nondet.

:- persistent(failed(md5:atom)).

%! finished(?Md5:atom) is nondet.

:- persistent(finished(md5:atom)).

:- initialization(init_lwm_history).



has_failed(Md5):-
  with_mutex(lwm_history, failed(Md5)).

has_finished(Md5):-
  with_mutex(lwm_history, finished(Md5)).

report_failed(Md5):-
  with_mutex(lwm_history, assert_failed(Md5)).

report_finished(Md5):-
  with_mutex(lwm_history, assert_finished(Md5)).



% INITIALIZATION

%! init_lwm_history is det.
% Read which URLs were loaded and have failed earlier.

init_lwm_history:-
  lwm_history_file(File),
  safe_db_attach(File).

%! lwm_history_file(-File:atom) is det.

lwm_history_file(File):-
  absolute_file_name(
    data(lwm_history),
    File,
    [access(write),file_type(logging)]
  ).

%! safe_db_attach(+File:atom) is det.

safe_db_attach(File):-
  exists_file(File), !,
  db_attach(File, []).
safe_db_attach(File):-
  touch_file(File),
  safe_db_attach(File).

