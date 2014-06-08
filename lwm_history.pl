:- module(
  lwm_history,
  [
    has_failed/2, % +Url:url
                  % +Coordinate:list(nonneg)
    has_finished/2, % +Url:url
                    % +Coordinate:list(nonneg)
    report_failed/2, % +Url:url
                     % +Coordinate:list(nonneg)
    report_finished/2 % +Url:url
                      % +Coordinate:list(nonneg)
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

%! failed(?Url:url, ?Coordinate:list(nonneg)) is nondet.

:- persistent(failed(url:atom,coordinate:list(nonneg))).

%! finished(?Url:url, ?Coordinate:list(nonneg)) is nondet.

:- persistent(finished(url:atom,coordinate:list(nonneg))).

:- initialization(init_lwm_history).



has_failed(Url, Coord):-
  failed(Url, Coord).

has_finished(Url, Coord):-
  finished(Url, Coord).

report_failed(Url, Coord):-
  assert_failed(Url, Coord).

report_finished(Url, Coord):-
  assert_finished(Url, Coord).



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

