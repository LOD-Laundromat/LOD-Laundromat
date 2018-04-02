:- module(
  ll_analyze,
  [
    empty_dataset/2, % -Name, -Dict
    error/1,         % -Error
    print_errors/0,
    print_status/0,
    unknown_error/1  % -Error
  ]
).

/** <module> Analyze LOD Laundromat error logs

@author Wouter Beek
@version 2018
*/

:- use_module(library(aggregate)).
:- use_module(library(debug)).
:- use_module(library(readutil)).
:- use_module(library(settings)).
:- use_module(library(uri)).
:- use_module(library(yall)).
:- use_module(library(zlib)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(tapir)).

:- dynamic
    known_error_/1.

:- nodebug(analyze).

:- initialization
   init_ll_analyze.

:- setting(ll:data_directory, any, _, "").





%! empty_dataset(-Name:compound, -Dict:dict) is nondet.
%
% Enumerated datasets with zero statements.  These may be due to a
% buggy upload.

empty_dataset(OName/DName, Dict) :-
  dataset(_, _, Dict),
  _{openJobs: [], statements: N} :< Dict,
  N =< 0,
  OName = Dict.owner.accountName,
  DName = Dict.name.



%! error(-Error:pair(atom,compound)) is nondet.

error(Error) :-
  setting(ll:data_directory, Dir),
  directory_file_path(Dir, 'err.log.gz', File),
  call_stream_file(File, error_(Error)).

error_(Dataset-Error, In) :-
  repeat,
  read_line_to_string(In, Line),
  (   Line == end_of_file
  ->  !
  ;   debug(analyze, "~s", [Line]),
      split_string(Line, "\t", "", [H|T]),
      atom_string(Dataset, H),
      atomics_to_string(T, String),
      read_term_from_atom(String, Error, [])
  ).



%! print_errors is det.

print_errors :-
  forall(
    error(_-Error),
    (
      flag(number_of_erros, M, M+1),
      (M mod 1 000 000 =:= 0 -> format("~D\n", [M]) ; true),
      (known_error_(Error, Flag) -> flag(Flag, N, N+1) ; true)
    )
  ),
  format("---\n"),
  forall(
    known_error_(_, Flag),
    (
      flag(Flag, N, N),
      format("~D\t~a\n", [N,Flag])
    )
  ).



%! print_status is det.
%
% Prints the current status of the LOD-Laundromat.

print_status :-
  aggregate_all(
    sum(N),
    (
      dataset(_, _, Dict),
      _{statements: N} :< Dict
    ),
    N
  ),
  format("~D\n", [N]).



%! unknown_error(-Error:pair(atom,compound)) is nondet.

unknown_error(Error) :-
  error(Error),
  Error = _-Error0,
  \+ known_error_(Error0, _).





% INITIALIZATION %

init_ll_analyze :-
  conf_json(Conf),
  set_setting(ll:data_directory, Conf.'data-directory').
