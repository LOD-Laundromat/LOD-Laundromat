:- module(ll_init, []).

/** <module> LOD Laundromat: Initialization

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(settings)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(ll/ll_loop)).
:- use_module(library(thread_ext)).
:- use_module(library(write_ext)).

:- initialization
   init_ll.

ll_portray(Blob, Options) :-
  blob(Blob, Type),
  \+ atom(Blob),
  Type \== reserved_symbol,
  write_term('BLOB'(Type), Options).

% Set global stack to 10GB for larger datasets.
:- set_prolog_stack(global, limit(10*10**9)).

:- setting(ll:authority, any, _,
           "URI scheme of the seedlist server location.").
:- setting(ll:data_directory, any, _,
           "The directory where clean data is stored and where logs are kept.").
:- setting(ll:password, any, _, "").
:- setting(ll:scheme, oneof([http,https]), https,
           "URI scheme of the seedlist server location.").
:- setting(ll:user, any, _, "").





% INITIALIZATION %

%! init_ll is det.

init_ll :-
  conf_json(Conf),
  % data directory
  create_directory(Conf.'data-directory'),
  set_setting(ll:data_directory, Conf.'data-directory'),
  maplist(
    set_setting,
    [ll:authority,ll:password,ll:scheme,ll:user],
    [
      Conf.seedlist.authority,
      Conf.seedlist.password,
      Conf.seedlist.scheme,
      Conf.seedlist.user
    ]
  ),
  % worker threads
  run_loop(ll_download, Conf.workers.download),
  run_loop(ll_decompress, Conf.workers.decompress),
  run_loop(ll_recode, Conf.workers.recode),
  run_loop(ll_parse, Conf.workers.parse),
  % unless under debug mode
  (    debugging(ll)
  ->   true
  ;    % error and output logs
       init_log(Conf.'data-directory')
  ).

init_log(Dir) :-
  init_out_log(Dir),
  init_err_log(Dir).

init_out_log(Dir) :-
  directory_file_path(Dir, 'out.log', File),
  protocol(File).

init_err_log(Dir) :-
  directory_file_path(Dir, 'err.log.gz', File),
  gzopen(File, write, Out),
  asserta((
    user:message_hook(E1, Kind, _) :-
      memberchk(Kind, [error,warning]),
      thread_self_property(alias(Alias)),
      replace_blobs(E1, E2),
      format(
        Out,
        "~a\t~W\n",
        [Alias,E2,[blobs(portray),portray_goal(ll_portray),quoted(true)]]
      )
  )),
  asserta(user:at_halt(close(Out))).
