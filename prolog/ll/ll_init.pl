:- module(ll_init, []).

/** <module> LOD Laundromat initialization

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(settings)).
:- use_module(library(zlib)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(ll/ll_workers)).
:- use_module(library(tapir)).
:- use_module(library(thread_ext)).
:- use_module(library(write_ext)).

:- initialization
   init_ll.

lod_cloud_portray(Blob, Options) :-
  blob(Blob, Type),
  \+ atom(Blob),
  Type \== reserved_symbol,
  write_term('BLOB'(Type), Options).

:- setting(authority, any, _,
           "The URI authority component of the seedlist endpoint.").
:- setting(password, any, _,
           "The password for accessing the seedlist endpoint.").
:- setting(scheme, oneof([http,https]), https,
           "The URI scheme component of the seedlist endpoint.").
:- setting(script, any, _,
           "Location of the Triply Client script.").
:- setting(temporary_directory, any, _,
           "The directory where clean data is temporarily stored and where logs are kept.").
:- setting(user, any, _,
           "The user name for accessing the seedlist endpoint.").





%! init_ll is det.

init_ll :-
  % Count-by-one for thread aliases.
  flag(number_of_workers, _, 1),
  conf_json(Conf),
  % seedlist
  _{
    authority: Auth,
    password: Password,
    scheme: Scheme,
    user: User
  } :< Conf.seedlist,
  maplist(
    set_setting,
    [authority,password,scheme,user],
    [Auth,Password,Scheme,User]
  ),
  % temporary directory
  create_directory(Conf.'data-directory'),
  set_setting(temporary_directory, Conf.'data-directory'),
  % error and output logs
  (debugging(ll) -> true ; init_log(Conf.'data-directory')),
  % Triply client script.
  _{client: Script} :< Conf.tapir,
  set_setting(script, Script),
  % number of workers
  add_workers(Conf.workers).

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
      (   skip_error(E1)
      ->  true
      ;   replace_blobs(E1, E2),
          format(
            Out,
            "~a\t~W\n",
            [Alias,E2,[blobs(portray),portray_goal(lod_cloud_portray),quoted(true)]]
          )
      )
  )),
  asserta(user:at_halt(close(Out))).

% Do not record errors for datatype IRIs whose parser and generator
% are not standards-compliant: `rdf:HTML' and `rdf:XMLLiteral'.
skip_error(non_canonical_lexical_form('http://www.w3.org/1999/02/22-rdf-syntax-ns#HTML',_,_)).
skip_error(non_canonical_lexical_form('http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral',_,_)).
