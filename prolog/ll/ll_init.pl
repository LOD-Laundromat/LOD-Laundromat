:- module(ll_init, [tmon/0]).

/** <module> LOD Laundromat: Initialization

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(settings)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(ll/ll_loop)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(thread_ext)).

:- dynamic
    user:message_hook/3.

:- initialization
   init_ll.

:- multifile
    user:message_hook/3.

:- setting(ll:authority, any, _,
           "URI scheme of the seedlist server location.").
:- setting(ll:data_directory, any, _,
           "The directory where clean data is stored and where logs are kept.").
:- setting(ll:password, any, _, "").
:- setting(ll:scheme, oneof([http,https]), https,
           "URI scheme of the seedlist server location.").
:- setting(ll:user, any, _, "").

tmon :-
  thread_monitor.

user:message_hook(E, Kind, _) :-
  memberchk(Kind, [error,warning]),
  thread_self_property(alias(Hash)),
  write_meta_error(Hash, E).





%! init_ll is det.

init_ll :-
  conf_json(Conf),
  % data directory
  directory_file_path(Conf.'data-directory', ll, Dir),
  create_directory(Dir),
  set_setting(ll:data_directory, Dir),
  % seedlist
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
  % workers
  (   debugging(ll(offline))
  ->  DebugConf = _{sleep: 1, threads: 1},
      Workers = _{
        decompress: DebugConf,
        download: DebugConf,
        parse: DebugConf,
        recode: DebugConf
      }
  ;   Workers = Conf.workers
  ),
  run_loop(ll_decompress:ll_decompress, Workers.decompress.sleep, Workers.decompress.threads),
  run_loop(ll_download:ll_download, Workers.download.sleep, Workers.download.threads),
  run_loop(ll_parse:ll_parse, Workers.parse.sleep, Workers.parse.threads),
  run_loop(ll_recode:ll_recode, Workers.recode.sleep, Workers.recode.threads),
  % log standard output
  directory_file_path(Dir, 'out.log', File),
  protocol(File).
