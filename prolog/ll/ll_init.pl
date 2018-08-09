:- module(ll_init, [add_thread/1]).

/** <module> LOD Laundromat: Initialization

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(error)).
:- use_module(library(settings)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(ll/ll_decompress)).
:- use_module(library(ll/ll_download)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(ll/ll_parse)).
:- use_module(library(ll/ll_recode)).
:- use_module(library(semweb/ldfs)).
:- use_module(library(thread_ext)).

:- dynamic
    user:message_hook/3.

:- initialization
   init_ll.

:- meta_predicate
    run_loop(0, +, +),
    running_loop_(0, +),
    start_loop_(0, +).

:- multifile
    user:message_hook/3.

:- setting(ll:authority, any, _,
           "URI scheme of the seedlist server location.").
:- setting(ll:password, any, _, "").
:- setting(ll:scheme, oneof([http,https]), https,
           "URI scheme of the seedlist server location.").
:- setting(ll:user, any, _, "").

user:message_hook(E, Kind, _) :-
  memberchk(Kind, [error,warning]),
  thread_self_property(alias(Hash)),
  write_message(Kind, Hash, E).





%! add_thread(+Type:oneof([download,decompress,recode,parse])) is det.

add_thread(Type) :-
  must_be(oneof([download,decompress,recode,parse]), Type),
  atom_concat(ll_, Type, Atom),
  run_loop(Atom:Atom, 120, 1).





% INITIALIZATION %

init_ll :-
  conf_json(Conf),
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
  ->  DebugConf = _{sleep: 30, threads: 1},
      Workers = _{
        decompress: DebugConf,
        download: DebugConf,
        parse: DebugConf,
        recode: DebugConf
      }
  ;   Workers = Conf.workers
  ),
  % download workers
  flag(ll_download, _, 1),
  run_loop(ll_download, Workers.download.sleep, Workers.download.threads),
  % decompression workers
  flag(ll_decompress, _, 1),
  run_loop(ll_decompress, Workers.decompress.sleep, Workers.decompress.threads),
  % recode workers
  flag(ll_recode, _, 1),
  run_loop(ll_recode, Workers.recode.sleep, Workers.recode.threads),
  % parse workers
  flag(ll_parse, _, 1),
  run_loop(ll_parse, Workers.parse.sleep, Workers.parse.threads),
  % log standard output
  ldfs_root(Root),
  directory_file_path(Root, 'out.log', File),
  protocol(File).

run_loop(Goal_0, Sleep, M) :-
  with_mutex(ll_loop,
    forall(
      between(1, M, _),
      start_loop_(Goal_0, Sleep)
    )
  ).

start_loop_(Goal_0, Sleep) :-
  strip_module(Goal_0, _, Pred),
  flag(Pred, N, N+1),
  format(atom(Alias), "~a-~D", [Pred,N]),
  thread_create(running_loop_(Goal_0, Sleep), _, [alias(Alias),detached(true)]),
  debug(ll(loop), "Thread ~a ~D started.", [Pred,N]).

running_loop_(Goal_0, Sleep) :-
  Goal_0, !,
  running_loop_(Goal_0, Sleep).
running_loop_(Goal_0, Sleep) :-
  (debugging(ll(idle)) -> debug(ll(idle), "💤 ~w", [Goal_0]) ; true),
  sleep(Sleep),
  running_loop_(Goal_0, Sleep).
