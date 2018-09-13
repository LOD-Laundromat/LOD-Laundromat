:- encoding(utf8).
:- module(
  ll_init,
  [
    add_thread/1,
    continue/0,
    start/0
  ]
).
:- reexport(library(rocks_ext)).

/** <module> LOD Laundromat initialization

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(error)).

:- use_module(library(conf_ext)).
:- use_module(library(date_time)).
:- use_module(library(dict)).
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(ll/ll_decompress)).
:- use_module(library(ll/ll_download)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(ll/ll_parse)).
:- use_module(library(ll/ll_recode)).
:- use_module(library(rocks_ext)).
:- use_module(library(semweb/ldfs)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).
:- use_module(library(tapir/tapir_api)).
:- use_module(library(thread_ext)).

:- at_halt(maplist(rocks_close, [seeds,stale,downloaded,decompressed,recoded])).

:- dynamic
    user:message_hook/3.

:- meta_predicate
    run_loop(0, +, +),
    running_loop_(0, +),
    start_loop_(0, +).

:- multifile
    user:message_hook/3.

user:message_hook(E, Kind, _) :-
  memberchk(Kind, [error,warning]),
  thread_self_property(alias(Hash)),
  write_message(Kind, Hash, E).

:- rdf_register_prefix(ldm).





%! add_thread(+Type:oneof([download,decompress,recode,parse])) is det.

add_thread(Type) :-
  must_be(oneof([download,decompress,recode,parse]), Type),
  atom_concat(ll_, Type, Atom),
  run_loop(Atom:Atom, 60, 1).



%! continue is det.

continue :-
  forall(
    processing_file(Hash, File),
    (
      ldfs_directory(Hash, false, Dir),
      delete_directory_and_contents(Dir),
      delete_file(File)
    )
  ),
  conf_json(Conf),
  % state store
  maplist(state_store_init, [seeds,stale,downloaded,decompressed,recoded]),
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
  % Start download workers.
  flag(ll_download, _, 1),
  run_loop(ll_download, Workers.download.sleep, Workers.download.threads),
  % Start decompression workers.
  flag(ll_decompress, _, 1),
  run_loop(ll_decompress, Workers.decompress.sleep, Workers.decompress.threads),
  % Start recode workers.
  flag(ll_recode, _, 1),
  run_loop(ll_recode, Workers.recode.sleep, Workers.recode.threads),
  % Start parse workers.
  flag(ll_parse, _, 1),
  run_loop(ll_parse, Workers.parse.sleep, Workers.parse.threads),
  % Log standard output to file.
  ldfs_root(Root),
  directory_file_path(Root, 'out.log', File1),
  protocol(File1),
  % Persist the currently processing hashes.
  directory_file_path(Root, ll_init, File2),
  db_attach(File2, []).

state_store_init(Alias) :-
  rocks_init(Alias, [key(atom),merge(ll_init:merge_dicts),value(term)]).

merge_dicts(partial, _, New, In, Out) :-
  merge_dicts(New, In, Out).
merge_dicts(full, _, Initial, Additions, Out) :-
  merge_dicts([Initial|Additions], Out).

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
  create_detached_thread(Alias, running_loop_(Goal_0, Sleep)),
  debug(ll(thread), "Thread ~a ~D started.", [Pred,N]).

running_loop_(Goal_0, Sleep) :-
  Goal_0, !,
  running_loop_(Goal_0, Sleep).
running_loop_(Goal_0, Sleep) :-
  (   debugging(ll(idle))
  ->  now(Dt),
      dt_label(Dt, DtLabel),
      debug(ll(idle), "[~a] 💤 ~w", [DtLabel,Goal_0])
  ;   true
  ),
  sleep(Sleep),
  running_loop_(Goal_0, Sleep).



%! start is det.

start :-
  continue,
  populate_seedlist.

populate_seedlist :-
  current_user(User),
  forall(
    statement(_, User, index, _, ldm:downloadLocation, Literal),
    (
      rdf_literal_value(Literal, Uri),
      md5(Uri, Hash),
      % Interval is set to 100 days (in seconds).
      rocks_put(seeds, Hash, _{interval: 8640000.0, processed: 0.0, uri: Uri})
    )
  ),
  update_seedlist.

update_seedlist :-
  get_time(Now),
  forall(
    rocks_enum(seeds, Hash, State),
    (
      _{interval: Interval, processed: Old} :< State,
      Stale is Old + Interval,
      (   Stale =< Now
      ->  rocks_delete(seeds, Hash, State),
          rocks_put(stale, Hash, State)
      ;   true
      )
    )
  ).
