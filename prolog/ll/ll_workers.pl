:- module(
  ll_workers,
  [
    add_worker/0,
    add_workers/1 % +NumWorkers
  ]
).

/** <module> LOD Laundromat: Workers performing a scrape

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(http/json)).
:- use_module(library(settings)).

:- use_module(library(http/http_client2)).
:- use_module(library(thread_ext)).
:- use_module(library(uri_ext)).

:- use_module(library(ll/ll_dataset)).





%! add_worker is det.

add_worker :-
  flag(number_of_workers, N, N+1),
  format(atom(Alias), 'worker-~d', [N]),
  thread_create(worker_loop, _, [alias(Alias),at_exit(work_ends)]).

% Something to do.
worker_loop :-
  next_seed(Seed), !,
  _{dataset: Dataset} :< Seed,
  _{name: DName} :< Dataset,
  thread_create(ll_dataset(Seed), Id, [alias(DName),at_exit(work_ends)]),
  thread_join(Id, Status),
  (Status == true -> true ; print_message(warning, worker_dies(Status))),
  worker_loop.
% Nothing to do.
worker_loop :-
  sleep(10),
  worker_loop.

work_ends :-
  thread_self_property(status(Status)),
  (   Status == true
  ->  true
  ;   thread_self_property(alias(Alias)),
      print_message(warning, work_ends(Alias,Status))
  ).



%! add_workers(+NumWorkers:nonneg) is det.
%
% Wrapper that allows multiple workers (add_worker/0) to be created at
% once.

add_workers(N) :-
  forall(
    between(1, N, _),
    add_worker
  ).



%! next_seed(-Seed:dict) is semidet.

next_seed(Seed) :-
  maplist(
    ll_init:setting,
    [authority,password,scheme,user],
    [Auth,Password,Scheme,User]
  ),
  uri_comps(Uri, uri(Scheme,Auth,[seed],_,_)),
  http_open2(Uri, In, [accept(json),
                       authorization(basic(User,Password)),
                       failure(404),
                       method(patch)]),
  call_cleanup(
    json_read_dict(In, Seed, [value_string_as(atom)]),
    close(In)
  ).
