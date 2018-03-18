:- module(
  ll_workers,
  [
    add_worker/0,
    add_workers/1 % +N
  ]
).

/** <module> LOD Laundromat: Workers performing a scrape

@author Wouter Beek
@version 2018
*/

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

worker_loop :-
  next_seed(Seed), !,
  _{name: DName} :< Seed,
  thread_create(ll_dataset(Seed), Id, [alias(DName),at_exit(work_ends)]),
  thread_join(Id, Status),
  (Status == true -> true ; print_message(warning, worker_dies(Status))),
  worker_loop.
worker_loop.

work_ends :-
  thread_self_property(status(Status)),
  (   Status == true
  ->  true
  ;   thread_self_property(alias(Alias)),
      print_message(warning, work_ends(Alias,Status))
  ).



%! add_workers(+N:nonneg) is det.
%
% Wrapper that allows multiple workers (add_worker/0) to be created at
% once.

add_workers(N) :-
  forall(
    between(1, N, _),
    add_worker
  ).



%! next_seed(-Seed:dict) is det.

next_seed(Seed) :-
  setting(ll:seedlist_url, Uri0),
  uri_comp_set(query, Uri0, [page(1),page_size(1),stale(true)], Uri),
  http_open2(Uri, In, [accept(json),status_code(Status)]),
  call_cleanup(
    (   Status =:= 200
    ->  json_read_dict(In, Seeds, [value_string_as(atom)]),
        Seeds = [Seed]
    ;   print_message(warning, seedlist_offline(Uri))
    ),
    close(In)
  ).
