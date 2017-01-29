:- module(
  seedlist,
  [
    add_seed/1,                  % +From
    add_seed/2,                  % +From, -Hash
    begin_seed_hash/1,           % +Hash
    end_seed_hash/1,             % +Hash
    is_seed_hash/1,              % +Hash
    number_of_seeds_by_status/2, % +Status, -NumSeeds
    print_seeds/0,
    remove_seed/1,               % +Hash
    reset_seed/1,                % +Hash
    seed/1,                      % -Dict
    seed_by_hash/2,              % +Hash, -Dict
    seed_by_status/2,            % +Status, -Dict
    seed_status/1,               % ?Status
    seeds_by_status/2            % +Status, -Result
  ]
).

/** <module> Seedlist

Status is either `added', `ended', or `started'.

@author Wouter Beek
@version 2017/01
*/

:- use_module(library(call_ext)).
:- use_module(library(debug)).
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(io)).
:- use_module(library(lists)).
:- use_module(library(math/math_ext)).
:- use_module(library(print_ext)).
:- use_module(library(q/q_fs)).
:- use_module(library(service/es_api)).
:- use_module(library(uri)).





%! add_seed(+From) is det.
%! add_seed(+From, -Hash) is det.
%
% Adds a URI to the seedlist.
%
% @throws existence_error if URI is already in the seedlist.

add_seed(From) :-
  add_seed(From, _).


add_seed(From1, Hash) :-
  iri_normalized(From1, From2),
  md5(From2, Hash),
  get_time(Now),
  retry0(
    es_create(
      [ll,seedlist,Hash],
      _{added: Now, ended: 0.0, from: From2, started: 0.0}
    )
  ),
  debug(seedlist(add), "Added to seedlist: ~a (~a)", [From2,Hash]).



%! begin_seed_hash(+Hash) is det.
%
% Pop a dirty seed off the seedlist.

begin_seed_hash(Hash) :-
  get_time(Started),
  retry0(es_update([ll,seedlist,Hash], _{doc: _{started: Started}})),
  debug(seedlist(begin), "Started cleaning seed ~a", [Hash]).



%! end_seed_hash(+Hash) is det.

end_seed_hash(Hash) :-
  get_time(Ended),
  retry0(es_update([ll,seedlist,Hash], _{doc: _{ended: Ended}})),
  debug(seedlist(end), "Ended cleaning seed ~a", [Hash]).



%! is_seed_hash(+Hash) is semidet.

is_seed_hash(Hash) :-
  seed_by_hash(Hash, _).



%! number_of_seeds_by_status(+Status, -NumSeeds) is det.

number_of_seeds_by_status(Status, NumSeeds) :-
  once(seeds_by_status(Status, Result)),
  NumSeeds = Result.total_number_of_results.



%! print_seeds is det.

print_seeds :-
  once(seeds_by_status(added, Result1)),
  once(seeds_by_status(started, Result2)),
  once(seeds_by_status(ended, Result3)),
  Num1 = Result1.total_number_of_results,
  Num2 = Result2.total_number_of_results,
  Num3 = Result3.total_number_of_results,
  sum_list([Num1,Num2,Num3], Num123),
  float_div_zero(Num1, Num123, Perc1),
  float_div_zero(Num2, Num123, Perc2),
  float_div_zero(Num3, Num123, Perc3),
  HeaderRow = head(["Category","Size","Percentage"]),
  DataRows = [
    ["All",           Num123, perc(1.0)  ],
    ["To be cleaned", Num1,   perc(Perc1)],
    ["Cleaning",      Num2,   perc(Perc2)],
    ["Cleaned",       Num3,   perc(Perc3)]
  ],
  print_table([HeaderRow|DataRows]).



%! remove_seed(+Hash) is det.

remove_seed(Hash) :-
  retry0(es_rm([ll,seedlist,Hash])),
  debug(seedlist(remove), "Removed seed ~a", [Hash]).



%! reset_seed(+Hash) is det.

reset_seed(Hash) :-
  get_time(Now),
  retry0(
    es_update(
      [ll,seedlist,Hash],
      _{doc: _{added: Now, started: 0.0, ended: 0.0}}
    )
  ),
  debug(seedlist(reset), "Reset seed ~a", [Hash]).



%! seed(-Dict) is nondet.

seed(Dict) :-
  seed_status(Status),
  seed_by_status(Status, Dict).



%! seed_by_hash(+Hash, -Dict) is semidet.

seed_by_hash(Hash, Dict) :-
  retry0(es_get([ll,seedlist,Hash], Dict)).



%! seed_by_status(+Status, -Dict) is nondet.

seed_by_status(Status, Dict) :-
  seeds_by_status(Status, Result),
  Results = Result.results,
  member(Dict, Results).
  


%! seed_status(+Status) is semidet.
%! seed_status(-Status) is multi.

seed_status(added).
seed_status(started).
seed_status(ended).



%! seeds_by_status(+Status, -Result) is nondet.
%
% Returns all seeds with the same status in pages.

seeds_by_status(Status, Result) :-
  status_query(Status, Query),
  retry0(es_search([ll,seedlist], _{query: Query}, _{}, Result)).

status_query(ended, Query) :- !,
  Query = _{range: _{ended: _{gt: 0.0}}}.
status_query(started, Query) :- !,
  Query = _{bool: _{must: [_{term: _{ended: 0.0}}, _{range: _{started: _{gt: 0.0}}}]}}.
status_query(added, Query) :-
  Query = _{bool: _{must: [_{term: _{started: 0.0}}, _{range: _{added: _{gt: 0.0}}}]}}.
