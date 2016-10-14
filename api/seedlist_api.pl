:- module(
  seedlist_api,
  [
    add_seed/1,       % +From
    add_seed/2,       % +From, -Hash
    begin_seed/1,     % +Seed
    end_seed/1,       % +Seed
    print_seeds/0,
    remove_seed/1,    % +Hash
    reset_seed/1,     % +Hash
    seed/1,           % -Dict
    seed_by_hash/2,   % +Hash, -Dict
    seed_by_status/2, % +Status:oneof([added,started,ended]), -Dict
    seed_status/1,    % ?Status
    seeds_by_status/2 % +Status:oneof([added,started,ended]), -Pagination
  ]
).

/** <module> LOD Laundromat: Seedlist API

Three stages for seeds:

  1. added

  2. started

  3. ended

@author Wouter Beek
@version 2016/01-2016/02, 2016/05, 2016/08-2016/10
*/

:- use_module(library(apply)).
:- use_module(library(dcg/dcg_ext)).
:- use_module(library(dcg/dcg_pl)).
:- use_module(library(dict_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(error)).
:- use_module(library(hash_ext)).
:- use_module(library(json_ext)).
:- use_module(library(list_ext)).
:- use_module(library(math/math_ext)).
:- use_module(library(os/file_ext)).
:- use_module(library(pair_ext)).
:- use_module(library(print_ext)).
:- use_module(library(q/q_iri)).
:- use_module(library(service/es_api)).
:- use_module(library(thread)).
:- use_module(library(yall)).





%! add_seed(+From) is det.
%! add_seed(+From, -Hash) is det.
% Adds an IRI to the seedlist.
%
% @throws existence_error if IRI is already in the seedlist.

add_seed(From) :-
  add_seed(From, _).


add_seed(From1, Hash) :-
  iri_normalized(From1, From2),
  md5(From2, Hash),
  get_time(Now),
  es_create_pp(
    [llw,seedlist,Hash],
    _{added: Now, ended: 0.0, from: From2, number_of_tuples: 0, started: 0.0}
  ),
  debug(seedlist, "Added to seedlist: ~a (~a)", [From2,Hash]).



%! begin_seed(+Seed) is det.
%
% Pop a dirty seed off the seedlist.

begin_seed(Seed) :-
  dict_tag(Seed, Hash),
  get_time(Started),
  es_update_pp([llw,seedlist,Hash], _{doc: _{started: Started}}),
  debug(seedlist(begin), "Started cleaning seed ~a", [Hash]).



%! end_seed(+Seed) is det.

end_seed(Seed) :-
  dict_tag(Seed, Hash),
  get_time(Ended),
  es_update_pp([llw,seedlist,Hash], _{doc: _{ended: Ended}}),
  debug(seedlist(end), "Ended cleaning seed ~a", [Hash]).



%! print_seeds is det.

print_seeds :-
  maplist(seeds_by_status, [added,started,ended], [Seeds1,Seeds2,Seeds3]),
  Num1 = Seeds1.total_number_of_results,
  Num2 = Seeds2.total_number_of_results,
  Num3 = Seeds3.total_number_of_results,
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
  es_rm_pp([llw,seedlist,Hash]),
  debug(seedlist(remove), "Removed seed ~a", [Hash]).



%! reset_seed(+Hash) is det.

reset_seed(Hash) :-
  get_time(Now),
  es_update_pp(
    [llw,seedlist,Hash],
    _{doc: _{added: Now, started: 0.0, ended: 0.0}}
  ),
  debug(seedlist(reset), "Reset seed ~a", [Hash]).



%! seed(-Dict) is nondet.

seed(Dict) :-
  seed_status(Status),
  seed_by_status(Status, Dict).



%! seed_by_hash(+Hash, -Dict) is nondet.

seed_by_hash(Hash, Dict) :-
  es_get([llw,seedlist,Hash], Dict).



%! seed_by_status(+Status:oneof([added,ended,started]), -Dict) is nondet.

seed_by_status(Status, Dict) :-
  seeds_by_status(Status, Pagination),
  member(Dict, Pagination.results).



%! seed_status(+Status) is semidet.
%! seed_status(-Status) is multi.

seed_status(added).
seed_status(started).
seed_status(ended).



%! seeds_by_status(+Status, -Pagination) is nondet.
%
% Returns all seeds with the same status in pages.

seeds_by_status(Status, Pagination) :-
  dict_pairs(Range, [Status-_{gt: 0.0}]),
  es_search(
    [llw,seedlist],
    _{query: _{filtered: _{filter: _{range: Range}}}},
    _{},
    Pagination
  ).
