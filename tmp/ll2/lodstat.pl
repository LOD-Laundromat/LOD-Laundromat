:- module(
  lodstat,
  [
    collect_lstat/0,
    collect_lstat/1, % -Dict
    create_lstat/0,
    create_lstat/1,  % +NumWorkers
    current_lstat/1, % -Dict
    ltuples/1,       % -NumTuples
    reset_lstat/0
  ]
).

/** <module> LOD Laundromat statistics

@author Wouter Beek
@version 2016/04-2016/05, 2016/08
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(assoc_ext)).
:- use_module(library(call_ext)).
:- use_module(library(csv)).
:- use_module(library(dcg/dcg_ext)).
:- use_module(library(dcg/dcg_table)).
:- use_module(library(dict_ext)).
:- use_module(library(file_ext)).
:- use_module(library(lists)).
:- use_module(library(lodcli/lodcli)).
:- use_module(library(pair_ext)).
:- use_module(library(pool)).
:- use_module(library(print_ext)).
:- use_module(library(semweb/hdt11)).
:- use_module(library(solution_sequences)).
:- use_module(library(string_ext)).
:- use_module(library(thread_ext)).
:- use_module(library(yall)).





%! collect_lstat is det.
%! collect_lstat(-Dict) is det.

collect_lstat :-
  collect_lstat(Dict),
  print_dict(Dict),
  dict_pairs(Dict, Pairs),
  maplist(pair_row, Pairs, Rows),
  csv_write_file('lstat.csv', Rows).


collect_lstat(_) :-
  empty_assoc(A0),
  nb_setval(lstat, A0),
  current_lstat(Dict),
  nb_getval(lstat, A1),
  add_dict_to_assoc(A1, Dict, A2),
  nb_setval(lstat, A2),
  fail.
collect_lstat(Dict) :-
  nb_getval(lstat, A),
  nb_delete(lstat),
  assoc_to_list(A, Pairs),
  dict_pairs(Dict, Pairs).



%! create_lstat is det.
%! create_lstat(+NumThreads) is det.

create_lstat :-
  default_number_of_threads(NumWorkers),
  create_lstat(NumWorkers).


create_lstat(NumWorkers) :-
  forall(create_lstat_resource0(File), add_resource(lstat, File)),
  call_n_times(NumWorkers, add_worker(lstat, create_lstat_worker0)).


create_lstat_resource0(File1) :-
  q_dir(Dir),
  q_dir_file(Dir, data, [hdt], File1),
  file_is_ready(File1),
  q_dir_file(Dir, stat, [json], File2),
  \+ exists_file(File2).


create_lstat_worker0(DataHdtFile) :-
  q_dir_file(Dir, DataHdtFile),
  q_dir_file(Dir, warn, [hdt], WarnHdtFile),
  exists_file(WarnHdtFile), !,
  hdt_call_on_file(DataHdtFile, lstat_data0(Dict1, Hdt)),
  hdt_call_on_file(WarnHdtFile, lstat_warn0(Dict2, Hdt)),
  merge_dicts(Dict1, Dict2, Dict3),
  q_file_lhash(File, stat, json, Hash),
  json_write_any(File, Dict3).
create_lstat_worker0(_).


lstat_data0(Dict, Hdt) :-
  aggregate_all(set(Key), distinct(Key, data_key0(Hdt, Key)), Keys),
  maplist(pair0, Keys, Pairs),
  dict_pairs(Dict, [literals-0,triples-0|Pairs]),
  lstat_data_count0(Hdt, Dict).


data_key0(Hdt, Key) :-
  q(hdt0, _, _, O, Hdt),
  rdf_is_literal(O),
  (O = _^^Key -> true ; (O = _@Key ; rdf_equal(rdf:langString, Key))).


pair0(Key, Key-0).


lstat_data_count0(Hdt, Dict) :-
  q(hdt0, _, _, O, Hdt),
  (   rdf_is_literal(O)
  ->  dict_inc(literals, Dict),
      (   O = _^^D
      ->  dict_inc(D, Dict)
      ;   O = _@LTag
      ->  rdf_equal(rdf:langString, D),
          dict_inc(D, Dict),
          dict_inc(LTag, Dict)
      )
  ;   true
  ),
  dict_inc(triples, Dict),
  fail.
lstat_data_count0(_, _).


lstat_warn0(Dict, Hdt) :-
  aggregate_all(set(Key), distinct(Key, warn_key0(Hdt, Key)), Keys),
  maplist(pair0, Keys, Pairs),
  dict_pairs(Dict, Pairs),
  lstat_warn_count0(Hdt, Dict).


warn_key0(Hdt, Key) :-
  q(hdt0, _, Key, _, Hdt).


lstat_warn_count0(Hdt, Dict) :-
  q(hdt0, _, P, _, Hdt),
  dict_inc(P, Dict),
  fail.
lstat_warn_count0(_, _).



%! current_lstat(-Dict) is det.

current_lstat(Dict) :-
  q_file(stat, json, File),
  exists_file(File),
  json_read_any(File, Dict).



%! ltuples(-NunTuples) is det.

ltuples(NumTuples) :-
  aggregate_all(
    sum(NumTuples),
    % @bug Literal notation does not work within aggregate_all/3.
    lm(_, llo:unique_tuples, ^^(NumTuples,_)),
    NumTuples
  ).



%! reset_lstat is det.

reset_lstat :-
  q_file(stat, json, File),
  delete_file(File).
