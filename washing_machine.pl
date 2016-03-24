:- module(
  washing_machine,
  [
    add_wm/0,
    add_wm/1,            % +N
    clean/1,             % +Hash
    clean_iri/1,         % +Iri
    current_wm/1,        % ?Alias
    current_wm/2,        % ?Alias, -Status
    ldoc_reset/1,        % +Doc
    load_all_metadata/0,
    number_of_wms/1,     % -N
    reset_and_clean/1    % +Hash
  ]
).

/* <module> LOD Laundromat washing machine

@author Wouter Beek
@version 2016/01-2016/03
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(filesex)).
:- use_module(library(gen/gen_ntuples)).
:- use_module(library(hash_ext)).
:- use_module(library(http/json)).
:- use_module(library(jsonld/jsonld_metadata)).
:- use_module(library(jsonld/jsonld_read)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pl/pl_term)).
:- use_module(library(print_ext)).
:- use_module(library(prolog_stack)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(semweb/rdf11)). % Operators.
:- use_module(library(string_ext)).
:- use_module(library(uri/uri_ext)).
:- use_module(library(zlib)).

:- use_module(cpack('LOD-Laundromat'/laundromat_fs)).
:- use_module(cpack('LOD-Laundromat'/laundromat_hdt)).
:- use_module(cpack('LOD-Laundromat'/seedlist)).

prolog_stack:stack_guard('C').
prolog_stack:stack_guard(none).

:- meta_predicate
    rdf_store_messages(+, +, 0, -).

:- rdf_meta
   rdf_store_messages(+, r, :, -).

:- dynamic
    currently_debugging0/1.

%currently_debugging0('0064ae0dd911eced3d034cce085d1ee7').

:- rdf_meta
   ldoc_reset(r).





%! add_wm is det.
%! add_wm(+N) is det.
% Add a LOD Laundromat thread.

add_wm :-
  add_wm(1).

add_wm(M) :-
  M =< 0, !.
add_wm(M1) :-
  number_of_wms(N1),
  N2 is N1 + 1,
  atom_concat(wm, N2, Alias),
  thread_create(start_wm0, _, [alias(Alias),detached(false)]),
  M2 is M1 - 1,
  add_wm(M2).



%! clean is det.
% Clean some seed from the seedlist.

clean :-
  clean_seed(_, _).


%! clean(+Hash) is det.
% Clean a specific seed from the seedlist.
% Does not re-clean documents.
%
% @throws existence_error If the seed is not in the seedlist.

clean(Hash) :-
  ldoc_hash(Doc, Hash),
  ldoc_file(Doc, data, nquads, File),
  exists_file(File), !,
  msg_notification("Already cleaned document ~a", [Doc]).
clean(Hash) :-
  clean_seed(Hash, _).


clean_seed(Hash, Iri) :-
  begin_seed(Hash, Iri),
  number_of_wms(N1),
  debug(wm(thread), "---- [~D] Cleaning ~a", [N1,Hash]),
  clean_seed0(Hash, Iri),
  number_of_wms(N2),
  debug(wm(thread), "---- [~D] Cleaned ~a", [N2,Hash]),
  end_seed(Hash).

clean_seed0(Hash, Iri) :-
  ldir_hash(Dir, Hash),
  ldoc_hash(Doc, Hash),
  with_mutex(wm, make_directory_path(Dir)),
  ldoc_file(Doc, data, nquads, DataFile),
  ldoc_file(Doc, meta, nquads, MetaFile),
  ldoc_file(Doc, warn, nquads, WarnFile),
  setup_call_cleanup(
    (
      gzopen(MetaFile, write, MetaSink, [format(gzip)]),
      gzopen(WarnFile, write, WarnSink, [format(gzip)])
    ),
    (
      State = _{meta: MetaSink, warn: WarnSink, warns: 0},
      CleanOpts = [compress(gzip),metadata(M1),relative_to(Dir),sort_dir(Dir),warn(WarnSink)],
      currently_debugging(Hash),
      rdf_store_messages(State, Doc, rdf_clean(Iri, DataFile, CleanOpts), M2),
      (var(M1) -> M = M2 ; M = M1),
      (var(M) -> format(user_output, "~w~n", [Hash]) ; true),
      rdf_store_metadata(State, Doc, M)
    ),
    (
      close(WarnSink),
      close(MetaSink)
    )
  ),
  ldoc_load(Doc, meta),
  %lhdt_build(Doc),
  %absolute_file_name('dirty.gz', DirtyTo, Opts),
  %call_collect_messages(rdf_download_to_file(Iri, DirtyTo, [compress(gzip)])),
  true.

currently_debugging(Hash) :-
  currently_debugging0(Hash), !,
  gtrace. %DEB
currently_debugging(_).



%! clean_iri(+Iri) is det.
% Clean a specific IRI, achieved by circumvents the seedlist.
% For debugging purposes only.

clean_iri(I1) :-
  iri_normalized(I1, I2),
  md5(I2, H),
  clean_seed(H, I2).



%! current_wm(+Alias) is semidet.
%! current_wm(-Alias) is nondet.
%! current_wm(+Alias, -Status) is semidet.
%! current_wm(-Alias, -Status) is nondet.

current_wm(Alias) :-
  current_wm(Alias, _).


current_wm(Alias, Status) :-
  thread_property(Id, alias(Alias)),
  atom_prefix(wm, Alias),
  thread_property(Id, status(Status)).



%! ldoc_reset(+Doc) is det.

ldoc_reset(Doc) :-
  % Step 1: Unload the RDF metadata from memory.
  rdf_unload_graph(Doc),

  % Step 2: Remove the data and metadata files from disk.
  ldir_ldoc(Dir, Doc),
  with_mutex(wm, (
    (exists_directory(Dir) -> delete_directory_and_contents(Dir) ; true)
  )),

  % Step 3: Reset the seed in the seedlist.
  ldoc_hash(Doc, Hash),
  reset_seed(Hash).



%! load_all_metadata is det.
% Loads the metadata of all cleaned documents into ClioPatria.

load_all_metadata :-
  forall(ldoc(Doc), ldoc_load(Doc, meta)).



monitor_wms :-
  current_wm(Alias),
  thread_statistics(Alias, stack, Stack),
  Stack >= 10^9, !,
  ansi_format(user_output, [fg(red)], "Thread ~a is running out of memory!~n").
monitor_wms.



%! number_of_wms(-N) is det.

number_of_wms(N) :-
  aggregate_all(count, current_wm(_), N).



%! reset_and_clean(+Hash) is det.

reset_and_clean(Hash) :-
  ldoc_hash(Doc, Hash),
  ldoc_reset(Doc),
  clean(Hash).



start_wm0 :-
  wm0(_{idle: 0}).

wm0(State) :-
  monitor_wms,
  clean,
  wm0(State).
wm0(State) :-
  M = 100,
  sleep(M),
  thread_name(Name),
  dict_inc(idle, State, N),
  S is M * N,
  debug(wm(idle), "==== Thread ~w idle ~D sec.", [Name,S]),
  wm0(State).



%! rdf_store_messages(+State, +S, :Goal_0, -Metadata) is det.
% Run Goal, unify Result with `true`, `false` or `exception(Error)`
% and messages with a list of generated error and warning messages.
% Each message is a term `message(Term,Kind,Lines)`.

rdf_store_messages(State, S, Goal_0, M) :-
  setup_call_cleanup(
    (
      asserta((
        user:thread_message_hook(Term,Kind,_) :-
          error_kind(Kind),
          format(State.warn, "~w~n", [Term]),
          dict_inc(warns, State)
      ))
    ),
    (
      catch(Goal_0, E, true),
      (   var(E)
      ->  End0 = true
      ;   E = error(existence_error(open_any2,M),_)
      ->  End0 = "No stream"
      ;   End0 = E,
          msg_warning("[FAILED] ~w~n", [End0]),
	  M = _{}
      ),
      with_output_to(string(End), write_term(End0)),
      with_output_to(State.meta, gen_ntriple(S, llo:end, End^^xsd:string))
    ),
    with_output_to(State.meta, gen_ntriple(S, llo:number_of_warnings, State.warns^^xsd:nonNegativeInteger))
  ).

error_kind(warning).
error_kind(error).


%! rdf_store_metadata(+State, +S, +M) is det.

rdf_store_metadata(State, S1, M) :-
  jsonld_metadata(M, Jsonld1),
  atom_string(S1, S2),
  Jsonld2 = Jsonld1.put(_{'@id': S2}),
  (debugging(wm(low)) -> json_write_dict(user_output, Jsonld2) ; true),
  forall(jsonld_tuple(Jsonld2, rdf(S,P,O)), (
    (debugging(wm(low)) -> with_output_to(user_output, rdf_print(S, P, O, _)) ; true),
    with_output_to(State.meta, gen_ntriple(S, P, O))
  )).
