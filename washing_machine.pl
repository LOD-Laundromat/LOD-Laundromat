:- module(
  washing_machine,
  [
    add_washing_machine/0,
    add_washing_machine/1, % +N
    clean/1,               % +Hash
    clean_iri/1,           % +Iri
    load_all_metadata/0,
    reset_ldoc/1           % +Doc
  ]
).

/* <module> LOD Laundromat washing machine

@author Wouter Beek
@version 2016/01-2016/03
*/

:- use_module(library(apply)).
:- use_module(library(debug_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(filesex)).
:- use_module(library(gen/gen_ntuples)).
:- use_module(library(hash_ext)).
:- use_module(library(http/json)).
:- use_module(library(jsonld/jsonld_metadata)).
:- use_module(library(jsonld/jsonld_read)).
:- use_module(library(msg_ext)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pl/pl_term)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(semweb/rdf11)). % Operators.
:- use_module(library(string_ext)).
:- use_module(library(uri/uri_ext)).
:- use_module(library(zlib)).

:- use_module(cpack('LOD-Laundromat'/laundromat_fs)).
:- use_module(cpack('LOD-Laundromat'/seedlist)).

:- meta_predicate
    rdf_store_messages(+, +, 0, -).

:- rdf_meta
   rdf_store_messages(+, r, :, -).

:- dynamic
    currently_debugging0/1.

%currently_debugging0('00385477bf97bd4ebe1d0719a65100e1').

:- rdf_meta
   reset_ldoc(r).





%! add_washing_machine is det.
%! add_washing_machine(+N) is det.
% Add a LOD Laundromat thread.

add_washing_machine :-
  detached_thread(start_washing_machine0).

add_washing_machine(N) :-
  N =< 0, !.
add_washing_machine(N1) :-
  add_washing_machine,
  N2 is N1 - 1,
  add_washing_machine(N2).



%! clean(+Hash) is det.
% Does not re-clean documents.
%
% @throws existence_error If the seed is not in the seedlist.

clean(Hash) :-
  ldoc_hash(Doc, Hash),
  ldoc_file(Doc, data, File),
  exists_file(File), !,
  msg_notification("Already cleaned document ~a", [Doc]).
clean(Hash) :-
  clean(Hash, _).


%! clean(-Hash, -Iri) is det.
% Cleans a dirty seed from the seedlist.

clean(Hash, Iri) :-
  begin_seed(Hash, Iri),
  clean0(Hash, Iri),
  end_seed(Hash).

clean0(Hash, Iri) :-
  currently_debugging(Hash),
  ldir_hash(Dir, Hash),
  ldoc_hash(Doc, Hash),
  make_directory_path(Dir),
  maplist(ldoc_file(Doc), [data,meta,warn], [DataFile,MetaFile,WarnFile]),
  setup_call_cleanup(
    (
      gzopen(MetaFile, write, MetaSink, [format(gzip)]),
      gzopen(WarnFile, write, WarnSink, [format(gzip)])
    ),
    (
      State = _{meta: MetaSink, warn: WarnSink, warns: 0},
      CleanOpts = [compress(gzip),metadata(M1),relative_to(Dir),sort_dir(Dir),warn(WarnSink)],
      rdf_store_messages(State, Doc, rdf_clean(Iri, DataFile, CleanOpts), M2),
      (var(M2) -> M = M1 ; M = M2),
      rdf_store_metadata(State, Doc, M)
    ),
    (
      close(WarnSink),
      close(MetaSink)
    )
  ),
  gtrace,
  ldoc_load(Doc, meta).
  %absolute_file_name('dirty.gz', DirtyTo, Opts),
  %call_collect_messages(rdf_download_to_file(Iri, DirtyTo, [compress(gzip)])).

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
  clean0(H, I2).



%! load_all_metadata is det.
% Loads the metadata of all cleaned documents into ClioPatria.

load_all_metadata :-
  forall(ldoc(Doc), ldoc_load(Doc, meta)).



%! reset_ldoc(+Doc) is det.

reset_ldoc(Doc) :-
  % Step 1: Unload the RDF metadata from memory.
  rdf_unload_graph(Doc),
  
  % Step 2: Remove the data and metadata files from disk.
  ldir_ldoc(Dir, Doc),
  delete_directory_and_contents(Dir),

  % Step 3: Reset the seed in the seedlist.
  ldoc_hash(Doc, Hash),
  reset_seed(Hash).



start_washing_machine0 :-
  washing_machine0(_{idle: 0}).

washing_machine0(State) :-
  clean(Hash, Iri),
  debug(washing_machine(thread), "Cleaned ~a (~a)", [Hash,Iri]),
  washing_machine0(State).
washing_machine0(State) :-
  M = 100,
  sleep(M),
  thread_name(Name),
  dict_inc(State, idle, N),
  S is M * N,
  debug(washing_machine(idle), "Washing machine ~w is ~D sec. idle", [Name,S]),
  washing_machine0(State).



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
          dict_inc(State, warns)
      ))
    ),
    (
      (   catch(Goal_0, E, true)
      ->  (   var(E)
          ->  End0 = true
          ;   E = error(existence_error(open_any2,M),_)
          ->  rdf_store_metadata(State, S, M),
              End0 = "No stream"
          ;   End0 = E
          ),
          debug(washing_machine(high), "[RESULT] ~w", [End0])
      ;   msg_warning("[FAILED]", []),
          End0 = fail
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
  (debugging(washing_machine(low)) -> json_write_dict(user_output, Jsonld2) ; true),
  forall(jsonld_tuple(Jsonld2, rdf(S,P,O)), (
    (debugging(washing_machine(low)) -> with_output_to(user_output, rdf_print(S, P, O, _)) ; true),
    with_output_to(State.meta, gen_ntriple(S, P, O))
  )).
