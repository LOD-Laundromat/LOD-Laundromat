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
:- use_module(library(filesex)).
:- use_module(library(gen/gen_ntuples)).
:- use_module(library(hash_ext)).
:- use_module(library(http/json)).
:- use_module(library(jsonld/jsonld_metadata)).
:- use_module(library(jsonld/jsonld_read)).
:- use_module(library(msg_ext)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/thread_counter)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pl/pl_term)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_debug)).
:- use_module(library(rdf/rdf_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(semweb/rdf11)). % Operators.
:- use_module(library(string_ext)).
:- use_module(library(uri/uri_ext)).
:- use_module(library(zlib)).

:- use_module(cpack('LOD-Laundromat'/laundromat_fs)).
:- use_module(cpack('LOD-Laundromat'/seedlist)).

:- meta_predicate
    rdf_store_messages(+, +, 0).

:- rdf_meta
   rdf_store_messages(+, r, :).

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
  maplist(threadsafe_name, [meta,warn], [MetaAlias,WarnAlias]),
  CleanOpts = [compress(gzip),metadata(M),relative_to(Dir),sort_dir(Dir)],
  FileOpts = [compress(gzip)],
  setup_call_cleanup(
    (
      open_any2(MetaFile, append, _, MetaClose_0, [alias(MetaAlias)|FileOpts]),
      open_any2(WarnFile, append, _, WarnClose_0, [alias(WarnAlias)|FileOpts])
    ),
    rdf_store_messages(WarnAlias, Doc, (
      rdf_clean(Iri, DataFile, CleanOpts),
      rdf_store_metadata(MetaAlias, Doc, M)
    )),
    (
      close_any2(WarnClose_0),
      close_any2(MetaClose_0)
    )
  ),
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
  create_thread_counter(washing_machine(idle)),
  washing_machine0.

washing_machine0 :-
  clean(Hash, Iri),
  debug(washing_machine(thread), "Cleaned ~a (~a)", [Hash,Iri]),
  washing_machine0.
washing_machine0 :-
  M = 100,
  sleep(M),
  thread_name(Name),
  increment_thread_counter(washing_machine(idle), N),
  S is M * N,
  debug(washing_machine(idle), "Washing machine ~w is ~D sec. idle", [Name,S]),
  washing_machine0.



%! rdf_store_messages(+Output, +S, :Goal_0) is det.
% Run Goal, unify Result with `true`, `false` or `exception(Error)`
% and messages with a list of generated error and warning messages.
% Each message is a term `message(Term,Kind,Lines)`.

rdf_store_messages(Output, S, Goal_0) :-
  setup_call_cleanup(
    (
      create_thread_counter(rdf_warning),
      asserta((
        user:thread_message_hook(Term,Kind,_) :-
          error_kind(Kind),
          threadsafe_format(warn, "~w~n", [Term]),
          increment_thread_counter(rdf_warning)
      ))
    ),
    (
      (   catch(Goal_0, E, true)
      ->  (   var(E)
          ->  Result = true,
              End0 = true
          ;   E = error(existence_error(open_any2,M),_)
          ->  rdf_store_metadata(Output, S, M),
              End0 = "No stream"
          ;   Result = exception(E),
              End0 = E
          ),
          debug(rdf(debug), "[RESULT] ~w ~w", [Result,Goal_0])
      ;   msg_warning("[FAILED] ~w", [Goal_0]),
          End0 = fail
      ),
      with_output_to(string(End), write_term(End0)),
      with_output_to(Output,
        gen_ntriple(S, llo:end, End^^xsd:string))
    ),
    (
      delete_thread_counter(rdf_warning, N),
      with_output_to(Output,
        gen_ntriple(S, llo:number_of_warnings, N^^xsd:nonNegativeInteger))
    )
  ).

error_kind(warning).
error_kind(error).


%! rdf_store_metadata(+Output, +S, +M) is det.

rdf_store_metadata(Output, S1, M) :-
  jsonld_metadata(M, Jsonld1),
  atom_string(S1, S2),
  Jsonld2 = Jsonld1.put(_{'@id': S2}),
  (debugging(rdf(debug)) -> json_write_dict(user_output, Jsonld2) ; true),
  forall(jsonld_tuple(Jsonld2, rdf(S,P,O)), (
    (debugging(rdf(debug)) -> rdf_print(S, P, O, _) ; true),
    with_output_to(Output, gen_ntriple(S, P, O))
  )).
