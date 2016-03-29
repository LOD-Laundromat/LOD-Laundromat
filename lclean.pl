:- module(
  lclean,
  [
    clean/1,          % +Hash
    clean_iri/1,      % +Iri
    reset/1,          % +Hash
    reset_and_clean/1 % +Hash
  ]
).

/** <module> LOD Laundromat cleaning

@author Wouter Beek
@version 2016/03
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(error)).
:- use_module(library(filesex)).
:- use_module(library(gen/gen_ntuples)).
:- use_module(library(hash_ext)).
:- use_module(library(http/json)).
:- use_module(library(jsonld/jsonld_metadata)).
:- use_module(library(jsonld/jsonld_read)).
:- use_module(library(os/dir_ext)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/process_ext)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pl/pl_term)).
:- use_module(library(print_ext)).
:- use_module(library(prolog_stack)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(rdf/rdf_print)).
:- use_module(library(semweb/rdf11)). % Operators.
:- use_module(library(string_ext)).
:- use_module(library(uri/uri_ext)).
:- use_module(library(zlib)).

:- use_module(cpack('LOD-Laundromat'/lfs)).
:- use_module(cpack('LOD-Laundromat'/lhdt)).
:- use_module(cpack('LOD-Laundromat'/seedlist)).

:- meta_predicate
    rdf_store_messages(+, +, 0, -).

:- rdf_meta
   rdf_store_messages(+, r, :, -).

:- dynamic
    currently_debugging0/1.

% Seems to have been cleaned multiple times.  Maybe archived entries?
currently_debugging0('020635c9a458946c8f4a5591de5f69c7').
currently_debugging0('07ecaef4979684bfa4507ecc28b54461').



%! clean(+Hash) is det.
% Clean a specific seed from the seedlist.
% Does not re-clean documents.
%
% @throws existence_error If the seed is not in the seedlist.

clean(Hash) :-
  lfile_lhash(File, data, nquads, Hash),
  exists_file(File), !,
  msg_notification("Already cleaned ~a", [Hash]).
clean(Hash) :-
  clean_seed(Hash, _).


clean_seed(Hash, Iri) :-
  begin_seed(Hash, Iri),
  clean_seed0(Hash, Iri),
  end_seed(Hash).

clean_seed0(Hash, Iri) :-
  ldir_lhash(Dir, Hash),
  ldoc_lhash(Doc, data, Hash),
  with_mutex(lfs, make_directory_path(Dir)),
  ldir_lfile(Dir, data, nquads, DataFile),
  ldir_lfile(Dir, meta, nquads, MetaFile),
  ldir_lfile(Dir, warn, nquads, WarnFile),
  setup_call_cleanup(
    (
      gzopen(MetaFile, write, MetaSink, [format(gzip)]),
      gzopen(WarnFile, write, WarnSink, [format(gzip)])
    ),
    (
      State = _{meta: MetaSink, warn: WarnSink, warns: 0},
      CleanOpts = [compress(gzip),metadata(M1),relative_to(Dir),sort_dir(Dir),warn(WarnSink)],
      currently_debugging(Hash, Iri),
      rdf_store_messages(State, Doc, rdf_clean(Iri, DataFile, CleanOpts), M2),
      (var(M1) -> M = M2 ; M = M1),
      (var(M) -> format(user_output, "~w~n", [Hash]) ; true), %DEB?
      rdf_store_metadata(State, Doc, M)
    ),
    (
      close(WarnSink),
      close(MetaSink)
    )
  ).
  %lhdt_build(Doc),
  %absolute_file_name('dirty.gz', DirtyTo, Opts),
  %call_collect_messages(rdf_download_to_file(Iri, DirtyTo, [compress(gzip)])),

currently_debugging(Hash, Iri) :-
  currently_debugging0(Hash), !,
  ansi_format(user_output, [bold], "~a ~a", [Hash,Iri]),
  gtrace. %DEB
currently_debugging(_).



%! clean_iri(+Iri) is det.
% Clean a specific IRI, achieved by circumvents the seedlist.
% For debugging purposes only.

clean_iri(I1) :-
  iri_normalized(I1, I2),
  md5(I2, Hash),
  clean_seed(Hash, I2).



%! reset(+Hash) is det.

reset(Hash) :-
  % Step 1: Unload the RDF data and metadata from memory.
  lrdf_unload(Hash),

  % Step 2: Remove the data and metadata files from disk.
  ldir_lhash(Dir, Hash),
  with_mutex(lfs, (
    (exists_directory(Dir) -> delete_directory_and_contents(Dir) ; true)
  )),

  % Step 3: Reset the seed in the seedlist.
  reset_seed(Hash).



%! reset_and_clean(+Hash) is det.

reset_and_clean(Hash) :-
  reset(Hash),
  clean(Hash).





% HELPERS %

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
    (debugging(wm(low)) -> with_output_to(user_output, rdf_print_triple(S, P, O)) ; true),
    with_output_to(State.meta, gen_ntriple(S, P, O))
  )).
