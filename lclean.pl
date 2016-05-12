:- module(
  lclean,
  [
    clean/0,
    clean/1,      % +Hash
    clean_iri/1,  % +Iri
    reset/1,      % +Hash
    thread_seed/2 % ?Alias, ?Hash
  ]
).

/** <module> LOD Laundromat cleaning

@author Wouter Beek
@version 2016/03-2016/05
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
:- use_module(library(lodcli/lodfs)).
:- use_module(library(lodcli/lodhdt)).
:- use_module(library(os/compress_ext)).
:- use_module(library(os/dir_ext)).
:- use_module(library(os/file_ext)).
:- use_module(library(os/gnu_wc)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/process_ext)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pl/pl_term)).
:- use_module(library(print_ext)).
:- use_module(library(prolog_stack)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_error)).
:- use_module(library(rdf/rdf_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(rdf/rdf_prefix)).
:- use_module(library(rdf/rdf_print)).
:- use_module(library(semweb/rdf11)). % Operators.
:- use_module(library(string_ext)).
:- use_module(library(uri)).
:- use_module(library(zlib)).

:- use_module(seedlist).

:- meta_predicate
    rdf_store_messages(+, +, 0, -).

:- rdf_meta
   rdf_store_messages(+, r, :, -).

:- dynamic
    currently_debugging0/1,
    thread_seed0/2.

%%%%currently_debugging0('9f251a7f61cff3fd85550a1b5c2f4efd').





%! clean is det.
% Clean an -- arbitrarily chosen - seed.

clean :-
  clean(_, _).


%! clean(+Hash) is det.
% Clean a specific seed from the seedlist.
% Does not re-clean documents.
%
% @throws existence_error If the seed is not in the seedlist.

clean(Hash) :-
  lready_hash(Hash), !,
  msg_notification("Already cleaned ~a", [Hash]).
clean(Hash) :-
  clean(Hash, _).


%! clean(?Hash, ?Iri) is det.

clean(Hash, Iri) :-
  begin_processing_seed(Hash, Iri),
  thread_name(Alias),
  thread_seed_update(Alias, Hash),
  debug(lclean, "~a is cleaning ~a ~a", [Alias,Hash,Iri]),
  clean_inner(Hash, Iri),
  debug(lclean, "~a has cleaned ~a ~a", [Alias,Hash,Iri]),
  end_processing_seed(Hash).


clean_inner(Hash, Iri) :-
  ldir_lhash(Dir, Hash),
  ldoc_lhash(Doc, data, Hash),
  with_mutex(lclean, make_directory_path(Dir)),
  ldir_lfile(Dir, data, nquads, DataFile),
  ldir_lfile(Dir, meta, ntriples, MetaFile),
  ldir_lfile(Dir, warn, ntriples, WarnFile),
  setup_call_cleanup(
    (
      gzopen(MetaFile, write, MetaSink, [format(gzip)]),
      open(WarnFile, write, WarnSink)
    ),
    (
      State = _{meta: MetaSink, warn: WarnSink, warns: 0},
      CleanOpts = [
        compress(gzip),
        metadata(M1),
        parse_headers(true),
        relative_to(Dir),
        sort_dir(Dir),
        warn(WarnSink)
      ],
      currently_debugging(Hash, Iri),
      rdf_store_messages(State, Doc, rdf_clean(Iri, DataFile, CleanOpts), M2),
      (var(M1) -> M = M2 ; M = M1),
      (var(M) -> format(user_output, "~w~n", [Hash]) ; true), %DEB?
      rdf_store_metadata(State, Doc, M)
    ),
    (
      close(WarnSink),
      source_numlines(WarnFile, NumWarns),
      compress_file(WarnFile),
      rdf_store(MetaSink, Doc, llo:number_of_warnings, NumWarns^^xsd:nonNegativeInteger),
      close(MetaSink)
    )
  ),
  lhdt_build(Hash),
  %absolute_file_name('dirty.gz', DirtyTo, Opts),
  %call_collect_messages(rdf_download_to_file(Iri, DirtyTo, [compress(gzip)])),
  directory_file_path(Dir, done, Done),
  touch(Done).



currently_debugging(Hash, Iri) :-
  currently_debugging0(Hash), !,
  ansi_format(user_output, [bold], "~a ~a", [Hash,Iri]),
  gtrace. %DEB
currently_debugging(_, _).



%! clean_iri(+Iri) is det.
% Clean a specific IRI, achieved by circumvents the seedlist.
% For debugging purposes only.

clean_iri(I1) :-
  iri_normalized(I1, I2),
  md5(I2, Hash),
  clean(Hash, I2).



%! reset(+Hash) is det.

reset(Hash) :-
  % Remove directory and contents from disk.
  ldir_lhash(Dir, Hash),
  with_mutex(lclean, (
    (exists_directory(Dir) -> delete_directory_and_contents(Dir) ; true)
  )),
  % Reset the seedpoint in the seedlist.
  reset_seed(Hash).



%! thread_seed(?Alias, ?Hash) is nondet.

thread_seed(Alias, Hash) :-
  thread_seed0(Alias, Hash).



%! thread_seed_update(+Hash) is det.
%! thread_seed_update(+Alias, +Hash) is det.

thread_seed_update(Hash) :-
  thread_name(Alias),
  thread_seed_update(Alias, Hash).


thread_seed_update(Alias, Hash) :-
  with_mutex(thread_seed, (
    retractall(thread_seed0(Alias, _)),
    assert(thread_seed0(Alias, Hash))
  )).





% HELPERS %

%! rdf_store_messages(+State, +Doc, :Goal_0, -Metadata) is det.
% Run Goal, unify Result with `true`, `false` or `exception(Error)`
% and messages with a list of generated error and warning messages.
% Each message is a term `message(Term,Kind,Lines)`.

rdf_store_messages(State, Doc, Goal_0, M) :-
  asserta((
    user:thread_message_hook(Term,Kind,_) :-
      error_kind(Kind),
      rdf_store_warning(State.warn, Doc, Term)
  )),
  (catch(Goal_0, E, true) -> true ; E = fail),
  (   var(E)
  ->  End0 = true
  ;   E = error(existence_error(open_any2,M),_)
  ->  End0 = "No stream"
  ;   End0 = E,
      msg_warning("[FAILED] ~w ~w~n", [End0,Doc]),
      M = _{}
  ),
  with_output_to(string(End), write_term(End0)),
  % @bug RDF prefix expansion does not work for `llo:end’ here.
  rdf_equal(llo:end, P),
  rdf_store(State.meta, Doc, P, End^^xsd:string).



%! rdf_store_metadata(+State, +Doc, +M) is det.

rdf_store_metadata(State, Doc1, M) :-
  jsonld_metadata(M, Jsonld1),
  atom_string(Doc1, Doc2),
  Jsonld2 = Jsonld1.put(_{'@id': Doc2}),
  (debugging(wm(low)) -> json_write_dict(user_output, Jsonld2) ; true),
  findall(Triple, jsonld_tuple(Jsonld2, Triple), Triples),
  if_debug(wm(low), rdf_print_triples(Triples)),
  maplist(rdf_store(State.meta), Triples).
