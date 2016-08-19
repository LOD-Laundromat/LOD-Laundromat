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
@version 2016/03-2016/05, 2016/08
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(error)).
:- use_module(library(filesex)).
:- use_module(library(hash_ext)).
:- use_module(library(http/json)).
:- use_module(library(jsonld/jsonld_metadata)).
:- use_module(library(jsonld/jsonld_read)).
:- use_module(library(os/compress_ext)).
:- use_module(library(os/directory_ext)).
:- use_module(library(os/file_ext)).
:- use_module(library(os/gnu_wc)).
:- use_module(library(os/io)).
:- use_module(library(os/process_ext)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pl_term)).
:- use_module(library(print_ext)).
:- use_module(library(prolog_stack)).
:- use_module(library(q/q_print)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_error)).
:- use_module(library(rdf/rdf__io)).
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

currently_debugging0('1cbe5a4bd869c2f5e64ce08480996a97').





%! clean is det.
% Clean an -- arbitrarily chosen - seed.

clean :-
  clean(_, _).


%! clean(+Hash) is det.
%
% Clean a specific seed from the seedlist.  Does not re-clean
% documents.
%
% @throws existence_error If the seed is not in the seedlist.

clean(Hash) :-
  q_file_hash(File, data, ntriples, Hash),
  q_file_is_ready(File), !,
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
  q_dir_hash(Dir, Hash),
  q_graph_hash(G, data, Hash),
  with_mutex(lclean, make_directory_path(Dir)),
  q_dir_file(Dir, data, nquads, DataFile),
  q_dir_file(Dir, meta, ntriples, MetaFile),
  q_dir_file(Dir, warn, ntriples, WarnFile),
  call_to_streams(MetaFile, WarnFile, clean_inner0),
  count_numlines(WarnFile, NumWarns),
  lhdt_build(Hash),
  %absolute_file_name('dirty.gz', DirtyTo, Opts),
  %call_collect_messages(rdf_download_to_file(Iri, DirtyTo)),
  directory_file_path(Dir, done, Done),
  touch(Done).


clean_inner0(MetaOut, WarnOut) :-
  State = _{meta: MetaOut, warn: WarnOut, warns: 0},
  CleanOpts = [
    compress(gzip),
    metadata(Meta1),
    parse_headers(true),
    relative_to(Dir),
    sort_dir(Dir),
    warn(WarnOut)
  ],
  currently_debugging(Hash, Iri),
  rdf_store_messages(State, G, rdf_clean(Iri, DataFile, CleanOpts), Meta2),
  (var(Meta1) -> Meta = Meta2 ; Meta = Meta1),
  (var(Meta) -> format(user_output, "~w~n", [Hash]) ; true),
  rdf_store_metadata(State, G, Meta).


currently_debugging(Hash, Iri) :-
  currently_debugging0(Hash), !,
  ansi_format(user_output, [bold], "~a ~a", [Hash,Iri]),
  gtrace. %DEB
currently_debugging(_, _).



%! clean_iri(+Iri) is det.
%
% Clean a specific IRI, achieved by circumvents the seedlist.
% For debugging purposes only.

clean_iri(I1) :-
  iri_normalized(I1, I2),
  md5(I2, Hash),
  clean(Hash, I2).



%! reset(+Hash) is det.

reset(Hash) :-
  % Remove directory and contents from disk.
  q_dir_hash(Dir, Hash),
  with_mutex(lclean, delete_directory_and_contents_msg(Dir)),
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

%! rdf_store_messages(+State, +G, :Goal_0, -Meta) is det.
%
% Run Goal, unify Result with `true`, `false` or `exception(Error)`
% and messages with a list of generated error and warning messages.
% Each message is a term `message(Term, Kind, Lines)`.

rdf_store_messages(State, G, Goal_0, Meta) :-
  asserta((
    user:thread_message_hook(Term,Kind,_) :-
      error_kind(Kind),
      rdf_store_warning(State.warn, G, Term)
  )),
  (catch(Goal_0, E, true) -> true ; E = fail),
  (   var(E)
  ->  End0 = true
  ;   E = error(existence_error(http_open, Meta),_)
  ->  End0 = "No stream"
  ;   End0 = E,
      msg_warning("[FAILED] ~w ~w~n", [End0,G]),
      Meta = _{}
  ),
  with_output_to(string(End), write_term(End0)),
  % @bug RDF prefix expansion does not work for `llo:end’ here.
  rdf_equal(llo:end, P),
  rdf_store(State.meta, G, P, End^^xsd:string).



%! rdf_store_metadata(+State, +G, +M) is det.

rdf_store_metadata(State, G1, M) :-
  jsonld_metadata(M, Jsonld1),
  atom_string(G1, G2),
  put_dict('@id', Jsonld1, G2, Jsonld1),
  (debugging(wm(low)) -> json_write_dict(user_output, Jsonld2) ; true),
  jsonld_tuples(Jsonld2, Triples),
  if_debug(wm(low), q_print_triples(Triples)),
  maplist(rdf_store(State.meta), Triples).
