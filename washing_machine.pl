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
:- use_module(library(hash_ext)).
:- use_module(library(msg_ext)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/thread_counter)).
:- use_module(library(os/thread_ext)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_debug)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(semweb/rdf11)). % Operators.
:- use_module(library(string_ext)).
:- use_module(library(uri/uri_ext)).
:- use_module(library(zlib)).

:- use_module(cpack('LOD-Laundromat'/laundromat_fs)).
:- use_module(cpack('LOD-Laundromat'/seedlist)).

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
  maplist(ldoc_file(Doc), [data,meta,msg], [DataFile,MetaFile,MsgFile]),
  CleanOpts = [compress(gzip),metadata(M),relative_to(Dir),sort_dir(Dir)],
  FileOpts = [compress(gzip)],
  setup_call_cleanup(
    (
      open_any2(MetaFile, append, _, MetaClose_0, [alias(meta)|FileOpts]),
      open_any2(MsgFile, append, _, MsgClose_0, [alias(msg)|FileOpts])
    ),
    rdf_store_messages(Doc, (
      rdf_clean(Iri, DataFile, CleanOpts),
      rdf_store_metadata(Doc, M)
    )),
    (
      close_any2(MsgClose_0),
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

:- dynamic
    currently_debugging0/1.

%currently_debugging0('00385477bf97bd4ebe1d0719a65100e1').



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
