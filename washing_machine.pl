:- module(
  washing_machine,
  [
    add_washing_machine/0,
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

:- use_module(cpack('LOD-Laundromat'/laundromat_fs)).
:- use_module(cpack('LOD-Laundromat'/seedlist)).

:- rdf_meta
   reset_ldoc(r).





%! add_washing_machine is det.
% Add a LOD Laundromat thread.

add_washing_machine :-
  detached_thread(washing_machine).



%! clean(+Hash) is det.
% Does not re-clean documents.
%
% @throws existence_error If the seed is not in the seedlist.

clean(Hash) :-
  ldoc_hash(Doc, Hash),
  ldoc_data_file(Doc, _), !,
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
  ldir_hash(Dir, Hash),
  make_directory_path(Dir),
  Opts = [access(write),relative_to(Dir)],
  absolute_file_name('data.nq.gz', DataTo, Opts),
  absolute_file_name('meta.nq.gz', MetaTo, Opts),
  ldoc_hash(Doc, Hash),
  CleanOpts = [compress(gzip),metadata(M),relative_to(Dir),sort_dir(Dir)],
  setup_call_cleanup(
    open_any2(MetaTo, append, Write, Close_0, [compress(gzip)]),
    with_output_to(Write,
      rdf_store_messages(Doc, (
        rdf_clean(Iri, DataTo, CleanOpts),
        rdf_store_metadata(Doc, M)
      ))
    ),
    close_any2(Close_0)
  ),
  ldoc_meta_load(Doc).
  %absolute_file_name('dirty.gz', DirtyTo, Opts),
  %call_collect_messages(rdf_download_to_file(Iri, DirtyTo, [compress(gzip)])).



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
  forall(ldoc(Doc), ldoc_meta_load(Doc)).



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



washing_machine :-
  clean(Hash, Iri),
  debug(washing_machine(thread), "Cleaned ~a (~a)", [Hash,Iri]),
  washing_machine.
washing_machine :-
  M = 100,
  sleep(M),
  thread_name(Name),
  increment_thread_counter(washing_machine(idle), N),
  S is M * N,
  debug(washing_machine(idle), "Washing machine ~w is ~D sec. idle", [Name,S]),
  washing_machine.
