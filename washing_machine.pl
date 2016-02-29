:- module(
  washing_machine,
  [
    add_washing_machine/0,
    clean_iri/1,             % +Iri
    clean_seed/1,            % +Hash
    document_hash/2,         % +Doc, -Hash
    document_to_directory/2, % +Doc, -Dir
    is_document/1,           % +Doc
    reset_document/1         % +Doc
  ]
).

/* <module> LOD Laundromat

@author Wouter Beek
@version 2016/01-2016/02
*/

:- use_module(library(apply)).
:- use_module(library(debug_ext)).
:- use_module(library(filesex)).
:- use_module(library(hash_ext)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/thread_counter)).
:- use_module(library(os/thread_ext)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_debug)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(rdf11/rdf11)). % Operators.
:- use_module(library(string_ext)).
:- use_module(library(uri/uri_ext)).

:- use_module(cpack('LOD-Laundromat'/seedlist)).





%! add_washing_machine is det.
% Add a LOD Laundromat thread.

add_washing_machine :-
  detached_thread(washing_machine).



%! clean_iri(+Iri) is det.
% Clean a specific IRI, achieved by circumvents the seedlist.
% For debugging purposes only.

clean_iri(I1) :-
  iri_normalized(I1, I2),
  md5(I2, H),
  clean0(H, I2).



%! clean_seed(+Hash) is det.
% @throws existence_error If the seed is not in the seedlist.

clean_seed(H) :-
  clean_seed(H, _).


%! clean_seed(-Hash, -Iri) is det.
% Cleans a dirty seed from the seedlist.

clean_seed(H, I) :-
  begin_seed(H, I),
  clean0(H, I),
  end_seed(H).

clean0(Hash, Iri) :-
  hash_to_directory(Hash, Dir),
  document_hash(Doc, Hash),
  make_directory_path(Dir),
  Opts = [access(write),relative_to(Dir)],
  absolute_file_name('data.nq.gz', DataTo, Opts),
  absolute_file_name('meta.nq.gz', MetaTo, Opts),
  setup_call_cleanup(
    open_any2(MetaTo, append, Write, Close_0, [compress(gzip)]),
    with_output_to(Write,
      rdf_store_messages(Doc, (
        rdf_clean(Iri, DataTo, [compress(gzip),metadata(M)]),
        rdf_store_metadata(Doc, M)
      ))
    ),
    close_any2(Close_0)
  ).
  %absolute_file_name('dirty.gz', DirtyTo, Opts),
  %call_collect_messages(rdf_download_to_file(Iri, DirtyTo, [compress(gzip)])).



%! document_to_directory(+Document, -Directory) is det.

document_to_directory(Doc, Dir) :-
  document_hash(Doc, Hash),
  hash_to_directory(Hash, Dir).



%! document_hash(+Document, -Hash) is det.

document_hash(Doc, Hash) :-
  rdf_global_id(data:Hash, Doc).



%! hash_to_directory(+Hash, -Directory) is det.

hash_to_directory(Hash, Dir4) :-
  atom_codes(Hash, [H1,H2|T]),
  maplist(atom_codes, [Dir1,Dir2], [[H1,H2],T]),
  atomic_list_concat([Dir1,Dir2], /, Dir3),
  directory_file_path('/home/wbeek/Data/', Dir3, Dir4).



%! is_document(+Document) is semidet.

is_document(Doc) :-
  document_to_directory(Doc, Dir),
  exists_directory(Dir).



%! reset_document(+Doc) is det.

reset_document(Doc) :-
  % Step 1: Unload the RDF metadata from memory.
  rdf_unload_graph(Doc),
  
  % Step 2: Remove the data and metadata files from disk.
  document_to_directory(Doc, Dir),
  delete_directory_and_contents(Dir),

  % Step 3: Reset the seed in the seedlist.
  document_hash(Doc, Hash),
  reset_seed(Hash).



washing_machine :-
  clean_seed(Hash, Iri),
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
