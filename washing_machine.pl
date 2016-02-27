:- module(
  washing_machine,
  [
    add_washing_machine/0,
    clean_iri/1,           % +Iri
    clean_seed/1,          % +Hash
    document_to_path/2,    % +Doc, -Path
    is_document/1,         % +Doc
    resolve_file/3         % +Doc, +Local, -File
  ]
).

/* <module> LOD Laundromat

@author Wouter Beek
@version 2016/01-2016/02
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(filesex)).
:- use_module(library(hash_ext)).
:- use_module(library(lodapi/lodapi_generics)).
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

clean0(H, I) :-
  document_name(Doc, H),
  % @tbd This should be a setting.
  %Dir1 = '/scratch/lodlab/crawls/13/',
  Dir1 = '/home/wbeek/Data/',
  document_path(Doc, Dir2),
  directory_file_path(Dir1, Dir2, Dir),
  make_directory_path(Dir),
  Opts = [access(write),relative_to(Dir)],
  absolute_file_name('dirty.gz', DirtyTo, Opts),
  absolute_file_name('data.nq.gz', DataTo, Opts),
  absolute_file_name('meta.nq.gz', MetaTo, Opts),
  rdf_download_to_file(I, DirtyTo, [compress(gzip)]),
  setup_call_cleanup(
    open_any2(MetaTo, append, Write, Close_0, [compress(gzip)]),
    with_output_to(Write,
      rdf_store_messages(Doc, (
        rdf_clean(I, DataTo, [compress(gzip),metadata(M)]),
        rdf_store_metadata(Doc, M)
      ))
    ),
    close_any2(Close_0)
  ).



%! document_to_path(+Document, -Path) is det.

document_to_path(Doc, Path) :-
  uri_path(Doc, Md5),
  atom_codes(Md5, [H1,H2|T]),
  maplist(atom_codes, [Dir1,Dir2], [[H1,H2],T]),
  atomic_list_concat([Dir1,Dir2], /, Path).



%! is_document(+Document) is semidet.

is_document(Doc) :-
  document_to_path(Doc, Dir1),
  resolve_dir(Dir1, Dir2),
  exists_directory(Dir2).



%! resolve_dir(+Directory, -ResolvedDirectory) is det.

resolve_dir(Dir1, Dir2) :-
  absolute_file_name(
    Dir1,
    Dir2,
    [
      access(read),
      file_type(directory),
      file_errors(fail),
      relative_to('/home/wbeek/Data/')
    ]
  ).



%! resolve_file(+Directory, +Local, -File) is det.

resolve_file(Dir1, Local, File) :-
  resolve_dir(Dir1, Dir2),
  directory_file_path(Dir2, Local, File).



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
