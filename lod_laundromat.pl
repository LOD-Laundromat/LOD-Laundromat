:- module(
  lod_laundromat,
  [
    add_thread/0,
    clean/1       % +Seed
  ]
).

/* <module> LOD Laundromat

@author Wouter Beek
@version 2016/01-2016/02
*/

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

:- use_module(cpack('LOD-Laundromat'/lod_basket)).

%:- rdf_register_prefix(lld, 'http://cliopatria.lod.labs.vu.nl/odm/data/').
%:- rdf_register_prefix(llm, 'http://cliopatria.lod.labs.vu.nl/odm/meta/').

%:- http_handler(root(wardrobe), lld, [prefix]).
%:- http_handler(root('wardrobe/data'), odm_data, [prefix]).
%:- http_handler(root('wardrobe/meta'), odm_meta, [prefix]).






%! add_thread is det.
% Add a LOD Laundromat thread.

add_thread :-
  detached_thread(thread).

thread :-
  clean(Hash, Iri),
  debug(lod_laundromat(thread), "Cleaned ~a (~a)", [Hash,Iri]),
  thread.
thread :-
  M = 100,
  sleep(M),
  thread_name(Name),
  increment_thread_counter(lod_laundromat(idle), N),
  S is M * N,
  debug(lod_laundromat(idle), "Thread ~w is ~D sec. idle", [Name,S]),
  thread.


%! clean(+Seed) is det.
% Cleans either a seed from the seedlist API or an IRI.

clean(Seed) :-
  (   Seed = seed(Hash,Iri,_,_,_)
  ->  true
  ;   iri_normalized(Seed, Iri),
      md5(Iri, Hash)
  ),
  clean(Hash, Iri).


%! clean(+Hash, +Iri) is det.

clean(Hash, Iri) :-
  with_mutex(seedlist, (
    once(current_seed(seed(Hash, Iri, _, 0.0, 0.0))),
    begin_seed(Hash)
  )),
  clean0(Hash, Iri),
  with_mutex(seedlist, end_seed(Hash)).

clean0(Hash, Iri) :-
  document_name(Doc, Hash),
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
  rdf_download_to_file(Iri, DirtyTo, [compress(gzip)]),
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
