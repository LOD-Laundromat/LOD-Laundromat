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
:- use_module(library(http/html_write)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/rest)).
:- use_module(library(lodapi/lodapi_generics)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/thread_counter)).
:- use_module(library(os/thread_ext)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_debug)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(rdf11/rdf11)). % Operators.
:- use_module(library(string_ext)).

:- use_module(cpack('LOD-Laundromat'/seedlist)).

:- rdf_register_prefix(data, 'http://cliopatria.lod.labs.vu.nl/data/').
:- rdf_register_prefix(meta, 'http://cliopatria.lod.labs.vu.nl/meta/').

:- http_handler(root(data), data, [prefix]).
:- http_handler(root(meta), meta, [prefix]).

data(Req) :- rest_handler(Req, data, is_document, document(data), documents(data)).
meta(Req) :- rest_handler(Req, meta, is_document, document(meta), documents(meta)).
document(Variant, Method, MTs, Doc) :- rest_mediatype(Method, MTs, Doc, document_mediatype(Variant)).
documents(Variant, Method, MTs) :- rest_mediatype(Method, MTs, documents_mediatype(Variant)).

document_mediatype(Alias, get, text/html, Doc) :-
  (   rdf_graph(Doc)
  ->  true
  ;   document_to_path(Doc, Dir),
      atomic_list_concat([Alias,nq,gz], ., Local),
      resolve_file(Dir, Local, File),
      rdf_load_file(File, [graph(Doc)])
  ),
  rdf_global_id(Alias:Md5, Doc),
  string_list_concat(['Open Data Market',Md5,Alias], " - ", Title),
  reply_html_page(cliopatria(default), title(Title),
    \(cpa_browse:list_triples(_, Doc, _, _))
  ).

documents_mediatype(Alias, get, text/html) :-
  {string_list_concat(["LOD Laundromat",Alias,"Documents"], " - ", Title)},
  reply_html_page(cliopatria(default), title(Title), "").


%! is_document(+Document) is semidet.

is_document(Doc) :-
  document_to_path(Doc, Dir1),
  resolve_dir(Dir1, Dir2),
  exists_directory(Dir2).



%! resolve_dir(+Dir1, -Dir2) is det.

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



%! document_to_path(+Document, -Path) is det.

document_to_path(Doc, Path) :-
  member(Alias, [data,meta]),
  rdf_global_id(Alias:Md5, Doc), !,
  atom_codes(Md5, [H1,H2|T]),
  maplist(atom_codes, [Dir1,Dir2], [[H1,H2],T]),
  atomic_list_concat([Dir1,Dir2], /, Path).



%! add_thread is det.
% Add a LOD Laundromat thread.

add_thread :-
  detached_thread(thread).



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
