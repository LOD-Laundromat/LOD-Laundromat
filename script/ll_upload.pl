:- module(
  ll_upload,
  [
    ll_clear/0,
    ll_clear_datasets/0,
    ll_clear_users/0,
    ll_compile/1,        % +Base
    ll_upload_data/1,    % ?Hash
    ll_upload_metadata/0
  ]
).

/** <module> LOD Laundromat: Upload

@author Wouter Beek
@version 2018
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(lists)).
:- use_module(library(settings)).
:- use_module(library(zlib)).

:- use_module(library(conf_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(file_ext)).
:- use_module(library(http/tapir)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(sw/hdt_db)).
:- use_module(library(sw/rdf_prefix)).

:- initialization
   init_ll_upload.

:- maplist(rdf_assert_prefix, [
     bnode-'https://lodlaundromat.org/.well-known/genid/',
     def-'https://lodlaundromat.org/def/',
     error-'https://lodlaundromat.org/error/def/',
     graph-'https://lodlaundromat.org/graph/',
     http-'https://lodlaundromat.org/http/def/',
     id-'https://lodlaundromat.org/id/'
   ]).

:- setting(ll:data_directory, any, _, "").





%! ll_clear is det.
%
% Delete all data currently stored in the LOD Laundromat.

ll_clear :-
  ll_clear_datasets,
  ll_clear_users.



%! ll_clear_datasets is det.

ll_clear_datasets :-
  forall(
    dataset(User, Dataset, _),
    dataset_delete(User, Dataset)
  ).



%! ll_clear_users is det.

ll_clear_users :-
  forall(
    user(User, _),
    user_delete(User)
  ).



%! ll_compile(+Base:oneof([data,meta])) is det.

ll_compile(Base) :-
  aggregate_all(set(Hash-RdfFile), rdf_source_file(Hash, Base, RdfFile), Pairs),
  length(Pairs, N),
  Counter = count(0,N),
  forall(
    member(Hash-RdfFile, Pairs),
    (
      Counter = count(N1,N),
      N2 is N1 + 1,
      format("~D/~D (~a)\n", [N2,N,Hash]),
      hdt_create(RdfFile),
      nb_setarg(1, Counter, N2)
    )
  ).

rdf_source_file(Hash, Base, RdfFile) :-
  find_hash(Hash, finished),
  file_name_extension(Base, 'nq.gz', Local),
  hash_file(Hash, Local, RdfFile),
  exists_file(RdfFile),
  % HDT creation throws an exception for empty files.
  \+ is_empty_file(RdfFile),
  hdt_file_name(RdfFile, HdtFile),
  \+ exists_file(HdtFile).



%! ll_upload_data(+Hash:atom) is det.
%! ll_upload_data(-Hash:atom) is nondet.

ll_upload_data(Hash) :-
  find_hash(Hash, parsed), %NONDET
  indent_debug(1, ll(upload), "> uploading ~a", [Hash]),
  ll_upload_data_file(Hash),
  indent_debug(-1, ll(upload), "< uploaded ~a", [Hash]).

ll_upload_data_file(Hash) :-
  maplist(hash_file(Hash), ['data.nq.gz','meta.nq.gz'], [DataFile,MetaFile]),
  ignore(dataset_delete('lod-laundromat', Hash)),
  dataset_upload('lod-laundromat', Hash, _{files: [DataFile,MetaFile]}).



%! ll_upload_metadata is det.

ll_upload_metadata :-
  ToFile = 'meta.nq.gz',
  setup_call_cleanup(
    gzopen(ToFile, write, Out),
    forall(
      find_hash(Hash, finished),
      (
        hash_file(Hash, ToFile, FromFile),
        setup_call_cleanup(
          gzopen(FromFile, read, In),
          copy_stream_data(In, Out),
          close(In)
        )
      )
    ),
    close(Out)
  ),
  ignore(dataset_delete(_, metadata)),
  dataset_upload(
    _,
    metadata,
    _{files: [ToFile], prefixes: [bnode,def,error,graph,http,id]}
  ),
  delete_file(ToFile).





% INITIALIZATION %

init_ll_upload :-
  conf_json(Conf),
  set_setting(ll:data_directory, Conf.'data-directory').
