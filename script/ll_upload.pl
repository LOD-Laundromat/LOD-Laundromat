:- module(
  ll_upload,
  [
    ll_upload_data/1,    % ?Hash
    ll_upload_metadata/0
  ]
).

/** <module> LOD Laundromat: Upload

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(settings)).
:- use_module(library(zlib)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(sw/rdf_prefix)).
:- use_module(library(tapir)).

:- debug(ll(_)).

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





%! ll_upload_data(+Hash:atom) is det.
%! ll_upload_data(-Hash:atom) is nondet.

ll_upload_data(Hash) :-
  find_hash_file(parsed, Hash), %NONDET
  debug(ll(upload), "┌─> uploading ~a", [Hash]),
  ll_upload_data_file(Hash),
  debug(ll(upload), "└─< uploaded ~a", [Hash]).

ll_upload_data_file(Hash) :-
  hash_file(Hash, 'clean.nq.gz', File1),
  hash_file(Hash, 'meta.nq', File2),
  compress_file(File2),
  dataset_create('lod-laundromat', Hash, _{}, _),
  dataset_upload('lod-laundromat', Hash, _{files: [File1,File2]}).



%! ll_upload_metadata is det.

ll_upload_metadata :-
  setup_call_cleanup(
    gzopen('meta.nq.gz', write, Out),
    forall(
      find_hash_file(parsed, Hash),
      (
        hash_file(Hash, 'meta.nq', File),
        setup_call_cleanup(
          open(File, read, In),
          copy_stream_data(In, Out),
          close(In)
        )
      )
    ),
    close(Out)
  ),
  ignore(dataset_delete('lod-laundromat', metadata)),
  dataset_create('lod-laundromat', metadata, _{}, _),
  dataset_upload(
    'lod-laundromat',
    metadata,
    _{files: ['meta.nq.gz'], prefixes: [bnode,def,error,graph,http,id]}
  ).





% INITIALIZATION %

init_ll_upload :-
  conf_json(Conf),
  set_setting(ll:data_directory, Conf.'data-directory').
