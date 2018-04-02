:- module(ll_upload, [ll_upload/1]).

/** <module> LOD Laundromat: Upload

@author Wouter Beek
@version 2018
*/

:- use_module(library(debug)).
:- use_module(library(settings)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(tapir)).

:- debug(ll(_)).

:- initialization
   init_ll_upload.

:- setting(ll:data_directory, any, _, "").

ll_upload(Hash) :-
  find_hash_file(parsed, Hash), %NONDET
  debug(ll(upload), "┌─> uploading ~a", [Hash]),
  upload_file(Hash),
  debug(ll(upload), "└─< uploaded ~a", [Hash]).

upload_file(Hash) :-
  hash_file(Hash, 'clean.nq.gz', File1),
  hash_file(Hash, 'meta.nq', File2),
  compress_file(File2),
  dataset_create('lod-laundromat', Hash, _{}, _),
  dataset_upload('lod-laundromat', Hash, _{files: [File1,File2]}).

init_ll_upload :-
  conf_json(Conf),
  set_setting(ll:data_directory, Conf.'data-directory').
