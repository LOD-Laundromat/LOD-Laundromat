:- module(
  ll_upload,
  [
    ll_clear/0,
    ll_clear_datasets/0,
    ll_clear_users/0,
    ll_compile/1,        % +Base
    ll_compile/2,        % +NumberOfThreads, +Base
    ll_source_file/3,    % -Hash, +Base, -File
    ll_upload_data/0,
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
     error-'https://lodlaundromat.org/error/def/',
     graph-'https://lodlaundromat.org/graph/',
     http-'https://lodlaundromat.org/http/def/',
     id-'https://lodlaundromat.org/id/',
     ll-'https://lodlaundromat.org/def/'
   ]).

:- setting(ll:data_directory, any, _, "").
:- setting(ll:tmp_directory, any, _, "").





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
%! ll_compile(+NumberOfThreads:positive_integer, +Base:oneof([data,meta])) is det.

ll_compile(Base) :-
  ll_compile(10, Base).


ll_compile(N, Base) :-
  aggregate_all(
    set(ll_compile_job(Hash,File)),
    ll_source_file(Hash, Base, File),
    Jobs1
  ),
  add_indices(Jobs1, Jobs2),
  concurrent(N, Jobs2, []).

ll_compile_job(N-Max, Hash, File) :-
  format("~D/~D (~a)\n", [N,Max,Hash]),
  hdt_create(File).



%! ll_source_file(-Hash:atom, +Base:oneof([data,meta]), -File:atom) is nondet.

ll_source_file(Hash, Base, RdfFile) :-
  find_hash(Hash, finished),
  file_name_extension(Base, 'nq.gz', Local),
  hash_file(Hash, Local, RdfFile),
  exists_file(RdfFile),
  % HDT creation throws an exception for empty files.
  \+ is_empty_file(RdfFile),
  hdt_file_name(RdfFile, HdtFile),
  \+ exists_file(HdtFile).



%! ll_upload_data is det.

ll_upload_data :-
  aggregate_all(set(Hash), find_hash(Hash, finished), L),
  length(L, N),
  Counter = counter(0,N),
  forall(
    member(Hash, L),
    (
      Counter = counter(N1,N),
      N2 is N1 + 1,
      format("~D/~D (~a)\n", [N2,N,Hash]),
      find_hash_file(Hash, 'meta.nq.gz', MetaFile),
      (find_hash_file(Hash, 'data.nq.gz', DataFile) -> T = [DataFile] ; T = []),
      dataset_upload(_, Hash, _{accessLevel: public, files: [MetaFile|T]}),
      nb_setarg(1, Counter, N2)
    )
  ).



ll_upload_metadata :-
  setting(ll:tmp_directory, TmpDir),
  Base = 'meta.nq.gz',
  directory_file_path(TmpDir, Base, TmpFile),
  setup_call_cleanup(
    gzopen(TmpFile, write, Out),
    forall(
      find_hash(Hash, finished),
      (
        hash_file(Hash, Base, FromFile),
        format("~a → ~a\n", [FromFile,TmpFile]),
        setup_call_cleanup(
          gzopen(FromFile, read, In),
          copy_stream_data(In, Out),
          close(In)
        )
      )
    ),
    close(Out)
  ),
  directory_file_path(Dir, 'split_meta.sh', Script),
  process_create(path(sh), [file(Script)], [cwd(Dir)]),
  %delete_file(TmpFile),
  expand_file_name('x*.nq.gz', TmpFiles),
  maplist(ll_upload_metadata_split, TmpFiles),
  %maplist(delete_file, TmpFiles),
  true.

ll_upload_metadata_split(File) :-
  file_base_name(Base, 'nq.gz', File),
  atom_concat(x, Suffix, Base),
  atomic_list_concat([metadata,Suffix], -, Name),
  dataset_upload(_, Name, _{accessLevel: public, files: [File]}).





% GENERICS %

%! add_indices(+Compounds1:list(compound), -Compounds2:list(compound)) is det.

add_indices(Compounds1, Compounds2) :-
  length(Compounds1, Max),
  add_indices(Compounds1, 1-Max, Compounds2).

add_indices([], _, []) :- !.
add_indices([H1|T1], N1-Max, [H2|T2]) :-
  add_index(H1, N1-Max, H2),
  N2 is N1 + 1,
  add_indices(T1, N2-Max, T2).

add_index(Compound1, N-Max, Compound2) :-
  Compound1 =.. [Pred|Args],
  Compound2 =.. [Pred,N-Max|Args].





% INITIALIZATION %

init_ll_upload :-
  conf_json(Conf),
  set_setting(ll:data_directory, Conf.'data-directory'),
  set_setting(ll:tmp_directory, Conf.'tmp-directory').
