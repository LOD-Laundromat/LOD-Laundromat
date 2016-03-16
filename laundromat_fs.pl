:- module(
  laundromat_fs,
  [
    ldir/1,           % ?Dir
    ldir_hash/2,      % ?Dir, ?Hash
    ldir_ldoc/2,      % ?Dir, ?Doc
    ldoc/1,           % ?Doc
    ldoc_data_file/2, % +Doc, -File
    ldoc_data_load/1, % +Doc
    ldoc_hash/2,      % ?Doc, ?Hash
    ldoc_hdt_file/2,  % +Doc, -File
    ldoc_lmod/2,      % +Doc, -LastModified
    ldoc_meta_file/2, % +Doc, -File
    ldoc_meta_load/1  % +Doc
  ]
).

/** <module> LOD Laundromat File System

    hash
    /  \
ldir -- ldoc

@author Wouter Beek
@version 2016/02-2016/03
*/

:- use_module(library(error)).
:- use_module(library(lists)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(settings)).

:- rdf_meta
   ldir_ldoc(?, r),
   ldoc(r),
   ldoc_data_file(r, -),
   ldoc_data_load(r),
   ldoc_hash(r, ?),
   ldoc_hdt_file(r, -),
   ldoc_lmod(r, -),
   ldoc_meta_file(r, -),
   ldoc_meta_load(r).

:- setting(data_dir, atom, '/scratch/lodlab/crawls/13/',
     'Directory where LOD Laundromat stores the cleaned data.'
   ).





%! ldir(+Dir) is semidet.
%! ldir(-Dir) is nondet.

ldir(Dir3) :-
  setting(data_dir, Dir1),
  subdir(Dir1, Dir2),
  subdir(Dir2, Dir3).



%! ldir_hash(+Dir, -Hash) is det.
%! ldir_hash(-Dir, +Hash) is det.

ldir_hash(Dir, Hash) :-
  ground(Dir), !,
  atomic_list_concat(Subdirs, /, Dir),
  reverse(Subdirs, [Postfix,Prefix|_]),
  atom_concat(Prefix, Postfix, Hash).
ldir_hash(Dir4, Hash) :-
  ground(Hash), !,
  atom_codes(Hash, [H1,H2|T]),
  maplist(atom_codes, [Dir1,Dir2], [[H1,H2],T]),
  atomic_list_concat([Dir1,Dir2], /, Dir3),
  setting(data_dir, Dir0),
  directory_file_path(Dir0, Dir3, Dir4).
ldir_hash(Dir, _) :-
  instantiation_error(Dir).



%! ldir_ldoc(+Dir, -Doc) is det.
%! ldir_ldoc(-Dir, +Doc) is det.

ldir_ldoc(Dir, Doc) :-
  ground(Dir), !,
  ldir_hash(Dir, Hash),
  ldoc_hash(Doc, Hash).
ldir_ldoc(Dir, Doc) :-
  ground(Doc), !,
  ldoc_hash(Doc, Hash),
  ldir_hash(Dir, Hash).
ldir_ldoc(Dir, _) :-
  instantiation_error(Dir).



%! ldoc(+Doc) is semidet.
%! ldoc(-Doc) is nondet.

ldoc(Doc) :-
  ground(Doc), !,
  ldir_ldoc(Dir, Doc),
  exists_directory(Dir).
ldoc(Doc) :-
  ldir(Dir),
  ldir_ldoc(Dir, Doc).



%! ldoc_data_file(+Doc, -File) is det.

ldoc_data_file(Doc, File) :-
  ldir_ldoc(Dir, Doc),
  directory_file_path(Dir, 'data.nq.gz', File),
  exists_file(File).



%! ldoc_data_load(+Doc) is det.

ldoc_data_load(Doc) :-
  ldoc_data_file(Doc, File),
  access_file(read, File),
  rdf_load_file(File, [graph(Doc)]).



%! ldoc_hash(+Doc, -Hash) is det.
%! ldoc_hash(-Doc, +Hash) is det.

ldoc_hash(Doc, Hash) :-
  rdf_global_id(data:Hash, Doc).



%! ldoc_hdt_file(+Doc, -File) is det.

ldoc_hdt_file(Doc, File) :-
  ldir_ldoc(Dir, Doc),
  directory_file_path(Dir, 'data.hdt', File),
  exists_file(File).



%! ldoc_lmod(+Doc, -Modified) is det.

ldoc_lmod(Doc, Mod) :-
  ldir_ldoc(Dir, Doc),
  time_file(Dir, Mod).



%! loc_meta_file(+Doc, -File) is det.

ldoc_meta_file(Doc, File) :-
  ldir_ldoc(Dir, Doc),
  directory_file_path(Dir, 'meta.nq.gz', File).



%! ldoc_meta_load(+Doc) is det.

ldoc_meta_load(Doc) :-
  rdf_graph(Doc), !.
ldoc_meta_load(Doc) :-
  ldoc_meta_file(Doc, File),
  rdf_load_file(File, [graph(Doc)]).





% HELPERS %

%! is_dummy_file(+File) is semidet.

is_dummy_file('.').
is_dummy_file('..').



% subdir(+Dir, -Subdir) is nondet.

subdir(Dir1, Dir2) :-
  directory_files(Dir1, Subdirs1),
  member(Subdir1, Subdirs1),
  \+ is_dummy_file(Subdir1),
  directory_file_path(Dir1, Subdir1, Dir2).
