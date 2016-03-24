:- module(
  laundromat_fs,
  [
    ldir/1,        % ?Dir
    ldir_lfile/4,  % ?Dir, ?Name, ?Kind, ?File
    ldir_lhash/2,  % ?Dir, ?Hash
    ldoc/2,        % ?Name, ?Doc
    ldoc_lfile/2,  % -Doc, +File
    ldoc_lfile/3,  % ?Doc, ?Kind, ?File
    ldoc_lhash/2,  % +Doc, -Hash
    ldoc_lhash/3,  % ?Doc, ?Name, ?Hash
    lfile/3,       % ?Name, ?Kind, ?File
    lfile_lhash/4, % ?File, ?Name, ?Kind, ?Hash
    lhash/1,       % ?Hash
    lrdf_load/2,   % +Hash, +Name
    lrdf_unload/1, % +Hash
    lrdf_unload/2  % +Hash, +Name
  ]
).

/** <module> LOD Laundromat File System

hash --- doc
 |
 |
dir ---- file

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
   ldoc_file(r, +, +, -),
   ldoc_hash(r, ?),
   ldoc_load(r, +).

:- setting(data_dir, atom, '/scratch/lodlab/crawls/13/',
     'Directory where LOD Laundromat stores the cleaned data.'
   ).





%! ldir(+Dir) is semidet.
%! ldir(-Dir) is nondet.

ldir(Dir3) :-
  setting(data_dir, Dir1),
  subdir(Dir1, Dir2),
  subdir(Dir2, Dir3).



%! ldir_lfile(+Dir, +Name, +Kind, -File) is det.
%! ldir_lfile(-Dir, -Name, -Kind, +File) is det.

ldir_lfile(Dir, Name, Kind, File) :-
  var(File), !,
  kind_exts(Kind, Exts),
  atomic_list_concat([Name|Exts], ., Local),
  directory_file_path(Dir, Local, File).
ldir_lfile(Dir, Name, Kind, File) :-
  directory_file_path(Dir, Local, File),
  atomic_list_concat([Name|Exts], ., Local),
  kind_exts(Kind, Exts).

kind_exts(hdt,      [hdt]).
kind_exts(nquads,   [nq,gz]).
kind_exts(ntriples, [nt,gz]).



%! ldir_lhash(+Dir, -Hash) is det.
%! ldir_lhash(-Dir, +Hash) is det.

ldir_lhash(Dir, Hash) :-
  var(Hash), !,
  atomic_list_concat(Subdirs, /, Dir),
  reverse(Subdirs, [Postfix,Prefix|_]),
  atom_concat(Prefix, Postfix, Hash).
ldir_lhash(Dir4, Hash) :-
  atom_codes(Hash, [H1,H2|T]),
  maplist(atom_codes, [Dir1,Dir2], [[H1,H2],T]),
  atomic_list_concat([Dir1,Dir2], /, Dir3),
  setting(data_dir, Dir0),
  directory_file_path(Dir0, Dir3, Dir4).



%! ldoc(+Name, -Doc) is nondet.
%! ldoc(-Name, +Doc) is nondet.

ldoc(Name, Doc) :-
  var(Doc), !,
  lhash(Hash),
  ldoc_lhash(Doc, Name, Hash).
ldoc(Name, Doc) :-
  ldoc_lhash(Doc, Name, Hash),
  lhash(Hash).



%! ldoc_lfile(-Doc, +File) is det.

ldoc_lfile(Doc, File) :-
  ldoc_lfile(Doc, _, File).


%! ldoc_lfile(+Doc, +Kind, -File) is det.
%! ldoc_lfile(-Doc, -Kind, +File) is det.

ldoc_lfile(Doc, Kind, File) :-
  var(File), !,
  ldoc_lhash(Doc, Name, Hash),
  lfile_lhash(File, Name, Kind, Hash).
ldoc_lfile(Doc, Kind, File) :-
  lfile_lhash(File, Name, Kind, Hash),
  ldoc_lhash(Doc, Name, Hash).



%! ldoc_lhash(+Doc, -Hash) is det.

ldoc_lhash(Doc, Hash) :-
  ldoc_lhash(Doc, _, Hash).


%! ldoc_lhash(+Doc, -Name, -Hash) is det.
%! ldoc_lhash(-Doc, +Name, +Hash) is det.

ldoc_lhash(Doc, Name, Hash) :-
  rdf_global_id(Name:Hash, Doc).



%! lfile(+Name, +Kind, -File) is nondet.
%! lfile(-Name, -Kind, +File) is nondet.

lfile(Name, Kind, File) :-
  var(File), !,
  ldir(Dir),
  ldir_lfile(Dir, Name, Kind, File),
  exists_file(File).
lfile(Name, Kind, File) :-
  ldir_lfile(Dir, Name, Kind, File),
  ldir(Dir).
  


%! lfile_lhash(+File, -Name, -Kind, -Hash) is det.
%! lfile_lhash(-File, +Name, +Kind, +Hash) is det.

lfile_lhash(File, Name, Kind, Hash) :-
  var(File), !,
  ldir_lhash(Dir, Hash),
  ldir_lfile(Dir, Name, Kind, File).
lfile_lhash(File, Name, Kind, Hash) :-
  ldir_lfile(Dir, Name, Kind, File),
  ldir_lhash(Dir, Hash).



%! lhash(+Hash) is semidet.
%! lhash(-Hash) is nondet.

lhash(Hash) :-
  var(Hash), !,
  ldir(Dir),
  ldir_lhash(Dir, Hash).
lhash(Hash) :-
  ldir_lhash(Dir, Hash),
  exists_directory(Dir).



%! lrdf_load(+Hash, +Name) is det.

lrdf_load(Hash, Name) :-
  ldir_lhash(Dir, Hash),
  ldir_lfile(Dir, Name, nquads, File),
  access_file(File, read),
  ldoc_lhash(Doc, Name, Hash),
  rdf_load_file(File, [graph(Doc)]).



%! lrdf_unload(+Hash) is det.

lrdf_unload(Hash) :-
  forall(lname(Name), lrdf_unload(Hash, Name)).


%! lrdf_unload(+Hash, +Name) is det.

lrdf_unload(Hash, Name) :-
  ldoc_lhash(Doc, Name, Hash),
  rdf_unload_graph(Doc).





% HELPERS %

%! is_dummy_file(+File) is semidet.

is_dummy_file('.').
is_dummy_file('..').



%! lname(+Name) is semidet.
%! lname(-Name) is multi.

lname(data).
lname(meta).
lname(warn).



% subdir(+Dir, -Subdir) is nondet.

subdir(Dir1, Dir2) :-
  directory_files(Dir1, Subdirs1),
  member(Subdir1, Subdirs1),
  \+ is_dummy_file(Subdir1),
  directory_file_path(Dir1, Subdir1, Dir2).
