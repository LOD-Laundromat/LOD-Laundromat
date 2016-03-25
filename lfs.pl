:- module(
  lfs,
  [
    ldir/1,        % ?Dir
    ldir_ldoc/2,   % ?Dir, ?Doc
    ldir_ldoc/3,   % ?Dir, ?Name, ?Doc
    ldir_lfile/4,  % ?Dir, ?Name, ?Kind, ?File
    ldir_lhash/2,  % ?Dir, ?Hash
    ldoc/2,        % ?Name, ?Doc
    ldoc_lfile/2,  % ?Doc, ?File
    ldoc_lfile/3,  % ?Doc, ?Kind, ?File
    ldoc_lhash/2,  % ?Doc, ?Hash
    ldoc_lhash/3,  % ?Doc, ?Name, ?Hash
    lfile/3,       % ?Name, ?Kind, ?File
    lfile_lhash/2, % ?File, ?Hash
    lfile_lhash/4, % ?File, ?Name, ?Kind, ?Hash
    lhash/1,       % ?Hash
    lname/1,       % ?Name
    lrdf_load/2,   % +Hash, ?Name
    lrdf_unload/1, % +Hash
    lrdf_unload/2, % +Hash, +Name
    lroot/1        % -Dir
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
:- use_module(library(os/dir_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(settings)).

:- multifile
    error:has_type/2.

error:has_type(lkind, Kind) :-
  lkind(Kind).

error:has_type(lname, Name) :-
  lname(Name).

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
  lroot(Dir1),
  direct_subdir(Dir1, Dir2),
  direct_subdir(Dir2, Dir3).



%! ldir_ldoc(+Dir, -Doc) is multi.
%! ldir_ldoc(-Dir, +Doc) is det.

ldir_ldoc(Dir, Doc) :-
  ldir_ldoc(Dir, _, Doc).


%! ldir_ldoc(+Dir, +Name, -Doc) is det.
%! ldir_ldoc(+Dir, -Name, -Doc) is multi.
%! ldir_ldoc(-Dir, -Name, +Doc) is det.

ldir_ldoc(Dir, Name, Doc) :-
  ground(Dir), !,
  ldir_lhash(Dir, Hash),
  ldoc_lhash(Doc, Name, Hash).
ldir_ldoc(Dir, Name, Doc) :-
  ldoc_lhash(Doc, Name, Hash),
  ldir_lhash(Dir, Hash).



%! ldir_lfile(+Dir, +Name, +Kind, -File) is det.
%! ldir_lfile(+Dir, +Name, -Kind, -File) is multi.
%! ldir_lfile(+Dir, -Name, +Kind, -File) is multi.
%! ldir_lfile(+Dir, -Name, -Kind, -File) is multi.
%! ldir_lfile(-Dir, -Name, -Kind, +File) is det.

ldir_lfile(Dir, Name, Kind, File) :-
  ground(File), !,
  directory_file_path(Dir, Local, File),
  atomic_list_concat([Name|Exts], ., Local),
  must_be(lname, Name),
  lkind(Kind, Exts).
ldir_lfile(Dir, Name, Kind, File) :-
  must_be(atom, Dir),
  lname(Name),
  lkind(Kind, Exts),
  atomic_list_concat([Name|Exts], ., Local),
  directory_file_path(Dir, Local, File).



%! ldir_lhash(+Dir, -Hash) is det.
%! ldir_lhash(-Dir, +Hash) is det.

ldir_lhash(Dir, Hash) :-
  ground(Dir), !,
  dir_subdirs(Dir, Subdirs),
  reverse(Subdirs, [Postfix,Prefix|_]),
  atom_concat(Prefix, Postfix, Hash).
ldir_lhash(Dir4, Hash) :-
  must_be(atom, Hash),
  atom_codes(Hash, [H1,H2|T]),
  maplist(atom_codes, [Dir1,Dir2], [[H1,H2],T]),
  append_dirs(Dir1, Dir2, Dir3),
  lroot(Dir0),
  directory_file_path(Dir0, Dir3, Dir4).



%! ldoc(+Name, -Doc) is nondet.
%! ldoc(-Name, +Doc) is nondet.

ldoc(Name, Doc) :-
  ground(Doc), !,
  ldoc_lhash(Doc, Name, Hash),
  lhash(Hash).
ldoc(Name, Doc) :-
  lhash(Hash),
  ldoc_lhash(Doc, Name, Hash).



%! ldoc_lfile(-Doc, +File) is det.
%! ldoc_lfile(+Doc, -File) is multi.

ldoc_lfile(Doc, File) :-
  ldoc_lfile(Doc, _, File).


%! ldoc_lfile(+Doc, +Kind, -File) is det.
%! ldoc_lfile(+Doc, -Kind, -File) is multi.
%! ldoc_lfile(-Doc, -Kind, +File) is det.

ldoc_lfile(Doc, Kind, File) :-
  ground(File), !,
  lfile_lhash(File, Name, Kind, Hash),
  ldoc_lhash(Doc, Name, Hash).
ldoc_lfile(Doc, Kind, File) :-
  ldoc_lhash(Doc, Name, Hash),
  lfile_lhash(File, Name, Kind, Hash).



%! ldoc_lhash(+Doc, -Hash) is det.
%! ldoc_lhash(-Doc, +Hash) is multi.

ldoc_lhash(Doc, Hash) :-
  ldoc_lhash(Doc, _, Hash).


%! ldoc_lhash(+Doc, -Name, -Hash) is det.
%! ldoc_lhash(-Doc, +Name, +Hash) is det.
%! ldoc_lhash(-Doc, -Name, +Hash) is multi.

ldoc_lhash(Doc, Name, Hash) :-
  lname(Name),
  rdf_global_id(Name:Hash, Doc).



%! lfile(?Name, ?Kind, -File) is nondet.
%! lfile(-Name, -Kind, +File) is nondet.

lfile(Name, Kind, File) :-
  ground(File), !,
  ldir_lfile(Dir, Name, Kind, File),
  ldir(Dir).
lfile(Name, Kind, File) :-
  ldir(Dir),
  ldir_lfile(Dir, Name, Kind, File),
  exists_file(File).



%! lfile_lhash(+File, -Hash) is det.
%! lfile_lhash(-File, +Hash) is multi.

lfile_lhash(File, Hash) :-
  lfile_lhash(File, _, _, Hash).


%! lfile_lhash(+File, -Name, -Kind, -Hash) is det.
%! lfile_lhash(-File, +Name, +Kind, +Hash) is det.
%! lfile_lhash(-File, +Name, -Kind, +Hash) is multi.
%! lfile_lhash(-File, -Name, +Kind, +Hash) is multi.
%! lfile_lhash(-File, -Name, -Kind, +Hash) is multi.

lfile_lhash(File, Name, Kind, Hash) :-
  ground(File), !,
  ldir_lfile(Dir, Name, Kind, File),
  ldir_lhash(Dir, Hash).
lfile_lhash(File, Name, Kind, Hash) :-
  must_be(atom, Hash),
  ldir_lhash(Dir, Hash),
  ldir_lfile(Dir, Name, Kind, File).



%! lhash(+Hash) is semidet.
%! lhash(-Hash) is nondet.

lhash(Hash) :-
  ground(Hash), !,
  ldir_lhash(Dir, Hash),
  exists_directory(Dir).
lhash(Hash) :-
  ldir(Dir),
  ldir_lhash(Dir, Hash).



%! lname(+Name) is semidet.
%! lname(-Name) is multi.

lname(data).
lname(meta).
lname(warn).



%! lrdf_load(+Hash, +Name) is det.
%! lrdf_load(+Hash, -Name) is multi.

lrdf_load(Hash, Name) :-
  ldoc_lhash(Doc, Name, Hash),
  (   rdf_graph(Doc)
  ->  true
  ;   ldir_lhash(Dir, Hash),
      ldir_lfile(Dir, Name, nquads, File),
      access_file(File, read),
      rdf_load_file(File, [graph(Doc)])
  ).



%! lrdf_unload(+Hash) is det.

lrdf_unload(Hash) :-
  forall(lname(Name), lrdf_unload(Hash, Name)).


%! lrdf_unload(+Hash, +Name) is det.

lrdf_unload(Hash, Name) :-
  ldoc_lhash(Doc, Name, Hash),
  rdf_unload_graph(Doc).



%! lroot(-Dir) is det.

lroot(Dir) :-
  setting(data_dir, Dir).





% HELPERS %

%! lkind(+Kind) is semidet.
%! lkind(-Kind) is multi.

lkind(Kind) :-
  lkind(Kind, _).


%! lkind(+Kind, -Exts) is det.
%! lkind(-Kind, -Exts) is multi.

lkind(hdt,      [hdt]).
lkind(nquads,   [nq,gz]).
lkind(ntriples, [nt,gz]).
