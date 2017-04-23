:- module(
  ll_api,
  [
    ckan/1, % ?File
    ckan/4, % ?S, ?P, ?O, ?File
    ll/1,   % ?File
    llm/1,  % ?File
    ll/4,   % ?S, ?P, ?O, ?File
    llm/4   % ?S, ?P, ?O, ?File
  ]
).

/** <module> LOD Laundromat API

@author Wouter Beek
@version 2017/04
*/

:- use_module(library(file_ext)).
:- use_module(library(hdt/hdt_api)).
:- use_module(library(lists)).
:- use_module(library(semweb/rdf11)).

:- rdf_meta
   ckan(r, r, o, ?),
   ll(r, r, o, ?),
   llm(r, r, o, ?).





%! ckan(?File) is nondet.
%! ckan(?S, ?P, ?O, ?File) is nondet.

ckan(File) :-
  absolute_file_name(
    '*.hdt',
    File,
    [access(read),expand(true),solutions(all)]
  ).
  
ckan(S, P, O, File) :-
  ckan(File),
  hdt_call_on_file(File, hdt0(S, P, O)).



%! ll(?File) is nondet.
%! llm(?File) is nondet.

ll(File) :-
  ll(data, File).

llm(File) :-
  ll(meta, File).

ll(Base, File) :-
  file_name_extension(Base, hdt, Local),
  directory_path('/scratch/wbeek/ll/', Dir),
  directory_path(Dir, Subdir),
  directory_file_path(Subdir, Local, File),
  exists_file(File).



%! ll(?S ?P, ?O, ?File) is nondet.
%! llm(?S ?P, ?O, ?File) is nondet.

ll(S, P, O, File) :-
  ll(File),
  ll0(S, P, O, File).

llm(S, P, O, File) :-
  llm(File),
  ll0(S, P, O, File).

ll0(S, P, O, File) :-
  hdt_call_on_file(File, hdt0(S, P, O)).
