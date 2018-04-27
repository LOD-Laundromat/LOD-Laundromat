:- module(
  ll_generics,
  [
    end_seed/1,            % +Hash
    failure_success/4,     % +Hash, +Local, ?Term, +E
    find_hash_directory/2, % -Directory, -Hash
    find_hash_file/2,      % +Local, -Hash
    find_hash_file/3,      % +Local, -Hash, -File
    hash_directory/2,      % +Hash, -Directory
    hash_entry_hash/3,     % +Hash1, +Entry, -Hash2
    hash_file/3,           % +Hash, +Local, -File
    hash_url/2,            % +Hash, -Url
    read_term_from_file/2, % +File, -Term
    start_seed/1,          % -Seed
    touch_hash_file/2      % +Hash, +Local
  ]
).

/** <module> LOD Laundromat: Generics

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(http/json)).
:- use_module(library(zlib)).

:- use_module(library(call_ext)).
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(uri_ext)).

:- dynamic
    ll:debug/2.

:- meta_predicate
    seedlist_request(+, ?, 1),
    seedlist_request(+, ?, 1, +).





%! end_seed(+Hash:atom) is det.

end_seed(Hash) :-
  touch_hash_file(Hash, finished),
  seedlist_request([seed,processing], [hash(Hash)], true, [method(patch)]).



%! failure_success(+Hash:atom, +Local:atom, ?Term:term, +E:compound) is det.

% success
failure_success(Hash, Local, Term, E) :-
  var(E), !,
  (   var(Term)
  ->  touch_hash_file(Hash, Local)
  ;   hash_file(Hash, Local, File),
      setup_call_cleanup(
        open(File, write, Out),
        format(Out, "~W\n", [Term,[quoted(true)]]),
        close(Out)
      )
  ).
% failure
failure_success(Hash, _, _, E) :-
  write_meta_error(Hash, E),
  end_seed(Hash).



%! find_hash_directory(-Directory:atom, -Hash:atom) is nondet.

find_hash_directory(Dir2, Hash) :-
  setting(ll:data_directory, Root),
  directory_subdirectory(Root, Hash1, Dir1),
  directory_subdirectory(Dir1, Hash2, Dir2),
  atom_concat(Hash1, Hash2, Hash).



%! find_hash_file(+Local:atom, -Hash:atom) is nondet.
%! find_hash_file(+Local:atom, -Hash:atom, -File:atom) is nondet.

find_hash_file(Local, Hash) :-
  find_hash_file(Local, Hash, _).


find_hash_file(Local, Hash, File) :-
  find_hash_directory(Dir, Hash),
  directory_file_path(Dir, Local, File),
  exists_file(File).



%! hash_directory(+Hash:atom, -Directory:atom) is det.

hash_directory(Hash, Dir) :-
  setting(ll:data_directory, Root),
  hash_directory(Root, Hash, Dir).



%! hash_entry_hash(+Hash1:atom, +Entry:atom, -Hash2:atom) is det.

hash_entry_hash(Hash1, Entry, Hash2) :-
  md5(Hash1-Entry, Hash2).



%! hash_file(+Hash:atom, +Local:atom, -File:atom) is det.

hash_file(Hash, Local, File) :-
  setting(ll:data_directory, Dir),
  hash_file(Dir, Hash, Local, File).



%! hash_url(+Hash:atom, -Url:atom) is det.

hash_url(Hash, Url) :-
  seedlist_request([seed], [hash(Hash)], seed_(Seed)),
  Url = Seed.url.



%! read_term_from_file(+File:atom, -Term:term) is semidet.

read_term_from_file(File, Term) :-
  call_stream_file(File, read_term_from_file_(Term)).

read_term_from_file_(Term, In) :-
  repeat,
  read_line_to_string(In, Line),
  (   Line == end_of_file
  ->  !
  ;   read_term_from_atom(Line, Term, [])
  ).



%! seedlist_request(+Segments:list(atom), ?Query:list(compound), :Goal_1) is semidet.
%! seedlist_request(+Segments:list(atom), ?Query:list(compound), :Goal_1,
%!                  +Options:list(compound)) is semidet.

seedlist_request(Segments, Query, Goal_1) :-
  seedlist_request(Segments, Query, Goal_1, []).


seedlist_request(Segments, Query, Goal_1, Options) :-
  setting(ll:authority, Auth),
  setting(ll:password, Password),
  setting(ll:scheme, Scheme),
  setting(ll:user, User),
  uri_comps(Uri, uri(Scheme,Auth,Segments,Query,_)),
  http_call(
    Uri,
    Goal_1,
    [accept(json),authorization(basic(User,Password))|Options]
  ).



%! start_seed(-Seed:dict) is semidet.

start_seed(Seed) :-
  seedlist_request([seed,stale], _, seed_(Seed), [method(patch)]).

seed_(Seed, In) :-
  call_cleanup(
    (
      json_read_dict(In, Seeds, [value_string_as(atom)]),
      (is_list(Seeds) -> member(Seed, Seeds) ; Seed = Seeds)
    ),
    close(In)
  ).



%! touch_hash_file(+Hash:atom, +Local:atom) is det.

touch_hash_file(Hash, Local) :-
  hash_file(Hash, Local, File),
  touch(File).
