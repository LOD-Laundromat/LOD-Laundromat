:- module(
  ll_generics,
  [
    copy_task_files/2,     % +FromHash, +ToHash
    end_task/2,            % +Hash, +Local
    finish/1,              % +Hash
    handle_status/3,       % +Hash, +Local, +Status
    hash_entry_hash/3,     % +Hash1, +Entry, -Hash2
    hash_file/3,           % +Hash, +Local, -File
    read_task_memory/3,    % +Hash, +Local, -Term
    start_seed/1,          % -Seed
    write_task_memory/3    % +Hash, +Local, +Term
  ]
).

/** <module> LOD Laundromat: Generics

Debug flags:

  - ll(connectivity)
  - ll(offline)
  - ll(seedlist)

---

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(debug)).
:- use_module(library(http/json)).
:- use_module(library(lists)).
:- use_module(library(yall)).

:- use_module(library(call_ext)).
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(semweb/ldfs)).
:- use_module(library(uri_ext)).

:- meta_predicate
    seedlist_request(+, ?, 1, +).





%! copy_task_files(+FromHash:atom, +ToHash:atom) is det.

copy_task_files(Hash1, Hash2) :-
  forall(
    task_file_local(Local),
    (   ldfs_file(Hash1, false, _, Hash1, Local, File1)
    ->  hash_file(Hash2, Local, File2),
        copy_file(File1, File2)
    ;   true
    )
  ).

task_file_local(base_uri).
task_file_local(http_media_type).



%! end_task(+Hash:atom, +Local:atom) is det.

end_task(Hash, Local) :-
  hash_file(Hash, Local, File),
  touch(File).



%! finish(+Hash:atom) is det.

finish(Hash) :-
  forall(
    (
      member(Base, [data,error,meta,warning]),
      file_name_extension(Base, nq, Local),
      hash_file(Hash, Local, File),
      exists_file(File)
    ),
    (
      compress_file(File),
      delete_file(File)
    )
  ),
  end_task(Hash, finished),
  (   debugging(ll(offline))
  ->  true
  ;   get_time(Begin),
      seedlist_request(processing, [hash(Hash)], true, [method(patch)]),
      get_time(End),
      Delta is End - Begin,
      debug(ll(seedlist), "PATCH /processing ~2f ~a", [Delta,Hash])
  ).



%! handle_status(+Hash:atom, +Local:atom, +Status:test) is det.

handle_status(Hash, Local, true) :- !,
  end_task(Hash, Local).
handle_status(Hash, _, Status) :-
  status_error(Status, E),
  write_message(error, Hash, E),
  finish(Hash).

status_error(exception(E), E) :- !.
status_error(E, E).



%! hash_entry_hash(+Hash1:atom, +Entry:atom, -Hash2:atom) is det.

hash_entry_hash(Hash1, Entry, Hash2) :-
  md5(Hash1-Entry, Hash2).



%! hash_file(+Hash:atom, +Local:atom, -File:atom) is det.

hash_file(Hash, Local, File) :-
  ldfs_root(Root),
  hash_file(Root, Hash, Local, File).



%! read_task_memory(+Hash:atom, +Local:atom, -Term:term) is semidet.

read_task_memory(Hash, Local, Term) :-
  ldfs_file(Hash, false, _, Hash, Local, File),
  read_from_file(
    File,
    {Term}/[In]>>(
      read_line_to_string(In, Line),
      read_term_from_atom(Line, Term, [])
    )
  ).



%! seedlist_request(+Status:atom, ?Query:list(compound), :Goal_1, +Options:list(compound)) is semidet.

seedlist_request(Status, Query, Goal_1, Options) :-
  setting(ll:authority, Auth),
  setting(ll:password, Password),
  setting(ll:scheme, Scheme),
  setting(ll:user, User),
  uri_comps(Uri, uri(Scheme,Auth,[seed,Status],Query,_)),
  Counter = counter(1),
  repeat,
  (   catch(
        http_call(
          Uri,
          Goal_1,
          [accept(json),authorization(basic(User,Password)),failure(404)|Options]
        ),
        E,
        true
      )
  ->  (   var(E)
      ->  !
      ;   Counter = counter(N1),
          debug(ll(connectivity), "Trouble connecting to seedlist (attempt ~D).", [N1]),
          N2 is N1 + 1,
          nb_setarg(1, Counter, N2),
          sleep(60),
          fail
      )
  ;   !, fail
  ).



%! start_seed(-Seed:dict) is semidet.

start_seed(Seed) :-
  get_time(Begin),
  seedlist_request(stale, _, seed_(Seed), [method(patch)]),
  get_time(End),
  Delta is End - Begin,
  _{hash: Hash, url: Uri} :< Seed,
  debug(ll(seedlist), "PATCH /stale ~2f ~a ~a", [Delta,Hash,Uri]).

seed_(Seed, In) :-
  call_cleanup(
    (
      json_read_dict(In, Seeds, [value_string_as(atom)]),
      (is_list(Seeds) -> member(Seed, Seeds) ; Seed = Seeds)
    ),
    close(In)
  ).



%! write_task_memory(+Hash:atom, +Local:atom, +Term:term) is det.

write_task_memory(Hash, Local, Term) :-
  hash_file(Hash, Local, File),
  setup_call_cleanup(
    open(File, write, Out),
    format(Out, "~W\n", [Term,[quoted(true)]]),
    close(Out)
  ).
