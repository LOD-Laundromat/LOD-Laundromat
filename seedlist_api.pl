:- module(
  seedlist_api,
  [
    add_seed/1,     % +Iri
    begin_seed/1,   % +Hash
    current_seed/1, % -Seed
    end_seed/1,     % +Hash
  % DEBUG
    add_old_seeds/0,
    add_seed_http/1 % +Iri
  ]
).

/** <module> Seedlist API

@author Wouter Beek
@version 2016/01-2016/02
*/

:- use_module(library(hash_ext)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_header)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_receive)).
:- use_module(library(http/http_request)).
:- use_module(library(http/http_server)).
:- use_module(library(os/gnu_sort)).
:- use_module(library(persistency)).
:- use_module(library(sparql/sparql_db)).
:- use_module(library(sparql/query/sparql_query)).
:- use_module(library(true)).

:- http_handler(root(seedlist), seedlist, []).

:- persistent seed(hash:atom, from:atom, added:float, started:float, ended:float).

:- sparql_register_endpoint(
     lod_laundromat,
     ['http://sparql.backend.lodlaundromat.org'],
     virtuoso
   ).

:- initialization(init_seedlist).

init_seedlist :-
  db_attach('seedlist.pl', [sync(flush)]),
  start_server([port(3000)]).



%! add_old_seeds is det.
% Extracts all seeds from the old LOD Laundromat server and stores them locally
% as a seedlist.  This is intended for debugging purposes only.

add_old_seeds :-
  absolute_file_name(seedlist, File, [access(write),file_type(prolog)]),
  Q = '\c
PREFIX llo: <http://lodlaundromat.org/ontology/>\n\c
SELECT ?url\n\c
WHERE {\n\c
  ?doc llo:url ?url\n\c
}\n',
  setup_call_cleanup(
    open(File, write, Write),
    forall(
      sparql_select(lod_laundromat, Q, Rows),
      forall(member([Iri], Rows), add_seed(Iri))
    ),
    close(Write)
  ),
  sort_file(File).



%! add_seed(+Iri) is det.
% Adds a seed to the seedlist.

add_seed(Iri1) :-
  iri_normalized(Iri1, Iri2),
  with_mutex(seedlist, add_seed0(Iri2)).

add_seed0(Iri) :-
  seed(_,Iri,_,_,_), !.
add_seed0(Iri) :-
  get_time(Now),
  md5(Iri, Hash),
  assert_seed(Hash, Iri, Now, 0.0, 0.0).



%! add_seed_http(+Iri) is det.
% Adds a seed through the slow HTTP API.
% This is intended for debugging purposes only.

add_seed_http(Iri) :-
  catch(
    http_post('http://localhost:3000/seedlist', json(_{seed: Iri}), true),
    E,
    writeln(E)
  ).



%! begin_seed(+Hash) is det.

begin_seed(Hash) :-
  retract_seed(Hash, Iri, Added, 0.0, 0.0),
  get_time(Started),
  assert_seed(Hash, Iri, Added, Started, 0.0).



%! current_seed(-Seed) is nondet.
% Enumerates the seeds in the currently loaded seedlist.

current_seed(seed(Hash,Iri,Added,Started,Ended)) :-
  seed(Hash,Iri,Added,Started,Ended).



%! end_seed(+Hash) is det.

end_seed(Hash) :-
  retract_seed(Hash, Iri, Added, Started, 0.0),
  get_time(Ended),
  assert_seed(Hash, Iri, Added, Started, Ended).



%! seedlist(+Request) is det.
% Implementation of the seedlist HTTP API:
%   - GET requests return the list of seeds (200).
%   - POST requests add a new seed to the list (201) if it is not already there
%     (409).  The HTPP body is expected to be `{"seed": $IRI}`.

seedlist(Req) :-
  http_method(Req, get), !,
  findall(
    _{added:Added, ended:Ended2, seed:Iri, hash:Hash, started:Started2},
    (
      seed(Hash, Iri, Added, Started1, Ended1),
      maplist(var_to_null, [Started1,Ended1], [Started2,Ended2])
    ),
    Ds
  ),
  length(Ds, N),
  reply_json(_{seeds:Ds,size:N}, [status(200)]).
seedlist(Req) :-
  http_method(Req, post), !,
  http_output(Req, Out),
  catch(
    (
      http_read_json_dict(Req, Data),
      get_time(Now),
      iri_normalized(Data.seed, Iri),
      md5(Iri, Hash),
      (   seed(Hash, _, _, _, _)
      ->  reply_json(_{}, [status(409)])
      ;   assert_seed(Hash, Iri, Now, 0.0, 0.0)
      )
    ),
    E,
    http_status_reply(bad_request(E), Out, [], _)
  ),
  reply_json(_{}, [status(201)]).

var_to_null(X, null) :- var(X), !.
var_to_null(X, X).
