:- module(
  lod_basket,
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

/** <module> LOD Basket

@author Wouter Beek
@version 2016/01-2016/02
*/

:- use_module(library(hash_ext)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_header)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_receive)).
:- use_module(library(http/http_request)).
:- use_module(library(http/http_server)).
:- use_module(library(http/rest)).
:- use_module(library(os/gnu_sort)).
:- use_module(library(pair_ext)).
:- use_module(library(persistency)).
:- use_module(library(sparql/sparql_db)).
:- use_module(library(sparql/query/sparql_query)).
:- use_module(library(true)).

:- use_module(cliopatria(components/basics)).

:- http_handler(root(basket), basket, []).

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

is_current_seed(Iri) :-
  current_seed(seed(_,Iri,_,_,_)).

%! basket(+Request) is det.
% Implementation of the seedlist HTTP API:
%   - GET requests return the list of seeds (200).
%   - POST requests add a new seed to the list (201) if it is not already there
%     (409).  The HTPP body is expected to be `{"seed": $IRI}`.

basket(Req) :- gtrace,rest_handler(Req, basket, is_current_seed, seed, seeds).
seed(Method, Seed, MTs) :- rest_mediatype(Method, Seed, MTs, seed_mediatype).
seeds(Method, MTs) :- rest_mediatype(Method, MTs, seeds_mediatype).

seed_mediatype(get, Iri, application/json) :-
  current_seed(seed(Hash,Iri,Added,Started,Ended)),
  seed_to_dict(seed(Hash,Iri,Added,Started,Ended), D),
  reply_json(D, [status(200)]).

seeds_mediatype(get, application/json) :- !,
  findall(D, (current_seed(Seed), seed_to_dict(Seed, D)), Ds),
  length(Ds, N),
  reply_json(_{seeds:Ds,size:N}, [status(200)]).
seeds_mediatype(get, text/html) :- !,
  findall(Iri-seed(H,Iri,A,S,E), current_seed(seed(H,Iri,A,S,E)), Pairs),
  asc_pairs_values(Pairs, Seeds),
  reply_html_page(cliopatria(default), title('LOD Basket - Contents'), [
    h1('LOD Basket Contents'),
    cp_table(['Iri','Added','Started','Ended','Hash'], seed_rows(Seeds))
  ]).
seeds_mediatype(post, application/json) :-
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

seed_to_dict(
  seed(Hash,Iri,Added,Started1,Ended1),
  _{added:Added, ended:Ended2, hash:Hash, seed:Iri, started:Started2}
):-
  maplist(var_to_null, [Started1,Ended1], [Started2,Ended2]).

var_to_null(X, null) :- var(X), !.
var_to_null(X, X).
