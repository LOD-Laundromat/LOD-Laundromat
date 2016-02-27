:- module(
  seedlist,
  [
    add_seed/1,     % +Iri
    begin_seed/1,   % +Hash
    current_seed/1, % -Seed
    end_seed/1,     % +Hash
    remove_seed/1,  % +Hash
    reset_seed/1,   % +Hash
  % DEBUG
    add_old_seeds/0,
    add_seed_http/1 % +Iri
  ]
).

/** <module> Seedlist

@author Wouter Beek
@version 2016/01-2016/02
*/

:- use_module(library(apply)).
:- use_module(library(debug_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(html/html_bs)).
:- use_module(library(html/html_date_time)).
:- use_module(library(html/html_link)).
:- use_module(library(html/html_meta)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/http_header)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_path)).
:- use_module(library(http/http_request)).
:- use_module(library(http/rest)).
:- use_module(library(list_ext)).
:- use_module(library(nlp/nlp_lang)).
:- use_module(library(os/gnu_sort)).
:- use_module(library(pair_ext)).
:- use_module(library(persistency)).
:- use_module(library(sparql/sparql_db)).
:- use_module(library(sparql/query/sparql_query)).
:- use_module(library(thread)).
:- use_module(library(true)).

:- use_module(cliopatria(components/basics)).

:- http_handler(root(seedlist), seedlist, []).

:- persistent seed(hash:atom, from:atom, added:float, started:float, ended:float).

:- sparql_register_endpoint(
     lod_laundromat,
     ['http://sparql.backend.lodlaundromat.org'],
     virtuoso
   ).

:- initialization((
     absolute_file_name(
       cpack('LOD-Laundromat/seedlist.db'),
       File,
       [access(read)]
     ),
     db_attach(File, [sync(flush)])
   )).



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
  with_mutex(seedlist, add_seed0(Iri2)),
  debug(seedlist, "Added to seedlist: ~a", [Iri1]).

add_seed0(Iri) :-
  seed(_,Iri,_,_,_), !.
add_seed0(Iri) :-
  md5(Iri, Hash),
  add_seed0(Hash, Iri).

add_seed0(Hash, Iri) :-
  get_time(Now),
  assert_seed(Hash, Iri, Now, 0.0, 0.0).



%! add_seed_http(+Iri) is det.
% Adds a seed through the slow HTTP API.
% This is intended for debugging purposes only.

add_seed_http(Iri) :-
  http_absolute_uri(root(seedlist), Endpoint),
  call_collect_messages(http_post(Endpoint, json(_{seed: Iri}), true)).



%! begin_seed(+Hash) is det.

begin_seed(Hash) :-
  get_time(Started),
  with_mutex(seedlist, (
    retract_seed(Hash, Iri, Added, 0.0, 0.0),
    assert_seed(Hash, Iri, Added, Started, 0.0)
  )),
  debug(seedlist(start), "Started cleaning seed ~a", [Hash]).



%! current_seed(-Seed) is nondet.
% Enumerates the seeds in the currently loaded seedlist.

current_seed(seed(Hash,Iri,Added,Started,Ended)) :-
  seed(Hash,Iri,Added,Started,Ended).



%! end_seed(+Hash) is det.

end_seed(Hash) :-
  get_time(Ended),
  with_mutex(seedlist, (
    retract_seed(Hash, Iri, Added, Started, 0.0),
    assert_seed(Hash, Iri, Added, Started, Ended)
  )),
  debug(seedlist(end), "Ended cleaning seed ~a", [Hash]).



%! is_current_seed(+Iri) is semidet.

is_current_seed(Iri) :-
  current_seed(seed(_,Iri,_,_,_)).



%! remove_seed(+Hash) is det.

remove_seed(Hash) :-
  with_mutex(seedlist, retract_seed(Hash, Iri, _, _, _)),
  debug(seedlist(remove), "Removed seed ~a (~a)", [Hash,Iri]).



%! reset_seed(+Hash) is det.

reset_seed(Hash) :-
  with_mutex(seedlist, (
    retract_seed(Hash, Iri, _, _, _),
    add_seed0(Hash, Iri)
  )),
  debug(seedlist(reset), "Reset seed ~a (~a)", [Hash,Iri]).



%! seedlist(+Request) is det.
% Implementation of the seedlist HTTP API:
%   - GET requests return the list of seeds (200).
%   - POST requests add a new seed to the list (201) if it is not already there
%     (409).  The HTPP body is expected to be `{"seed": $IRI}`.

seedlist(Req) :- rest_handler(Req, seedlist, is_current_seed, seed, seeds).
seed(Method, MTs, Seed) :- rest_mediatype(Method, MTs, Seed, seed_mediatype).
seeds(Method, MTs) :- rest_mediatype(Method, MTs, seeds_mediatype).

seed_mediatype(get, application/json, Iri) :-
  current_seed(seed(Hash,Iri,Added,Started,Ended)),
  seed_to_dict(seed(Hash,Iri,Added,Started,Ended), D),
  reply_json(D, [status(200)]).

seeds_mediatype(get, application/json) :- !,
  findall(D, (current_seed(Seed), seed_to_dict(Seed, D)), Ds),
  length(Ds, N),
  reply_json(_{seeds:Ds,size:N}, [status(200)]).
seeds_mediatype(get, text/html) :- !,
  findall(A-seed(H,Iri,A,S,E), current_seed(seed(H,Iri,A,S,E)), Seeds0),
  partition(seed_status0, Seeds0, Cleaned0, Cleaning0, ToBeCleaned0),
  concurrent_maplist(
    desc_pairs_values,
    [Cleaned0,Cleaning0,ToBeCleaned0],
    [Cleaned,Cleaning,ToBeCleaned]
  ),
  list_truncate(Cleaned, 10, CleanedPage1),
  list_truncate(Cleaning, 10, CleaningPage1),
  list_truncate(ToBeCleaned, 10, ToBeCleanedPage1),
  reply_html_page(cliopatria(default), title("LOD Laundromat - Seedlist"), [
    h1("Seedlist"),
    \bs_panels(seeds_table, [
      'Cleaned'-CleanedPage1,
      'Cleaning'-CleaningPage1,
      'To be cleaned'-ToBeCleanedPage1
    ])
  ]).
seeds_mediatype(post, application/json) :-
  catch(
    (
      http_read_json_dict(Data),
      get_time(Now),
      iri_normalized(Data.seed, Iri),
      md5(Iri, Hash),
      (   seed(Hash, _, _, _, _)
      ->  reply_json(_{}, [status(409)])
      ;   assert_seed(Hash, Iri, Now, 0.0, 0.0),
          reply_json(_{}, [status(201)])
      )
    ),
    E,
    http_status_reply(bad_request(E))
  ).
  
seed_status0(_-seed(_,_,_,0.0,0.0), >) :- !.
seed_status0(_-seed(_,_,_,_  ,0.0), =) :- !.
seed_status0(_                    , <).

seed_to_dict(
  seed(Hash,Iri,Added,Started1,Ended1),
  _{added:Added, ended:Ended2, hash:Hash, seed:Iri, started:Started2}
):-
  maplist(var_to_null, [Started1,Ended1], [Started2,Ended2]).

var_to_null(X, null) :- var(X), !.
var_to_null(X, X).

seeds_table(Seeds) -->
  cp_table(['Seed','Actions','Added','Started','Ended'], \html_maplist(seed_row, Seeds)).

seed_row(seed(H,I,A,S,E)) -->
  html(
    tr([
      td([div(\html_link(I)),div(H)]),
      td(\seed_actions(seed(H,I,A,S,E))),
      td(\seed_date_time(A)),
      td(\seed_date_time(S)),
      td(\seed_date_time(E))
    ])
  ).

seed_date_time(DT) -->
  {DT =:= 0.0}, !,
  html('∅').
seed_date_time(DT) -->
  {current_ltag(LTag)},
  html_date_time(DT, _{ltag: LTag, masks: [offset], month_abbr: true}).

% Start crawling.
seed_actions(seed(H,_,_,0.0,_)) --> !,
  {format(atom(Func), 'startCrawling("~a")', [H])},
  bs_button(Func).
seed_actions(seed(_,_,_,_,0.0)) --> !, [].
% Show results.
seed_actions(seed(H,_,_,_,_  )) -->
  html(div([\bs_button_link(data, H),\bs_button_link(meta, H)])).

bs_button_link(Alias, Postfix) -->
  {http_link_to_id(Alias, path_postfix(Postfix), Uri)},
  html(a([class=[btn,'btn-default'],href=Uri], Alias)).
