:- module(
  seedlist_http,
  [
  % DEBUG
    add_iri_http/1 % +Iri
  ]
).

/** <module> HTTP API on top of seedlist

| *Path*             | *Method* | *Media type*        | *Status codes* |
|:-------------------|:---------|:--------------------|:---------------|
| `/seedlist`        | `GET`    | `appplication/json` | 200            |
| `/seedlist`        | `GET`    | `text/html`         | 200            |
| `/seedlist`        | `POST`   | `application/json`  | 201, 409       |
| `/seedlist/$HASH$` | `DELETE` |                     | 404            |
| `/seedlist/$HASH$` | `GET`    | `application/json`  | 200, 404       |
| `/seedlist/$HASH$` | `GET`    | `text/html`         | 200, 404       |

A POST request adds a new seed to the list (201) if it is not already there
(409).  The HTTP body is expected to be `{"seed": $IRI$}`.

---

@author Wouter Beek
@tbd Add authorization for DELETE and POST requests.
@tbd Add pagination for GET requests.
@version 2016/02
*/

:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(html/html_bs)).
:- use_module(library(html/html_date_time)).
:- use_module(library(html/html_ext)).
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
:- use_module(library(pair_ext)).
:- use_module(library(string_ext)).
:- use_module(library(true)).
:- use_module(library(uri/uri_ext)).

:- use_module(cliopatria(components/basics)).

:- use_module(cpack('LOD-Laundromat'/seedlist)).

:- http_handler(root(seedlist), seedlist, []).

seedlist(Req) :- rest_handler(Req, seedlist, is_current_seed0, seed, seeds).
seed(Method, MTs, Seed) :- rest_mediatype(Method, MTs, Seed, seed_mediatype).
seeds(Method, MTs) :- rest_mediatype(Method, MTs, seeds_mediatype).

is_current_seed0(Iri) :-
  uri_path(Iri, Hash),
  is_current_seed(Hash).

seed_mediatype(delete, application/json, Iri) :- !,
  uri_path(Iri, Hash),
  remove_seed(Hash).
seed_mediatype(get, application/json, Iri) :- !,
  uri_path(Iri, Hash),
  current_seed(Hash, Seed),
  seed_to_dict(Seed, Dict),
  reply_json_dict(Dict, [status(200)]).
seed_mediatype(get, text/html, Iri) :-
  uri_path(Iri, Hash),
  current_seed(Hash, Seed),
  string_list_concat(["LOD Laundromat","Seed",Hash], " - ", Title),
  reply_html_page(cliopatria(default), title(Title), \html_seed(Seed)).

html_seed(seed(H,I,A,S,E)) -->
  html(
    section(
      \bs_unary_row([
        \external_link(I, H),
        bs_table([
          tr([th("Added"),td(\seed_date_time(A))]),
          tr([th("Started"),td(\seed_date_time(S))]),
          tr([th("Ended"),td(\seed_date_time(E))])
        ])
      ])
    )
  ).

seeds_mediatype(get, application/json) :- !,
  findall(D, (current_seed(Seed), seed_to_dict(Seed, D)), Ds),
  length(Ds, N),
  reply_json_dict(_{seeds:Ds,size:N}, [status(200)]).
seeds_mediatype(get, text/html) :- !,
  findall(A-seed(H,I,A,S,E), current_seed(seed(H,I,A,S,E)), Seeds0),
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
  http_read_json_dict(Data),
  catch(add_iri(Data.seed), E,
    (    var(E)
    ->  reply_json_dict(_{}, [status(201)])
    ;   E = existence_error(_,_)
    ->  reply_json_dict(_{}, [status(409)])
    )
  ).
  
seed_status0(_-seed(_,_,_,0.0,0.0), >) :- !.
seed_status0(_-seed(_,_,_,_  ,0.0), =) :- !.
seed_status0(_                    , <).



%! seed_to_dict(+Seed, -Dict) is det.
% Prolog term conversion that make formulating a JSON response very easy.

seed_to_dict(
  seed(Hash,Iri,Added,Started1,Ended1),
  _{added:Added, ended:Ended2, hash:Hash, seed:Iri, started:Started2}
):-
  maplist(var_to_null, [Started1,Ended1], [Started2,Ended2]).

var_to_null(X, null) :- var(X), !.
var_to_null(X, X).

seeds_table(Seeds) -->
  bs_table(
    \bs_table_header(['Seed','Actions','Added','Started','Ended']),
    \html_maplist(seed_row, Seeds)
  ).

seed_row(seed(H,I1,A,S,E)) -->
  {atom_truncate(I1, 40, I2)},
  html(
    tr([
      td([div(\external_link(I1, I2)),div(H)]),
      td(\seed_actions(seed(H,I1,A,S,E))),
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
  html(button([class=[btn,'default-btn'],onclick=Func], 'Start')).
seed_actions(seed(_,_,_,_,0.0)) --> !, [].
% Show results.
seed_actions(seed(H,_,_,_,_  )) -->
  html(div([\bs_button_link(data, H),\bs_button_link(meta, H)])).

bs_button_link(Alias, Postfix) -->
  {http_link_to_id(Alias, path_postfix(Postfix), Uri)},
  html(a([class=[btn,'btn-default'],href=Uri], Alias)).



%! add_iri_http(+Iri) is det.
% Adds an IRI through the slow HTTP API.
% This is intended for debugging purposes only.

add_iri_http(Iri) :-
  http_absolute_uri(root(seedlist), Endpoint),
  call_collect_messages(http_post(Endpoint, json(_{seed: Iri}), true)).
