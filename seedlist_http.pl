:- module(
  seedlist_http,
  [
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
@version 2016/02-2016/03
*/

:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(html/html_bs)).
:- use_module(library(html/html_date_time)).
:- use_module(library(html/html_ext)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_io)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_path)).
:- use_module(library(http/js_write)).
:- use_module(library(http/rest)).
:- use_module(library(list_ext)).
:- use_module(library(nlp/nlp_lang)).
:- use_module(library(pair_ext)).
:- use_module(library(string_ext)).
:- use_module(library(true)).
:- use_module(library(uri)).

:- use_module(seedlist).

:- http_handler(root(seedlist), seedlist, [prefix]).

seedlist(Req) :- rest_handler(Req, seedlist, iri_seed0, seed, seeds).
seed(Method, MTs, Seed) :- rest_mediatype(Method, MTs, Seed, seed_mediatype).
seeds(Method, MTs) :- rest_mediatype(Method, MTs, seeds_mediatype).

seed_mediatype(delete, application/json, Iri) :- !,
  hash_iri(Hash, Iri),
  remove_seed(Hash),
  reply_json_dict(_{}, [status(200)]).
seed_mediatype(get, application/json, Iri) :- !,
  hash_iri(Hash, Iri),
  seed_dict(Hash, D),
  reply_json_dict(D, [status(200)]).
seed_mediatype(get, text/html, Iri) :-
  hash_iri(H, Iri),
  current_seed(seed(H,I,A,S,E)),
  string_list_concat(["LOD Laundromat","Seed",Hash], " - ", Title),
  reply_html_page(cliopatria(default),
    title(Title),
    \html_seed(seed(H,I,A,S,E))
  ).

seeds_mediatype(get, application/json) :- !,
  seeds_dicts(D),
  reply_json_dict(D, [status(200)]).
seeds_mediatype(get, text/html) :- !,
  seeds_status(Done, Doing, Todo),
  list_truncate(Done, 10, DonePage1),
  list_truncate(Doing, 10, DoingPage1),
  list_truncate(Todo, 10, TodoPage1),
  reply_html_page(cliopatria(default), title("LOD Laundromat - Seedlist"), [
    h1("Seedlist"),
    \bs_panels(seeds_table, [
      'Cleaned'-DonePage1,
      'Cleaning'-DoingPage1,
      'To be cleaned'-TodoPage1
    ])
  ]).
seeds_mediatype(post, application/json) :-
  http_read_json_dict(Data),
  add_iri(Data.seed, Hash),
  hash_iri(Hash, Iri),
  reply_json_dict(_{hash: Iri}, [status(201)]).



%! seeds_table(+Seeds:list(compound))// is det.
% Generates an HTML table representing the given seeds.

seeds_table(Seeds) -->
  {http_link_to_id(data, [], Iri)},
  html([
    \js_script({|javascript(Iri)||
function deleteData(about) {
  $.ajax(about, {
    "error": function(xhr, textStatus, errorThrown) {error(xhr.responseText);},
    "success": function() {location.reload();},
    "type": "DELETE"
  });
}
function deleteSeed(about) {
  $.ajax(about, {
    "error": function(xhr, textStatus, errorThrown) {error(xhr.responseText);},
    "success": function() {location.reload();},
    "type": "DELETE"
  });
}
function startSeed(hash) {
  $.ajax(Iri, {
    "contentType": "application/json",
    "data": JSON.stringify({"seed": hash}),
    "error": function(xhr, textStatus, errorThrown) {error(xhr.responseText);},
    "success": function() {location.reload();},
    "type": "POST"
  });
}
    |}),
    \bs_table(
      \html_table_header_row(["Seed","Actions","Added","Started","Ended"]),
      \html_maplist(seed_row, Seeds)
    )
  ]).


%! seed_row(+Seed:compound)// is det.
% Generates a row in an HTML table representing seeds.

seed_row(seed(H,I1,A,S,E)) -->
  {
    atom_truncate(I1, 40, I2),
    hash_iri(H, Iri)
  },
  html(
    tr([
      td([div(\external_link(I1, I2)),\internal_link(Iri, H)]),
      td(\seed_actions(seed(H,I1,A,S,E))),
      td(\seed_date_time0(A)),
      td(\seed_date_time0(S)),
      td(\seed_date_time0(E))
    ])
  ).


% Start crawling of ‘todo’ seeds.
seed_actions(seed(H,_,_,0.0,_)) --> !,
  {
    format(atom(SFunc), 'startSeed("~a");', [H]),
    http_link_to_id(seedlist, path_postfix(H), Seed),
    format(atom(DFunc), 'deleteSeed("~a");', [Seed])
  },
  bs_button(SFunc, "Start"),
  bs_button(DFunc, "Delete").
% No buttons for ‘cleaning’.
seed_actions(seed(H,_,_,_,0.0)) --> !,
  reset_button(H).
% Show results for ‘cleaned’.
seed_actions(seed(H,_,_,_,_  )) -->
  {http_link_to_id(data, path_postfix(H), Iri)},
  bs_link_button(Iri, "Data"),
  reset_button(H).



%! add_iri_http(+Iri) is det.
% Adds an IRI through the slow HTTP API.
% This is intended for debugging purposes only.

add_iri_http(Iri) :-
  http_absolute_uri(root(seedlist), Endpoint),
  call_collect_messages(http_post(Endpoint, json(_{seed: Iri}), true)).





% HELPERS %

%! iri_seed0(+Iri) is semidet.
% Succeeds if the given IRI denotes a seed point in the seedlist.

iri_seed0(Iri) :-
  hash_iri(Hash, Iri),
  current_seed(seed(Hash,_,_,_,_)).



%! hash_iri(+Hash, -Iri) is det.
%! hash_iri(-Hash, +Iri) is det.
% Translate between seedlist hashes and seedlist HTTP IRIs.

hash_iri(Hash, Iri) :-
  ground(Hash), !,
  http_link_to_id(seedlist, path_postfix(Hash), Iri).
hash_iri(Hash, Iri) :-
  ground(Iri), !,
  uri_components(Iri, uri_components(_,_,Path,_,_)),
  atomic_list_concat(['',seedlist,Hash], /, Path).
hash_iri(_, _) :-
  instantiation_error(_).



%! html_seed(+Seed:compound)// is det.
% Generates a simple HTML representation of the given seed compound term.

html_seed(seed(H,I,A,S,E)) -->
  html([
    h1(\external_link(I, H)),
    \bs_table([
      tr([th("Added"),td(\seed_date_time0(A))]),
      tr([th("Started"),td(\seed_date_time0(S))]),
      tr([th("Ended"),td(\seed_date_time0(E))])
    ])
  ]).



%! reset_button(+Hash)// is det.

reset_button(H) -->
  {
    rdf_global_id(data:H, Iri),
    format(atom(Func), 'deleteData("~a");', [Iri])
  },
  bs_button(Func, "Reset").



%! seed_date_time0(+DT)// is det.
% Generate a human- and machine-processable date/time representation
% if a date/time is present.  Display a stub otherwise.

seed_date_time0(DT) -->
  {DT =:= 0.0}, !,
  html('∅').
seed_date_time0(DT) -->
  {current_ltag(LTag)},
  html_date_time(DT, _{ltag: LTag, masks: [offset], month_abbr: true}).
