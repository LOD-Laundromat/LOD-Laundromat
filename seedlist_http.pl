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
@version 2016/02
*/

:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(html/html_bs)).
:- use_module(library(html/html_date_time)).
:- use_module(library(html/html_ext)).
:- use_module(library(html/html_meta)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_path)).
:- use_module(library(http/http_request)).
:- use_module(library(http/js_write)).
:- use_module(library(http/rest)).
:- use_module(library(list_ext)).
:- use_module(library(nlp/nlp_lang)).
:- use_module(library(pair_ext)).
:- use_module(library(string_ext)).
:- use_module(library(true)).
:- use_module(library(uri)).

:- use_module(cliopatria(components/basics)). % HTML tables.

:- use_module(cpack('LOD-Laundromat'/seedlist)).

:- http_handler(root(seedlist), seedlist, [prefix]).

seedlist(Req) :- rest_handler(Req, seedlist, is_current_seed0, seed, seeds).
seed(Method, MTs, Seed) :- rest_mediatype(Method, MTs, Seed, seed_mediatype).
seeds(Method, MTs) :- rest_mediatype(Method, MTs, seeds_mediatype).

seed_mediatype(delete, application/json, Iri) :- !,
  iri_to_hash(Iri, Hash),
  remove_seed(Hash).
seed_mediatype(get, application/json, Iri) :- !,
  iri_to_hash(Iri, Hash),
  current_seed(Hash, Seed),
  seed_to_dict0(Seed, Dict),
  reply_json_dict(Dict, [status(200)]).
seed_mediatype(get, text/html, Iri) :-
  iri_to_hash(Iri, Hash),
  current_seed(Hash, Seed),
  string_list_concat(["LOD Laundromat","Seed",Hash], " - ", Title),
  reply_html_page(cliopatria(default), title(Title), \html_seed(Seed)).

seeds_mediatype(get, application/json) :- !,
  findall(D, (current_seed(Seed), seed_to_dict0(Seed, D)), Ds),
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
  add_iri(Data.seed, Hash),
  hash_to_iri(Hash, Iri),
  reply_json_dict(_{hash: Iri}, [status(201)]).



%! seeds_table(+Seeds:list(compound))// is det.
% Generates an HTML table representing the given seeds.

seeds_table(Seeds) -->
  bs_table(
    \bs_table_header(['Seed','Actions','Added','Started','Ended']),
    \html_maplist(seed_row, Seeds)
  ).


%! seed_row(+Seed:compound)// is det.
% Generates a row in an HTML table representing seeds.

seed_row(seed(H,I1,A,S,E)) -->
  {
    atom_truncate(I1, 40, I2),
    hash_to_iri(H, Uri)
  },
  html(
    tr([
      td([div(\external_link(I1, I2)),\internal_link(Uri, H)]),
      td(\seed_actions(seed(H,I1,A,S,E))),
      td(\seed_date_time0(A)),
      td(\seed_date_time0(S)),
      td(\seed_date_time0(E))
    ])
  ).


% Start crawling of ‘todo’ seeds.
seed_actions(seed(H,_,_,0.0,_)) --> !,
  {
    http_link_to_id(seedlist, path_postfix(H), About),
    format(atom(SFunc), 'startSeed("~a");', [About]),
    format(atom(DFunc), 'deleteSeed("~a");', [About])
  },
  html([
    \js_script({|javascript(_)||
function deleteSeed(about) {
  $.ajax(about, {
    "error": function(xhr, textStatus, errorThrown) {error(xhr.responseText);},
    "success": function() {location.reload();},
    "type": "DELETE"
  });
}
    |}),
    button([class=[btn,'default-btn'],onclick=SFunc], 'Start'),
    button([class=[btn,'default-btn'],onclick=DFunc], 'Delete')
  ]).
% No buttons for ‘cleaning’.
seed_actions(seed(_,_,_,_,0.0)) --> !, [].
% Show results for ‘cleaned’.
seed_actions(seed(H,_,_,_,_  )) -->
  html(div([\bs_button_link(data, H),\bs_button_link(meta, H)])).



%! add_iri_http(+Iri) is det.
% Adds an IRI through the slow HTTP API.
% This is intended for debugging purposes only.

add_iri_http(Iri) :-
  http_absolute_uri(root(seedlist), Endpoint),
  call_collect_messages(http_post(Endpoint, json(_{seed: Iri}), true)).





% HELPERS %

%! bs_button_link(+Alias, +Postfix)// is det.
% Generate an HTML link that looks like a button and that uses the convenient
% http_link_to_id/3 in order to build the request IRI.

bs_button_link(Alias, Postfix) -->
  {http_link_to_id(Alias, path_postfix(Postfix), Iri)},
  html(a([class=[btn,'btn-default'],href=Iri], Alias)).



%! is_current_seed0(+Iri) is semidet.
% Succeeds if the given IRI denotes a seed point in the seedlist.

is_current_seed0(Iri) :-
  iri_to_hash(Iri, Hash),
  is_current_seed(Hash).



%! hash_to_iri(+Hash:atom, -Iri:atom) is det.
% Translate from a seedlist hash to a seedlist HTTP IRI.

hash_to_iri(Hash, Uri) :-
  http_link_to_id(seedlist, path_postfix(Hash), Uri).



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



%! iri_to_hash(+Iri, -Hash) is det.
% Translate from a seedlist HTTP IRI to a seedlist hash.

iri_to_hash(Uri, Hash) :-
  uri_components(Uri, uri_components(_,_,Path,_,_)),
  atomic_list_concat(['',seedlist,Hash], /, Path).



%! seed_date_time0(+DT)// is det.
% Generate a human- and machine-processable date/time representation
% if a date/time is present.  Display a stub otherwise.

seed_date_time0(DT) -->
  {DT =:= 0.0}, !,
  html('∅').
seed_date_time0(DT) -->
  {current_ltag(LTag)},
  html_date_time(DT, _{ltag: LTag, masks: [offset], month_abbr: true}).



%! seed_status0(+Pair, -Order:oneof([<,=,>]))is det.
% Partition the seeds into ‘done’ (<), ‘doing’ (=) and ‘todo’ (>).

seed_status0(_-seed(_,_,_,0.0,0.0), >) :- !.
seed_status0(_-seed(_,_,_,_  ,0.0), =) :- !.
seed_status0(_                    , <).



%! seed_to_dict0(+Seed, -Dict) is det.
% Prolog term conversion that make formulating a JSON response very easy.

seed_to_dict0(
  seed(Hash,Iri,Added,Started1,Ended1),
  _{added:Added, ended:Ended2, hash:Hash, seed:Iri, started:Started2}
):-
  maplist(var_to_null0, [Started1,Ended1], [Started2,Ended2]).



%! var_to_null0(+Term, -NullifiedTerm) is det.
% Maps Prolog terms to themselves unless they are variables,
% in which case they are mapped to the atom `null`.
%
% The is used for exporting seedpoints, where Prolog variables
% have no equivalent in JSON.

var_to_null0(X, null) :- var(X), !.
var_to_null0(X, X).