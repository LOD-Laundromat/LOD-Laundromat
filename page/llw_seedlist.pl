:- module(llw_seedlist, []).

/** <module> LOD Laundromat seedlist: HTTP API

| *Path*             | *Method* | *Media type*        | *Status codes* |
|:-------------------|:---------|:--------------------|:---------------|
| `/seedlist`        | `GET`    | `appplication/json` | 200            |
| `/seedlist`        | `GET`    | `text/html`         | 200            |
| `/seedlist`        | `POST`   | `application/json`  | 201, 409       |
| `/seedlist/$HASH$` | `DELETE` |                     | 404            |
| `/seedlist/$HASH$` | `GET`    | `application/json`  | 200, 404       |
| `/seedlist/$HASH$` | `GET`    | `text/html`         | 200, 404       |

A POST request adds a new seed to the list (201) if it is not already
there (409).  The HTTP body is expected to be `{"from": $IRI$}`.

@author Wouter Beek
@tbd Add authorization for DELETE and POST requests.
@version 2016/02-2016/03, 2016/08-2016/09
*/

:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(html/html_date_time)).
:- use_module(library(html/html_ext)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/http_io)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_path)).
:- use_module(library(http/js_write)).
:- use_module(library(http/rest)).
:- use_module(library(list_ext)).
:- use_module(library(nlp/nlp_lang)).
:- use_module(library(pagination)).
:- use_module(library(pair_ext)).
:- use_module(library(q/q_iri)).
:- use_module(library(settings)).
:- use_module(library(string_ext)).
:- use_module(library(true)).
:- use_module(library(uri)).

:- use_module(llw(seedlist)).
:- use_module(llw(html/llw_html)).

:- http_handler(llw(seed), seed_handler, [prefix]).





seed_handler(Req) :-
  rest_method(Req, [delete,get,post], seed_handler).


seed_handler(Req, delete, MTs) :-
  http_relative_iri(Req, Iri),
  rest_media_type(Req, delete, MTs, seed_media_type(Iri)).
seed_handler(Req, get, MTs) :-
  http_relative_iri(Req, Iri),
  rest_media_type(Req, get, MTs, seed_media_type(Iri)).
seed_handler(Req, post, MTs) :-
  http_read_json_dict(Data),
  add_seed(Data.from, Iri),
  rest_media_type(Req, post, MTs, seed_media_type(Iri)).


seed_media_type(delete, application/json, Iri) :-
  iri_to_hash(Iri, Hash),
  remove_seed(Hash),
  reply_json_dict(_{}, [status(200)]).
seed_media_type(get, application/json, Iri) :-
  (   iri_to_hash(Iri, Hash)
  ->  pagination(Dict, seed_by_hash(Hash, Dict), Result)
  ;   pagination(Dict, seed(Dict), Result)
  ),
  reply_json_dict(Result.results, [status(200)]).
seed_media_type(get, text/html, Iri) :-
  (   iri_to_hash(Iri, Hash)
  ->  seed_by_hash(Hash, Dict),
      reply_html_page(
        llw([]),
        \q_title(["Seed",Hash]),
        \html_seed(Dict)
      )
  ;   N = 3,
      % @bug Cannot send requests to ElasticSearch before generating
      %      an HTML page?
      findnsols(N, Seed1, seed_by_status(ended, Seed1), L1),
      findnsols(N, Seed2, seed_by_status(started, Seed2), L2),
      findnsols(N, Seed3, seed_by_status(added, Seed3), L3),
      reply_html_page(
        llw([]),
        \q_title(["Seedlist"]),
        [
          h1("Seedlist"),
          \panels([
            \panel(1, "Cleaned", \seeds_table(L1)),
            \panel(2, "Cleaning", \seeds_table(L2)),
            \panel(3, "To be cleaned", \seeds_table(L3))
          ])
        ]
      )
  ).
seed_media_type(post, application/json, Iri) :-
  reply_json_dict(_{seed: Iri}, [status(201)]).



%! seeds_table(+Seeds)// is det.
%
% Generates an HTML table representing the given seeds.

seeds_table(Seeds) -->
  {http_link_to_id(seed_handler, Iri)},
  html([
    \js_script({|javascript(Iri)||
function deleteData(about) {
  $.ajax(about, {
    "error": function(xhr, textStatus, errorThrown) {
      error(xhr.responseText);
    },
    "success": function() {location.reload();},
    "type": "DELETE"
  });
}
function deleteSeed(about) {
  $.ajax(about, {
    "error": function(xhr, textStatus, errorThrown) {
      error(xhr.responseText);
    },
    "success": function() {location.reload();},
    "type": "DELETE"
  });
}
function startSeed(hash) {
  $.ajax(Iri, {
    "contentType": "application/json",
    "data": JSON.stringify({"seed": hash}),
    "error": function(xhr, textStatus, errorThrown) {
      error(xhr.responseText);
    },
    "success": function() {location.reload();},
    "type": "POST"
  });
}
    |}),
    \table(
      tr([th("Seed"),th("Actions"),th("Added"),th("Started"),th("Ended")]),
      \html_maplist(seed_row, Seeds)
    )
  ]).



%! seed_row(+Seed:compound)// is det.
%
% Generates a row in an HTML table representing seeds.

seed_row(Dict) -->
  {
    From1 = Dict.from,
    atom_truncate(From1, 40, From2)
  },
  html(
    tr([
      td(\external_link(From1, From2)),
      td(\seed_actions0(Dict)),
      td(\seed_date_time0(Dict.added)),
      td(\seed_date_time0(Dict.started)),
      td(\seed_date_time0(Dict.ended))
    ])
  ).


seed_actions0(Dict) -->
  {dict_tag(Dict, Hash)},
  (   % Start crawling of ‘todo’ seeds.
      {Dict.started =:= 0.0}
  ->  {
        format(atom(SFunc), 'startSeed("~a");', [Hash]),
        format(atom(DFunc), 'deleteSeed("~a");', [Hash])
      },
      button(SFunc, "Start"),
      button(DFunc, "Delete")
  ;   % No buttons for ‘cleaning’.
      {Dict.ended =:= 0.0}
  ->  llw_reset_button(Hash)
  ;   % Show results for ‘cleaned’.
      link_button(Hash, "Data"),
      llw_reset_button(Hash)
  ).



%! add_seed_http(+Hash) is det.
%
% Adds an IRI through the slow HTTP API.
% This is intended for debugging purposes only.

add_seed_http(Hash) :-
  http_link_to_id(seed, path_postfix(Hash), Iri),
  http_post(Iri, json(_{})).





% HELPERS %

%! html_seed(+Dict)// is det.
%
% Generates a simple HTML representation of the given seed compound
% term.

html_seed(Dict) -->
  html([
    h1(\external_link(Dict.from)),
    \table([
      tr([th("Added"),td(\seed_date_time0(Dict.added))]),
      tr([th("Started"),td(\seed_date_time0(Dict.started))]),
      tr([th("Ended"),td(\seed_date_time0(Dict.ended))])
    ])
  ]).



%! iri_to_hash(+Path, -Hash) is semidet.

iri_to_hash(Path, Hash) :-
  iri_to_hash(seed, Path, Hash).


iri_to_hash(Type, Path, Hash) :-
  atomic_list_concat(['',Type|PathComps], /, Path),
  last(PathComps, Hash).



%! llw_reset_button(+Iri)// is det.

llw_reset_button(Iri) -->
  {format(atom(Func), 'deleteData("~a");', [Iri])},
  button(Func, "Reset").



%! seed_date_time0(+DT)// is det.
%
% Generate a human- and machine-processable date/time representation
% if a date/time is present.  Display a stub otherwise.

seed_date_time0(DT) -->
  {DT =:= 0.0}, !,
  html('∅').
seed_date_time0(DT) -->
  {current_ltag(LTag)},
  html_date_time(DT, _{ltag: LTag, masks: [offset], month_abbr: true}).
