:- module(
  seedlist,
  [
    add_seed/1,       % +From
    add_seed/2,       % +From, -Hash
    begin_seed/1,     % +Seed
    end_seed/1,       % +Seed
    print_seeds/0,
    remove_seed/1,    % +Hash
    reset_seed/1,     % +Hash
    seed/1,           % -Dict
    seed_by_hash/2,   % +Hash, -Dict
    seed_by_status/2, % +Status:oneof([added,started,ended]), -Dict
    seed_status/1,    % ?Status
    seeds_by_status/2 % +Status:oneof([added,started,ended]), -Result
  ]
).

/** <module> LOD Laundromat: Seedlist API

Three stages for seeds:

  1. added

  2. started

  3. ended

---

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

---

@author Wouter Beek
@tbd Add authorization for DELETE and POST requests.
@version 2016/01-2016/12
*/

:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(dcg/dcg_ext)).
:- use_module(library(dcg/dcg_pl)).
:- use_module(library(debug_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(error)).
:- use_module(library(hash_ext)).
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
:- use_module(library(json_ext)).
:- use_module(library(list_ext)).
:- use_module(library(math/math_ext)).
:- use_module(library(nlp/nlp_lang)).
:- use_module(library(os/file_ext)).
:- use_module(library(pagination)).
:- use_module(library(pair_ext)).
:- use_module(library(print_ext)).
:- use_module(library(q/q_iri)).
:- use_module(library(service/es_api)).
:- use_module(library(settings)).
:- use_module(library(string_ext)).
:- use_module(library(thread)).
:- use_module(library(true)).
:- use_module(library(uri)).
:- use_module(library(yall)).

:- use_module(ll(style/ll_style)).

:- http_handler(llw(seed), seedlist_handler, [prefix]).

:- multifile
    http_param/1,
    media_type/1.

http_param(page).
http_param(page_size).

media_type(application/json).
media_type(text/html).





% HTTP API %

seedlist_handler(Req) :-
  rest_method(Req, [delete,get,post], seedlist_handler_method).


seedlist_handler_method(Req, delete, MTs) :-
  http_relative_iri(Req, Iri),
  rest_media_type(Req, delete, MTs, seedlist_media_type(Iri)).
seedlist_handler_method(Req, Method, MTs) :-
  http_is_get(Method),
  http_relative_iri(Req, Iri),
  rest_media_type(Req, Method, MTs, seedlist_media_type(Iri)).
seedlist_handler_method(Req, post, MTs) :-
  http_read_json_dict(Data),
  add_seed(Data.from, Iri),
  rest_media_type(Req, post, MTs, seedlist_media_type(Iri)).


seedlist_media_type(Iri, delete, application/json) :-
  iri_to_hash(Iri, Hash),
  remove_seed(Hash),
  reply_json_dict(_{}, [status(200)]).
seedlist_media_type(Iri, Method, MT) :-
  MT = application/json,
  (   iri_to_hash(Iri, Hash)
  ->  pagination(Dict, seed_by_hash(Hash, Dict), Result)
  ;   pagination(Dict, seed(Dict), Result)
  ),
  rest_reply(
    Method,
    reply_content_type(MT),
    json_write_dict(Result.results)
  ).
seedlist_media_type(Iri, Method, text/html) :-
  (   iri_to_hash(Iri, Hash)
  ->  seed_by_hash(Hash, Dict),
      reply_html_page(
        Method,
        ll([]),
        \q_title(["Seed",Hash]),
        \html_seed(Dict)
      )
  ;   N = 3,
      findnsols(N, Seed1, seed_by_status(ended, Seed1), L1),
      findnsols(N, Seed2, seed_by_status(started, Seed2), L2),
      findnsols(N, Seed3, seed_by_status(added, Seed3), L3),
      reply_html_page(
        Method,
        ll([]),
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
seedlist_media_type(Iri, post, application/json) :-
  reply_json_dict(_{seed: Iri}, [status(201)]).



%! seeds_table(+Seeds)// is det.
%
% Generates an HTML table representing the given seeds.

seeds_table(Seeds) -->
  {http_link_to_id(seedlist_handler, Iri)},
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





% API %

%! add_seed(+From) is det.
%! add_seed(+From, -Hash) is det.
% Adds an IRI to the seedlist.
%
% @throws existence_error if IRI is already in the seedlist.

add_seed(From) :-
  add_seed(From, _).


add_seed(From1, Hash) :-
  iri_normalized(From1, From2),
  md5(From2, Hash),
  get_time(Now),
  es_create_pp(
    [llw,seedlist,Hash],
    _{added: Now, ended: 0.0, from: From2, number_of_tuples: 0, started: 0.0}
  ),
  debug(seedlist, "Added to seedlist: ~a (~a)", [From2,Hash]).



%! begin_seed(+Seed) is det.
%
% Pop a dirty seed off the seedlist.

begin_seed(Seed) :-
  dict_tag(Seed, Hash),
  get_time(Started),
  es_update_pp([llw,seedlist,Hash], _{doc: _{started: Started}}),
  debug(seedlist(begin), "Started cleaning seed ~a", [Hash]).



%! end_seed(+Seed) is det.

end_seed(Seed) :-
  dict_tag(Seed, Hash),
  get_time(Ended),
  es_update_pp([llw,seedlist,Hash], _{doc: _{ended: Ended}}),
  debug(seedlist(end), "Ended cleaning seed ~a", [Hash]).



%! print_seeds is det.

print_seeds :-
  maplist(seeds_by_status, [added,started,ended], [Seeds1,Seeds2,Seeds3]),
  Num1 = Seeds1.total_number_of_results,
  Num2 = Seeds2.total_number_of_results,
  Num3 = Seeds3.total_number_of_results,
  sum_list([Num1,Num2,Num3], Num123),
  float_div_zero(Num1, Num123, Perc1),
  float_div_zero(Num2, Num123, Perc2),
  float_div_zero(Num3, Num123, Perc3),
  HeaderRow = head(["Category","Size","Percentage"]),
  DataRows = [
    ["All",           Num123, perc(1.0)  ],
    ["To be cleaned", Num1,   perc(Perc1)],
    ["Cleaning",      Num2,   perc(Perc2)],
    ["Cleaned",       Num3,   perc(Perc3)]
  ],
  print_table([HeaderRow|DataRows]).



%! remove_seed(+Hash) is det.

remove_seed(Hash) :-
  es_rm_pp([llw,seedlist,Hash]),
  debug(seedlist(remove), "Removed seed ~a", [Hash]).



%! reset_seed(+Hash) is det.

reset_seed(Hash) :-
  get_time(Now),
  es_update_pp(
    [llw,seedlist,Hash],
    _{doc: _{added: Now, started: 0.0, ended: 0.0}}
  ),
  debug(seedlist(reset), "Reset seed ~a", [Hash]).



%! seed(-Dict) is nondet.

seed(Dict) :-
  seed_status(Status),
  seed_by_status(Status, Dict).



%! seed_by_hash(+Hash, -Dict) is nondet.

seed_by_hash(Hash, Dict) :-
  es_get([llw,seedlist,Hash], Dict).



%! seed_by_status(+Status:oneof([added,ended,started]), -Dict) is nondet.

seed_by_status(Status, Dict) :-
  seeds_by_status(Status, Pagination),
  member(Dict, Pagination.results).



%! seed_status(+Status) is semidet.
%! seed_status(-Status) is multi.

seed_status(added).
seed_status(started).
seed_status(ended).



%! seeds_by_status(+Status, -Pagination) is nondet.
%
% Returns all seeds with the same status in pages.

seeds_by_status(Status, Pagination) :-
  dict_pairs(Range, [Status-_{gt: 0.0}]),
  es_search(
    [llw,seedlist],
    _{query: _{filtered: _{filter: _{range: Range}}}},
    _{},
    Pagination
  ).





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
