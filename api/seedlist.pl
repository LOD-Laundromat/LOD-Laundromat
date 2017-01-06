:- module(
  seedlist,
  [
    add_seed/1,                  % +From
    add_seed/2,                  % +From, -Hash
    begin_seed_hash/1,           % +Hash
    end_seed_hash/1,             % +Hash
    number_of_seeds_by_status/2, % +Status, -NumSeeds
    print_seeds/0,
    remove_seed/1,               % +Hash
    reset_seed/1,                % +Hash
    seed/1,                      % -Dict
    seed_by_hash/2,              % +Hash, -Dict
    seed_by_status/2,            % +Status, -Dict
    seed_status/1,               % ?Status
    seeds_by_status/2            % +Status, -Result
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
@version 2016/01-2017/01
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(call_ext)).
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
:- use_module(library(http/json)).
:- use_module(library(http/rest)).
:- use_module(library(json_ext)).
:- use_module(library(list_ext)).
:- use_module(library(math/math_ext)).
:- use_module(library(nlp/nlp_lang)).
:- use_module(library(os/archive_ext)).
:- use_module(library(os/file_ext)).
:- use_module(library(os/io)).
:- use_module(library(pagination/html_pagination)).
:- use_module(library(pair_ext)).
:- use_module(library(print_ext)).
:- use_module(library(q/q_fs)).
:- use_module(library(q/q_iri)).
:- use_module(library(service/es_api)).
:- use_module(library(settings)).
:- use_module(library(string_ext)).
:- use_module(library(thread)).
:- use_module(library(true)).
:- use_module(library(uri)).
:- use_module(library(yall)).

:- use_module(ll(style/ll_style)).

:- http_handler(ll(seed), seedlist_handler, [methods([delete,get,post]),prefix]).

:- multifile
    http_param/1,
    media_type/1.

http_param(page).
http_param(page_size).

media_type(application/json).
media_type(text/html).





% HTTP API %

seedlist_handler(Req) :-
  rest_method(Req, seedlist_handler_method(Req)).


seedlist_handler_method(Req, delete, MTs) :-
  memberchk(request_uri(Iri), Req),
  rest_media_type(MTs, seedlist_delete(Iri)).
seedlist_handler_method(Req, get, MTs) :-
  memberchk(request_uri(Iri), Req),
  rest_media_type(MTs, seedlist_get(Iri)).
seedlist_handler_method(Req, post, MTs) :-
  http_read_json_dict(Req, Data),
  add_seed(Data.from, Iri),
  rest_media_type(MTs, seedlist_post(Iri)).


seedlist_delete(Iri, application/json) :-
  iri_to_hash(Iri, Hash),
  remove_seed(Hash),
  reply_json_dict(_{}, [status(204)]).


seedlist_get(Iri, application/json) :-
  (   iri_to_hash(Iri, Hash)
  ->  create_pagination(Dict, seed_by_hash(Hash, Dict), Result)
  ;   create_pagination(Dict, seed(Dict), Result)
  ),
  format("Content-Type: application/json~n"),
  http_pagination_header(Result),
  nl,
  json_write_dict(Result.results).
seedlist_get(Iri, text/html) :-
  (   iri_to_hash(Iri, Hash)
  ->  seed_by_hash(Hash, Dict),
      reply_html_page(
        ll([]),
        \cp_title(["Seed",Hash]),
        \html_seed(Dict)
      )
  ;   seeds_by_status(ended, Result1),
      seeds_by_status(started, Result2),
      seeds_by_status(added, Result3),
      reply_html_page(
        ll([]),
        \cp_title(["Seedlist"]),
        [
          h1("Seedlist"),
          \panels([
            \seedlist_panel(1, "Cleaned", Result1),
            \seedlist_panel(2, "Cleaning", Result2),
            \seedlist_panel(3, "To be cleaned", Result3)
          ])
        ]
      )
  ).


seedlist_post(Iri, application/json) :-
  reply_json_dict(_{seed: Iri}, [status(201)]).

seedlist_panel(Id, Lbl, Result) -->
  panel(
    Id,
    html([
      Lbl,
      " ",
      \out_of(
        Result.number_of_results,
        Result.total_number_of_results
      )
    ]),
    \seeds_table(Result.results)
  ).

out_of(M, N) -->
  html(["(",\html_thousands(M),"/",\html_thousands(N),")"]).



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
  {
    dict_tag(Dict, Hash),
    http_link_to_id(data_handler, [hash=Hash], DataIri),
    http_link_to_id(meta_handler, [hash=Hash], MetaIri)
  },
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
  ->  ll_reset_button(Hash)
  ;   % Show results for ‘cleaned’.
      link_button(DataIri, "Data"),
      link_button(MetaIri, "Meta"),
      ll_reset_button(Hash)
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
  retry0(
    es_create(
      [ll,seedlist,Hash],
      _{added: Now, ended: 0.0, from: From2, started: 0.0}
    )
  ),
  debug(seedlist, "Added to seedlist: ~a (~a)", [From2,Hash]).



%! begin_seed_hash(+Hash:atom) is det.
%
% Pop a dirty seed off the seedlist.

begin_seed_hash(Hash) :-
  get_time(Started),
  retry0(es_update([ll,seedlist,Hash], _{doc: _{started: Started}})),
  debug(seedlist(begin), "Started cleaning seed ~a", [Hash]).



%! end_seed_hash(+Hash) is det.

end_seed_hash(Hash) :-
  get_time(Ended),
  retry0(es_update([ll,seedlist,Hash], _{doc: _{ended: Ended}})),
  debug(seedlist(end), "Ended cleaning seed ~a", [Hash]).



%! number_of_seeds_by_status(+Status, -NumSeeds) is det.

number_of_seeds_by_status(Status, NumSeeds) :-
  once(seeds_by_status(Status, Result)),
  NumSeeds = Result.total_number_of_results.



%! print_seeds is det.

print_seeds :-
  once(seeds_by_status(added, Result1)),
  once(seeds_by_status(started, Result2)),
  once(seeds_by_status(ended, Result3)),
  Num1 = Result1.total_number_of_results,
  Num2 = Result2.total_number_of_results,
  Num3 = Result3.total_number_of_results,
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
  retry0(es_rm([ll,seedlist,Hash])),
  debug(seedlist(remove), "Removed seed ~a", [Hash]).



%! reset_seed(+Hash) is det.

reset_seed(Hash) :-
  seed_by_hash(Hash, Seed),
  atom_string(From, Seed.from),
  % Remove the directories for all seed entries, if any.
  ignore(
    forall(
      call_on_stream(uri(From), reset_seed_entry(From)),
      true
    )
  ),
  % Remove the directory for the seed,
  q_dir_hash(Dir, Hash),
  with_mutex(lclean, delete_directory_and_contents_msg(Dir)),
  
  % Update the seedlist.
  get_time(Now),
  retry0(
    es_update(
      [ll,seedlist,Hash],
      _{doc: _{added: Now, started: 0.0, ended: 0.0}}
    )
  ),
  debug(seedlist(reset), "Reset seed ~a", [Hash]).


reset_seed_entry(From, _, InPath, InPath) :-
  path_entry_name(InPath, EntryName),
  md5(From-EntryName, EntryHash),
  q_dir_hash(EntryDir, EntryHash),
  with_mutex(lclean, delete_directory_and_contents_msg(EntryDir)).



%! seed(-Dict) is nondet.

seed(Dict) :-
  seed_status(Status),
  seed_by_status(Status, Dict).



%! seed_by_hash(+Hash, -Dict) is nondet.

seed_by_hash(Hash, Dict) :-
  retry0(es_get([ll,seedlist,Hash], Dict)).



%! seed_by_status(+Status:oneof([added,ended,started]), -Dict) is nondet.

seed_by_status(Status, Dict) :-
  seeds_by_status(Status, Result),
  Results = Result.results,
  member(Dict, Results).
  


%! seed_status(+Status) is semidet.
%! seed_status(-Status) is multi.

seed_status(added).
seed_status(started).
seed_status(ended).



%! seeds_by_status(+Status, -Result) is nondet.
%
% Returns all seeds with the same status in pages.

seeds_by_status(Status, Result) :-
  status_query(Status, Query),
  retry0(es_search([ll,seedlist], _{query: Query}, _{}, Result)).

status_query(ended, Query) :- !,
  Query = _{range: _{ended: _{gt: 0.0}}}.
status_query(started, Query) :- !,
  Query = _{bool: _{must: [_{term: _{ended: 0.0}}, _{range: _{started: _{gt: 0.0}}}]}}.
status_query(added, Query) :-
  Query = _{bool: _{must: [_{term: _{started: 0.0}}, _{range: _{added: _{gt: 0.0}}}]}}.





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



%! ll_reset_button(+Iri)// is det.

ll_reset_button(Iri) -->
  {format(atom(Func), 'deleteData("~a");', [Iri])},
  button(Func, "Reset").



%! seed_date_time0(+DT)// is det.
%
% Generate a human- and machine-processable date/time representation
% if a date/time is present.  Display a stub otherwise.

seed_date_time0(DT) -->
  {DT =:= 0.0}, !,
  html("∅").
seed_date_time0(DT) -->
  {current_ltag(LTag)},
  html_date_time(DT, _{ltag: LTag, masks: [offset], month_abbr: true}).
