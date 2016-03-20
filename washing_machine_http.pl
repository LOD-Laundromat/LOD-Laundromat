:- module(washing_machine_http, []).

/** <module> HTTP API on top of the Washing Machine

| *Path*      | *Method* | *Media type*         | *Status codes* |
|:------------|:---------|:---------------------|:---------------|
| `/data`     | `GET`    | `text/html`          | 200            |
| `/data`     | `POST`   | `application/json`   | 201            |
| `/data/MD5` | `DELETE` |                      |                |
| `/data/MD5` | `GET`    | `application/nquads` | 200            |
| `/data/MD5` | `GET`    | `text/html`          | 200            |

---

@author Wouter Beek
@tbd Authorization for DELETE and POST request.
@version 2016/02-2016/03
*/

:- use_module(library(aggregate)).
:- use_module(library(html/dataTables)).
:- use_module(library(html/html_bs)).
:- use_module(library(html/html_date_time)).
:- use_module(library(html/html_ext)).
:- use_module(library(html/rdfh_grid)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/http_wrapper)).
:- use_module(library(http/http_json)).
:- use_module(library(http/rest)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pair_ext)).
:- use_module(library(rdf/rdf_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(string_ext)).
:- use_module(library(uri/uri_ext)).

:- use_module(cpack('LOD-Laundromat'/seedlist)).
:- use_module(cpack('LOD-Laundromat'/laundromat_fs)).
:- use_module(cpack('LOD-Laundromat'/washing_machine)).

:- rdf_register_prefix(data, 'http://cliopatria.lod.labs.vu.nl/data/').

:- http_handler(root(data), data, [prefix]).

data(Req) :- rest_handler(Req, data, ldoc, ldoc, ldocs).
ldoc(Method, MTs, Doc) :- rest_mediatype(Method, MTs, Doc, ldoc_mediatype).
ldocs(Method, MTs) :- rest_mediatype(Method, MTs, ldocs_mediatype).

ldoc_mediatype(delete, application/json, Doc) :- !,
  reset_ldoc(Doc),
  reply_json_dict(_{}, [status(200)]).
ldoc_mediatype(get, application/nquads, Doc) :- !,
  ldoc_file(Doc, data, File),
  access_file(File, read),
  http_reply_file(File).
ldoc_mediatype(get, text/html, Doc) :-
  ldoc_load(Doc, meta),
  ldoc_hash(Doc, Hash),
  string_list_concat(["Washing Machine",Hash], " - ", Title),
  reply_html_page(cliopatria(default), title(Title), [
    \rdfh_grid(Doc),
    \(cpa_browse:list_triples(_, Doc, _, _))
  ]).

ldocs_mediatype(get, application/json) :-
  desc_ldocs(Pairs),
  findall(Row, (member(Pair, Pairs), pair_row0(Pair, Row)), Rows),
  reply_json_dict(Rows, [status(200)]).
ldocs_mediatype(get, text/html) :-
  string_list_concat(["LOD Laundromat","Documents"], " - ", Title),
  reply_html_page(cliopatria(default),
    [title(Title),\html_requires(dataTables)],
    [
      h1(Title),
      \tuple_counter,
      table([class=display,id=table_id],
        thead(
          tr([
            th("Document"),
            th("Last modified"),
            th("End status"),
            th("Tuples"),
            th("Number of warnings"),
            th("HTTP status")
          ])
        )
      ),
      \js_script({|javascript(_)||
$(document).ready( function () {
  $.ajax({"contentType": "application/json", "dataType": "json", "type": "GET", "url": "data"}).then(function(data) {
    $('#table_id').DataTable({ data: data });
  })
});
      |})
    ]
  ).
ldocs_mediatype(post, application/json) :- !,
  http_read_json_dict(Data),
  atom_string(Hash, Data.seed),
  (   seed(Hash)
  ->  detached_thread(clean(Hash)),
      reply_json_dict(_{}, [status(201)])
  ;   reply_json_dict(_{}, [status(404)])
  ).





% HELPERS %

desc_ldocs(SortedPairs) :-
  findall(Mod-Doc, (ldoc(Doc), ldoc_lmod(Doc, Mod)), Pairs),
  desc_pairs(Pairs, SortedPairs).


pair_row0(Mod0-Doc, [Doc,Mod,End,Tuples,Warnings,Status]) :-
  format_time(atom(Mod), "%FT%T%:z", Mod0),
  (rdf_has(Doc, llo:processed_tuples, Tuples^^xsd:nonNegativeInteger) -> true ; Tuples = 0),
  (rdf_has(Doc, llo:number_of_warnings, Warnings^^xsd:nonNegativeInteger) -> true ; Warnings = 0),
  (rdf_has(Doc, llo:status_code, Status^^xsd:integer) -> true ; Status = "∅"),
  (rdf_has(Doc, llo:end, End^^xsd:string) -> true ; End = '∅').


tuple_counter -->
  {
    aggregate_all(count, rdf_has(_, llo:unique_tuples, _), N1),
    aggregate_all(
      sum(N),
      rdf_has(_, llo:unique_tuples, N^^xsd:nonNegativeInteger),
      N2
    )
  },
  html([
    "Processed ",
    \html_thousands(N1),
    " documents containing ",
    \html_thousands(N2),
    " tuples."
  ]).
