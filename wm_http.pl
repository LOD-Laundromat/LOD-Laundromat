:- module(wm_http, []).

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
:- use_module(library(html/rdfh)).
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

:- use_module(cpack('LOD-Laundromat'/lfs)).
:- use_module(cpack('LOD-Laundromat'/lhdt)).
:- use_module(cpack('LOD-Laundromat'/seedlist)).
:- use_module(cpack('LOD-Laundromat'/wm)).

:- rdf_register_prefix(data, 'http://cliopatria.lod.labs.vu.nl/data/').
:- rdf_register_prefix(meta, 'http://cliopatria.lod.labs.vu.nl/meta/').

:- http_handler(root(data), data, [prefix]).
:- http_handler(root(meta), meta, [prefix]).

data(Req) :- rest_handler(Req, data, ldoc(data), data, datas).
data(Method, MTs, Doc) :- rest_mediatype(Method, MTs, Doc, data_mediatype).
datas(Method, MTs) :- rest_mediatype(Method, MTs, datas_mediatype).

meta(Req) :- rest_handler(Req, meta, ldoc(meta), meta, metas).
meta(Method, MTs, Doc) :- rest_mediatype(Method, MTs, Doc, meta_mediatype).
metas(Method, MTs) :- rest_mediatype(Method, MTs, metas_mediatype).

data_mediatype(get, application/'vnd.hdt', Doc) :- !,
  ldoc_lfile(Doc, hdt, File),
  access_file(File, read),
  http_reply_file(File).
data_mediatype(delete, application/json, Doc) :- !,
  ldoc_lhash(Doc, Hash),
  reset(Hash),
  reply_json_dict(_{}, [status(200)]).
data_mediatype(get, application/nquads, Doc) :- !,
  ldoc_lfile(Doc, nquads, File),
  access_file(File, read),
  http_reply_file(File).
data_mediatype(get, text/html, Doc) :-
  ldoc_lhash(Doc, Hash),
  string_list_concat(["Washing Machine","Data",Hash], " - ", Title),
  reply_html_page(cliopatria(default), title(Title),
    \lhdt_data_table(_, _, _, Doc, _{page: 1})
  ).

meta_mediatype(get, application/nquads, Doc) :- !,
  ldoc_lfile(Doc, nquads, File),
  access_file(File, read),
  http_reply_file(File).
meta_mediatype(get, text/html, Doc) :-
  ldoc_lhash(Doc, Name, Hash),
  lrdf_load(Hash, Name),
  string_list_concat(["Washing Machine","Metadata",Hash], " - ", Title),
  reply_html_page(cliopatria(default), title(Title), [
    \rdfh_grid(Doc),
    \rdfh_triple_table(_, _, _, Doc)
  ]).

datas_mediatype(get, application/json) :-
  desc_ldocs(data, hdt, Pairs),
  findall(Row, (member(Pair, Pairs), pair_row0(Pair, Row)), Rows),
  reply_json_dict(Rows, [status(200)]).
datas_mediatype(get, text/html) :-
  string_list_concat(["LOD Laundromat","Documents"], " - ", Title),
  reply_html_page(cliopatria(default),
    [title(Title),\html_requires(dataTables)],
    [
      h1("Documents"),
      \tuple_counter,
      table([class=display,id=table_id],
        thead(
          tr([
            th("Document"),
            th("Last modified"),
            th("End status"),
            th("Tuples"),
            th("Number of warnings"),
            th("HTTP status"),
            th("RDF format")
          ])
        )
      ),
      \js_script({|javascript(_)||
$(document).ready( function () {
  $.ajax({"contentType": "application/json", "dataType": "json", "type": "GET", "url": "data"}).then(function(data) {
    $('#table_id').DataTable({ data: data });
  })
});
      |}),
      \wm_table
    ]
  ).
datas_mediatype(post, application/json) :- !,
  http_read_json_dict(Data),
  atom_string(Hash, Data.seed),
  (   seed(Hash)
  ->  detached_thread(clean(Hash)),
      reply_json_dict(_{}, [status(201)])
  ;   reply_json_dict(_{}, [status(404)])
  ).

metas_mediatype(get, text/html) :-
  reply_html_page(cliopatria(default), title("LOD Laundromat - Metadata"),
    p("test")
  ).

wm_table -->
  {
    aggregate_all(
      set([Alias,Global,Local]),
      (
        current_wm(Alias),
        thread_statistics(Alias, global, Global),
        thread_statistics(Alias, local, Local)
      ),
      Rows
    )
  },
  html([
    h1("Washing Machines"),
    \bs_table(
      \bs_table_header(["Washing Machine","Global","Local"]),
      \html_maplist(bs_table_row, Rows)
    )
  ]).





% HELPERS %

%! desc_ldocs(+Name, +Kind, -Pairs) is det.

desc_ldocs(Name, Kind, SortedPairs) :-
  findall(
    Mod-Doc,
    (
      lhash(Hash),
      ldoc_lhash(Doc, Name, Hash),
      lfile_lhash(File, Name, Kind, Hash),
      time_file(File, Mod)
    ),
    Pairs
  ),
  desc_pairs(Pairs, SortedPairs).


pair_row0(Mod0-Doc, [Doc,Mod,End,Tuples,Warnings,Status,Format]) :-
  format_time(atom(Mod), "%FT%T%:z", Mod0),
  (rdf_has(Doc, llo:processed_tuples, Tuples^^xsd:nonNegativeInteger) -> true ; Tuples = 0),
  (rdf_has(Doc, llo:number_of_warnings, Warnings^^xsd:nonNegativeInteger) -> true ; Warnings = 0),
  (rdf_has(Doc, llo:status_code, Status^^xsd:integer) -> true ; Status = "∅"),
  (rdf_has(Doc, llo:end, End^^xsd:string) -> true ; End = '∅'),
  (rdf_has(Doc, llo:rdf_format, Format^^xsd:string) -> true ; Format = '∅').


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
