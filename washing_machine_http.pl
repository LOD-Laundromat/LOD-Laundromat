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
  ldir_ldoc(Dir, Doc),
  directory_file_path(Dir, 'data.nq.gz', File),
  http_reply_file(File).
ldoc_mediatype(get, text/html, Doc) :-
  (   rdf_graph(Doc)
  ->  true
  ;   ldir_ldoc(Dir, Doc),
      directory_file_path(Dir, 'meta.nq.gz', File),
      rdf_load_file(File, [graph(Doc)])
  ),
  ldoc_hash(Doc, Hash),
  string_list_concat(["Washing Machine",Hash], " - ", Title),
  reply_html_page(cliopatria(default), title(Title), [
    \rdfh_grid(Doc),
    \(cpa_browse:list_triples(_, Doc, _, _))
  ]).

ldocs_mediatype(post, application/json) :- !,
  http_read_json_dict(Data),
  atom_string(H, Data.seed),
  (   is_current_seed(H)
  ->  detached_thread(clean_seed(H)),
      reply_json_dict(_{}, [status(201)])
  ;   reply_json_dict(_{}, [status(404)])
  ).
ldocs_mediatype(get, text/html) :-
  string_list_concat(["LOD Laundromat","Documents"], " - ", Title),
  findall(Mod-Doc, (ldoc(Doc), ldoc_lmod(Doc, Mod)), Pairs),
  desc_pairs(Pairs, SortedPairs),
  reply_html_page(cliopatria(default), title(Title), [
    h1("LOD Laundromat Documents"),
    ol(\html_maplist(ldoc_card, SortedPairs))
  ]).

ldoc_card(Mod-Doc) -->
  {ldoc_hash(Doc, Hash)},
  card(\html_date_time(Mod), \bs_link_button(Doc, Hash)).
