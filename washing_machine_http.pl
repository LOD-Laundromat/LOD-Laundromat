:- module(washing_machine_http, []).

/** <module> HTTP API on top of the Washing Machine

| *Path*      | *Method* | *Media type*         | *Status codes* |
|:------------|:---------|:---------------------|:---------------|
| `/data`     | `POST`   | `application/json`   | 201            |
| `/data/MD5` | `DELETE` |                      |                |
| `/data/MD5` | `GET`    | `application/nquads` | 200            |
| `/data/MD5` | `GET`    | `text/html`          | 200            |

---

@author Wouter Beek
@tbd Authorization for DELETE and POST request.
@version 2016/02
*/

:- use_module(library(html/rdf_html_grid)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/http_wrapper)).
:- use_module(library(http/http_json)).
:- use_module(library(http/rest)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pair_ext)).
:- use_module(library(rdf/rdf_api)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(string_ext)).
:- use_module(library(uri/uri_ext)).

:- use_module(cpack('LOD-Laundromat'/seedlist)).
:- use_module(cpack('LOD-Laundromat'/washing_machine)).

:- rdf_register_prefix(data, 'http://cliopatria.lod.labs.vu.nl/data/').
:- rdf_register_prefix(meta, 'http://cliopatria.lod.labs.vu.nl/meta/').

:- http_handler(root(data), data, [prefix]).

data(Req) :- rest_handler(Req, data, is_document, document, documents).
document(Method, MTs, Doc) :- rest_mediatype(Method, MTs, Doc, document_mediatype).
documents(Method, MTs) :- rest_mediatype(Method, MTs, documents_mediatype).

document_mediatype(delete, application/json, Doc) :- !,
  document_hash(Doc, Hash),
  reset_seed(Hash),
  reply_json_dict(_{}, [status(200)]).
document_mediatype(get, application/nquads, Doc) :- !,
  document_to_directory(Doc, Dir),
  directory_file_path(Dir, 'data.nq.gz', File),
  http_reply_file(File).
document_mediatype(get, text/html, Doc) :-
  (   rdf_graph(Doc)
  ->  true
  ;   document_to_directory(Doc, Dir),
      directory_file_path(Dir, 'meta.nq.gz', File),
      rdf_load_file(File, [graph(Doc)])
  ),
  document_hash(Doc, Hash),
  string_list_concat(["Washing Machine",Hash], " - ", Title),
  reply_html_page(cliopatria(default), title(Title), [
    \rdf_html_grid(Doc),
    \(cpa_browse:list_triples(_, Doc, _, _))
  ]).

documents_mediatype(get, text/html) :- !,
  string_list_concat(["LOD Laundromat","Documents"], " - ", Title),
  reply_html_page(cliopatria(default), title(Title), "").
documents_mediatype(post, application/json) :-
  http_read_json_dict(Data),
  atom_string(H, Data.seed),
  (   is_current_seed(H)
  ->  detached_thread(clean_seed(H)),
      reply_json_dict(_{}, [status(201)])
  ;   reply_json_dict(_{}, [status(404)])
  ).
