:- module(
  washing_machine_http,
  [
  ]
).

/** <module> HTTP API on top of the Washing Machine

| *Path*         | *Method* | *Media type*         | *Status codes* |
|:---------------|:---------|:---------------------|:---------------|
| `/laundry`     | `POST`   | `application/json`   | 201            |
| `/laundry/MD5` | `DELETE` |                      |                |
| `/laundry/MD5` | `GET`    | `application/nquads` | 200            |
| `/laundry/MD5` | `GET`    | `text/html`          | 200            |

---

@author Wouter Beek
@tbd Authorization for DELETE and POST request.
@version 2016/02
*/

:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/http_wrapper)).
:- use_module(library(http/http_json)).
:- use_module(library(http/rest)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pair_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(string_ext)).
:- use_module(library(uri/uri_ext)).

:- use_module(cpack('LOD-Laundromat'/seedlist)).
:- use_module(cpack('LOD-Laundromat'/washing_machine)).

:- http_handler(root(laundry), laundry, [prefix]).

laundry(Req) :- rest_handler(Req, laundry, is_document, document, documents).
document(Method, MTs, Doc) :- rest_mediatype(Method, MTs, Doc, document_mediatype).
documents(Method, MTs) :- rest_mediatype(Method, MTs, documents_mediatype).

document_mediatype(delete, application/json, Doc) :- !,
  uri_path(Doc, Md5),
  reset_seed(Md5).
document_mediatype(get, application/nquads, Doc) :- !,
  document_to_path(Doc, Dir),
  resolve_file(Dir, 'data.nq.gz', File),
  http_current_request(Req),
  http_reply_file(File, [], Req).
document_mediatype(get, text/html, Doc) :-
  uri_path(Doc, Md5),
  rdf_global_id(llr:Md5, Res),
  (   rdf_graph(Res)
  ->  true
  ;   document_to_path(Doc, Dir),
      resolve_file(Dir, 'meta.nq.gz', File),
      rdf_load_file(File, [graph(Res)])
  ),
  string_list_concat(['Open Data Market',Res], " - ", Title),
  reply_html_page(cliopatria(default), title(Title),
    \(cpa_browse:list_triples(_, Res, _, _))
  ).

documents_mediatype(get, text/html) :- !,
  string_list_concat(["LOD Laundromat","Documents"], " - ", Title),
  reply_html_page(cliopatria(default), title(Title), "").
documents_mediatype(post, application/json) :-
  http_read_json_dict(Data),
  H = Data.seed,
  (   is_current_seed(H)
  ->  detached_thread(clean_seed(H)),
      reply_json_dict(_{}, [status(201)])
  ;   reply_json_dict(_{}, [status(404)])
  ).
