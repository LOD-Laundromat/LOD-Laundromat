:- module(
  lod_laundry,
  [
    lwm/2, % +Request:list
           % +HtmlStyle:atom
    lwm_basket/1 % +Request:list
  ]
).

/** <module> LOD laundry

@author Wouter Beek
@version 2014/05-2014/06
*/

:- use_module(library(http/html_write)).
:- use_module(library(http/http_cors)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_parameters)).
:- use_module(library(http/http_server_files)).
:- use_module(library(http/http_session)).
:- use_module(library(uri)).

:- use_module(generics(typecheck)).

:- use_module(plHtml(html)).
:- use_module(plHtml(html_pl_term)).

:- use_module(plTabular(rdf_html_table)).

:- use_module(lwm(lod_basket)).
:- use_module(lwm(lwm_db)).

:- dynamic(url_md5_translation/2).

:- http_handler(
  cliopatria(data),
  serve_files_in_directory_with_cors(data),
  [prefix]
).



% Response to requesting a JSON description of all LOD URL.
lwm(_, HtmlStyle):-
  reply_html_page(HtmlStyle, title('LOD Laundry'), \sources).


lwm_basket(Request):-
  cors_enable,
  (
    catch(http_parameters(Request, [url(Url1, [])]), _, fail),
    is_url(Url1)
  ->
    % Make sure that it is a URL.
    uri_iri(Url2, Url1),
    add_to_basket(Url2),

    % HTTP status code 202 Accepted: The request has been accepted
    % for processing, but the processing has not been completed.
    reply_json(json{}, [status(202)])
  ;
    % HTTP status code 400 Bad Request: The request could not
    % be understood by the server due to malformed syntax.
    reply_json(json{}, [status(400)])
  ).


serve_files_in_directory_with_cors(Alias, Request):-
  cors_enable,
  serve_files_in_directory(Alias, Request).


sources -->
  {
    once(lwm_endpoint(Endpoint)),
    lwm_sparql_select(Endpoint, _, [lwm], true, [md5res,md5,added,start,end],
        [rdf(var(md5res),lwm:added,var(added)),
         optional([rdf(var(md5res),lwm:end,var(end))]),
         optional([rdf(var(md5res),lwm:start,var(start))]),
         rdf(var(md5res),lwm:md5,var(md5))],
        250, _, _, Rows)
  },
  rdf_html_table(
    [header_column(true),header_row(true),indexed(true)],
    html('Overview of LOD sources.'),
    [['Source','Time added','Time started','Time ended']|Rows]
  ).



lod_laundry_cell(Term) -->
  {
    nonvar(Term),
    Term = Name-InternalLink-ExternalLink
  }, !,
  html([
    \html_link(InternalLink-Name),
    ' ',
    \html_external_link(ExternalLink)
  ]).
lod_laundry_cell(Term) -->
  html_pl_term(plDev(.), Term).

