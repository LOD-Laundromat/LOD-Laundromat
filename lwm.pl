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

:- use_module(library(aggregate)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_parameters)).
:- use_module(library(http/http_server_files)).
:- use_module(library(semweb/rdf_db)).

:- use_module(generics(typecheck)).

:- use_module(plHtml(html)).
:- use_module(plHtml(html_pl_term)).
:- use_module(plHtml(html_table)).

:- use_module(lwm(url_pool)).

:- dynamic(url_md5_translation/2).

:- http_handler(
  cliopatria(lwm/clean),
  serve_files_in_directory(data),
  [prefix]
).



% Response to requesting a JSON description of all LOD URL.
lwm(_, HtmlStyle):-
  reply_html_page(HtmlStyle, title('LOD Laundry'), pending_urls).


lwm_basket(Request):-
  (
    http_parameters(Request, [url(Url, [])]),
    is_url(Url)
  ->
    add_to_lod_basket(Url),

    % HTTP status code 202 Accepted: The request has been accepted
    % for processing, but the processing has not been completed.
    reply_json(json{}, [status(202)])
  ;
    % HTTP status code 400 Bad Request: The request could not
    % be understood by the server due to malformed syntax.
    reply_json(json{}, [status(400)])
  ).


pending_urls -->
  {findall(
    [Url],
    current_pending_url(Url),
    Rows
  )},
  html_table(
    [header_column(true),header_row(true),indexed(true)],
    html('The pending LOD URLs'),
    [['Url']|Rows]
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

