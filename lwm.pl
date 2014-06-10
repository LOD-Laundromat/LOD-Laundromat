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
:- use_module(library(uri)).

:- use_module(generics(typecheck)).
:- use_module(sparql(sparql_api)).

:- use_module(plHtml(html)).
:- use_module(plHtml(html_pl_term)).
:- use_module(plHtml(html_table)).

:- use_module(lwm(lod_basket)).
:- use_module(lwm(lwm_db)).
:- use_module(lwm(lwm_generics)).

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
    catch(http_parameters(Request, [url(Url1, [])]), _, fail),
    is_url(Url1)
  ->
    % Make sure that it is a URL.
    uri_iri(Url2, Url1),
    add_source_to_basket(Url2),

    % HTTP status code 202 Accepted: The request has been accepted
    % for processing, but the processing has not been completed.
    reply_json(json{}, [status(202)])
  ;
    % HTTP status code 400 Bad Request: The request could not
    % be understood by the server due to malformed syntax.
    reply_json(json{}, [status(400)])
  ).


pending_urls -->
  {
    once(lwm_endpoint(Endpoint)),
    sparql_select(Endpoint, _, [lwm], true, [url,entry_path,added],
        [rdf(Md5Url,lwm:url,var(url)),
         rdf(Md5Url,lwm:has_entry,Md5Entry),
         rdf(Md5Entry,lwm:path,string(entry_path)),
         rdf(Md5Entry,lwm:added,var(added))], inf, _, _, Rows)
  },
  html_table(
    [header_column(true),header_row(true),indexed(true)],
    html('The pending LOD sources.'),
    [['Url','Entry path','Time added']|Rows]
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

