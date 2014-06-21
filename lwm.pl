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
:- use_module(library(http/http_cors)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_parameters)).
:- use_module(library(http/http_server_files)).
:- use_module(library(http/http_session)). % HTTP session support.
:- use_module(library(semweb/rdf_db)).
:- use_module(library(lists)).
:- use_module(library(uri)).

:- use_module(generics(typecheck)).

:- use_module(plHtml(html)).
:- use_module(plHtml(html_pl_term)).

:- use_module(plRdf_term(rdf_datatype)).
:- use_module(plRdf_term(rdf_literal)).
:- use_module(plRdf_term(rdf_string)).

:- use_module(plTabular(rdf_html_table)).

:- use_module(lwm(lod_basket)).
:- use_module(lwm(lwm_db)).

:- dynamic(url_md5_translation/2).

:- http_handler(
  cliopatria(data),
  serve_files_in_directory_with_cors(data),
  [id(clean),prefix]
).



% Response to requesting a JSON description of all LOD URL.
lwm(_, HtmlStyle):-
  reply_html_page(HtmlStyle, title('LOD Laundry'), \datadoc_overview).

datadoc_overview -->
  html([
    h2('Cleaned'),
    \cleaned_datadocs,
    h2('Cleaning'),
    \cleaning_datadocs
    %h2('To be cleaned'),
    %\pending_datadocs
  ]).

cleaned_datadocs -->
  {
    aggregate_all(
      set(Triples-[Location-Md5,Triples,Added,Started,Ended]),
      (
        rdf(Md5res, lwm:end, Ended),
        rdf(Md5res, lwm:start, Started),
        rdf(Md5res, lwm:added, Added),
        rdf_string(Md5res, lwm:md5, Md5, _),
        datadoc_location(Md5, Location),
	number_of_triples(Md5res, Triples)
      ),
      Pairs
    )
  },
  rdf_html_table_pairs(Pairs, [summation_row(true)]).

cleaning_datadocs -->
  {
    aggregate_all(
      set(Started-[Location-Md5,Added,Started]),
      (
        rdf(Md5res, lwm:start, Started),
        \+ rdf(Md5res, lwm:end, _),
        rdf(Md5res, lwm:added, Added),
        rdf_string(Md5res, lwm:md5, Md5, _),
        datadoc_location(Md5, Location)
      ),
      Pairs
    )
  },
  rdf_html_table_pairs(Pairs, []).

pending_datadocs -->
  {
    aggregate_all(
      set(Added-[Location-Md5,Added]),
      (
        rdf(Md5res, lwm:added, Added),
        \+ rdf(Md5res, lwm:start, _),
        rdf_string(Md5res, lwm:md5, Md5, _),
        datadoc_location(Md5, Location)
      ),
      Pairs
    )
  },
  rdf_html_table_pairs(Pairs, []).


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



% Legacy: SPARQL

sources -->
  {
    once(lwm_endpoint(Endpoint)),
    lwm_sparql_select(Endpoint, [lwm], [md5,triples,added,start,end],
        [rdf(var(md5res),lwm:added,var(added)),
         optional([rdf(var(md5res),lwm:end,var(end))]),
         optional([rdf(var(md5res),lwm:start,var(start))]),
         rdf(var(md5res),lwm:md5,var(md5)),
         rdf(var(md5res),lwm:triples,var(triples))],
        Rows1, [distinct(true),limit(250)]),
    findall(
      [Location-Md5|T],
      (
        member([Md5Literal|T], Rows1),
        rdf_literal(Md5Literal, Md5, _),
        datadoc_location(Md5, Location)
      ),
      Rows2
    )
  },
  rdf_html_table(
    [header_column(true),header_row(true),indexed(true)],
    html('Overview of LOD sources.'),
    [['Source','Time added','Time started','Time ended']|Rows2]
  ).



% Helpers

datadoc_location(Md5, Location):-
  atomic_list_concat([Md5,'clean.nt.gz'], '/', Path),
  http_link_to_id(clean, path_postfix(Path), Location).


%! number_of_triples(+Md5res:iri, -Triples:nonneg) is det.

number_of_triples(Md5res, Triples):-
  rdf_datatype(Md5res, lwm:triples, Triples, xsd:integer, _), !.
number_of_triples(_, 0).
