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
:- use_module(library(http/http_session)). % HTTP session support.
:- use_module(library(semweb/rdf_db)).

:- use_module(generics(typecheck)).

:- use_module(plHtml(html_pl_term)).

:- use_module(plRdf_term(rdf_literal)).

:- use_module(plTabular(rdf_html_table_pairs)).

:- use_module(lwm(lod_basket)).
:- use_module(lwm(lwm_generics)).

:- dynamic(url_md5_translation/2).

:- http_handler(
  cliopatria(data),
  serve_files_in_directory_with_cors(data),
  [id(clean),prefix]
).



serve_files_in_directory_with_cors(Alias, Request):-
  cors_enable,
  serve_files_in_directory(Alias, Request).


lwm_basket(Request):-
  cors_enable,
  (
    catch(http_parameters(Request, [url(Url, [])]), _, fail),
    is_url(Url)
  ->
    % Make sure that it is a URL.
    add_to_basket(Url),

    % HTTP status code 202 Accepted: The request has been accepted
    % for processing, but the processing has not been completed.
    reply_json(json{}, [status(202)])
  ;
    % HTTP status code 400 Bad Request: The request could not
    % be understood by the server due to malformed syntax.
    reply_json(json{}, [status(400)])
  ).


% Response to requesting a JSON description of all LOD URL.
lwm(_, HtmlStyle):-
  lwm_default_graph(Graph),
  reply_html_page(
    HtmlStyle,
    title('LOD Laundry'),
    html([
      \pending(Graph),
      \unpacking(Graph),
      \unpacked(Graph),
      \cleaning(Graph),
      \cleaned(Graph)
    ])
  ).

pending(Graph) -->
  {
    findall(
      Added-[Datadoc,Added],
      (
        rdf(Datadoc, ll:added, Added, Graph),
        \+ rdf(Datadoc, ll:start_unpack, _, Graph)
      ),
      Pairs
    ),
    length(Pairs, Length)
  },
  rdf_html_table_pairs(
    ['Data document','Added'],
    Pairs,
    html([\html_pl_term(lwm,Length),' pending data documents.']),
    [
      header_column(true),
      header_row(true),
      indexed(true),
      maximum_number_of_rows(10)
    ]
  ).

unpacking(Graph) -->
  {
    findall(
      StartUnpack2-[Datadoc,StartUnpack1],
      (
        rdf(Datadoc, ll:start_unpack, StartUnpack1, Graph),
        \+ rdf(Datadoc, ll:end_unpack, _, Graph),
        rdf_literal(StartUnpack1, StartUnpack2, _)
      ),
      Pairs
    ),
    length(Pairs, Length)
  },
  rdf_html_table_pairs(
    ['Data document','Unpacking start'],
    Pairs,
    html([\html_pl_term(lwm,Length),' data documents are being unpacked.']),
    [
      header_column(true),
      header_row(true),
      indexed(true),
      maximum_number_of_rows(10)
    ]
  ).

unpacked(Graph) -->
  {
    findall(
      EndUnpack-[Datadoc,EndUnpack],
      (
        rdf(Datadoc, ll:end_unpack, EndUnpack, Graph),
        \+ rdf(Datadoc, ll:start_clean, _, Graph)
      ),
      Pairs
    ),
    length(Pairs, Length)
  },
  rdf_html_table_pairs(
    ['Data document','Unpacking end'],
    Pairs,
    html([\html_pl_term(lwm,Length),' unpacked data documents.']),
    [
      header_column(true),
      header_row(true),
      indexed(true),
      maximum_number_of_rows(10)
    ]
  ).

cleaning(Graph) -->
  {
    findall(
      StartClean-[Datadoc,StartClean],
      (
        rdf(Datadoc, ll:start_clean, StartClean, Graph),
        \+ rdf(Datadoc, ll:end_clean, _, Graph)
      ),
      Pairs
    ),
    length(Pairs, Length)
  },
  rdf_html_table_pairs(
    ['Data document','Cleaning start'],
    Pairs,
    html([\html_pl_term(lwm,Length),' data documents are being cleaned.']),
    [
      header_column(true),
      header_row(true),
      indexed(true),
      maximum_number_of_rows(10)
    ]
  ).

cleaned(Graph) -->
  {
    findall(
      EndClean-[Datadoc,EndClean],
      (
        rdf(Datadoc, ll:end_clean, EndClean, Graph)
      ),
      Pairs
    ),
    length(Pairs, Length)
  },
  rdf_html_table_pairs(
    ['Data document','Cleaning end'],
    Pairs,
    html([\html_pl_term(lwm,Length),' cleaned data documents.']),
    [
      header_column(true),
      header_row(true),
      indexed(true),
      maximum_number_of_rows(10)
    ]
  ).

