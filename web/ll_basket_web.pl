:- module(
  ll_basket_web,
  [
    lwm_basket/1 % +Request:list(nvpair)
  ]
).

/** <module> LOD Laundromat: basket web

Web-based front-end to the LOD basket.

@author Wouter Beek
@version 2014/06, 2014/08
*/

:- use_module(library(http/http_cors)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_parameters)).

:- use_module(generics(typecheck)).

:- use_module(ll_web(ll_basket)).



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

