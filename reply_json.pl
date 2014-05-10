:- module(
  reply_json,
  [
    reply_json_dict/2 % +Dict:dict
                      % +Options:list(nvpair)
  ]
).

/** <module> Reply JSON

@author Wouter Beek
@version 2014/05
*/

:- use_module(library(http/http_json)). % Private predicate.
:- use_module(library(option)).



%! reply_json_dict(+Dict:dict, +Options:list(nvpair)) is det.
% Formulate a JSON HTTP reply.
% See json_write/2 for details.
% The processed options are listed below.
% Remaining options are forwarded to json_write/3.
%
%      * cache(+Seconds:nonneg)
%
%      * content_type(+Type)
%      The default =|Content-type|= is =|application/json;
%      charset=UTF8|=. =|charset=UTF8|= should not be required
%      because JSON is defined to be UTF-8 encoded, but some
%      clients insist on it.
%
%      * cors(+Enabled:boolean)
%
%      * json_object(+As)
%      One of =term= (classical json representation) or =dict=
%      to use the new dict representation.   If omitted and Term
%      is a dict, =dict= is assumed.  SWI-Prolog Version 7.
%
%      * status(+Code)
%      The default status is 200.  REST API functions may use
%      other values from the 2XX range, such as 201 (created).

reply_json_dict(Dict, O1) :-
  is_dict(Dict), !,
  merge_options([json_object(dict)], O1, O2),

  % HTTP status.
  (
    select_option(status(Code), O2, O3)
  ->
    format('Status: ~d~n', [Code])
  ;
    O3 = O2
  ),

  % CORS.
  (
    select_option(cors(true), O3, O4)
  ->
    format('Access-Control-Allow-Origin: *~n')
  ;
    O4 = O3
  ),
  
  % Client-side caching.
  (
    select_option(cache(Seconds), O4, O5)
  ->
    format('Cache-Control: max-age=~d~n', [Seconds])
  ;
    O5 = O4
  ),
  
  % HTTP content type.
  select_option(content_type(Type), O5, O6, 'application/json'),
  format('Content-type: ~w~n~n', [Type]),

  http_json:json_write_to(current_output, Dict, O6).

