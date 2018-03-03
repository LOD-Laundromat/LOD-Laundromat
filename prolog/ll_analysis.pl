:- module(ll_analysis, [ll_analysis/0]).

/** <module> LOD Laundromat: Analysis

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(dict)).
:- use_module(library(error)).
:- use_module(library(http/http_header)).

:- use_module(ll_seedlist).



ll_analysis :-
  seed(Seed),
  forall(ll_analysis1(Seed), true).

% HTTP `Content-Type' header
ll_analysis1(Seed) :-
  Hash{content: ContentMeta, http: HttpMeta} :< Seed,
  HttpMeta = [HttpDict|_],
  (   get_dict('content-type', HttpDict.headers, [ContentType|_])
  ->  (   http_parse_header_value(content_type, ContentType, _MediaType)
      ->  true
      ;   print_message(warning, http_content_type(ContentType))
      )
  ;   % If there is no `Content-Type' header, the `Content-Length' --
      % if present -- must be 0.
      (   dict_get('content-length', HttpDict.headers, ContentLength)
      ->  (   atom_number(ContentLength, Length),
              Length =:= 0
          ->  true
          ;   print_message(
                warning,
                no_content_type_yet_non_zero_content_length(ContentLength)
              )
          )
      ;   true
      ),
      % If there is no `Content-Type' header, the stream _must_ be
      % empty.
      (   ContentMeta.number_of_bytes == 0
      ->  true
      ;   print_message(warning, no_content_type_yet_nonempty_body(Hash))
      )
  ).
