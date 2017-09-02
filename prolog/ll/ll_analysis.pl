:- module(ll_analysis, [ll_analysis/0]).

/** <module> LOD Laundromat: Analysis

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(dcg/dcg_ext)).
:- use_module(library(error)).
:- use_module(library(http/rfc7231)).
:- use_module(library(ll/ll_seedlist)).



ll_analysis :-
  seed(Seed),
  forall(ll_analysis1(Seed), true).

% HTTP `Content-Type' header
ll_analysis1(Seed) :-
  Hash{content: ContentMeta, http: HttpMeta} :< Seed,
  HttpMeta = [HttpDict|_],
  (   get_dict('content-type', HttpDict.headers, [ContentType|_])
  ->  (   atom_phrase('content-type'(_), ContentType)
      ->  true
      ;   type_error(media_type, ContentType)
      )
  ;   (   ContentMeta.number_of_bytes == 0
      ->  true
      ;   print_message(warning, no_content_type_yet_nonempty_body(Hash))
      )
  ).
