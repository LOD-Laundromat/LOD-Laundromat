:- module(ll_analysis, [ll_analysis/0]).

/** <module> LOD Laundromat: Analysis

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(ll/ll_seedlist)).
:- use_module(library(stream_ext)).



ll_analysis :-
  seed(Seed),
  forall(ll_analysis1(Seed), true).

% HTTP `Content-Type' header
ll_analysis1(Seed) :-
  Hash{content: ContentMeta, http: HttpMeta} :< Seed,
  HttpMeta = [HttpDict|_],
  ignore(metadata_content_type([HttpDict], MediaType)),
  (   var(MediaType)
  ->  (   ContentMeta.number_of_bytes == 0
      ->  true
      ;   print_message(warning, no_content_type_yet_nonempty_body(Hash))
      )
  ;   true
  ).
