:- module(
  ll_analysis,
  [
    ll_analysis/1 % ?Hash
  ]
).

/** <module> LOD Laundromat: Analysis

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(ll/ll_seedlist)).
:- use_module(library(stream_ext)).



ll_analysis(Hash) :-
  rocks(seedlist, Hash, SeedDict),
  ll_analysis1(SeedDict).

ll_analysis1(SeedDict) :-
  seed{content: ContentMeta, http: HttpMeta} :< SeedDict,
  HttpMeta = [HttpDict|_],
  ignore(metadata_content_type([HttpDict], MediaType)),
  (var(MediaType) -> assertion(ContentMeta.number_of_bytes == 0) ; true).
