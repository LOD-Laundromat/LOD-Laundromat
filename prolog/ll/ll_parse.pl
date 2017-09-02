:- module(ll_parse, [ll_parse/0]).

/** <module> LOD Laundromat: Parse RDF

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(ll/ll_seedlist)).



ll_parse :-
  with_mutex(ll_parse, (
    seed(Seed),
    Hash{format: Format, status: guessed} :< Seed,
    seed_merge(Hash{status: parsing})
  )),
  ll_download1(Hash, Uri, RdfMeta),
  with_mutex(ll_parse, seed_merge(Hash{rdf: RdfMeta, status: parsed})).
