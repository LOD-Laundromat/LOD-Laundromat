:- module(ll_index, [ll_index/0]).

/** <module> LOD Laundromat: Parse RDF

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(hdt_term)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_seedlist)).
:- use_module(library(semweb/rdf_api)).

:- rdf_register_prefix(base, 'https://lodlaundromat.org/header/').



ll_index :-
  with_mutex(ll_index, (
    seed(Seed),
    Hash{status: parsed} :< Seed,
    seed_merge(Hash{status: indexing})
  )),
  hash_file(Hash, 'clean.nq.gz', RdfFile),
  rdf_global_id(base:Hash, BaseUri),
  hdt_create(RdfFile, _, [base_uri(BaseUri)]),
  with_mutex(ll_index, seed_merge(Hash{status: indexed})).
