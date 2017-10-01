:- module(ll_parse, [ll_parse/0]).

/** <module> LOD Laundromat: Parse RDF

@author Wouter Beek
@version 2017/09-2017/10
*/

:- use_module(library(apply)).
:- use_module(library(dict_ext)).
:- use_module(library(hash_stream)).
:- use_module(library(lists)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_seedlist)).
:- use_module(library(semweb/rdf_api)).
:- use_module(library(semweb/rdf_export)).
:- use_module(library(semweb/rdf_ntriples)).
:- use_module(library(semweb/rdfa)).
:- use_module(library(semweb/turtle)).
:- use_module(library(zlib)).

:- rdf_register_prefix(
     '_',
     'https://lodlaundromat.org/.well-known/genid/',
     [force(true)]
   ).





ll_parse :-
  with_mutex(ll_parse, (
    seed(Seed),
    Hash1{format: Format, status: guessed} :< Seed,
    seed_merge(Hash1{status: parsing})
  )),
  debug(ll(parse), "┌─> parsing ~a (~a)", [Format,Hash1]),
  get_time(Begin),
  seed_base_uri(Seed, BaseUri),
  hash_file(Hash1, 'clean.nq.gz', File1),
  setup_call_cleanup(
    gzopen(File1, write, Out, [format(gzip)]),
    ll_parse1(Hash1, Format, BaseUri, Out, Meta),
    close(Out)
  ),
  % The tag of this dictionary is the content-based hash.
  dict_tag(Meta, Hash2),
  hash_file(Hash2, 'clean.nq.gz', File2),
  rename_file(File1, File2),
  seed_merge(Hash1{clean: Hash2, status: parsed}),
  get_time(End),
  debug(ll(parse), "└─< parsed ~a", [Format]),
  Dict1 = Hash2{dirty: Hash1, status: generated, timestamp: Begin-End},
  merge_dicts(Dict1, Meta, Dict2),
  seed_store(Dict2).

ll_parse1(Hash1, Format, BaseUri, Out1, Meta2) :-
  setup_call_cleanup(
    open_hash_stream(Out1, Out2, [algorithm(md5),close_parent(false)]),
    (
      ll_parse2(Hash1, Format, BaseUri, Counter, Out2),
      stream_meta(Out2, Meta1)
    ),
    close(Out2)
  ),
  arg(1, Counter, NumberOfTriples),
  arg(2, Counter, NumberOfQuads),
  merge_dicts(
    Meta1,
    _{number_of_quads: NumberOfQuads, number_of_triples: NumberOfTriples},
    Meta2
  ).

ll_parse2(Hash, Format, BaseUri, Counter, Out) :-
  hash_file(Hash, dirty, File),
  rdf_global_id('_':Hash, BNodePrefix),
  Counter = counter(0,0),
  setup_call_cleanup(
    open(File, read, In),
    ll_parse3(Format, In, Out, BNodePrefix, BaseUri, Counter),
    close(In)
  ).

% N-Quads
ll_parse3(nquads, In, Out, BNodePrefix, BaseUri, Counter) :-
  rdf_process_ntriples(
    In,
    ll_generate_tuples(Out, Counter),
    [anon_prefix(BNodePrefix),base_uri(BaseUri),format(nquads)]
  ).
% N-Triples
ll_parse3(ntriples, In, Out, BNodePrefix, BaseUri, Counter) :-
  rdf_process_ntriples(
    In,
    ll_generate_triples(Out, Counter),
    [anon_prefix(BNodePrefix),base_uri(BaseUri),format(ntriples)]
  ).
% RDFa
ll_parse3(rdfa, In, Out, BNodePrefix, BaseUri, Counter) :-
  read_rdfa(In, Triples, [anon_prefix(BNodePrefix),base(BaseUri)]),
  ll_generate_triples(Out, Counter, Triples, _).
% RDF/XML
ll_parse3(rdfxml, In, Out, _BNodePrefix, BaseUri, Counter) :-
  process_rdf(
    In,
    ll_generate_triples(Out, Counter),
    [base_uri(BaseUri),blank_nodes(noshare)]
  ).
% TriG
ll_parse3(trig, In, Out, BNodePrefix, BaseUri, Counter) :-
  rdf_process_turtle(
    In,
    ll_generate_tuples(Out, Counter),
    [anon_prefix(BNodePrefix),base_uri(BaseUri),format(trig),resources(iri)]
  ).
% Turtle
ll_parse3(turtle, In, Out, BNodePrefix, BaseUri, Counter) :-
  rdf_process_turtle(
    In,
    ll_generate_triples(Out, Counter),
    [anon_prefix(BNodePrefix),base_uri(BaseUri),format(turtle),resources(iri)]
  ).

ll_generate_triple(Out, Counter, rdf(S,P,O)) :-
  arg(1, Counter, N1),
  N2 is N1 + 1,
  nb_setarg(1, Counter, N2),
  rdf_write_triple(Out, S, P, O).

ll_generate_triples(Out, Counter, Triples, _) :-
  maplist(ll_generate_triple(Out, Counter), Triples).

ll_generate_tuple(Out, Counter, rdf(S,P,O)) :- !,
  ll_generate_triple(Out, Counter, rdf(S,P,O)).
ll_generate_tuple(Out, Counter, rdf(S,P,O,G)) :-
  arg(2, Counter, N1),
  N2 is N1 + 1,
  nb_setarg(2, Counter, N2),
  rdf_write_quad(Out, S, P, O, G).

ll_generate_tuples(Out, Counter, Tuples, _) :-
  maplist(ll_generate_tuple(Out, Counter), Tuples).
