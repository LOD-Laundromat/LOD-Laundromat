:- module(ll_parse, [ll_parse/0]).

/** <module> LOD Laundromat: Parse RDF

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(apply)).
:- use_module(library(hash_stream)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_seedlist)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(semweb/rdf_ntriples)).
:- use_module(library(semweb/rdfa)).
:- use_module(library(semweb/turtle)).
:- use_module(library(zlib)).

:- rdf_register_prefix(bnode, 'https://lodlaundromat.org/.well-known/genid/').



ll_parse :-
  with_mutex(ll_parse, (
    seed(Seed),
    Hash1{format: Format, status: guessed} :< Seed,
    seed_merge(Hash1{status: parsing})
  )),
  seed_base_uri(Seed, BaseUri),
  hash_file(Hash1, 'clean.nq.gz', File1),
  setup_call_cleanup(
    gzopen(File1, write, Out, [format(gzip)]),
    ll_parse1(Hash1, Format, BaseUri, Out, ContentMeta),
    close(Out)
  ),
  content{hash: Hash2} :< ContentMeta,
  hash_file(Hash2, 'clean.nq.gz', File2),
  rename_file(File1, File2),
  seed_create(Hash2{content: ContentMeta, dirty: Hash1, status: cleaned}),
  with_mutex(ll_parse, seed_merge(Hash1{status: parsed})).

ll_parse1(Hash1, Format, BaseUri, Out1, ContentMeta) :-
  setup_call_cleanup(
    open_hash_stream(Out1, Out2, [algorithm(md5),close_parent(false)]),
    (
      ll_parse2(Hash1, Format, BaseUri, Out2),
      stream_meta(Out2, ContentMeta)
    ),
    close(Out2)
  ).

ll_parse2(Hash, Format, BaseUri, Out) :-
  hash_file(Hash, dirty, File),
  rdf_global_id(bnode:Hash, BNodePrefix),
  setup_call_cleanup(
    open(File, read, In),
    ll_parse3(Format, In, Out, BNodePrefix, BaseUri),
    close(In)
  ).

% N-Quads
ll_parse3(nquads, In, Out, BNodePrefix, BaseUri) :-
  rdf_process_ntriples(
    In,
    ll_generate_tuples(Out),
    [anon_prefix(BNodePrefix),base_uri(BaseUri),format(nquads)]
  ).
% N-Triples
ll_parse3(ntriples, In, Out, BNodePrefix, BaseUri) :-
  rdf_process_ntriples(
    In,
    ll_generate_triples(Out),
    [anon_prefix(BNodePrefix),base_uri(BaseUri),format(ntriples)]
  ).
% RDFa
ll_parse3(rdfa, In, Out, BNodePrefix, BaseUri) :-
  read_rdfa(In, Triples, [anon_prefix(BNodePrefix),base(BaseUri)]),
  ll_generate_triples(Out, Triples, _).
% RDF/XML
ll_parse3(rdfxml, In, Out, _BNodePrefix, BaseUri) :-
  process_rdf(
    In,
    ll_generate_triples(Out),
    [base_uri(BaseUri),blank_nodes(noshare)]
  ).
% TriG
ll_parse3(trig, In, Out, BNodePrefix, BaseUri) :-
  rdf_process_turtle(
    In,
    ll_generate_tuples(Out),
    [anon_prefix(BNodePrefix),base_uri(BaseUri),format(trig),resources(iri)]
  ).
% Turtle
ll_parse3(turtle, In, Out, BNodePrefix, BaseUri) :-
  rdf_process_turtle(
    In,
    ll_generate_triples(Out),
    [anon_prefix(BNodePrefix),base_uri(BaseUri),format(turtle),resources(iri)]
  ).

ll_generate_triples(Out, Triples, _) :-
  maplist(write_triple(Out), Triples).

ll_generate_tuples(Out, Tuples, _) :-
  maplist(write_tuple(Out), Tuples).

write_iri(Out, Iri) :-
  turtle:turtle_write_uri(Out, Iri).

write_literal(Out, literal(type(D,Lex))) :- !,
  turtle:turtle_write_quoted_string(Out, Lex),
  write(Out, '^^'),
  write_iri(Out, D).
write_literal(Out, literal(lang(LTag,Lex))) :- !,
  turtle:turtle_write_quoted_string(Out, Lex),
  write(Out, '@'),
  write(Out, LTag).
write_literal(Out, literal(Lex)) :-
  rdf_equal(xsd:string, D),
  write_literal(Out, literal(type(D,Lex))).

write_quad(Out, rdf(S,P,O,G)) :-
  write_triple_(Out, rdf(S,P,O)),
  write_iri(Out, G),
  write(Out, ' .\n').

write_object(Out, Iri) :-
  atom(Iri), !,
  write_iri(Out, Iri).
write_object(Out, Lit) :-
  write_literal(Out, Lit).

write_triple(Out, Triple) :-
  write_triple_(Out, Triple),
  write(Out, '.\n').

write_triple_(Out, rdf(S,P,O)) :-
  write_iri(Out, S),
  write(Out, ' '),
  write_iri(Out, P),
  write(Out, ' '),
  write_object(Out, O),
  write(Out, ' ').

write_tuple(Out, rdf(S,P,O)) :-
  write_triple(Out, rdf(S,P,O)), !.
write_tuple(Out, Quad) :-
  write_quad(Out, Quad), !.
