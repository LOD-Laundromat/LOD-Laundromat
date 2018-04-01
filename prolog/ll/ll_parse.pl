:- module(ll_parse, [ll_parse/0]).

/** <module> LOD Laundromat: Parsing

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).
:- use_module(library(settings)).
:- use_module(library(zlib)).

:- use_module(library(debug_ext)).
:- use_module(library(dict)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(sw/rdf_clean)).
:- use_module(library(sw/rdf_deref)).
:- use_module(library(sw/rdf_export)).
:- use_module(library(sw/rdf_guess)).
:- use_module(library(sw/rdf_term)).
:- use_module(library(uri_ext)).

:- initialization
   set_setting(rdf_term:bnode_prefix_scheme, http),
   set_setting(rdf_term:bnode_prefix_authority, 'lodlaundromat.org').





ll_parse :-
  % precondition
  with_mutex(ll_parse, (
    find_hash_file(recoded, Hash, File),
    delete_file(File)
  )),
  gtrace,
  debug(ll(parse), "┌─> parsing ~a", [Hash]),
  write_meta_now(Hash, parseBegin),
  % operation
  catch(parse_file(Hash), E, true),
  % postcondition
  write_meta_now(Hash, parseEnd),
  failure_success(Hash, parsed, _, E),
  debug(ll(parse), "└─< parsed ~a", [Hash]).

parse_file(Hash) :-
  maplist(hash_file(Hash), ['dirty.gz','clean.nq.gz'], [File1,File2]),
  ignore(rdf_guess_file(File1, 10 000, MediaType)),
  format(atom(Lex), "~w", [MediaType]),
  write_meta_triple(Hash, serializationFormat, literal(type(xsd:string,Lex))),
  bnode_prefix([Hash], BNodePrefix),
  RdfMeta = _{number_of_quadruples: 0, number_of_triples: 0},
  uri_file_name(BaseUri, File1),
  setup_call_cleanup(
    maplist(gzopen, [File1,File2], [read,write], [In,Out]),
    rdf_deref_stream(
      BaseUri,
      In,
      clean_tuples(RdfMeta, Out),
      [bnode_prefix(BNodePrefix),media_type(MediaType)]
    ),
    maplist(close_metadata, [In,Out], [ReadMeta,WriteMeta])
  ),
  write_meta_stream(Hash, parseRead, ReadMeta),
  write_meta_stream(Hash, parseWritten, WriteMeta),
  write_meta_triple(Hash, numberOfQuadruples, RdfMeta.number_of_quadruples),
  write_meta_triple(Hash, numberOfTriples, RdfMeta.number_of_triples).

bnode_prefix(Segments, BNodePrefix) :-
  setting(rdf_term:bnode_prefix_scheme, Scheme),
  setting(rdf_term:bnode_prefix_authority, Auth),
  uri_comps(BNodePrefix, uri(Scheme,Auth,['.well-known',genid|Segments],_,_)).

clean_tuples(Meta, Out, BNodePrefix, Tuples, _) :-
  maplist(clean_tuple(Meta, Out, BNodePrefix), Tuples).

clean_tuple(Meta, Out, BNodePrefix, rdf(S,P,O)) :- !,
  nb_increment_dict(Meta, number_of_triples),
  clean_triple_(Out, BNodePrefix, rdf(S,P,O)).
clean_tuple(Meta, Out, BNodePrefix, rdf(S,P,O,_)) :-
  nb_increment_dict(Meta, number_of_quadruples),
  clean_triple_(Out, BNodePrefix, rdf(S,P,O)).

clean_triple_(Out, BNodePrefix, rdf(S0,P0,O0)) :-
  (   rdf_clean_triple(BNodePrefix, rdf(S0,P0,O0), rdf(S,P,O))
  ->  rdf_write_triple(Out, BNodePrefix, S, P, O)
  ;   true
  ).
