:- module(
  hdt_build,
  [
    hdt_build/1, % +Doc
    hdt_read/4   % ?S, ?P, ?O, +Doc
  ]
).

/** <module> HDT build

@author Wouter Beek
@version 2016/03
*/

:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(hdt)).
:- use_module(library(msg_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(simple/write_SimpleRDF)).

:- use_module(cpack('LOD-Laundromat'/laundromat_fs)).





%! hdt_build(+Doc) is det.

hdt_build(Doc) :-
  var(Doc), !,
  instantiation_error(Doc).
hdt_build(Doc) :-
  ldoc_hdt_file(Doc, _), !,
  msg_notification("HDT file for ~a already exists.", [Doc]).
hdt_build(Doc) :-
  ldoc_data_file(Doc, RdfFile1),
  access_file(read, RdfFile1),
  ldir_ldoc(Dir, Doc),
  directory_file_path(Dir, 'data.nt', RdfFile2),
  gzipped_nquads_to_ntriples(RdfFile1, RdfFile2),
  rdf_has(Doc, llo:base_iri, BaseIri^^xsd:anyURI),
  directory_file_path(Dir, 'data.hdt', HdtFile),
  hdt_create_from_file(HdtFile, RdfFile2, [base_uri(BaseIri)]).
hdt_build(Doc) :-
  msg_warning("N-Triples file for ~a is missing.", [Doc]).



%! hdt_read(?S, ?P, ?O, +Doc) is nondet.

hdt_read(S, P, O, Doc) :-
  ldoc_hdt_file(Doc, File),
  access_file(read, File),
  setup_call_cleanup(
    hdt_open(Hdt, File),
    hdt_search(Hdt, S, P, O),
    hdt_close(Hdt)
  ).



%! gzipped_nquads_to_ntriples(+From, +To) is det.

gzipped_nquads_to_ntriples(From, To) :-
  rdf_call_on_tuples(From, write_ntriples0(To)).

write_ntriples0(To, Tuples, _) :-
  maplist(write_ntriple0(To), Tuples).

write_ntriple0(To, rdf(S,P,O)) :-
  with_output_to(To, write_simple_triple(S, P, O)).
