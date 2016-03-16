:- module(
  laundromat_hdt,
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

:- rdf_meta
   hdt_build(r),
   hdt_read(r, r, o, r).





%! hdt_build(+Doc) is det.

hdt_build(Doc) :-
  var(Doc), !,
  instantiation_error(Doc).
hdt_build(Doc) :-
  ldoc_hdt_file(Doc, _), !,
  msg_notification("HDT file for ~a already exists.", [Doc]).
hdt_build(Doc) :-
  ldoc_data_file(Doc, NQuadsFile), !,
  access_file(NQuadsFile, read),
  ldir_ldoc(Dir, Doc),
  rdf_has(Doc, llo:base_iri, BaseIri^^xsd:anyURI),
  directory_file_path(Dir, 'data.hdt', HdtFile),
  setup_call_cleanup(
    ensure_ntriples(Dir, NQuadsFile, NTriplesFile),
    hdt_create_from_file(HdtFile, NTriplesFile, [base_uri(BaseIri)]),
    delete_file(NTriplesFile)
  ).
hdt_build(Doc) :-
  msg_warning("Data file for ~a is missing.", [Doc]).



%! hdt_read(?S, ?P, ?O, +Doc) is nondet.

hdt_read(S, P, O, Doc) :-
  ldoc_hdt_file(Doc, File),
  access_file(File, read),
  setup_call_cleanup(
    hdt_open(Hdt, File),
    hdt_search(Hdt, S, P, O),
    hdt_close(Hdt)
  ).



%! ensure_ntriples(+Dir, +From, -To) is det.

ensure_ntriples(Dir, From, To) :-
  directory_file_path(Dir, 'data.nt', To),
  setup_call_cleanup(
    open(To, write, Sink),
    rdf_call_on_tuples(From, write_ntriples0(Sink)),
    close(Sink)
  ).

write_ntriples0(Sink, Tuples, _) :-
  maplist(write_ntriple0(Sink), Tuples).

write_ntriple0(Sink, rdf(S,P,O)) :- !,
  with_output_to(Sink, write_simple_triple(S, P, O)).
write_ntriple0(Sink, rdf(S,P,O,_)) :-
  with_output_to(Sink, write_simple_triple(S, P, O)).
