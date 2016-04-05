:- module(
  lhdt,
  [
    lhdt/3,             % ?S, ?P, ?O
    lhdt/4,             % ?S, ?P, ?O, ?File
    lhdt_build/1,       %             +File
    lhdt_build/2,       %             +Hash, +Name
    lhdt_cost/5,        % ?S, ?P, ?O, +File, -Cost
    lhdt_header/4,      % ?S, ?P, ?O, ?File
    lhdt_pagination//1, %             +Hash
    lhdt_pagination//2, %             +Hash, +Opts
    lhdt_pagination//5, % ?S, ?P, ?O, +Hash, +Opts
    lhdt_print/4,       % ?S, ?P, ?O, +File
    lhdt_print/5        % ?S, ?P, ?O, +File, +Opts
  ]
).

/** <module> LOD Laundromat HDT

@author Wouter Beek
@version 2016/03-2016/04
*/

:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(gen/gen_ntuples)).
:- use_module(library(hdt)).
:- use_module(library(html/html_ext)).
:- use_module(library(html/rdfh)).
:- use_module(library(pagination)).
:- use_module(library(print_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(rdf/rdf_print)).
:- use_module(library(semweb/rdf11)).

:- use_module(cpack('LOD-Laundromat'/lfs)).

:- meta_predicate
    lhdt_setup_call_cleanup(+, 1).

:- rdf_meta
   lhdt(r, r, o),
   lhdt(r, r, o, r),
   lhdt_cost(r, r, o, r, -),
   lhdt_header(r, r, o, r),
   lhdt_pagination(r, r, o, +, +, ?, ?),
   lhdt_print(r, r, o, r).





%! lhdt(?S, ?P, ?O) is nondet.
%! lhdt(?S, ?P, ?O, ?File) is nondet.

lhdt(S, P, O) :-
  lhdt(S, P, O, _).


lhdt(S, P, O, File) :-
  lhdt_setup_call_cleanup(File, hdt_search0(S, P, O)).
hdt_search0(S, P, O, Hdt) :- hdt_search(Hdt, S, P, O).



%! lhdt_build(+File) is det.
%! lhdt_build(+Hash, +Name) is det.

lhdt_build(File) :-
  lfile_lhash(File, Name, _, Hash),
  lhdt_build(Hash, Name).

lhdt_build(Hash, meta) :- !,
  lhdt_build(Hash, meta, _).
lhdt_build(Hash, Name) :-
  % Make sure the metadata is converted to HDT.
  lhdt_build(Hash, meta),
  lfile_lhash(File, meta, hdt, Hash),
  once(lhdt(_, llo:base_iri, BaseIri^^xsd:anyURI, File)),
  lhdt_build(Hash, Name, BaseIri).

lhdt_build(Hash, Name, BaseIri) :-
  lfile_lhash(HdtFile, Name, hdt, Hash),
  (var(BaseIri) -> Opts = [] ; Opts = [base_uri(BaseIri)]),
  (   % HDT file exits.
      exists_file(HdtFile)
  ->  msg_notification("File ~a already exists.~n", [HdtFile])
  ;   % N-Triples files exists.
      lfile_lhash(NTriplesFile, Name, ntriples, Hash),
      access_file(NTriplesFile, read)
  ->  hdt_create_from_file(HdtFile, NTriplesFile, Opts)
  ;   % N-Quads file exists.
      lfile_lhash(NQuadsFile, Name, nquads, Hash),
      access_file(NQuadsFile, read)
  ->  ldir_lhash(Dir, Hash),
      setup_call_cleanup(
        ensure_ntriples(Dir, NQuadsFile, NTriplesFile),
        hdt_create_from_file(HdtFile, NTriplesFile, Opts),
        delete_file(NTriplesFile)
      )
  ;   existence_error(lfile(Name), Hash)
  ).



%! lhdt_cost(?S, ?P, ?O, +File, -Cost) is det.

lhdt_cost(S, P, O, File, Cost) :-
  lhdt_setup_call_cleanup(File, hdt_search_cost0(S, P, O, Cost)).
hdt_search_cost0(S, P, O, Cost, Hdt) :- hdt_search_cost(Hdt, S, P, O, Cost).



%! lhdt_header(?S, ?P, ?O) is nondet.
%! lhdt_header(?S, ?P, ?O, ?File) is nondet.

lhdt_header(S, P, O) :-
  lhdt_header(S, P, O, _).


lhdt_header(S, P, O, File) :-
  lhdt_setup_call_cleanup(File, hdt_header0(S, P, O)).
hdt_header0(S, P, O, Hdt) :- hdt_header(Hdt, S, P, O).



%! lhdt_pagination(+Hash)// is det.
%! lhdt_pagination(+Hash, +Opts)// is det.
%! lhdt_pagination(?S, ?P, ?O, +Hash, +Opts)// is det.

lhdt_pagination(Hash) -->
  lhdt_pagination(Hash, _{}).


lhdt_pagination(Hash, Opts) -->
  {
    ignore(get_dict(subject, Opts, S)),
    ignore(get_dict(predicate, Opts, P)),
    ignore(get_dict(object, Opts, O))
  },
  lhdt_pagination(S, P, O, Hash, Opts).


lhdt_pagination(S, P, O, Hash, Opts) -->
  {
    lfile_lhash(File, data, hdt, Hash),
    pagination(rdf(S,P,O), lhdt(S, P, O, File), Opts, Result)
  },
  pagination_result(Result, rdfh_triple_table(_{query: [hash=Hash]}), Opts).



%! lhdt_print(?S, ?P, ?O, +File) is nondet.
%! lhdt_print(?S, ?P, ?O, +File, +Opts) is nondet.

lhdt_print(S, P, O, File) :-
  lhdt_print(S, P, O, File, _{}).


lhdt_print(S, P, O, File, Opts) :-
  pagination(rdf(S,P,O), lhdt(S, P, O, File), Opts, Result),
  rdf_print_triples(Result.results, Opts).




% HELPERS %

%! ensure_ntriples(+Dir, +From, -To) is det.

ensure_ntriples(Dir, From, To) :-
  directory_file_path(Dir, 'data.nt', To),
  setup_call_cleanup(
    open(To, write, Sink),
    with_output_to(Sink, rdf_call_on_tuples(From, gen_nquad)),
    close(Sink)
  ).



%! lhdt_setup_call_cleanup(+File, :Goal_1) is det.
%! lhdt_setup_call_cleanup(-File, :Goal_1) is nondet.

lhdt_setup_call_cleanup(File, Goal_1) :-
  (   exists_file(File)
  ->  access_file(File, read),
      setup_call_cleanup(
        hdt_open(Hdt, File),
        call(Goal_1, Hdt),
        hdt_close(Hdt)
      )
  ;   lhdt_build(File),
      lhdt_setup_call_cleanup(File, Goal_1)
  ).



%! lhdt_triples(+File, -NumTriples) is det.

lhdt_triples(File, N) :-
  once(
    lhdt_header(_, '<http://rdfs.org/ns/void#triples>', N^^xsd:integer, File)
  ).
