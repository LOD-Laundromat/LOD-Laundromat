:- module(
  lhdt,
  [
    lhdt/3,             % ?S, ?P, ?O
    lhdt/4,             % ?S, ?P, ?O, ?Doc
    lhdt_build/2,       % +Hash, +Name
    lhdt_data_table//5, % ?S, ?P, ?O, ?Doc, +Opts
    lhdt_delete/1,      % +Hash
    lhdt_delete/2,      % +Hash, +Name
    lhdt_header/4       % ?S, ?P, ?O, ?Doc
  ]
).

/** <module> HDT build

@author Wouter Beek
@version 2016/03
*/

:- use_module(library(apply)).
:- use_module(library(dict_ext)).
:- use_module(library(error)).
:- use_module(library(gen/gen_ntuples)).
:- use_module(library(hdt)).
:- use_module(library(html/html_bs)).
:- use_module(library(html/rdfh)).
:- use_module(library(print_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(semweb/rdf11)).

:- use_module(cpack('LOD-Laundromat'/lfs)).

:- meta_predicate
    lhdt_setup_call_cleanup(+, 1).

:- rdf_meta
   lhdt(r, r, o),
   lhdt(r, r, o, r),
   lhdt_build(+, +, r),
   lhdt_header(r, r, o, r),
   lhdt_data_table(r, r, o, r, +, ?, ?).





%! lhdt(?S, ?P, ?O) is nondet.
%! lhdt(?S, ?P, ?O, ?Doc) is nondet.

lhdt(S, P, O) :-
  lhdt(S, P, O, _).


lhdt(S, P, O, Doc) :-
  lhdt_setup_call_cleanup(Doc, hdt_search0(S, P, O)).
hdt_search0(S, P, O, Hdt) :- hdt_search(Hdt, S, P, O).



%! lhdt_build(+Hash, +Name) is det.

lhdt_build(Hash, meta) :- !,
  lhdt_build0(Hash, meta, _).
lhdt_build(Hash, Name) :-
  % Make sure the metadata is converted to HDT.
  lhdt_build(Hash, meta),
  ldoc_lhash(Doc, meta, Hash),
  once(lhdt(_, llo:base_iri, BaseIri^^xsd:anyURI, Doc)),
  lhdt_build0(Hash, Name, BaseIri).


lhdt_build0(Hash, Name, BaseIri) :-
  lfile_lhash(HdtFile, Name, hdt, Hash),
  (   exists_file(HdtFile)
  ->  msg_notification("File ~a already exists.~n", [HdtFile])
  ;   lfile_lhash(NQuadsFile, Name, nquads, Hash),
      access_file(NQuadsFile, read)
  ->  ldir_lhash(Dir, Hash),
      (var(BaseIri) -> Opts = [] ; Opts = [base_uri(BaseIri)]),
      setup_call_cleanup(
        ensure_ntriples(Dir, NQuadsFile, NTriplesFile),
        hdt_create_from_file(HdtFile, NTriplesFile, Opts),
        delete_file(NTriplesFile)
      )
  ;   msg_warning("Data file for ~a is missing.", [Hash])
  ).



%! lhdt_data_table(?S, ?P, ?O, ?Doc, +Opts)// is det.
% The following options are supported:
%   - page(+nonneg)
%     Default is 1.
%   - page_size(+nonneg)
%     Default is 100.

lhdt_data_table(S, P, O, Doc, Opts1) -->
  {
    mod_dict(page_size, Opts1, 100, PageSize, Opts2),
    lhdt_triples(Doc, NumTriples),
    NumPages is ceil(NumTriples / PageSize),
    mod_dict(page, Opts2, 1, Page, Opts3),
    put_dict(page0, Opts3, 0, Opts4),
    % NONDET
    findnsols(PageSize, rdf(S,P,O), lhdt(S, P, O, Doc), Triples),
    dict_inc(page0, Opts4),
    (Opts4.page0 =:= Page -> !, true ; false)
  },
  rdfh_triple_table(Triples),
  bs_pagination(NumPages, 3).



%! lhdt_delete(+Hash) is det.
%! lhdt_delete(+Hash, +Name) is det.

lhdt_delete(Hash) :-
  forall(lname(Name), lhdt_delete(Hash, Name)).


lhdt_delete(Hash, Name) :-
  lfile_lhash(File, Name, hdt, Hash),
  (exists_file(File) -> delete_file(File) ; true).



%! lhdt_header(?S, ?P, ?O) is nondet.
%! lhdt_header(?S, ?P, ?O, ?Doc) is nondet.

lhdt_header(S, P, O) :-
  lhdt_header(S, P, O, _).


lhdt_header(S, P, O, Doc) :-
  lhdt_setup_call_cleanup(Doc, hdt_header0(S, P, O)).
hdt_header0(S, P, O, Hdt) :- hdt_header(Hdt, S, P, O).





% HELPERS %

%! ensure_ntriples(+Dir, +From, -To) is det.

ensure_ntriples(Dir, From, To) :-
  directory_file_path(Dir, 'data.nt', To),
  setup_call_cleanup(
    open(To, write, Sink),
    with_output_to(Sink, rdf_call_on_tuples(From, gen_nquad)),
    close(Sink)
  ).



%! lhdt_setup_call_cleanup(+Doc, :Goal_1) is det.
%! lhdt_setup_call_cleanup(-Doc, :Goal_1) is nondet.

lhdt_setup_call_cleanup(Doc, Goal_1) :-
  ldoc_lfile(Doc, hdt, File),
  access_file(File, read),
  setup_call_cleanup(
    hdt_open(Hdt, File),
    call(Goal_1, Hdt),
    hdt_close(Hdt)
  ).



%! lhdt_triples(+Doc, -NumTriples) is det.

lhdt_triples(Doc, N) :-
  once(
    lhdt_header(_, '<http://rdfs.org/ns/void#triples>', N^^xsd:integer, Doc)
  ).
