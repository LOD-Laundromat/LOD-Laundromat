:- module(
  laundromat_hdt,
  [
    lhdt/3,            % ?S, ?P, ?O
    lhdt/4,            % ?S, ?P, ?O, ?Doc
    lhdt_build/1,      % +Doc
    lhdt_header/4,     % ?S, ?P, ?O, ?Doc
    lhdt_data_table//5 % ?S, ?P, ?O, ?Doc, +Opts
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

:- use_module(cpack('LOD-Laundromat'/laundromat_fs)).

:- meta_predicate
    lhdt_setup_call_cleanup(+, 1).

:- rdf_meta
   lhdt(r, r, o),
   lhdt(r, r, o, r),
   lhdt_build(r),
   lhdt_header(r, r, o, r),
   lhdt_triple_table(r, r, o, r).





%! lhdt(?S, ?P, ?O) is nondet.
%! lhdt(?S, ?P, ?O, ?Doc) is nondet.

lhdt(S, P, O) :-
  lhdt(S, P, O, _).


lhdt(S, P, O, Doc) :-
  lhdt_setup_call_cleanup(Doc, hdt_search0(S, P, O)).
hdt_search0(S, P, O, Hdt) :- hdt_search(Hdt, S, P, O).



%! lhdt_build(+Doc) is det.

lhdt_build(Doc) :-
  var(Doc), !,
  instantiation_error(Doc).
lhdt_build(Doc) :-
  ldoc_file(Doc, data, hdt, File),
  exists_file(File), !,
  msg_notification("HDT file for ~a already exists.", [Doc]).
lhdt_build(Doc) :-
  ldoc_file(Doc, data, nquads, NQuadsFile), !,
  access_file(NQuadsFile, read),
  ldir_ldoc(Dir, Doc),
  rdf_has(Doc, llo:base_iri, BaseIri^^xsd:anyURI),
  directory_file_path(Dir, 'data.hdt', HdtFile),
  setup_call_cleanup(
    ensure_ntriples(Dir, NQuadsFile, NTriplesFile),
    hdt_create_from_file(HdtFile, NTriplesFile, [base_uri(BaseIri)]),
    delete_file(NTriplesFile)
  ).
lhdt_build(Doc) :-
  msg_warning("Data file for ~a is missing.", [Doc]).



%! lhdt_data_table(?S, ?P, ?O, ?Doc, +Opts)// is det.
% The following options are supported:
%   - page(+nonneg)
%     Default is 1.
%   - page_size(+nonneg)
%     Default is 100.

lhdt_data_table(S, P, O, Doc, Opts1) -->
  {
    mod_dict(page_size, Opts1, 100, PageSize, Opts2),
    lhdt_header(_, '<http://rdfs.org/ns/void#triples>', NumTriples^^xsd:integer, Doc),
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
  ldoc(Doc),
  ldoc_file(Doc, data, hdt, File),
  access_file(File, read),
  setup_call_cleanup(
    hdt_open(Hdt, File),
    call(Goal_1, Hdt),
    hdt_close(Hdt)
  ).
