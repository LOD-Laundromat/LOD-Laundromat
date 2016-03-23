:- module(
  laundromat_hdt,
  [
    hdt/3,            % ?S, ?P, ?O
    hdt/4,            % ?S, ?P, ?O, ?Doc
    hdt_build/1,      % +Doc
    hdt_data_table//5 % ?S, ?P, ?O, ?Doc, +Opts
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
:- use_module(library(html/rdfh)).
:- use_module(library(print_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(semweb/rdf11)).

:- use_module(cpack('LOD-Laundromat'/laundromat_fs)).

:- rdf_meta
   hdt(r, r, o),
   hdt(r, r, o, r),
   hdt_build(r),
   hdt_triple_table(r, r, o, r).





%! hdt(?S, ?P, ?O) is nondet.
%! hdt(?S, ?P, ?O, ?Doc) is nondet.

hdt(S, P, O) :-
  hdt(S, P, O, _).


hdt(S, P, O, Doc) :-
  ldoc(Doc),
  ldoc_file(Doc, data, hdt, File),
  access_file(File, read),
  setup_call_cleanup(
    hdt_open(Hdt, File),
    hdt_search(Hdt, S, P, O),
    hdt_close(Hdt)
  ).



%! hdt_build(+Doc) is det.

hdt_build(Doc) :-
  var(Doc), !,
  instantiation_error(Doc).
hdt_build(Doc) :-
  ldoc_file(Doc, data, hdt, File),
  exists_file(File), !,
  msg_notification("HDT file for ~a already exists.", [Doc]).
hdt_build(Doc) :-
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
hdt_build(Doc) :-
  msg_warning("Data file for ~a is missing.", [Doc]).



%! hdt_data_table(?S, ?P, ?O, ?Doc, +Opts)// is det.
% The following options are supported:
%   - page(+nonneg)
%     Default is 1.
%   - page_size(+nonneg)
%     Default is 100.

hdt_data_table(S, P, O, Doc, Opts1) -->
  {
    mod_dict(page_size, Opts1, 100, PageSize, Opts2),
    mod_dict(page, Opts2, 1, Page, Opts3),
    put_dict(page0, Opts3, 0, Opts4),
    % NONDET
    findnsols(PageSize, rdf(S,P,O), hdt(S, P, O, Doc), Triples),
    dict_inc(page0, Opts4),
    (Opts4.page0 =:= Page -> !, true ; false)
  },
  rdfh_triple_table(Triples).



%! ensure_ntriples(+Dir, +From, -To) is det.

ensure_ntriples(Dir, From, To) :-
  directory_file_path(Dir, 'data.nt', To),
  setup_call_cleanup(
    open(To, write, Sink),
    with_output_to(Sink, rdf_call_on_tuples(From, gen_nquad)),
    close(Sink)
  ).
