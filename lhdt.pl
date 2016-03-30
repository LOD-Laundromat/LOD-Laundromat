:- module(
  lhdt,
  [
    lhdt/3,             % ?S, ?P, ?O
    lhdt/4,             % ?S, ?P, ?O, ?Doc
    lhdt_build/2,       % +Hash, +Name
    lhdt_cost/5,        % ?S, ?P, ?O, +Doc, -Cost
    lhdt_data_table//5, % ?S, ?P, ?O, ?Doc, +Opts
    lhdt_delete/1,      % +Hash
    lhdt_delete/2,      % +Hash, +Name
    lhdt_header/4,      % ?S, ?P, ?O, ?Doc
    lhdt_page/6,        % ?S, ?P, ?O, +Doc, +Opts, -Result
    lhdt_print/4,       % ?S, ?P, ?O, +Doc
    lhdt_print/5        % ?S, ?P, ?O, +Doc, +Opts
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
:- use_module(library(rdf/rdf_print)).
:- use_module(library(semweb/rdf11)).

:- use_module(cpack('LOD-Laundromat'/lfs)).

:- meta_predicate
    lhdt_setup_call_cleanup(+, 1).

:- rdf_meta
   lhdt(r, r, o),
   lhdt(r, r, o, r),
   lhdt_build(+, +, r),
   lhdt_cost(r, r, o, r, -),
   lhdt_header(r, r, o, r),
   lhdt_data_table(r, r, o, r, +, ?, ?),
   lhdt_print(r, r, o, r).





%! lhdt(?S, ?P, ?O) is nondet.
%! lhdt(?S, ?P, ?O, ?Doc) is nondet.

lhdt(S, P, O) :-
  lhdt(S, P, O, _).


lhdt(S, P, O, Doc) :-
  lhdt_setup_call_cleanup(Doc, hdt_search0(S, P, O)).
hdt_search0(S, P, O, Hdt) :- hdt_search(Hdt, S, P, O).



%! lhdt_build(+Doc) is det.

lhdt_build(Doc) :-
  ldoc_lhash(Doc, Name, Hash),
  lhdt_build(Hash, Name).


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
  (   % HDT file exits.
      exists_file(HdtFile)
  ->  msg_notification("File ~a already exists.~n", [HdtFile])
  ;   % N-Triples file exists.
      lfile_lhash(NTriplesFile, Name, ntriples, Hash),
      access_file(NTriplesFile, read)
  ->  (var(BaseIri) -> Opts = [] ; Opts = [base_uri(BaseIri)]),
      hdt_create_from_file(HdtFile, NTriplesFile, Opts)
  ;   % N-Quads file exists.
      lfile_lhash(NQuadsFile, Name, nquads, Hash),
      access_file(NQuadsFile, read)
  ->  ldir_lhash(Dir, Hash),
      setup_call_cleanup(
        ensure_ntriples(Dir, NQuadsFile, NTriplesFile),
        lhdt_build0(Hash, Name, _),
        delete_file(NTriplesFile)
      )
  ;   msg_warning("Data file for ~a is missing.", [Hash])
  ).



%! lhdt_cost(?S, ?P, ?O, +Doc, -Cost) is det.

lhdt_cost(S, P, O, Doc, Cost) :-
  lhdt_setup_call_cleanup(Doc, hdt_search_cost0(S, P, O, Cost)).
hdt_search_cost0(S, P, O, Cost, Hdt) :- hdt_search_cost(Hdt, S, P, O, Cost).



%! lhdt_data_table(?S, ?P, ?O, ?Doc, +Opts)// is det.
% The following options are supported:
%   - page_size(+nonneg)
%     Default is 100.
%   - start_page(+nonneg)
%     Default is 1.

lhdt_data_table(S, P, O, Doc, Opts) -->
  {lhdt_page(S, P, O, Doc, Opts, Result)},
  rdfh_triple_table(Result.triples),
  bs_pagination(Result.number_of_pages, Result.page).
  


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



%! lhdt_page(?S, ?P, ?O, +Doc, +Opts, -Result) is nondet.
% The following keys are defined for Opts:
%   - page_size
%   - start_page
%
% The following keys are defined for Results:
%   - number_for_pattern
%   - number_of_pages
%   - number_of_triples
%   - number_of_triples_per_page
%   - page
%   - triples

lhdt_page(S, P, O, Doc, Opts1, Result) :-
  mod_dict(page_size, Opts1, 100, PageSize, Opts2),
  lhdt_triples(Doc, NumTriples),
  NumPages is ceil(NumTriples / PageSize),
  mod_dict(start_page, Opts2, 1, StartPage, Opts3),
  put_dict(page0, Opts3, 0, Opts4),
  % NONDET
  findnsols(PageSize, rdf(S,P,O), lhdt(S, P, O, Doc), Triples),
  dict_inc(page0, Opts4),
  (Opts4.page0 >= StartPage -> true ; false),
  Result = _{
    number_of_pages: NumPages,
    number_of_triples: NumTriples,
    number_of_triples_per_page: PageSize,
    page: Opts4.page0,
    triples: Triples
  }.



%! lhdt_print(?S, ?P, ?O, +Doc) is nondet.
%! lhdt_print(?S, ?P, ?O, +Doc, +Opts) is nondet.
% The following keys are defined for Opts:
%   - page_size
%   - start_page

lhdt_print(S, P, O, Doc) :-
  lhdt_print(S, P, O, Doc, _{}).


lhdt_print(S, P, O, Doc, Opts) :-
  lhdt_page(S, P, O, Doc, Opts, Result),
  rdf_print_triples(Result.triples, Opts).




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
  (   exists_file(File)
  ->  access_file(File, read),
      setup_call_cleanup(
        hdt_open(Hdt, File),
        call(Goal_1, Hdt),
        hdt_close(Hdt)
      )
  ;   lhdt_build(Doc),
      lhdt_setup_call_cleanup(Doc, Goal_1)
  ).



%! lhdt_triples(+Doc, -NumTriples) is det.

lhdt_triples(Doc, N) :-
  once(
    lhdt_header(_, '<http://rdfs.org/ns/void#triples>', N^^xsd:integer, Doc)
  ).
