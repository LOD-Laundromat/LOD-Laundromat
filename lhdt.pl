:- module(
  lhdt,
  [
    lhdt/3,             % ?S, ?P, ?O
    lhdt/4,             % ?S, ?P, ?O, ?File
    lhdt_build/1,       %             +File
    lhdt_cost/5,        % ?S, ?P, ?O, +File, -Cost
    lhdt_data_table//5, % ?S, ?P, ?O, ?File, +Opts
    lhdt_header/4,      % ?S, ?P, ?O, ?File
    lhdt_print/4,       % ?S, ?P, ?O, +File
    lhdt_print/5        % ?S, ?P, ?O, +File, +Opts
  ]
).

/** <module> HDT build

@author Wouter Beek
@version 2016/03
*/

:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(gen/gen_ntuples)).
:- use_module(library(hdt)).
:- use_module(library(html/html_bs)).
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
   lhdt_data_table(r, r, o, r, +, ?, ?),
   lhdt_print(r, r, o, r).





%! lhdt(?S, ?P, ?O) is nondet.
%! lhdt(?S, ?P, ?O, ?File) is nondet.

lhdt(S, P, O) :-
  lhdt(S, P, O, _).


lhdt(S, P, O, File) :-
  lhdt_setup_call_cleanup(File, hdt_search0(S, P, O)).
hdt_search0(S, P, O, Hdt) :- hdt_search(Hdt, S, P, O).



%! lhdt_build(+File) is det.

lhdt_build(File) :-
  lfile_lhash(File, Name, _, Hash),
  lhdt_build0(Hash, Name).

lhdt_build0(Hash, meta) :- !,
  lhdt_build0(Hash, meta, _).
lhdt_build0(Hash, Name) :-
  % Make sure the metadata is converted to HDT.
  lhdt_build0(Hash, meta),
  lfile_lhash(File, meta, hdt, Hash),
  once(lhdt(_, llo:base_iri, BaseIri^^xsd:anyURI, File)),
  lhdt_build0(Hash, Name, BaseIri).

lhdt_build0(Hash, Name, BaseIri) :-
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



%! lhdt_data_table(?S, ?P, ?O, ?File, +Opts)// is det.

lhdt_data_table(S, P, O, File, Opts) -->
  {lhdt_page(S, P, O, File, Opts, Result)},
  rdfh_triple_table(Result.triples),
  bs_pagination(Result.number_of_pages, Result.page).
  %High is Result.page * Result.page_size,
  %Low is High - Result.page_size + 1,



%! lhdt_header(?S, ?P, ?O) is nondet.
%! lhdt_header(?S, ?P, ?O, ?File) is nondet.

lhdt_header(S, P, O) :-
  lhdt_header(S, P, O, _).


lhdt_header(S, P, O, File) :-
  lhdt_setup_call_cleanup(File, hdt_header0(S, P, O)).
hdt_header0(S, P, O, Hdt) :- hdt_header(Hdt, S, P, O).



%! lhdt_print(?S, ?P, ?O, +File) is nondet.
%! lhdt_print(?S, ?P, ?O, +File, +Opts) is nondet.

lhdt_print(S, P, O, File) :-
  lhdt_print(S, P, O, File, _{}).


lhdt_print(S, P, O, File, Opts) :-
  lhdt_page(S, P, O, File, Opts, Result),
  rdf_print_triples(Result.results, Opts).
  %High is Result.page * Result.page_size,
  %Low is High - Result.page_size + 1,




% HELPERS %

%! ensure_ntriples(+Dir, +From, -To) is det.

ensure_ntriples(Dir, From, To) :-
  directory_file_path(Dir, 'data.nt', To),
  setup_call_cleanup(
    open(To, write, Sink),
    with_output_to(Sink, rdf_call_on_tuples(From, gen_nquad)),
    close(Sink)
  ).



%! lhdt_page(?S, ?P, ?O, +File, +Opts, -Result) is nondet.
% The following keys are defined for Results:
%   - number_of_pages
%   - total_number_of_results
      
lhdt_page(S, P, O, File, Opts, Result2) :-
  pagination(rdf(S,P,O), lhdt(S, P, O, File), Opts, Result1),
  (   maplist(var, [S,P,O])
  ->  lhdt_triples(File, NumTriples),
      NumPages is ceil(NumTriples / Result1.page_size),
      Result2 = Result1.put({
       number_of_pages: NumPages,
       total_number_of_results: NumTriples
      })
  ;   Result2 = Result1
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
