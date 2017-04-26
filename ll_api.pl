:- module(
  ll_api,
  [
    ckan/1,       % ?File
    ckan/4,       % ?S, ?P, ?O, ?File
    ckan_formats/0,
    ll/1,         % ?File
    ll/4,         % ?S, ?P, ?O, ?File
    ll_doc/2,     % -Doc, -NumTriples
    ll_triples/0,
    ll_triples/1, % -NumTriples
    llm/1,        % ?File
    llm/4         % ?S, ?P, ?O, ?File
  ]
).

/** <module> LOD Laundromat API

@author Wouter Beek
@version 2017/04
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(file_ext)).
:- use_module(library(hdt/hdt_api)).
:- use_module(library(lists)).
:- use_module(library(pairs)).
:- use_module(library(semweb/rdf11)).

:- rdf_register_prefix(bnode, 'https://lodlaundromat.org/.well-known/genid/', [force(true)]).
:- rdf_register_prefix(ckan, 'https://triply.cc/ckan/').
:- rdf_register_prefix(llh, 'https://lodlaundromat.org/http/').
:- rdf_register_prefix(llo, 'https://lodlaundromat.org/ontology/').
:- rdf_register_prefix(llr, 'https://lodlaundromat.org/resource/').
:- rdf_register_prefix(void, 'http://rdfs.org/ns/void#').

:- rdf_meta
   ckan(r, r, o, ?),
   ll(r, r, o, ?),
   llm(r, r, o, ?).





%! ckan(?File) is nondet.
%! ckan(?S, ?P, ?O, ?File) is nondet.

ckan(File) :-
  absolute_file_name(
    '*.hdt',
    File,
    [access(read),expand(true),solutions(all)]
  ).
  
ckan(S, P, O, File) :-
  ckan(File),
  hdt_call_on_file(File, hdt0(S, P, O)).



%! ckan_formats is det.

ckan_formats :-
  findall(
    Format-dummy,
    ckan(_, ckan:format, Format^^xsd:string, _),
    Pairs
  ),
  sort(1, @=<, Pairs, SortedPairs),
  group_pairs_by_key(SortedPairs, JoinedPairs),
  maplist(number_of_values, JoinedPairs, NumberPairs),
  transpose_pairs(NumberPairs, InvPairs),
  keysort(InvPairs, Rows),
  maplist(writeln, Rows).

number_of_values(Key-Vals, Key-NumVals) :-
  length(Vals, NumVals).



%! ll(?File) is nondet.
%! llm(?File) is nondet.

ll(File) :-
  ll0(data, File).



%! ll(?S ?P, ?O, ?File) is nondet.

ll(S, P, O, File) :-
  ll(File),
  ll0(S, P, O, File).



%! ll_doc(-Doc, -NumTriples) is nondet.

ll_doc(doc(Uri,Names), NumTriples) :-
  llm(Entry, void:triples, NumTriples^^xsd:nonNegativeInteger, EntryG),
  (   llm(Entry, llo:parent, Parent, EntryG)
  ->  llm(Entry, llo:name, Name^^xsd:string, EntryG),
      Names = [Name],
      llm(Parent, llo:uri, Uri^^xsd:anyURI, _ParentG)
  ;   llm(Entry, llo:uri, Uri^^xsd:anyURI, EntryG),
      Names = []
  ).



%! ll_triples is det.
%! ll_triples(-NumTriples) is det.

ll_triples :-
  ll_triples(NumTriples),
  format(user_output, "~D\n", [NumTriples]).


ll_triples(NumTriples) :-
  aggregate_all(
    sum(NumTriples),
    llm(_, void:triples, NumTriples^^xsd:nonNegativeInteger, _),
    NumTriples
  ).



%! llm(?File) is nondet.
%! llm(?S ?P, ?O, ?File) is nondet.

llm(File) :-
  ll0(meta, File).

llm(S, P, O, File) :-
  llm(File),
  ll0(S, P, O, File).





% HELPERS %

ll0(Base, File) :-
  file_name_extension(Base, hdt, Local),
  directory_path('/scratch/wbeek/ll/', Dir),
  directory_path(Dir, Subdir),
  directory_file_path(Subdir, Local, File),
  exists_file(File).



ll0(S, P, O, File) :-
  hdt_call_on_file(File, hdt0(S, P, O)).
