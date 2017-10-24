:- module(
  ll_api,
  [
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
:- use_module(library(lists)).
:- use_module(library(pairs)).
:- use_module(library(semweb/hdt11)).
:- use_module(library(semweb/rdf11)).

:- rdf_register_prefix(bnode, 'https://lodlaundromat.org/.well-known/genid/', [force(true)]).
:- rdf_register_prefix(ckan, 'https://lodlaundromat.org/ckan/').
:- rdf_register_prefix(llh, 'https://lodlaundromat.org/http/').
:- rdf_register_prefix(llo, 'https://lodlaundromat.org/ontology/').
:- rdf_register_prefix(llr, 'https://lodlaundromat.org/resource/').
:- rdf_register_prefix(void, 'http://rdfs.org/ns/void#').

:- rdf_meta
   ll(r, r, o, ?),
   llm(r, r, o, ?).





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
