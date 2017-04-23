:- module(
  scrape_ckan,
  [
    scrape_ckan/0,
    scrape_ckan_thread/0,
    scrape_ckan/1         % +Site
  ]
).

/** <module> Scrape CKAN

@author Wouter Beek
@version 2017/04
*/

:- use_module(library(apply)).
:- use_module(library(ckan_api)).
:- use_module(library(debug)).
:- use_module(library(lists)).
:- use_module(library(md5)).
:- use_module(library(pairs)).
:- use_module(library(uri)).
:- use_module(library(zlib)).

:- use_module(library(atom_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(rdf/rdf_build)).
:- use_module(library(rdf/rdf_print)).
:- use_module(library(rdf/rdf_term)).
:- use_module(library(rdf/rdfio)).
:- use_module(library(thread_ext)).

:- debug(scrape_ckan).
:- debug(scrape_ckan2).

:- nodebug(http(_)).

:- rdf_create_alias(ckan, 'https://triply.cc/ckan/').

:- rdf_meta
   key_datatype(+, r).





%! scrape_ckan is det.
%! scrape_ckan(+Site) is det.

scrape_ckan :-
  forall(
    ckan_site_uri(Site),
    scrape_ckan(Site)
  ).


scrape_ckan(Site) :-
  md5_hash(Site, Hash, []),
  file_name_extension(Hash, nt, File),
  scrape_ckan(Site, Hash, File),
  finish_ntriples_file(File).

scrape_ckan(Site, _, File) :-
  exists_file(File), !,
  debug(scrape_ckan, "Skipping site: ~a", [Site]).
scrape_ckan(Site, Hash, File) :-
  debug(scrape_ckan, "Started: ~a", [Site]),
  M = trp,
  atomic_list_concat([graph,Hash], /, Local),
  rdf_global_id(ckan:Local, G),
  rdf_retractall(M, _, _, _, G),
  scrape_site(M, G, Site, Hash),
  write_ntriples(G, File, [mode(append)]),
  rdf_retractall(M, _, _, _, G),
  debug(scrape_ckan, "Finished: ~a", [Site]).

scrape_site(M, G, I, Hash) :-
  rdf_assert(M, I, rdf:type, ckan:'Site', G),
  forall(
    ckan_group(I, Dict),
    (
      assert_pair(M, G, I, containsGroup, Dict),
      ckan_debug(Hash, group)
    )
  ),
  forall(
    ckan_license(I, Dict),
    (
      assert_pair(M, G, I, containsLicense, Dict),
      ckan_debug(Hash, license)
    )
  ),
  forall(
    ckan_organization(I, Dict),
    (
      assert_pair(M, G, I, containsOrganization, Dict),
      ckan_debug(Hash, organization)
    )
  ).
/*
  forall(
    ckan_package(I, Dict),
    (
      assert_pair(M, G, I, containsPackage, Dict),
      ckan_debug(Hash, package)
    )
  ),
  forall(
    ckan_resource(I, Dict),
    (
      assert_pair(M, G, I, containsResource, Dict),
      ckan_debug(Hash, resource)
    )
  ),
  forall(
    ckan_tag(I, Dict),
    (
      assert_pair(M, G, I, containsTag, Dict),
      ckan_debug(Hash, tag)
    )
  ),
  forall(
    ckan_user(I, Dict),
    (
      assert_pair(M, G, I, containsUser, Dict),
      ckan_debug(Hash, user)
    )
  ).
*/

ckan_debug(Hash, Type) :-
  atom_concat(Hash, Type, Flag),
  flag(Flag, N, N+1),
  debug(scrape_ckan2, "~a: ~D", [Flag,N]).

assert_pair(_, _, _, _, "") :- !.
assert_pair(_, _, _, _, null) :- !.
assert_pair(_, _, _, archiver, _) :- !.
assert_pair(_, _, _, concepts_eurovoc, _) :- !.
assert_pair(_, _, _, config, _) :- !.
assert_pair(_, _, _, default_extras, _) :- !.
assert_pair(_, _, _, extras, _) :- !.
assert_pair(_, _, _, keywords, _) :- !.
assert_pair(_, _, _, pids, _) :- !.
assert_pair(_, _, _, qa, _) :- !.
assert_pair(_, _, _, relationships_as_object, _) :- !.
assert_pair(_, _, _, status, _) :- !.
assert_pair(_, _, _, tracking_summary, _) :- !.
assert_pair(M, G, I, Key, L) :-
  is_list(L), !,
  maplist(assert_pair(M, G, I, Key), L).
assert_pair(M, G, I1, Key, Dict) :-
  is_dict(Dict), !,
  dict_pairs(Dict, Pairs1),
  (   selectchk(id-Lex, Pairs1, Pairs2)
  ->  pairs_keys_values(Pairs2, Keys, Vals),
      atom_string(Id, Lex),
      key_predicate_class(Key, LocalP, Local),
      rdf_global_id(ckan:LocalP, P),
      capitalize_atom(Local, LocalC),
      rdf_global_id(ckan:LocalC, C),
      atomic_list_concat([Local,Id], /, LocalI),
      rdf_global_id(ckan:LocalI, I2),
      rdf_assert(M, I2, rdf:type, C, G),
      rdf_assert(M, I1, P, I2, G),
      maplist(assert_pair(M, G, I2), Keys, Vals)
  ;   true
  ).
assert_pair(M, G, I, Key, Lex) :-
  rdf_global_id(ckan:Key, P),
  (   catch(xsd_time_string(_, D, Lex), _, fail)
  ->  true
  ;   memberchk(Lex, ["false","true"])
  ->  rdf_equal(xsd:boolean, D)
  ;   integer(Lex)
  ->  rdf_equal(xsd:integer, D)
  ;   rdf_equal(xsd:string, D)
  ),
  rdf_literal(Lit, D, Lex, _),
  rdf_assert(M, I, P, Lit, G).

key_predicate_class(activity, hasActivity, activity) :- !.
key_predicate_class(containsGroup, containsGroup, group) :- !.
key_predicate_class(containsLicense, containsLicense, license) :- !.
key_predicate_class(containsOrganization, containsOrganization, organization) :- !.
key_predicate_class(containsPackage, containsPackage, package) :- !.
key_predicate_class(containsResource, containsResource, resource) :- !.
key_predicate_class(containsTag, containsTag, tag) :- !.
key_predicate_class(containsUser, containsUser, user) :- !.
key_predicate_class(datasets, hasDataset, dataset) :- !.
key_predicate_class(default_group_dicts, hasGroup, group) :- !.
key_predicate_class(groups, hasGroup, group) :- !.
key_predicate_class(individual_resources, hasIndividualResource, individualResource) :- !.
key_predicate_class(organization, hasOrganization, organization) :- !.
key_predicate_class(packages, hasPackage, package) :- !.
key_predicate_class(resources, hasResource, resource) :- !.
key_predicate_class(tags, hasTag, tag) :- !.
key_predicate_class(users, hasUser, user) :- !.
key_predicate_class(X, Y, Z) :-
  gtrace,
  key_predicate_class(X, Y, Z).



%! scrape_ckan_thread is det.

scrape_ckan_thread :-
  findall(scrape_ckan(Site), ckan_site_uri(Site), Goals),
  concurrent(10, Goals, []).
