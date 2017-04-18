:- module(
  scape_ckan,
  [
    scrape_formats/0,
    scrape_sites/0
  ]
).

/** <module> Scape CKAN

@author Wouter Beek
@version 2017/04
*/

:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(ckan_api)).
:- use_module(library(debug)).
:- use_module(library(dict_ext)).
:- use_module(library(http/json)).
:- use_module(library(lists)).
:- use_module(library(pairs)).
:- use_module(library(rdf/rdf_build)).
:- use_module(library(rdf/rdf_print)).
:- use_module(library(rdf/rdf_term)).
:- use_module(library(uri)).
:- use_module(library(zlib)).

:- debug(ckan).

:- rdf_create_alias(ckan, 'https://triply.cc/ckan/').

:- rdf_meta
   key_datatype(+, r),
   rdf_assert_deb(+, r, r, o, r).





%! scrape_formats is det.

scrape_formats :-
  retractall(format(_,_)),
  ckan_site_uri(Site),
  ckan_resource(Site, Res),
  Format = Res.format,
  (retract(format(Format, N1)) -> N2 is N1 + 1 ; N2 = 1),
  assert(format(Format, N2)),
  fail.
scrape_formats :-
  forall(
    format(Format, N),
    format(user_output, "~a\t~D\n", [Format,N])
  ).



%! scrape_sites is det.

scrape_sites :-
  M = trp,
  rdf_equal(graph:ckan, G),
  Uri = 'http://dados.gov.br',
  forall(
    ckan_site_uri(Uri),
    (
      uri_normalized(Uri, I),
      scrape_site(M, G, I)
    )
  ).

scrape_site(M, G, I) :-
  rdf_assert_deb(M, I, rdf:type, ckan:'Site', G),
  forall(ckan_group(I, Dict), assert_pair(M, G, I, containsGroup, Dict)),
  forall(ckan_license(I, Dict), assert_pair(M, G, I, containsLicense, Dict)),
  forall(ckan_organization(I, Dict), assert_pair(M, G, I, containsOrganization, Dict)),
  forall(ckan_package(I, Dict), assert_pair(M, G, I, containsPackage, Dict)),
  forall(ckan_resource(I, Dict), assert_pair(M, G, I, containsResource, Dict)),
  forall(ckan_tag(I, Dict), assert_pair(M, G, I, containsTag, Dict)),
  forall(ckan_user(I, Dict), assert_pair(M, G, I, containsUser, Dict)).

assert_pair(_, _, _, _, "") :- !.
assert_pair(_, _, _, _, null) :- !.
assert_pair(_, _, _, archiver, _) :- !.
assert_pair(_, _, _, config, _) :- !.
assert_pair(_, _, _, extras, _) :- !.
assert_pair(_, _, _, qa, _) :- !.
assert_pair(_, _, _, status, _) :- !.
assert_pair(_, _, _, tracking_summary, _) :- !.
assert_pair(M, G, I, Key, L) :-
  is_list(L), !,
  maplist(assert_pair(M, G, I, Key), L).
assert_pair(M, G, I1, Key, Dict) :-
  is_dict(Dict), !,
  dict_pairs(Dict, Pairs1),
  selectchk(id-Lex, Pairs1, Pairs2),
  pairs_keys_values(Pairs2, Keys, Vals),
  atom_string(Id, Lex),
  key_predicate_class(Key, LocalP, Local),
  rdf_global_id(ckan:LocalP, P),
  capitalize_atom(Local, LocalC),
  rdf_global_id(ckan:LocalC, C),
  atomic_list_concat([Local,Id], /, LocalI),
  rdf_global_id(ckan:LocalI, I2),
  rdf_assert_deb(M, I2, rdf:type, C, G),
  rdf_assert_deb(M, I1, P, I2, G),
  maplist(assert_pair(M, G, I2), Keys, Vals).
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
  rdf_assert_deb(M, I, P, Lit, G).

key_predicate_class(containsGroup, containsGroup, group) :- !.
key_predicate_class(containsLicense, containsLicense, license) :- !.
key_predicate_class(containsOrganization, containsOrganization, organization) :- !.
key_predicate_class(containsPackage, containsPackage, package) :- !.
key_predicate_class(containsResource, containsResource, resource) :- !.
key_predicate_class(containsTag, containsTag, tag) :- !.
key_predicate_class(containsUser, containsUser, user) :- !.
key_predicate_class(groups, hasGroup, group) :- !.
key_predicate_class(individual_resources, hasIndividualResource, individualResource) :- !.
key_predicate_class(organization, hasOrganization, organization) :- !.
key_predicate_class(resources, hasResource, resource) :- !.
key_predicate_class(tags, hasTag, tag) :- !.
key_predicate_class(users, hasUser, user) :- !.
key_predicate_class(X, Y, Z) :-
  gtrace,
  key_predicate_class(X, Y, Z).

rdf_assert_deb(M, S, P, O, G) :-
  (   debugging(ckan)
  ->  with_output_to(string(Str), rdf_print_triple(S, P, O)),
      debug(ckan, "~s", [Str])
  ;   true
  ),
  rdf_assert(M, S, P, O, G).
