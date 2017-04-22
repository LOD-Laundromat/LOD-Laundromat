:- module(
  scrape_ckan,
  [
    % EVERYTHING
    scrape_ckan/0,
    scrape_ckan_thread/0,
    scrape_ckan/1,           % +Site
    scrape_ckan_thread/1,    % +Site
    % FORMATS
    print_formats/0,
    scrape_formats/0,
    scrape_formats/1,        % +Site
    scrape_formats_thread/0,
    scrape_formats_thread/1  % +Site
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

:- dynamic
    ckan_format/2.

:- nodebug(http(_)).

:- rdf_create_alias(ckan, 'https://triply.cc/ckan/').

:- rdf_meta
   key_datatype(+, r).





%! print_formats is det.

print_formats :-
  print_formats(current_output).

print_formats(Out) :-
  findall(N-Format, ckan_format(Format, N), Pairs),
  keysort(Pairs, Sorted),
  maplist(print_format(Out), Sorted).

print_format(Out, N-Format) :-
  format(Out, "~a\t~d\n", [Format,N]).



%! scrape_formats is det.
%! scrape_formats(+Site) is det.

scrape_formats :-
  forall(
    ckan_site_uri(Site),
    scrape_formats(Site)
  ).


scrape_formats(Site) :-
  md5_hash(Site, Hash, []),
  file_name_extension(Hash, 'tsv.gz', File),
  scrape_formats(Site, File).

scrape_formats(Site, File) :-
  exists_file(File), !,
  debug(scrape_ckan, "Skipping site: ~a", [Site]).
scrape_formats(Site, File) :-
  debug(scrape_ckan, "Started: ~a", [Site]),
  retractall(ckan_format(_,_)),
  forall(
    ckan_resource(Site, Res),
    (
      Format = Res.format,
      with_mutex(ckan_format, (
        (retract(ckan_format(Format, N1)) -> N2 is N1 + 1 ; N2 = 1),
        assert(ckan_format(Format, N2))
      ))
    )
  ),
  setup_call_cleanup(
    gzopen(File, write, Out),
    (
      format(Out, "~a\n", [Site]),
      print_formats(Out)
    ),
    close(Out)
  ),
  debug(scrape_ckan, "Finished: ~a", [Site]).



%! scrape_formats_thread is det.
%! scrape_formats_thread(+Site) is det.

scrape_formats_thread :-
  detached_thread(scrape_formats).


scrape_formats_thread(Site) :-
  detached_thread(scrape_formats(Site)).



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
  scrape_site(M, G, Site),
  write_ntriples(G, File),
  rdf_retractall(M, _, _, _, G),
  debug(scrape_ckan, "Finished: ~a", [Site]).

scrape_site(M, G, I) :-
  rdf_assert(M, I, rdf:type, ckan:'Site', G),
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
assert_pair(_, _, _, default_extras, _) :- !.
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

key_predicate_class(containsGroup, containsGroup, group) :- !.
key_predicate_class(containsLicense, containsLicense, license) :- !.
key_predicate_class(containsOrganization, containsOrganization, organization) :- !.
key_predicate_class(containsPackage, containsPackage, package) :- !.
key_predicate_class(containsResource, containsResource, resource) :- !.
key_predicate_class(containsTag, containsTag, tag) :- !.
key_predicate_class(containsUser, containsUser, user) :- !.
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



%! scrape_ckan is det.
%! scrape_ckan(+Site) is det.

scrape_ckan_thread :-
  findnsols(20, Site, ckan_site_uri(Site), Sites), % @deb
  maplist(scrape_ckan_thread, Sites).


scrape_ckan_thread(Site) :-
  thread_create(scrape_ckan(Site), _, [alias(Site),detached(true)]).
