:- module(
  lod_cloud,
  [
    add_worker/0,
    add_workers/1,    % +N
    load_seed_list/0,
    seed/2,           % ?Dataset, ?Uri
    upload/1,         % +DatasetOrUri
    upload/2          % +Dataset, Uri
  ]
).

/** <module> LOD Cloud

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(http/json)).
:- use_module(library(settings)).
:- use_module(library(zlib)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(sw/rdf_export)).
:- use_module(library(tapir)).
:- use_module(library(thread_ext)).
:- use_module(library(uri_ext)).
:- use_module(library(write_ext)).

:- [download_data].

:- dynamic
    user:clean_triple_hook/6.

http:encoding_filter('x-gzip', In0, In) :-
  http:encoding_filter(gzip, In0, In).

:- initialization
   flag(number_of_workers, _, 1),
   conf_json(Conf),
   _{directory: Dir} :< Conf.data,
   create_directory(Dir),
   set_setting(data_directory, Dir),
   directory_file_path(Dir, 'error.log.gz', File),
   gzopen(File, write, Out),
   asserta((
     user:message_hook(E1, Kind, _) :-
       memberchk(Kind, [error,warning]),
       replace_blobs(E1, E2),
       writeln(Out, E2),
       flush_output(Out)
   )),
   asserta(user:at_halt(close(Out))),
   db_attach('seeds.pl', []).

:- maplist(rdf_create_prefix, [
     factbook-'http://www.daml.org/2001/12/factbook/factbook-ont#'
   ]).

:- rdf_meta
   clean_triple(+, +, r, r, o).

:- setting(data_directory, any, _, "").



% SEED LIST %

%! load_seed_list is det.

load_seed_list :-
  setup_call_cleanup(
    open('seeds.json', read, In),
    (
      json_read_dict(In, Dicts, [value_string_as(atom)]),
      maplist(assert_seed, Dicts)
    ),
    close(In)
  ).

assert_seed(Dict) :-
  assert_seed(Dict.dataset, Dict.uri).



% DATA CLEANING %

user:clean_triple_hook('cia-world-factbook', Out, BNodePrefix, S, factbook:largeFlag, literal(Lex)) :- !,
  uri_is_global(Lex), !,
  rdf_write_triple(Out, BNodePrefix, S, factbook:largeFlag, literal(type(xsd:anyURI,Lex))).



% TRIPLY OPTIONS %

%! triply_options(+Dataset:atom, -Options:dict) is det.

triply_options('cia-world-factbook', _{
  avatar: 'img/cia-world-factbook.jpg',
  description: "The World Factbook provides information on the history, people, government, economy, geography, communications, transportation, military, and transnational issues for 267 world entities.  Our Reference tab includes: maps of the major world regions, as well as Flags of the World, a Physical Map of the World, a Political Map of the World, a World Oceans map, and a Standard Time Zones of the World map.",
  prefixes: [
    'agriculture-product'-'http://www.daml.org/2001/12/factbook/agricultureProducts#',
    commodity-'http://www.daml.org/2001/12/factbook/commodities#',
    country-'http://www.daml.org/2001/09/countries/fips#',
    'ethnic-group'-'http://www.daml.org/2001/12/factbook/ethnicGroups#',
    factbook-'http://www.daml.org/2001/12/factbook/factbook-ont#',
    'government-type'-'http://www.daml.org/2001/12/factbook/governmentTypes#',
    graph-'http://simile.mit.edu/repository/datasets/cia-wfb/data/',
    industry-'http://www.daml.org/2001/12/factbook/industries#',
    language-'http://www.daml.org/2001/12/factbook/languages#',
    'natural-resource'-'http://www.daml.org/2001/12/factbook/naturalResources#',
    occupation-'http://www.daml.org/2001/12/factbook/occupations#',
    organization-'http://www.daml.org/2001/12/factbook/internationalOrganizations#',
    religion-'http://www.daml.org/2001/12/factbook/religions#'
  ]
}).
triply_options(citeseer, _{
  avatar: 'img/citeseer.png',
  description: "This is one of several semantic repositories that contains and publishes RDF linked data and co-reference information, forming the underlying distributed storage model behind the RKB Explorer initiative.  This repository contains data from CiteSeer (Research Index)."
}).
triply_options('dbtune-classical', _{
  avatar: 'img/dbtune-classical.jpg',
  description: "A set of resources describes concepts and individuals related to the canon of Western Classical Music.  The data is aggregated and to hand curated by Chris Cannam at Queen Mary University of London.  It includes information about composers, compositions, performers, relationships of influence, and other data.  The intention is to provide a Linked Data resource that provides references to existing information sources.",
  %license: 'https://creativecommons.org/licenses/by-nc-sa/3.0/',
  prefixes: [
    classical-'http://dbtune.org/classical/resource/type/',
    dbr-'http://dbpedia.org/resource/',
    graph-'https://c4dm.eecs.qmul.ac.uk/rdr/bitstream/handle/123456789/41/'
  ]
}).
triply_options('open-course-ware', _{
  avatar: 'img/open-course-ware.jpg',
  description: "Some of the MIT OpenCourseWare Material RDF-ized by the SIMILE Team."
}).
triply_options(wordnet, _{
  avatar: "img/wordnet.jpg",
  description: "WordNet is a large lexical database of English.  Nouns, verbs, adjectives and adverbs are grouped into sets of cognitive synonyms (synsets), each expressing a distinct concept.  Synsets are interlinked by means of conceptual-semantic and lexical relations.  WordNet's structure makes it a useful tool for computational linguistics and natural language processing.",
  prefixes: [
    def-'http://www.cogsci.princeton.edu/~wn/schema/',
    concept-'http://www.cogsci.princeton.edu/~wn/concept#',
    graph-'http://simile.mit.edu/repository/datasets/wordnet/data/'
  ]
}).
triply_options('w3c-technical-reports', _{
  avatar: "img/w3c.png",
  description: "",
  prefixes: [
    doc-'http://www.w3.org/2000/10/swap/pim/doc#',
    graph-'http://simile.mit.edu/repository/datasets/w3c-tr/data/',
    pim-'http://www.w3.org/2000/10/swap/pim/contact#'
  ]
}).
triply_options(_, _{}).





% WORKER POOL %

add_worker :-
  flag(number_of_workers, M, M+1),
  format(atom(Alias), 'worker-~d', [M]),
  thread_create(worker_loop, _, [alias(Alias),at_exit(work_ends),detached(true)]).

add_workers(N) :-
  forall(between(1, N, _), add_worker).

worker_loop :-
  with_mutex(seeds, retract(seed(Dataset,Uri))), !,
  thread_create(work(Dataset, Uri), Id, [alias(Dataset),at_exit(work_ends)]),
  thread_join(Id, _),
  worker_loop.
worker_loop.

work(Dataset, Uri) :-
  catch(
    upload(Dataset, Uri),
    Error0,
    (
      once(clean_error(Error0, Error)),
      %debug(lod_cloud, "KRAK! ~a ~w", [Dataset,Error]),
      print_message(warning, lod_cloud(Dataset,Error))
    )
  ).

clean_error(error(http_status(Status,_)), error(http_status(Status))).
clean_error(error(instantiation_error,_), error(instantiation_error)).
clean_error(error(socket_error(Label),_), error(socket_error(Label))).
clean_error(error(timeout_error(Label,_),_), error(timeout_error(Label))).
clean_error(Error, Error).

work_ends :-
  thread_self_property(status(Status)),
  (   Status == true
  ->  true
  ;   thread_self_property(alias(Alias)),
      print_message(warning, work_ends(Alias,Status))
  ).





% UPLOAD %

%! upload(+DatasetOrUri:atom) is det.

upload(Dataset) :-
  seed(Dataset, Uri), !,
  upload(Dataset, Uri).
upload(Uri) :-
  md5(Uri, Dataset),
  upload(Dataset, Uri).


%! upload(+Dataset:atom, +Uri:atom) is det.

upload(Dataset, Uri) :-
  setting(data_directory, Dir),
  directory_file_path(Dir, Dataset, Subdir),
  create_directory(Subdir),
  download_data(Dataset, Subdir, Uri),
  directory_file_path(Subdir, '*.trig.gz', Wildcard),
  expand_file_name(Wildcard, Files),
  (   Files == []
  ->  true
  ;   once(triply_options(Dataset, Options1)),
      Options2 = Options1.put(_{accessLevel: public, files: Files}),
      dataset_upload(Dataset, Options2)
  ),
  delete_directory_and_contents(Subdir).
