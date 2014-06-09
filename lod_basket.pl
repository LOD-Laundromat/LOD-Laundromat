:- module(
  lod_basket,
  [
    add_source_to_basket/1, % +Source
    current_pending_source/1, % ?Md5:atom
    current_processed_source/1, % ?Md5:atom
    remove_source_from_basket/1 % +Md5:atom
  ]
).

/** <module> LOD basket

The LOD basket for URLs that are to be processed by the LOD Washing Machine.

@author Wouter Beek
@version 2014/05-2014/06
*/

:- use_module(library(persistency)).

:- use_module(generics(db_ext)).
:- use_module(os(file_ext)).
:- use_module(sparql(sparql_build)).
:- use_module(sparql(sparql_ext)).

:- use_module(lwm(lwm_db)).
:- use_module(lwm(lwm_generics)).
:- use_module(lwm(lwm_store_triple)).
:- use_module(lwm(noRdf_store)).

%! pending_source(?Md5:atom) is nondet.

:- persistent(pending_source(md5:atom)).

%! processed_source(?Md5:atom) is nondet.

:- persistent(processed_source(md5:atom)).

:- db_add_novel(user:prolog_file_type(log, logging)).

:- initialization(lod_basket).



%! add_source_to_basket(+Source) is det.

add_source_to_basket(Source):-
  with_mutex(lod_baqsket,
    add_source_to_basket_under_mutex(Source)
  ).

add_source_to_basket_under_mutex(Source):-
  is_processed_source(Source), !,
  print_message(informational, already_processed(Source)).
add_source_to_basket_under_mutex(Source):-
  is_pending_source(Source), !,
  print_message(informational, already_pending(Source)).
add_source_to_basket_under_mutex(Source):-
  source_to_md5(Source, Md5),
  store_source(Md5, Source),
  assert_pending_source(Md5).


%! current_pending_source(?Md5:atom) is nondet.

current_pending_source(Md5):-
  with_mutex(lod_baqsket, pending_source(Md5)).


%! current_processed_source(?Md5:atom) is nondet.

current_processed_source(Md5):-
  with_mutex(lod_basket, processed_source(Md5)).


%! is_pending_source(+Source) is semidet.

is_pending_source(Url-EntryPath):- !,
  default_graph(DefaultGraph),
  phrase(
    sparql_formulate_ask(_, DefaultGraph, [ap],
        [rdf(var(md5_url),ap:url,Url),
         rdf(var(md5_url),ap:has_entry,var(md5_entry)),
         rdf(var(md5_entry),ap:path,string(EntryPath)),
         rdf(var(md5_entry),ap:added,var(added))]),
    Query
  ),
  sparql_query(cliopatria, Query, _, _).
is_pending_source(Url):-
  default_graph(DefaultGraph),
  phrase(
    sparql_formulate_ask(_, DefaultGraph, [ap],
        [rdf(var(md5),ap:url,Url),
         rdf(var(md5),ap:added,var(added))]),
    Query
  ), sparql_query(cliopatria, Query, _, _).


%! is_processed_source(+Source) is semidet.

is_processed_source(Url-EntryPath):- !,
  default_graph(DefaultGraph),
  phrase(
    sparql_formulate_ask(_, DefaultGraph, [ap],
        [rdf(var(md5_url),ap:url,Url),
         rdf(var(md5_url),ap:has_entry,var(md5_entry)),
         rdf(var(md5_entry),ap:path,string(EntryPath)),
         rdf(var(md5_entry),ap:lwm_end,var(end))]),
    Query
  ),
  sparql_query(cliopatria, Query, _, _).
is_processed_source(Url):-
  default_graph(DefaultGraph),
  phrase(
    sparql_formulate_ask(_, DefaultGraph, [ap],
        [rdf(var(md5),ap:url,Url),
         rdf(var(md5),ap:lwm_end,var(end))]),
    Query
  ),
  sparql_query(cliopatria, Query, _, _).


% remove_source_from_basket(+Md5:atom) is det.

remove_source_from_basket(Md5):-
  with_mutex(lod_baqsket, (
    retractall_pending_source(Md5),
    store_lwm_start(Md5),
    assert_processed_source(Md5)
  )).



% Initialization

lod_basket:-
  absolute_file_name(
    data(lod_basket),
    File,
    [access(write),file_type(logging)]
  ),
  safe_db_attach(File).

%! safe_db_attach(+File:atom) is det.

safe_db_attach(File):-
  exists_file(File), !,
  db_attach(File, []).
safe_db_attach(File):-
  touch_file(File),
  safe_db_attach(File).



% Messages

prolog:message(already_pending(Md5)) -->
  cannot_add(Md5),
  ['already pending'].

prolog:message(already_processed(Md5)) -->
  cannot_add(Md5),
  ['already processed'].

cannot_add(Md5) -->
  ['MD5 ~w cannot be added to the pool: '-[Md5]].

