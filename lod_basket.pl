:- module(
  lod_basket,
  [
    add_source_to_basket/1, % +Source
    current_pending_source/2, % ?Source
                              % ?TimeAdded:nonneg
    current_processed_source/3, % ?Source
                                % ?TimeAdded:nonneg
                                % ?TimeProcessed:nonneg
    remove_source_from_basket/1 % +Source
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

%! pending_source(?Source, ?TimeAdded:nonneg) is nondet.

:- persistent(pending_source(source,time_added:nonneg)).

%! processed_source(?Source, ?TimeAdded:nonneg, ?TimeProcessed:nonneg) is nondet.

:- persistent(
  processed_source(source,time_added:nonneg,time_processed:nonneg)
).

:- db_add_novel(user:prolog_file_type(log, logging)).

:- initialization(lod_basket).



%! add_source_to_basket(+Source) is det.

add_source_to_basket(Source, Coord):-
  with_mutex(lod_baqsket,
    add_source_to_basket_under_mutex(Source)
  ).

add_source_to_basket_under_mutex(Source):-
  processed_url(Source, _, _), !,
  print_message(informational, already_processed(Source)).
add_source_to_basket_under_mutex(Source):-
  pending_url(Source, _), !,
  print_message(informational, already_pending(Source)).
add_source_to_basket_under_mutex(Source):-
  get_time(TimeAdded),
  assert_pending_url(Source, TimeAdded).


%! current_pending_source(?Source, ?TimeAdded:nonneg) is nondet.

current_pending_source(Source, TimeAdded):-
  with_mutex(lod_baqsket,
    pending_source(Source, TimeAdded)
  ).


%! current_processed_source(?Source, ?TimeAdded:nonneg, ?TimeProcessed:nonneg) is nondet.

current_processed_source(Source, TimeAdded, TimeProcessed):-
  with_mutex(lod_baqsket,
    processed_source(Source, TimeAdded, TimeProcessed)
  ).


% remove_source_from_basket(+Source) is det.

remove_source_from_basket(Source):-
  with_mutex(lod_baqsket, (
    retractall_pending_source(Source, TimeAdded),
    get_time(TimeProcessed),
    assert_processed_source(Source, TimeAdded, TimeProcessed)
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

prolog:message(already_pending(Source)) -->
  cannot_add(Source),
  ['already pending'].

prolog:message(already_processed(Source)) -->
  cannot_add(Source),
  ['already processed'].

cannot_add(Source) -->
  ['Source ~w cannot be added to the pool: '-[Source]].

