:- module(
  lod_basket,
  [
    add_to_lod_basket/1, % +Url:url
    current_pending_url/2, % ?Url:url
                           % ?TimeAdded:nonneg
    current_processed_url/3, % ?Url:url
                             % ?TimeAdded:nonneg
                             % ?TimeProcessed:nonneg
    remove_url_from_pool/1 % +Url:url
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

%! pending_url(?Url:url, ?TimeAdded:nonneg) is nondet.

:- persistent(pending_url(url:atom,coord:list(nonneg),time_added:nonneg)).

%! processed_url(?Url:url, ?TimeAdded:nonneg, ?TimeProcessed:nonneg) is nondet.

:- persistent(
  processed_url(
    url:atom,
    coord:list(nonneg),
    time_added:nonneg,
    time_processed:nonneg
  )
).

:- db_add_novel(user:prolog_file_type(log, logging)).

:- initialization(init_url_pool).



%! add_to_lod_basket(+Url:url, +Coodinate:list(nonneg)) is det.

add_to_lod_basket(Url, Coord):-
  with_mutex(url_pool,
    add_url_to_pool_under_mutex(Url, Coord)
  ).

add_url_to_pool_under_mutex(Url, Coord):-
  processed_url(Url, _, _), !,
  print_message(informational, already_processed(Url)).
add_url_to_pool_under_mutex(Url):-
  pending_url(Url, _), !,
  print_message(informational, already_pending(Url)).
add_url_to_pool_under_mutex(Url):-
  get_time(TimeAdded),
  assert_pending_url(Url, TimeAdded).


%! current_pending_url(?Url:url, ?TimeAdded:nonneg) is nondet.

current_pending_url(Url, TimeAdded):-
  with_mutex(url_pool,
    pending_url(Url, TimeAdded)
  ).


%! current_processed_url(
%!   ?Url:url,
%!   ?TimeAdded:nonneg,
%!   ?TimeProcessed:nonneg
%! ) is nondet.

current_processed_url(Url, TimeAdded, TimeProcessed):-
  with_mutex(url_pool,
    processed_url(Url, TimeAdded, TimeProcessed)
  ).


% remove_url_from_pool(+Url:url) is det.

remove_url_from_pool(Url):-
  with_mutex(url_pool, (
    retractall_pending_url(Url, TimeAdded),
    get_time(TimeProcessed),
    assert_processed_url(Url, TimeAdded, TimeProcessed)
  )).



% Initialization

init_url_pool:-
  absolute_file_name(data(url_pool), File, [access(write),file_type(logging)]),
  safe_db_attach(File).

%! safe_db_attach(+File:atom) is det.

safe_db_attach(File):-
  exists_file(File), !,
  db_attach(File, []).
safe_db_attach(File):-
  touch_file(File),
  safe_db_attach(File).



% Messages

prolog:message(already_pending(Url)) -->
  cannot_add(Url),
  ['already pending'].

prolog:message(already_processed(Url)) -->
  cannot_add(Url),
  ['already processed'].

cannot_add(Url) -->
  ['Url ',Url,' cannot be added to the pool: '].

