:- module(
  lod_basket,
  [
    add_source_to_basket/1, % +Source
    cleaned_source/1, % -Md5:atom
    pending_source/1, % -Md5:atom
    remove_source_from_basket/1 % +Md5:atom
  ]
).

/** <module> LOD basket

The LOD basket for URLs that are to be processed by the LOD Washing Machine.

~~~{.sh}
$ curl --data "url=http://acm.rkbexplorer.com/id/998550" http://lodlaundry.wbeek.ops.few.vu.nl/lwm/basket
~~~

@author Wouter Beek
@version 2014/05-2014/06
*/

:- use_module(sparql(sparql_api)).

:- use_module(plRdf_term(rdf_literal)).

:- use_module(lwm(lwm_db)).
:- use_module(lwm(lwm_generics)).
:- use_module(lwm(lwm_store_triple)).
:- use_module(lwm(noRdf_store)).



%! add_source_to_basket(+Source) is det.

add_source_to_basket(Source):-
  with_mutex(lod_basket, add_source_to_basket_under_mutex(Source)).

add_source_to_basket_under_mutex(Source):-
  is_processed_source(Source), !,
  print_message(informational, already_processed(Source)).
add_source_to_basket_under_mutex(Source):-
  is_pending_source(Source), !,
  print_message(informational, already_pending(Source)).
add_source_to_basket_under_mutex(Source):-
  source_to_md5(Source, Md5),
  store_source(Md5, Source),
  post_rdf_triples.


%! cleaned_source(-Md5:atom) is nondet.

cleaned_source(Md5):-
  with_mutex(lod_basket, cleaned_source_under_mutex(Md5)).

cleaned_source_under_mutex(Md5):-
  once(lwm_endpoint(Endpoint)),
  sparql_select(Endpoint, _, [lwm], true, [md5],
      [rdf(var(md5res),lwm:lwm_end,var(end)),
       rdf(var(md5res),lwm:md5,var(md5))], 1, 0, _, [[Literal]]),
  rdf_literal(Literal, Md5, _).



%! pending_source(-Md5:atom) is nondet.

pending_source(Md5):-
  with_mutex(lod_basket, pending_source_under_mutex(Md5)).

pending_source_under_mutex(Md5):-
  once(lwm_endpoint(Endpoint)),
  sparql_select(Endpoint, _, [lwm], true, [md5],
      [rdf(var(md5res),lwm:added,var(added)),
       not([rdf(var(md5res),lwm:lwm_start,var(start))]),
       rdf(var(md5res),lwm:md5,var(md5))],
      1, 0, _, [[Literal]]),
  rdf_literal(Literal, Md5, _).


%! is_pending_source(+Source) is semidet.

is_pending_source(Url-EntryPath):- !,
  once(lwm_endpoint(Endpoint)),
  sparql_ask(Endpoint, _, [lwm],
      [rdf(var(md5_url),lwm:url,Url),
       rdf(var(md5_url),lwm:has_entry,var(md5_entry)),
       rdf(var(md5_entry),lwm:path,literal(xsd:string,EntryPath)),
       rdf(var(md5_entry),lwm:added,var(added))]).
is_pending_source(Url):-
  once(lwm_endpoint(Endpoint)),
  sparql_ask(Endpoint, _, [lwm],
      [rdf(var(md5),lwm:url,Url),
       rdf(var(md5),lwm:added,var(added))]).


%! is_processed_source(+Source) is semidet.

is_processed_source(Url-EntryPath):- !,
  once(lwm_endpoint(Endpoint)),
  sparql_ask(Endpoint, _, [lwm],
      [rdf(var(md5_url),lwm:url,Url),
       rdf(var(md5_url),lwm:has_entry,var(md5_entry)),
       rdf(var(md5_entry),lwm:path,literal(xsd:string,EntryPath)),
       rdf(var(md5_entry),lwm:lwm_end,var(end))]).
is_processed_source(Url):-
  once(lwm_endpoint(Endpoint)),
  sparql_ask(Endpoint, _, [lwm],
      [rdf(var(md5),lwm:url,Url),
       rdf(var(md5),lwm:lwm_end,var(end))]).


% remove_source_from_basket(+Md5:atom) is det.

remove_source_from_basket(Md5):-
  with_mutex(lod_baqsket, (
    store_lwm_start(Md5),
    post_rdf_triples
  )).



% Messages

prolog:message(already_pending(Md5)) -->
  cannot_add(Md5),
  ['already pending'].

prolog:message(already_processed(Md5)) -->
  cannot_add(Md5),
  ['already processed'].

cannot_add(Md5) -->
  ['MD5 ~w cannot be added to the pool: '-[Md5]].

