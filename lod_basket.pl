:- module(
  lod_basket,
  [
    add_to_basket/1, % +Url:url
    get_cleaned/1, % -Md5:atom
    get_pending/1, % -Md5:atom
    remove_from_basket/1 % +Md5:atom
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



%! add_to_basket(+Source) is det.

add_to_basket(Url):-
  with_mutex(lod_basket, (
    source_to_md5(Url, Md5),
    (
      is_cleaned(Md5)
    ->
      print_message(informational, already_processed(Url))
    ;
      is_pending(Md5)
    ->
      print_message(informational, already_pending(Url))
    ;
      store_url(Md5, Url),
      post_rdf_triples
    )
  )).


%! get_cleaned(-Md5:atom) is nondet.

get_cleaned(Md5):-
  with_mutex(lod_basket, (
    once(lwm_endpoint(Endpoint)),
    sparql_select(Endpoint, _, [lwm], true, [md5],
        [rdf(var(md5res),lwm:lwm_end,var(end)),
         rdf(var(md5res),lwm:md5,var(md5))], 1, 0, _, [[Literal]]),
    rdf_literal(Literal, Md5, _)
  )).


%! get_pending(-Md5:atom) is nondet.

get_pending(Md5):-
  with_mutex(lod_basket, (
    once(lwm_endpoint(Endpoint)),
    sparql_select(Endpoint, _, [lwm], true, [md5],
        [rdf(var(md5res),lwm:added,var(added)),
         not([rdf(var(md5res),lwm:lwm_start,var(start))]),
         rdf(var(md5res),lwm:md5,var(md5))],
        1, 0, _, [[Literal]]),
    rdf_literal(Literal, Md5, _)
  )).


%! is_cleaned(+Md5:atom) is semidet.

is_cleaned(Md5):-
  with_mutex(lod_basket, (
    once(lwm_endpoint(Endpoint)),
    sparql_ask(Endpoint, _, [lwm],
        [rdf(var(md5),lwm:md5,literal(type(xsd:string,Md5))),
         rdf(var(md5),lwm:lwm_end,var(end))])
  )).


%! is_pending(+Md5:atom) is semidet.

is_pending(Md5):-
  with_mutex(lod_basket, (
    once(lwm_endpoint(Endpoint)),
    sparql_ask(Endpoint, _, [lwm],
        [rdf(var(md5),lwm:md5,literal(type(xsd:string,Md5))),
         rdf(var(md5),lwm:added,var(added)),
         not([rdf(var(md5),lwm:lwm_start,var(start))])])
  )).


% remove_from_basket(+Md5:atom) is det.

remove_from_basket(Md5):-
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

