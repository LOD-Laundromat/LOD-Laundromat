:- module(
  lwm_basket,
  [
    pick_pending/1, % +Md5:atom
    pick_unpacked/1 % +Md5:atom
  ]
).

/** <module> LOD Laundromat: basket

The LOD basket for URLs that are to be processed by the LOD Washing Machine.

~~~{.sh}
$ curl --data "url=http://acm.rkbexplorer.com/id/998550" http://lodlaundry.wbeek.ops.few.vu.nl/lwm/basket
~~~

@author Wouter Beek
@version 2014/05-2014/06, 2014/08
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(semweb/rdf_db)).

:- use_module(generics(meta_ext)).

:- use_module(plSparql_query(sparql_query_api)).
:- use_module(plSparql_update(sparql_update_api)).

:- use_module(lwm(lwm_settings)).
:- use_module(lwm(lwm_sparql_query)).
:- use_module(lwm(store_triple)).



% pick_pending(-Md5:atom) is det.

pick_pending(Md5):-
  with_mutex(lwm_endpoint, (
    md5_pending(Md5),

    % For the debug tools to work,
    % details from the LOD Basket have to be copied over.
    (   debugging(lwm)
    ->  lod_basket_graph(BasketGraph),
        loop_until_true(
          sparql_select(
            virtuoso_query,
            [ll],
            [p,o],
            [rdf(ll:Md5,var(p),var(o))],
            Result,
            [default_graph(BasketGraph),distinct(true)]
          )
        ),
        rdf_global_id(ll:Md5, S),
        maplist(pair_to_triple(S), Result, Triples),
        lwm_version_graph(NG),
        loop_until_true(
          sparql_insert_data(cliopatria_update, Triples, [NG], [])
        )
    ;   true
    ),

    store_start_unpack(Md5)
  )).

pair_to_triple(S, [P,O], rdf(S,P,O)).


% pick_unpacked(-Md5:atom) is det.

pick_unpacked(Md5):-
  with_mutex(lwm_endpoint, (
    md5_unpacked(Md5),
    store_start_clean(Md5)
  )).

