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

:- use_module(lwm(lwm_sparql_query)).
:- use_module(lwm(store_triple)).



% pick_pending(-Md5:atom) is det.

pick_pending(Md5):-
  with_mutex(lwm_basket, (
    md5_pending(Md5),
    store_start_unpack(Md5)
  )).


% pick_unpacked(-Md5:atom) is det.

pick_unpacked(Md5):-
  with_mutex(lwm_basket, (
    md5_unpacked(Md5),
    store_start_clean(Md5)
  )).
