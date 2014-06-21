:- module(
  lod_basket,
  [
    add_to_basket/1, % +Url:url
    is_cleaned/1, % +Md5:atom
    is_pending/1, % +Md5:atom
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

:- use_module(library(semweb/rdf_db)).

:- use_module(plRdf_term(rdf_literal)).

:- use_module(lwm(lwm_generics)).
:- use_module(lwm(lwm_store_triple)).



%! add_to_basket(+Source) is det.

add_to_basket(Url):-
  with_mutex(lod_basket, (
    rdf_atom_md5(Url, 1, Md5),
    (
      is_cleaned(Md5)
    ->
      print_message(informational, already_processed(Url))
    ;
      is_pending(Md5)
    ->
      print_message(informational, already_pending(Url))
    ;
      store_url(Md5, Url)
    )
  )).


%! get_cleaned(-Md5:atom) is nondet.

get_cleaned(Md5):-
  with_mutex(lod_basket, (
    once(lwm_endpoint(Endpoint)),
    lwm_sparql_select(Endpoint, [lwm], [md5],
        [rdf(var(md5res),lwm:end,var(end)),
         rdf(var(md5res),lwm:md5,var(md5))], [[Literal]], [limit(1)]),
    rdf_literal(Literal, Md5, _)
  )).


%! get_pending(-Md5:atom) is nondet.

get_pending(Md5):-
  with_mutex(lod_basket, (
    once(lwm_endpoint(Endpoint)),
    catch(
      lwm_sparql_select(Endpoint, [lwm], [md5],
          [rdf(var(md5res),lwm:added,var(added)),
           not([rdf(var(md5res),lwm:end,var(end))]),
           not([rdf(var(md5res),lwm:start,var(start))]),
           rdf(var(md5res),lwm:md5,var(md5))],
          [[Literal]], [limit(1)]),
      error(socket_error('Connection refused'),_),
      fail
    ),
    rdf_literal(Literal, Md5, _)
  )).


%! is_cleaned(+Md5:atom) is semidet.

is_cleaned(Md5):-
  with_mutex(lod_basket, (
    once(lwm_endpoint(Endpoint)),
    lwm_sparql_ask(Endpoint, [lwm],
        [rdf(var(md5),lwm:md5,literal(xsd:string,Md5)),
         rdf(var(md5),lwm:end,var(end))], [])
  )).


%! is_pending(+Md5:atom) is semidet.

is_pending(Md5):-
  with_mutex(lod_basket, (
    once(lwm_endpoint(Endpoint)),
    lwm_sparql_ask(Endpoint, [lwm],
        [rdf(var(md5),lwm:md5,literal(xsd:string,Md5)),
         rdf(var(md5),lwm:added,var(added)),
         not([rdf(var(md5),lwm:start,var(start))])], [])
  )).


% remove_from_basket(-Md5:atom) is det.

remove_from_basket(Md5):-
  with_mutex(lod_basket, (
    get_pending(Md5),
    store_start(Md5)
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

