:- module(
  lwm_continue,
  [
    lwm_continue/0
  ]
).

/** <module> LOD Washing Machine: Continue

Continues an interrupted LOD Washing Machine crawl.

@author Wouter Beek
@version 2014/09, 2015/01-2015/03
*/

:- use_module(library(apply)).
:- use_module(library(lists), except([delete/3,subset/2])).
:- use_module(library(semweb/rdf_db), except([rdf_node/1])).
:- use_module(library(ordsets)).

:- use_module(plc(process/list_script)).

:- use_module(lwm(lwm_reset)).
:- use_module(lwm(query/lwm_sparql_query)).





%! lwm_continue is det.

lwm_continue:-
  datadoc_unpacking(L1),
  datadoc_cleaning(L2),
  debug_datadocs(L3),
  findall(
    X2,
    (
      erroneous_datadocs0(X1),
      flatten(X1, X2)
    ),
    Ls
  ),
  maplist(list_to_ord_set, [L1,L2,L3|Ls], Sets),
  ord_union(Sets, Set),
  list_script(
    reset_datadoc,
    Set,
    [message('LWM Reset'),with_mutex(lwm_endpoint_access)]
  ).



%! debug_datadocs(-Datadocs:list(atom)) is det.

debug_datadocs(L):-
  findall(
    Datadoc,
    (
      debug:debug_md5(Md5, _),
      rdf_global_id(ll:Md5, Datadoc)
    ),
    L
  ).



erroneous_datadocs0([]).
/*
% Unrecognized RDF format.
erroneous_datadocs0(L):-
  lwm_sparql_select(
    [error,llo],
    [datadoc],
    [rdf(var(datadoc), llo:serializationFormat, error:unrecognizedFormat)],
    L,
    []
  ).
*/
% fedBench
erroneous_datadocs0(Datadocs):-
  findall(
    Datadoc,
    (
      erroneous_md5(Md5),
      rdf_global_id(ll:Md5, Datadoc)
    ),
    Datadocs
  ).

erroneous_md5('6c7100daab8122f770ae9feaa45fc47c').
erroneous_md5('d1e641a73d97429e3fe5cd5e2d8dd37c').
erroneous_md5('03f6f715d4339d7a62d0b693634f1cbc').
erroneous_md5('7d24b8cc9fb5255d8d53dee3138c56d3').
erroneous_md5('1c4c8920a6e136d6bf2f050d31d78cd2').
erroneous_md5('6c5e3bba18d848704c882f23f21813fd').
erroneous_md5('69801d94cb75c15bfda0d8265b6e7d75').
erroneous_md5('d11b99e99ca29ec3dd4231f4681a3b7b').
erroneous_md5('31d1024d3cfbdb30358c208e8b09ba64').
erroneous_md5('f4285af57cb8152a874fdf611bf875f9').
erroneous_md5('b5075134338585f5f5ff47299a8bda30').
erroneous_md5('65a2b25a4df5b593ce1c4a7a80d7b5b6').
erroneous_md5('c26458fd28f02e90292e60565d2ce427').
erroneous_md5('4fbd5c29adbbaba306fcdf1f9012556e').
erroneous_md5('51631129c96af54f69792459a6395ee0').
erroneous_md5('6108b9db4f7ffe62ad52d62c9bc2e3db').
erroneous_md5('c951af2cd6bd89917c0ff08185e0db33').
erroneous_md5('0a54e7b9edc759c8d78d15e649c39dd1').
erroneous_md5('d1153876a11e0ff78023ab43e708c843').
erroneous_md5('69bac2bdcf003789787e6ac0a0a292a4').
erroneous_md5('7c034cdabbe0d82eda31ea22d353d23e').
erroneous_md5('a6c84464231c829eeb951dd9e69634d5').
erroneous_md5('65c8fe18943bfb26667c8d428f3d52a5').
