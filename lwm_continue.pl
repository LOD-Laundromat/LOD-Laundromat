:- module(
  lwm_continue,
  [
    lwm_continue/0
  ]
).

/** <module> LOD Washing Machine: Continue

Continues an interrupted LOD Washing Machine crawl.

@author Wouter Beek
@version 2014/09
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(filesex)).

:- use_module(plSparql_update(sparql_update_api)).

:- use_module(lwm(lwm_settings)).
:- use_module(lwm(lwm_sparql_query)).
:- use_module(lwm(md5)).



lwm_continue:-
  % Collect zombie MD5s.
  aggregate_all(
    set(Md5),
    (
      md5_unpacking(Md5)
    ;
      md5_cleaning(Md5)
    ),
    Md5s
  ),

  maplist(reset_md5, Md5s).


reset_md5(Md5):-
  % Remove the MD5 directory.
  md5_directory(Md5, Directory),
  delete_directory_and_contents(Directory),

  % Remove the MD5 metadata triples.
  lwm_version_graph(NG),
  md5_describe(Md5, Triples),
  sparql_delete_data(cliopatria_update, Triples, [NG], []).
