:- module(conf_lod_laundromat, []).

/** <module> LOD Laundromat
*/

:- use_module(library(http/http_host)).

:- set_setting(http:public_host, 'cliopatria.lod.labs.vu.nl').
:- set_setting(http:public_scheme, http).

:- dynamic
    user:file_search_path/2.

:- multifile
    user:file_search_path/2.

user:file_search_path(web, cpack('LOD-Laundromat'/web)).
user:file_search_path(img, web(img)).

:- use_module(cpack('LOD-Laundromat'/lod_basket)).
:- use_module(cpack('LOD-Laundromat'/lod_laundromat)).

cliopatria:menu_item(600=places/basket, 'LOD Basket').

:- use_module(library(debug)).

:- debug(http(parse)).
:- debug(lod_basket(_)).
:- debug(lod_laundromat(_)).
:- debug(rdf(_)).
:- debug(sparql(_)).
