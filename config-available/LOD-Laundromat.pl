:- module('conf_LOD-Laundromat', []).

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

:- reexport(cpack('LOD-Laundromat'/laundromat_fs)).
:- reexport(cpack('LOD-Laundromat'/laundromat_hdt)).
:- use_module(cpack('LOD-Laundromat'/seedlist_http)).
:- use_module(cpack('LOD-Laundromat'/washing_machine_http)).

cliopatria:menu_item(600=places/seedlist, 'Seedist').
cliopatria:menu_item(700=places/data, 'Data').

:- use_module(library(debug)).

:- debug(http(parse)).
%:- debug(http(raw)).
:- debug(rdf(clean)).
%:- debug(rdf(grid)).
%:- debug(seedlist(_)).
%:- debug(sparql(_)).
:- debug(wm(thread)).
