:- module(conf_lod_laundromat, []).

/** <module> LOD Laundromat
*/

:- use_module(cpack('LOD-Laundromat'/lod_basket)).
:- use_module(cpack('LOD-Laundromat'/lod_laundromat)).

cliopatria:menu_item(600=places/basket, 'LOD Basket').
