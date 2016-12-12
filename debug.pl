:- use_module(library(debug)).

:- dynamic
    currently_debugging0/1.

:- multifile
    currently_debugging0/1.

currently_debugging0('2fe28e9cd18dd28c3df8d19e67260cb3'). % GLOBAL STACK
%currently_debugging0('b5fbc0e6251553444938a40057b8e208').

%:- debug(es_api).
%:- debug(lclean).
%:- debug(seedlist(_)).
%:- debug(wm(_)).
