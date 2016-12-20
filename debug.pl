:- use_module(library(debug)).

:- dynamic
    currently_debugging0/1.

:- multifile
    currently_debugging0/1.

currently_debugging0('a27d8c6ed29512294e4beabb87a250fd'). % INSTANTIATION EROR

%:- debug(es_api).
%:- debug(http_io).
%:- debug(io).
:- debug(lclean).
:- debug(rdf__io).
:- debug(seedlist(_)).
:- debug(wm(_)).
