:- use_module(library(debug)).

:- dynamic
    currently_debugging0/1.

:- multifile
    currently_debugging0/1.

currently_debugging0('90e6e385cf478520091db54dac0aa5b8'). % INSTANTIATION EROR
currently_debugging0('a27d8c6ed29512294e4beabb87a250fd'). % INSTANTIATION EROR

%:- debug(es_api).
%:- debug(http_io).
%:- debug(io).
%:- debug(lclean).
%:- debug(seedlist(_)).
%:- debug(wm(_)).
