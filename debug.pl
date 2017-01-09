:- use_module(library(debug)).

:- dynamic
    currently_debugging0/1.

:- multifile
    currently_debugging0/1.

currently_debugging0('121c7500fbf827c064e7affe0d517c69').
currently_debugging0('5eb6e64653740aac40a4ca9a32544f6d').
currently_debugging0('beea23e94e0f9b967d6e9ad5b94fc7ec').

%:- debug(es_api).
%:- debug(http(send_request)).
%:- debug(http(reply)).
%:- debug(http_io).
%:- debug(io(close)).
%:- debug(io(open)).
%:- debug(seedlist(_)).
% @tbd Document that ‘wm(idle)’ overrules ‘wm(_)’.
%:- debug(wm(_)).
