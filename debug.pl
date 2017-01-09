:- use_module(library(debug)).

:- dynamic
    currently_debugging0/1.

:- multifile
    currently_debugging0/1.

currently_debugging0('c1f0bedf6d6ef52c78c7b6a2a14f2773').

%:- debug(es_api).
%:- debug(http(send_request)).
%:- debug(http(reply)).
%:- debug(http_io).
%:- debug(io(close)).
%:- debug(io(open)).
%:- debug(seedlist(_)).
% @tbd Document that ‘wm(idle)’ overrules ‘wm(_)’.
:- debug(wm(_)).
%:- debug(wm(finish)).
%:- debug(wm(idle)).
