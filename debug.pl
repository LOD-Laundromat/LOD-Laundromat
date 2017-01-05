:- use_module(library(debug)).

:- dynamic
    currently_debugging0/1.

:- multifile
    currently_debugging0/1.

% io_error
currently_debugging0('e7bade8cb5d9808b7322b8604c7a1f5f').

%:- debug(es_api).
%:- debug(http(send_request)).
%:- debug(http(reply)).
%:- debug(http_io).
:- debug(io(close)).
:- debug(io(open)).
:- debug(lclean).
:- debug(rdf__io).
:- debug(seedlist(_)).
:- debug(wm(_)).
