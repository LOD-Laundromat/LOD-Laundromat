:- use_module(library(debug)).

:- dynamic
    currently_debugging0/1.

:- multifile
    currently_debugging0/1.

% error(existence_error(http_reply,'http://localhost:8890/sparql?query=define%20sql%3Adescribe-mode%20%22CBD%22%20%20DESCRIBE%20%3Chttp%3A%2F%2Fbio2rdf.org%2Fhgnc%3Ahxk1%3E&format=text%2Fturtle'),_11684)
currently_debugging0('7f694f3eda71367fe1465be11403c221').
% error(instantiation_error,context(prolog_stack([frame(67,call(system:atom_string/2),atom_string(_838,_840)),…
currently_debugging0('e93cfd844c26bf9f4a0df640720eae6b').

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
