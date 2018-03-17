:- module(
  analyze,
  [
    empty/2,
    test/1,
    analyze/1, % -Pair:pair(string)
    status/0
  ]
).

/** <module> Analyze

@author Wouter Beek
@version 2018
*/

:- use_module(library(aggregate)).
:- use_module(library(debug)).
:- use_module(library(readutil)).
:- use_module(library(settings)).
:- use_module(library(uri)).
:- use_module(library(zlib)).

:- use_module(library(tapir)).

:- nodebug(analyze).

test(Pair) :-
  analyze(Pair),
  Pair = _Dataset-Error,
  \+ known_error(Error).

known_error(error(domain_error(set_cookie,_Value),_)).
known_error(error(existence_error(turtle_prefix,_Prefix),stream(_Stream,_X,_Y,_Z))).
known_error(error(http_status(_Status,_Msg),_Uri)).
known_error(error(socket_error(_Msg),_)).
known_error(error(syntax_error(_Msg),stream(_Stream,_X,_Y,_Z))).
known_error(error(timeout_error(read, _Stream),_Context)).
known_error(http(max_redirect(_N,_Uris))).
known_error(http(no_content_type, _Uri)).
known_error(http(redirect_loop(_Uri))).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#date',_)).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#dateTime',_)).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#decimal',_)).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#double',_)).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#float',_)).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gDay',_)).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gMonth',_)).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gMonthDay',_)).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gYear',_)).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#int',_)).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#integer',_)).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#nonNegativeInteger',_)).
known_error(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#time',_)).
known_error(missing_language_tag(_LTag)).
known_error(non_canonical_language_tag(_LTag)).
known_error(no_image(_Url)).
known_error(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#dateTime',_,_)).
known_error(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#decimal',_,_)).
known_error(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#double',_,_)).
known_error(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#float',_,_)).
known_error(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#int',_,_)).
known_error(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#integer',_,_)).
known_error(rdf(non_rdf_format(_Uri-_Entry,_Content))).
known_error(rdf(redefined_id(_Term))).
known_error(rdf(unparsed(_Dom))).
known_error(rdf(unsupported_format(_MediaType,_Content))).
known_error(sgml(sgml_parser(_Parser),_File,_Line,_Msg)).
known_error(unclear_encoding(_Encodings,_Encoding)).

analyze(Pair) :-
  setup_call_cleanup(
    gzopen('/scratch/wbeek/datahub/err.log.gz', read, In),
    analyze(In, Pair),
    close(In)
  ).

analyze(In, Dataset-Error) :-
  repeat,
  read_line_to_string(In, Line),
  (   Line == end_of_file
  ->  !
  ;   debug(analyze, "~s", [Line]),
      split_string(Line, "\t", "", [H|T]),
      atom_string(Dataset, H),
      atomics_to_string(T, String),
      read_term_from_atom(String, Error, [])
  ).

empty(Uri, Dict) :-
  tapir:user_(Site, User),
  dataset(Site, User, Dataset, _),
  dataset(Site, User, Dataset, Dict),
  _{openJobs: [], statements: N} :< Dict,
  N =< 0,
  tapir:host__(Site, Host0),
  atomic_list_concat([_|T], ., Host0),
  atomic_list_concat(T, ., Host),
  atomic_list_concat(['',User,Dataset], /, Path),
  uri_components(Uri, uri_components(https,Host,Path,_,_)).

status :-
  tapir:user_(Site, User),
  aggregate_all(
    sum(N),
    (
      dataset(Site, User, _, Dict),
      _{statements: N} :< Dict
    ),
    N
  ),
  format(user_output, "~D\n", [N]).
