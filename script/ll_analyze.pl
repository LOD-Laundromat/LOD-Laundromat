:- module(
  ll_analyze,
  [
    empty_dataset/2, % -Dataset, -Dict
    error/1,         % -Error
    print_status/0,
    unknown_error/1  % -Error
  ]
).

/** <module> Analyze LOD Laundromat error logs

@author Wouter Beek
@version 2018
*/

:- use_module(library(aggregate)).
:- use_module(library(debug)).
:- use_module(library(readutil)).
:- use_module(library(settings)).
:- use_module(library(uri)).
:- use_module(library(yall)).
:- use_module(library(zlib)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(tapir)).

:- dynamic
    known_error_/1.

:- nodebug(analyze).

:- initialization
   init_ll_analyze.

:- setting(temporary_directory, any, _, "").





%! empty_dataset(-Dataset:atom, -Dict:dict) is nondet.

empty_dataset(Dataset, Dict) :-
  dataset(_, Dataset, Dict),
  _{openJobs: [], statements: N} :< Dict,
  N =< 0.



%! error(-Error:pair(atom,compound)) is nondet.

error(Error) :-
  setting(temporary_directory, Dir),
  directory_file_path(Dir, 'err.log.gz', File),
  call_stream_file(File, error_(Error)).

error_(Dataset-Error, In) :-
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



%! unknown_error(-Error:pair(atom,compound)) is nondet.

unknown_error(Error) :-
  error(Error),
  Error = _-Error0,
  \+ known_error_(Error0).

%known_error_(error(domain_error(set_cookie,_Value),_)).
%%%%known_error_(error(domain_error(url,_),_)). % TBD: ?
%known_error_(error(existence_error(turtle_prefix,_Prefix),stream(_Stream,_X,_Y,_Z))).
known_error_(error(http_status(400,_Msg),_Uri)).
known_error_(error(http_status(404,_Msg),_Uri)).
known_error_(error(http_status(406,_Msg),_Uri)).
known_error_(error(http_status(500,_Msg),_Uri)).
%%%%known_error_(error(io_error(read,_Stream),_Context)). % TBD: ?
known_error_(error(socket_error('Connection timed out'),_)).
known_error_(error(socket_error('Host not found'),_)).
known_error_(error(syntax_error('EOF in string'),_Stream)).
known_error_(error(syntax_error('Illegal character in uriref'),_Stream)).
known_error_(error(syntax_error('Illegal control character in uriref'),_Stream)).
known_error_(error(syntax_error('illegal escape'),_Stream)).
%known_error_(error(timeout_error(read, _Stream),_Context)).
%known_error_(http(max_redirect(_N,_Uris))).
%known_error_(http(no_content_type, _Uri)).
%known_error_(http(redirect_loop(_Uri))).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#date',_)).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#dateTime',_)).
%known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#decimal',_)).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#double',_)).
%known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#float',_)).
%known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gDay',_)).
%known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gMonth',_)).
%known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gMonthDay',_)).
%known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gYear',_)).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#int',_)).
%known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#integer',_)).
%known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#nonNegativeInteger',_)).
%known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#time',_)).
%known_error_(missing_language_tag(_LTag)).
%known_error_(non_canonical_language_tag(_LTag)).
%known_error_(no_image(_Url)).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#boolean',_,_)).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#dateTime',_,_)).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#decimal',_,_)).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#double',_,_)).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#float',_,_)).
%known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#int',_,_)).
%known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#integer',_,_)).
known_error_(rdf(non_rdf_format(_Uri-_Entry,_Content))).
%known_error_(rdf(redefined_id(_Term))).
known_error_(rdf(unexpected(_Tag, _Parser))). % TBD: Enable this after looking into RSS.
%known_error_(rdf(unparsed(_Dom))).
known_error_(rdf(unsupported_format(_MediaType,_Content))).
known_error_(sgml(sgml_parser(_Parser),_File,_Line,_Msg)).
known_error_(unclear_encoding(_Encodings,_Encoding)).



%! print_status is det.
%
% Prints the current status of the LOD-Laundromat.

print_status :-
  aggregate_all(
    sum(N),
    (
      dataset(_, _, Dict),
      _{statements: N} :< Dict
    ),
    N
  ),
  format("~D\n", [N]).





% INITIALIZATION %

init_ll_analyze :-
  conf_json(Conf),
  set_setting(temporary_directory, Conf.'data-directory').
