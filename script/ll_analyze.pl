:- module(
  ll_analyze,
  [
    empty_dataset/2, % -Dataset, -Dict
    error/1,         % -Error
    print_errors/0,
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



%! print_errors is det.

print_errors :-
  forall(
    catch(error(_-Error), _, fail),
    (known_error_(Error, Flag) -> flag(Flag, N, N+1) ; true)
  ),
  format("---\n"),
  forall(
    known_error_(_, Flag),
    (
      flag(Flag, N, N),
      format("~D\t~a\n", [N,Flag])
    )
  ).



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



%! unknown_error(-Error:pair(atom,compound)) is nondet.

unknown_error(Error) :-
  error(Error),
  Error = _-Error0,
  \+ known_error_(Error0, _).

% archive error
known_error_(error(archive_error(2, 'Missing type keyword in mtree specification'),_Context), mtree).
known_error_(error(archive_error(29, 'Missing type keyword in mtree specification'),_Context), mtree).
known_error_(error(archive_error(104, 'Truncated input file (needed 444997632 bytes, only 0 available)'),_Context), trancated_archive).
known_error_(error(domain_error(http_encoding,identity),_Context), http_encoding).
known_error_(error(domain_error(set_cookie,_Value),_Context), http_cookie).
known_error_(error(domain_error(url,_Url),_Context), url).
known_error_(error(existence_error(turtle_prefix,_Prefix),_Stream), ttl_prefix).
known_error_(error(io_error(read,_Stream),_Context), io_error).
% socket error
known_error_(error(socket_error('Connection refused'),_), connection_refused).
known_error_(error(socket_error('Connection reset by peer'),_), connection_reset).
known_error_(error(socket_error('Connection timed out'),_), connection_timed_out).
known_error_(error(socket_error('Host not found'),_), host_not_found).
known_error_(error(socket_error('No Data'),_), no_data).
known_error_(error(socket_error('No Recovery'),_), no_recovery).
known_error_(error(socket_error('No route to host'),_), no_route_to_host).
known_error_(error(socket_error('Try Again'),_), try_again).
% syntax error
known_error_(error(syntax_error('end-of-line expected'),_Stream), eol_expected).
known_error_(error(syntax_error('End of statement expected'),_Stream), eos_expected).
known_error_(error(syntax_error('EOF in string'),_Stream), eof_in_string).
known_error_(error(syntax_error('Expected ":"'),_Stream), expected_colon).
known_error_(error(syntax_error('Expected "]"'),_Stream), expected_bracket).
known_error_(error(syntax_error('Expected ":" after "_"'),_Stream), expected_bnode).
known_error_(error(syntax_error(http_parameter(_Param)),_), http_parameter).
known_error_(error(syntax_error('Illegal \\-escape'),_Stream), illegal_backslash_escape).
known_error_(error(syntax_error('Illegal character in uriref'),_Stream), illegal_char_uriref).
known_error_(error(syntax_error('Illegal control character in uriref'),_Stream), illegal_control_char_uriref).
known_error_(error(syntax_error('illegal escape'),_Stream), illegal_escape).
known_error_(error(syntax_error('Illegal IRIREF'),_Stream), illegal_iriref).
known_error_(error(syntax_error('Invalid @prefix directive'),_Stream), invalid_prefix_directive).
known_error_(error(syntax_error('newline in string'),_Stream), newline_in_string).
known_error_(error(syntax_error('PN_PREFIX expected'),_Stream), pn_prefix_expected).
known_error_(error(syntax_error('predicate expected'),_Stream), predicate_expected).
known_error_(error(syntax_error('predicate not followed by whitespace'),_Stream), predicate_whitespace).
known_error_(error(syntax_error('subject expected'),_Stream), subject_expected).
known_error_(error(syntax_error('subject not followed by whitespace'),_Stream), subject_whitespace).
known_error_(error(syntax_error('Unexpected "." (missing object)'),_Stream), missing_object).
known_error_(error(syntax_error('Unexpected newline in short string'),_Stream), newline_in_short_string).
known_error_(error(syntax_error('LANGTAG expected'),_Stream), ltag_expected).
known_error_(error(timeout_error(read,_Stream),_Context), timeout).
known_error_(http(max_redirect(_N,_Uris)), http_max_redirect).
known_error_(http(no_content_type,_Uri), http_no_content_type).
% HTTP redirection loops can co-occur with 3xx status codes.
known_error_(http(redirect_loop(_Uri)), http_redirect_loop).
% HTTP status
known_error_(http(status(301),_Uri), http_redirect_moved_permanently).
known_error_(http(status(302),_Uri), http_redirect_no_content).
known_error_(http(status(400),_Uri), http_client_bad_request).
known_error_(http(status(401),_Uri), http_client_unauthorized).
known_error_(http(status(403),_Uri), http_client_forbidden).
known_error_(http(status(404),_Uri), http_client_not_found).
known_error_(http(status(406),_Uri), http_client_not_acceptable).
known_error_(http(status(410),_Uri), http_client_gone).
known_error_(http(status(500),_Uri), http_server_internal_error).
known_error_(http(status(502),_Uri), http_server_bad_gateway).
known_error_(http(status(503),_Uri), http_server_unavailable).
known_error_(http(status(504),_Uri), http_server_gateway_timeout).
known_error_(http(status(522),_Uri), http_server_cloudflare_connection_timeout).
% incorrect literal
known_error_(incorrect_lexical_form('http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral',_), 'XMLLiteral').
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#date',_), date).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#dateTime',_), dateTime).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#decimal',_), decimal).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#double',_), double).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#float',_), float).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gDay',_), gDay).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gMonth',_), gMonth).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gMonthDay',_), gMonthDay).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gYear',_), gYear).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#gYearMonth',_), gYearMonth).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#int',_), int).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#integer',_), integer).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#nonNegativeInteger',_), nonNegativeInteger).
known_error_(incorrect_lexical_form('http://www.w3.org/2001/XMLSchema#time',_), time).
known_error_(io_warning(_Stream,'Illegal UTF-8 continuation'), illegal_utf8_continuation).
known_error_(io_warning(_Stream,'Illegal UTF-8 start'), illegal_utf8_start).
known_error_(missing_language_tag(_LTag), missing_ltag).
% non-canonical literal
known_error_(non_canonical_language_tag(_LTag), noncan_langString).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#boolean',_,_), noncan_boolean).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#dateTime',_,_), noncan_dateTime).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#decimal',_,_), noncan_decimal).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#double',_,_), noncan_double).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#float',_,_), noncan_float).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#int',_,_), noncan_int).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#integer',_,_), noncan_integer).
known_error_(non_canonical_lexical_form('http://www.w3.org/2001/XMLSchema#nonNegativeInteger',_,_), noncan_nonNegativeInteger).
known_error_(not_an_image(_Url), no_image).
known_error_(rdf(non_rdf_format(_Hash,_Content)), non_rdf).
known_error_(rdf(redefined_id(_Term)), redefined_id).
known_error_(rdf(unexpected(_Tag, _Parser)), rdfxml_tag).
known_error_(rdf(unparsed(_Dom)), rdfxml_unparsed).
known_error_(rdf(unsupported_format(_MediaType,_Content)), rdf_support).
known_error_(sgml(sgml_parser(_Parser),_File,_Line,_Msg), rdfxml_sgml).
known_error_(unclear_encoding(_Encodings,_Encoding), stream_encoding).
known_error_(unsupported_license(_License), license).





% INITIALIZATION %

init_ll_analyze :-
  conf_json(Conf),
  set_setting(temporary_directory, Conf.'data-directory').
