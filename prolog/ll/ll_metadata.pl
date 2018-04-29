:- module(
  ll_metadata,
  [
    close_metadata/3,      % +Hash, +PLocal, +Out
    write_meta/2,          % +Hash, :Goal_1
    write_meta_archive/2,  % +Hash, +Metadata
    write_meta_encoding/2, % +Hash, +Encodings
    write_meta_entry/2,    % +Hash1, +Hash2
    write_meta_error/2,    % +Hash, +Error
    write_meta_http/2,     % +Hash, +Metadata
    write_meta_now/2,      % +Hash, +PLocal
    write_meta_quad/4,     % +Hash, +P, +O, +G
    write_meta_quad/5      % +Hash, +S, +P, +O, +G
  ]
).

/** <module> LOD Laundromat: Metadata assertions

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(yall)).

:- use_module(library(dict)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(stream_ext)).
:- use_module(library(sw/rdf_export)).
:- use_module(library(sw/rdf_prefix)).
:- use_module(library(sw/rdf_term)).

:- discontiguous
    write_meta_error/2.

:- maplist(rdf_assert_prefix, [
     def-'https://lodlaundromat.org/def/',
     error-'https://lodlaundromat.org/error/def/',
     graph-'https://lodlaundromat.org/graph/',
     http-'https://lodlaundromat.org/http/def/',
     id-'https://lodlaundromat.org/id/',
     rdf-'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
   ]).

:- meta_predicate
    write_meta(+, 1).

:- rdf_meta
   error_iri(+, r),
   write_meta_quad(+, r, o, r),
   write_meta_quad(+, r, r, o, r).





%! close_metadata(+Hash:atom, +PLocal:atom, +Out:stream) is det.

close_metadata(Hash, PLocal, Out) :-
  stream_metadata(Out, Meta),
  rdf_global_id(id:Hash, S),
  rdf_global_id(def:PLocal, P),
  write_meta(Hash, write_meta_stream_(S, P, Meta)),
  close(Out).

write_meta_stream_(S, P, Meta, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, P, O, graph:meta),
  rdf_write_quad(Out, O, def:newline, literal(type(xsd:string,Meta.newline)), graph:meta),
  maplist(
    atom_number,
    [NumBytes,NumChars,NumLines],
    [Meta.bytes,Meta.characters,Meta.lines]
  ),
  rdf_write_quad(Out, O, def:bytes, literal(type(xsd:nonNegativeInteger,NumBytes)), graph:meta),
  rdf_write_quad(Out, O, def:characters, literal(type(xsd:string,NumChars)), graph:meta),
  rdf_write_quad(Out, O, def:lines, literal(type(xsd:string,NumLines)), graph:meta).



%! write_meta(+Hash:atom, :Goal_1) is det.

write_meta(Hash, Goal_1) :-
  hash_file(Hash, 'meta.nq', File),
  setup_call_cleanup(
    open(File, append, Out),
    call(Goal_1, Out),
    close(Out)
  ).



%! write_meta_archive(+Hash:atom, +Metadata:list(dict)) is det.

write_meta_archive(Hash, L) :-
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_archive_list1(S, L)).

write_meta_archive_list1(S, L, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:archive, O, graph:meta),
  write_meta_archive_list2(O, L, Out).

write_meta_archive_list2(Node, [H|T], Out) :-
  rdf_bnode_iri(First),
  rdf_write_quad(Out, Node, rdf:first, First, graph:meta),
  write_meta_archive_item(First, H, Out),
  (   T == []
  ->  rdf_equal(Next, rdf:type)
  ;   rdf_bnode_iri(Next),
      write_meta_archive_list2(Next, T, Out)
  ),
  rdf_write_quad(Out, Node, rdf:next, Next, graph:meta).

write_meta_archive_item(Item, Meta, Out) :-
  atomic_list_concat(Meta.filters, ' ', Filters),
  maplist(
    atom_number,
    [Mtime,Permissions,Size],
    [Meta.mtime,Meta.permissions,Meta.size]
  ),
  rdf_write_quad(Out, Item, def:filetype, literal(type(xsd:string,Meta.filetype)), graph:meta),
  rdf_write_quad(Out, Item, def:filter, literal(type(xsd:string,Filters)), graph:meta),
  rdf_write_quad(Out, Item, def:format, literal(type(xsd:string,Meta.format)), graph:meta),
  rdf_write_quad(Out, Item, def:mtime, literal(type(xsd:float,Mtime)), graph:meta),
  rdf_write_quad(Out, Item, def:name, literal(type(xsd:string,Meta.name)), graph:meta),
  rdf_write_quad(Out, Item, def:permissions, literal(type(xsd:positiveInteger,Permissions)), graph:meta),
  rdf_write_quad(Out, Item, def:permissions, literal(type(xsd:float,Size)), graph:meta).



%! write_meta_encoding(+Hash:atom, +Encodings:list(atom)) is det.

write_meta_encoding(Hash, Encodings) :-
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_encoding(S, Encodings)).

write_meta_encoding(S, [GuessEnc,HttpEnc,XmlEnc,Enc], Out) :-
  rdf_write_quad(Out, S, def:encoding, literal(type(xsd:string,Enc)), graph:meta),
  (   var(GuessEnc)
  ->  true
  ;   rdf_write_quad(Out, S, def:uchardet, literal(type(xsd:string,GuessEnc)), graph:meta)
  ),
  (   var(HttpEnc)
  ->  true
  ;   rdf_write_quad(Out, S, def:httpEncoding, literal(type(xsd:string,HttpEnc)), graph:meta)
  ),
  (   var(XmlEnc)
  ->  true
  ;   rdf_write_quad(Out, S, def:xmlEncoding, literal(type(xsd:string,XmlEnc)), graph:meta)
  ).



%! write_meta_entry(+Hash1:atom, +Hash2:atom) is det.

write_meta_entry(Hash1, Hash2) :-
  rdf_global_id(id:Hash1, O1),
  rdf_global_id(id:Hash2, O2),
  write_meta_quad(Hash1, def:hasEntry, O2, graph:meta),
  write_meta_quad(Hash2, def:hasArchive, O1, graph:meta).



%! write_meta_error(+Hash:atom, +Error:compound) is det,

% Archive errors.
write_meta_error(Hash, error(archive_error(Code,Msg),_Context)) :- !,
  rdf_global_id(id:Hash, S),
  atom_number(Lex, Code),
  write_meta(Hash, write_archive_error_(S, Lex, Msg)).
write_archive_error_(S, Lex, Msg, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:error, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'ArchiveError', graph:meta),
  rdf_write_quad(Out, O, def:code, literal(type(xsd:positiveInteger,Lex)), graph:meta),
  rdf_write_quad(Out, O, def:message, literal(type(xsd:string,Msg)), graph:meta).

% Exisntence error: the HTTP reply is completely empty.
write_meta_error(Hash, error(existence_error(http_reply,Url),_Context)) :- !,
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_empty_http_reply_(S,Url)).
write_empty_http_reply_(S, Url, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:error, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'MissingTurtlePrefixDefinition', graph:meta),
  rdf_write_quad(Out, O, def:alias, literal(type(xsd:anyURI,Url)), graph:meta).

% Existence error: Turtle prefix
write_meta_error(Hash, error(existence_error(turtle_prefix,Alias),_Stream)) :- !,
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_error_1(S, Alias)).
write_meta_error_1(S, Alias, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:error, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'MissingTurtlePrefixDefinition', graph:meta),
  rdf_write_quad(Out, O, def:alias, literal(type(xsd:string,Alias)), graph:meta).

% Socket errors:
%   - Connection refused
%   - Connection reset by peer
%   - Connection timed out
%   - Host not found
%   - No Data
%   - No Recovery
%   - No route to host
%   - Try Again
write_meta_error(Hash, error(socket_error(Msg),_Context)) :- !,
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_socket_error(S, Msg)).
write_socket_error(S, Msg, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:error, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'SocketError', graph:meta),
  rdf_write_quad(Out, O, def:message, literal(type(xsd:string,Msg)), graph:meta).

% Syntax error: HTTP parameter
write_meta_error(Hash, error(syntax_error(http_parameter(Param)),_)) :- !,
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_http_parameter_error(S, Param)).
write_http_parameter_error(S, Param, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:error, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'HttpParameterError', graph:meta),
  rdf_write_quad(Out, O, def:parameter, literal(type(xsd:string,Param)), graph:meta).

% RDF syntax error:
%   - end-of-line expected
%   - End of statement expected
%   - EOF in string
%   - EOF in uriref
%   - Expected ":"
%   - Expected "]"
%   - Expected ":" after "_"
%   - Illegal \\-escape
%   - Illegal character in uriref
%   - Illegal control character in uriref
%   - illegal escape
%   - Illegal IRIREF
%   - Invalid @base directive
%   - Invalid @prefix directive
%   - invalid node ID
%   - newline in string
%   - newline in uriref
%   - PN_PREFIX expected
%   - predicate expected
%   - predicate not followed by whitespace
%   - subject expected
%   - subject not followed by whitespace
%   - Unexpected "." (missing object)
%   - Unexpected newline in short string
%   - LANGTAG expected
write_meta_error(Hash, error(syntax_error(Msg),Stream0)) :- !,
  (Stream0 = stream(Stream,_,_,_) -> true ; Stream = Stream0),
  stream_property(Stream, position(Pos)),
  stream_position_data(line_count, Pos, Line),
  stream_position_data(line_position, Pos, Column),
  write_syntax_error_1(Hash, Msg, Line, Column).
write_meta_error(Hash, error(syntax_error(Msg),_Stream,Line,Column,_)) :- !,
  write_syntax_error_1(Hash, Msg, Line, Column).
write_syntax_error_1(Hash, Msg, Line, Column) :-
  rdf_global_id(id:Hash, S),
  maplist(atom_number, [Lex1,Lex2], [Line,Column]),
  write_meta(Hash, write_syntax_error_2(S, Msg, Lex1, Lex2)).
write_syntax_error_2(S, Msg, Lex1, Lex2, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:error, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'ParseError', graph:meta),
  rdf_write_quad(Out, O, def:line, literal(type(xsd:nonNegativeInteger,Lex1)), graph:meta),
  rdf_write_quad(Out, O, def:column, literal(type(xsd:nonNegativeInteger,Lex2)), graph:meta),
  rdf_write_quad(Out, O, def:message, literal(type(xsd:string,Msg)), graph:meta).

write_meta_error(Hash, http(max_redirect(_Length,_Urls))) :- !,
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_http_max_redirect(S)).
write_http_max_redirect(S, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:error, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'HttpMaxRedirect', graph:meta).

write_meta_error(Hash, http(redirect_loop(_Urls))) :- !,
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_http_redirect_loop(S)).
write_http_redirect_loop(S, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:error, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'HttpRedirectLoop', graph:meta).

% RDF syntax error: the lexical form does not occur in the lexical
% space of the indicated datatype.
write_meta_error(Hash, rdf(incorrect_lexical_form(D,Lex))) :- !,
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_error_3(S, D, Lex)).
write_meta_error_3(S, D, Lex, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:error, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'CannotMapLexicalForm', graph:meta),
  rdf_write_quad(Out, O, def:datatype, D, graph:meta),
  rdf_write_quad(Out, O, def:lexicalForm, literal(type(xsd:string,Lex)), graph:meta).

% RDF syntax error: a language-tagged string where the language tag is
% missing.
write_meta_error(Hash, rdf(missing_language_tag(LTag))) :- !,
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_error_4(S, LTag)).
write_meta_error_4(S, LTag, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:error, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'MissingLanguageTag', graph:meta),
  rdf_write_quad(Out, O, def:languageTag, literal(type(xsd:string,LTag)), graph:meta).

% RDF non-canonicity: a language-tagged string where the language tag is
% not in canonical form.
write_meta_error(Hash, rdf(non_canonical_language_tag(LTag))) :- !,
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_error_5(S, LTag)).
write_meta_error_5(S, LTag, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:canonicity, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'NonCanonicalLanguageTag', graph:meta),
  rdf_write_quad(Out, O, def:languageTag, literal(type(xsd:string,LTag)), graph:meta).

% RDF: redefined ID?
write_meta_error(Hash, rdf(redefined_id(Term))) :- !,
  rdf_global_id(id:Hash, S),
  format(atom(Lex), "~w", [Term]),
  write_meta(Hash, write_meta_error_6(S, Lex)).
write_meta_error_6(S, Lex, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:error, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'RedefinedId', graph:meta),
  rdf_write_quad(Out, O, def:id, literal(type(xsd:string,Lex)), graph:meta).

% RDF non-canonicicty: a lexical form that belongs to the lexical
% space, but is not canonical.
write_meta_error(Hash, rdf(non_canonical_lexical_form(D,Lex1,Lex2))) :- !,
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_error_7(S, D, Lex1, Lex2)).
write_meta_error_7(S, D, Lex1, Lex2, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:canonicity, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'NonCanonicalLexicalForm', graph:meta),
  rdf_write_quad(Out, O, def:datatype, D, graph:meta),
  rdf_write_quad(Out, O, def:lexicalForm, literal(type(xsd:string,Lex1)), graph:meta),
  rdf_write_quad(Out, O, def:canonicalLexicalForm, literal(type(xsd:string,Lex2)), graph:meta).

% RDF/XML parse error.
write_meta_error(Hash, sgml(sgml_parser(_Parser),_File,Line,Msg)) :- !,
  rdf_global_id(id:Hash, S),
  atom_number(Lex, Line),
  write_meta(Hash, write_meta_error_8(S, Lex, Msg)).
write_meta_error_8(S, Lex, Msg, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:error, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'ParseError', graph:meta),
  rdf_write_quad(Out, O, def:line, literal(type(xsd:nonNegativeInteger,Lex)), graph:meta),
  rdf_write_quad(Out, O, def:message, literal(type(xsd:string,Msg)), graph:meta).

% Single-statement errors.
write_meta_error(Hash, E) :-
  error_iri(E, O), !,
  write_meta_quad(Hash, def:error, O, graph:meta).

% Not yet handled.
write_meta_error(Hash, E) :-
  gtrace,
  writeln(Hash-E).

% domain errors
error_iri(error(domain_error(http_encoding,identity),_Context), error:httpEncodingIdentity).
error_iri(error(domain_error(set_cookie,_Value),_Context), error:setCookie).
error_iri(error(domain_error(url,_Url),_Context), error:url).
% I/O errors
error_iri(error(io_error(read,_Stream),_Context), http:ioError).
error_iri(error(timeout_error(read,_Stream),_Context), error:timeout).
error_iri(http(no_content_type,_Uri), error:httpNoContentType).
error_iri(io_warning(_Stream,'Illegal UTF-8 continuation'), error:illegalUtf8Continuation).
error_iri(io_warning(_Stream,'Illegal UTF-8 start'), error:illegalUtf8Start).
error_iri(rdf(non_rdf_format(_Hash,_Content)), error:nonRdfFormat).
error_iri(rdf(unexpected(_Tag,_Parser)), error:rdfxmlTag).
error_iri(rdf(unparsed(_Dom)), error:rdfxmlUnparsed).
error_iri(rdf(unsupported_format(media(application/'ld+json',[]),_Content)), error:jsonld).



%! write_meta_http(+Hash:atom, +Metadata:list(dict)) is det,

write_meta_http(Hash, L) :-
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_http_list1(S, L)).

write_meta_http_list1(S, L, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, def:http, O, graph:meta),
  write_meta_http_list2(O, L, Out).

write_meta_http_list2(Node, [H|T], Out) :-
  rdf_bnode_iri(First),
  rdf_write_quad(Out, Node, rdf:first, First, graph:meta),
  write_meta_http_item(First, H, Out),
  (   T == []
  ->  rdf_equal(Next, rdf:nil)
  ;   rdf_bnode_iri(Next),
      write_meta_http_list2(Next, T, Out)
  ),
  rdf_write_quad(Out, Node, rdf:next, Next, graph:meta).

write_meta_http_item(Item, Meta, Out) :-
  dict_pairs(Meta.headers, Pairs),
  maplist(write_meta_http_header(Out, Item), Pairs),
  atom_number(Lex, Meta.status),
  rdf_write_quad(Out, Item, def:status, literal(type(xsd:positiveInteger,Lex)), graph:meta),
  rdf_write_quad(Out, Item, def:url, literal(type(xsd:anyURI,Meta.uri)), graph:meta).

% TBD: Multiple values should emit a warning in `http/http_client2'.
write_meta_http_header(Out, Item, PLocal-[Lex|_]) :-
  rdf_global_id(def:PLocal, P),
  rdf_write_quad(Out, Item, P, literal(type(xsd:string,Lex)), graph:meta).



%! write_meta_now(+Hash:atom, +PLocal:atom) is det.

write_meta_now(Hash, PLocal) :-
  rdf_global_id(id:Hash, S),
  rdf_global_id(def:PLocal, P),
  write_meta(Hash, write_meta_now_(S, P)).

write_meta_now_(S, P, Out) :-
  get_time(Now),
  format_time(atom(Lex), "%FT%T%:z", Now),
  rdf_write_quad(Out, S, P, literal(type(xsd:dateTime,Lex)), graph:meta).



%! write_meta_quad(+Hash:atom, +P:iri, +O:rdf_term, +G:rdf_graph) is det.
%! write_meta_quad(+Hash:atom, +S:rdf_nonliteral, +P:iri, +O:rdf_term, +G:rdf_graph) is det.

write_meta_quad(Hash, P, O, G) :-
  rdf_global_id(id:Hash, S),
  write_meta_quad(Hash, S, P, O, G).


write_meta_quad(Hash, S, P, O, G) :-
  write_meta(Hash, {S,P,O,G}/[Out]>>rdf_write_quad(Out, S, P, O, G)).
