:- module(
  ll_metadata,
  [
    close_metadata/3,                  % +Hash, +PLocal, +Stream
    write_meta_archive/2,              % +Hash, +Metadata
    write_meta_encoding/4,             % +Hash, ?GuessEncoding, ?HttpEncoding, ?XmlEncoding
    write_meta_entry/2,                % +Hash1, +Hash2
    write_meta_error/2,                % +Hash, +Error
    write_meta_http/2,                 % +Hash, +Metadata
    write_meta_now/2,                  % +Hash, +PLocal
    write_meta_quad/4,                 % +Hash, +P, +O, +G
    write_meta_serialization_format/2, % +Hash, +MediaType
    write_meta_statements/2            % +Hash, +RdfMeta
  ]
).

/** <module> LOD Laundromat: Metadata assertions

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(yall)).

:- use_module(library(call_ext)).
:- use_module(library(dcg)).
:- use_module(library(dict)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(media_type)).
:- use_module(library(stream_ext)).
:- use_module(library(sw/rdf_export)).
:- use_module(library(sw/rdf_prefix)).
:- use_module(library(sw/rdf_term)).

:- discontiguous
    write_meta_error/2.

:- maplist(rdf_assert_prefix, [
     error-'https://lodlaundromat.org/error/def/',
     graph-'https://lodlaundromat.org/graph/',
     http-'https://lodlaundromat.org/http/def/',
     id-'https://lodlaundromat.org/id/',
     ll-'https://lodlaundromat.org/def/',
     rdf-'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
     xsd-'http://www.w3.org/2001/XMLSchema#'
   ]).

:- meta_predicate
    write_meta(+, 1),
    write_meta_error_(+, +, 2),
    write_meta_error_(+, +, 2, +).

:- rdf_meta
   error_iri(+, r),
   write_meta_quad(+, r, o, r).





%! close_metadata(+Hash:atom, +PLocal:atom, +Stream:stream) is det.

close_metadata(Hash, PLocal, Stream) :-
  call_cleanup(
    (
      stream_metadata(Stream, Meta),
      rdf_global_id(id:Hash, S),
      rdf_global_id(ll:PLocal, P),
      write_meta(Hash, write_meta_stream_(S, P, Meta))
    ),
    close(Stream)
  ).
write_meta_stream_(S, P, Meta, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, P, O, graph:meta),
  rdf_write_quad(Out, O, ll:newline, literal(type(xsd:string,Meta.newline)), graph:meta),
  maplist(
    atom_number,
    [NumBytes,NumChars,NumLines],
    [Meta.bytes,Meta.characters,Meta.lines]
  ),
  rdf_write_quad(Out, O, ll:bytes, literal(type(xsd:nonNegativeInteger,NumBytes)), graph:meta),
  rdf_write_quad(Out, O, ll:characters, literal(type(xsd:nonNegativeInteger,NumChars)), graph:meta),
  rdf_write_quad(Out, O, ll:lines, literal(type(xsd:nonNegativeInteger,NumLines)), graph:meta).



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
  write_meta(Hash, write_meta_archive_list_1(S, L)).
write_meta_archive_list_1(S, L, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, ll:archive, O, graph:meta),
  write_meta_archive_list_2(O, L, Out).
write_meta_archive_list_2(Node, [H|T], Out) :-
  rdf_bnode_iri(First),
  rdf_write_quad(Out, Node, rdf:first, First, graph:meta),
  write_meta_archive_item(First, H, Out),
  (   T == []
  ->  rdf_equal(Next, rdf:nil)
  ;   rdf_bnode_iri(Next),
      write_meta_archive_list_2(Next, T, Out)
  ),
  rdf_write_quad(Out, Node, rdf:next, Next, graph:meta).

write_meta_archive_item(Item, Meta, Out) :-
  atomic_list_concat(Meta.filters, ' ', Filters),
  maplist(atom_number, [Mtime,Permissions,Size], [Meta.mtime,Meta.permissions,Meta.size]),
  rdf_write_quad(Out, Item, ll:filetype, literal(type(xsd:string,Meta.filetype)), graph:meta),
  (Filters == '' -> true ; rdf_write_quad(Out, Item, ll:filter, literal(type(xsd:string,Filters)), graph:meta)),
  rdf_write_quad(Out, Item, ll:format, literal(type(xsd:string,Meta.format)), graph:meta),
  rdf_write_quad(Out, Item, ll:mtime, literal(type(xsd:float,Mtime)), graph:meta),
  rdf_write_quad(Out, Item, ll:name, literal(type(xsd:string,Meta.name)), graph:meta),
  rdf_write_quad(Out, Item, ll:permissions, literal(type(xsd:positiveInteger,Permissions)), graph:meta),
  rdf_write_quad(Out, Item, ll:size, literal(type(xsd:float,Size)), graph:meta).



%! write_meta_encoding(+Hash:atom, ?GuessEncoding:atom, ?HttpEncoding:atom, ?XmlEncoding:atom) is det.

write_meta_encoding(Hash, GuessEnc, HttpEnc, XmlEnc) :-
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_encoding_(S, GuessEnc, HttpEnc, XmlEnc)).
write_meta_encoding_(S, GuessEnc, HttpEnc, XmlEnc, Out) :-
  (   var(GuessEnc)
  ->  true
  ;   rdf_write_quad(Out, S, ll:uchardet, literal(type(xsd:string,GuessEnc)), graph:meta)
  ),
  (   var(HttpEnc)
  ->  true
  ;   rdf_write_quad(Out, S, ll:httpEncoding, literal(type(xsd:string,HttpEnc)), graph:meta)
  ),
  (   var(XmlEnc)
  ->  true
  ;   rdf_write_quad(Out, S, ll:xmlEncoding, literal(type(xsd:string,XmlEnc)), graph:meta)
  ).



%! write_meta_entry(+Hash1:atom, +Hash2:atom) is det.

write_meta_entry(Hash1, Hash2) :-
  rdf_global_id(id:Hash1, O1),
  rdf_global_id(id:Hash2, O2),
  write_meta_quad(Hash1, ll:hasEntry, O2, graph:meta),
  write_meta_quad(Hash2, ll:hasArchive, O1, graph:meta).



%! write_meta_error(+Hash:atom, +Error:compound) is det,

% Oh no!
write_meta_error(Hash, error(resource_error(stack),global)) :- !,
  write_meta_quad(Hash, ll:error, error:'GlobalStack', graph:meta).
% Archive errors.
write_meta_error(Hash, error(archive_error(Code,Msg),_Context)) :- !,
  atom_number(Lex, Code),
  write_meta_error_(Hash, 'ArchiveError', write_error_1(Lex, Msg)).
write_error_1(Lex, Msg, Out, O) :-
  rdf_write_quad(Out, O, ll:code, literal(type(xsd:positiveInteger,Lex)), graph:meta),
  rdf_write_quad(Out, O, ll:message, literal(type(xsd:string,Msg)), graph:meta).

% HTTP error: ???
write_meta_error(Hash, error(domain_error(http_encoding,identity),_Context)) :- !,
  write_meta_error_(Hash, 'HttpEncodingIdentity').

% Set cookie error: ???
write_meta_error(Hash, error(domain_error(set_cookie,Value),_Context)) :- !,
  write_meta_error_(Hash, 'SetCookieError', write_error_2(Value)).
write_error_2(Value, Out, O) :-
  rdf_write_quad(Out, O, ll:value, literal(type(xsd:string,Value)), graph:meta).

write_meta_error(Hash, error(domain_error(url,Url),_Context)) :- !,
  write_meta_error_(Hash, 'InvalidUrl', write_error_3(Url)).
write_error_3(Url, Out, O) :-
  rdf_write_quad(Out, O, ll:value, literal(type(xsd:string,Url)), graph:meta).

% Existence error: the HTTP reply is completely empty.
write_meta_error(Hash, error(existence_error(http_reply,Url),_Context)) :- !,
  write_meta_error_(Hash, 'EmptyHttpReply', write_error_4(Url)).
write_error_4(Url, Out, O) :-
  rdf_write_quad(Out, O, ll:alias, literal(type(xsd:anyURI,Url)), graph:meta).

% Existence error: Turtle prefix is not declared.
write_meta_error(Hash, error(existence_error(turtle_prefix,Alias),_Stream)) :- !,
  write_meta_error_(Hash, 'MissingTurtlePrefixDeclaration', write_error_5(Alias)).
write_error_5(Alias, Out, O) :-
  rdf_write_quad(Out, O, ll:alias, literal(type(xsd:string,Alias)), graph:meta).

% HTTP maximum redirection sequence length exceeded.
write_meta_error(Hash, error(http_error(max_redirect,_Length,_Urls),_Context)) :- !,
  write_meta_error_(Hash, 'HttpMaxRedirect').

% HTTP no content type header in reply.
write_meta_error(Hash, error(http_error(no_content_type,_Uri),_Context)) :- !,
  write_meta_error_(Hash, 'HttpNoContentType').

% HTTP redirection loop detected.
write_meta_error(Hash, error(http_error(redirect_loop,_Urls),_Context)) :- !,
  write_meta_error_(Hash, 'HttpRedirectLoop').

% I/O error: ???
write_meta_error(Hash, error(io_error(read,_Stream),_Context)) :- !,
  write_meta_error_(Hash, 'ReadStreamError').

% No non-binary encoding could be found.
write_meta_error(Hash, error(no_encoding,_Context)) :-
  write_meta_error_(Hash, 'NoEncoding').

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
  write_meta_error_(Hash, 'SocketError', write_error_6(Msg)).
write_error_6(Msg, Out, O) :-
  rdf_write_quad(Out, O, ll:message, literal(type(xsd:string,Msg)), graph:meta).

% Syntax error: HTTP parameter
write_meta_error(Hash, error(syntax_error(http_parameter(Param)),_)) :- !,
  write_meta_error_(Hash, 'HttpParameterParseError', write_error_7(Param)).
write_error_7(Param, Out, O) :-
  rdf_write_quad(Out, O, ll:parameter, literal(type(xsd:string,Param)), graph:meta).

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
  stream_line_column(Stream, Line, Column),
  write_syntax_error_1(Hash, Msg, Line, Column).
write_meta_error(Hash, error(syntax_error(Msg),_Stream,Line,Column,_)) :- !,
  write_syntax_error_1(Hash, Msg, Line, Column).
write_syntax_error_1(Hash, Msg, Line, Column) :-
  maplist(atom_number, [Lex1,Lex2], [Line,Column]),
  write_meta_error_(Hash, 'RdfParseError', write_syntax_error_2(Msg, Lex1, Lex2)).
write_syntax_error_2(Msg, Lex1, Lex2, Out, O) :-
  rdf_write_quad(Out, O, ll:message, literal(type(xsd:string,Msg)), graph:meta),
  write_error_stream(Lex1, Lex2, Out, O).

write_meta_error(Hash, error(timeout_error(read,_Stream),_Context)) :- !,
  write_meta_error_(Hash, 'HttpTimeout').

write_meta_error(Hash, io_warning(Stream,'Illegal UTF-8 continuation')) :- !,
  write_meta_error_(Hash, 'IllegalUtf8Continuation'),
  stream_line_column(Stream, Line, Column),
  maplist(atom_number, [Lex1,Lex2], [Line,Column]),
  write_meta_error_(Hash, 'IllegalUtf8Continuation', write_error_stream(Lex1, Lex2)).
write_error_stream(Lex1, Lex2, Out, O) :-
  rdf_write_quad(Out, O, ll:line, literal(type(xsd:nonNegativeInteger,Lex1)), graph:meta),
  rdf_write_quad(Out, O, ll:column, literal(type(xsd:nonNegativeInteger,Lex2)), graph:meta).

write_meta_error(Hash, io_warning(Stream,'Illegal UTF-8 start')) :- !,
  stream_line_column(Stream, Line, Column),
  maplist(atom_number, [Lex1,Lex2], [Line,Column]),
  write_meta_error_(Hash, 'IllegalUtf8Start', write_error_stream(Lex1, Lex2)).

% RDF syntax error: the lexical form does not occur in the lexical
% space of the indicated datatype.
write_meta_error(Hash, rdf(incorrect_lexical_form(D,Lex))) :- !,
  write_meta_error_(Hash, 'CannotMapLexicalForm', write_error_8(D, Lex)).
write_error_8(D, Lex, Out, O) :-
  rdf_write_quad(Out, O, ll:datatype, D, graph:meta),
  rdf_write_quad(Out, O, ll:lexicalForm, literal(type(xsd:string,Lex)), graph:meta).

% RDF syntax error: a language-tagged string where the language tag is
% missing.
write_meta_error(Hash, rdf(missing_language_tag(LTag))) :- !,
  write_meta_error_(Hash, 'MissingLanguageTag', write_error_9(LTag)).
write_error_9(LTag, Out, O) :-
  rdf_write_quad(Out, O, ll:languageTag, literal(type(xsd:string,LTag)), graph:meta).

% RDF non-canonicity: a language-tagged string where the language tag is
% not in canonical form.
write_meta_error(Hash, rdf(non_canonical_language_tag(LTag))) :- !,
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_error_10(S, LTag)).
write_error_10(S, LTag, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, ll:canonicity, O, graph:meta),
  rdf_write_quad(Out, O, rdf:type, error:'NonCanonicalLanguageTag', graph:meta),
  rdf_write_quad(Out, O, ll:languageTag, literal(type(xsd:string,LTag)), graph:meta).

% RDF non-canonicicty: a lexical form that belongs to the lexical
% space, but is not canonical.
write_meta_error(Hash, rdf(non_canonical_lexical_form(D,Lex1,Lex2))) :- !,
  write_meta_error_(Hash, 'NonCanonicalLexicalForm', write_error_11(D, Lex1, Lex2)).
write_error_11(D, Lex1, Lex2, Out, O) :-
  rdf_write_quad(Out, O, ll:datatype, D, graph:meta),
  rdf_write_quad(Out, O, ll:lexicalForm, literal(type(xsd:string,Lex1)), graph:meta),
  rdf_write_quad(Out, O, ll:canonicalLexicalForm, literal(type(xsd:string,Lex2)), graph:meta).

% Not an RDF serialization format.
write_meta_error(Hash, error(rdf(non_rdf_format,Str),_Context)) :- !,
  write_meta_error_(Hash, 'NonRdfFormat', write_error_12(Str)).
write_error_12(Str, Out, O) :-
  rdf_write_quad(Out, O, ll:content, literal(type(xsd:string,Str)), graph:meta).

% RDF: redefined ID?
write_meta_error(Hash, rdf(redefined_id(Term))) :- !,
  format(atom(Lex), "~w", [Term]),
  write_meta_error_(Hash, 'RdfRedefinedId', write_error_13(Lex)).
write_error_13(Lex, Out, O) :-
  rdf_write_quad(Out, O, ll:id, literal(type(xsd:string,Lex)), graph:meta).

% RDF/XML parser error: unexpected tag
write_meta_error(Hash, rdf(unexpected(Tag,_Parser))) :- !,
  write_meta_error_(Hash, 'RdfXmlParseError', write_error_14(Tag)).
write_error_14(Tag, Out, O) :-
  rdf_write_quad(Out, O, ll:tag, literal(type(xsd:string,Tag)), graph:meta).

% RDF/XML parser error: unparseable DOM.
write_meta_error(Hash, rdf(unparsed(Dom))) :- !,
  write_meta_error_(Hash, 'RdfXmlParseError', write_error_15(Dom)).
write_error_15(Dom, Out, O) :-
  rdf_literal_value(Literal, rdf:'XMLLiteral', Dom),
  rdf_write_quad(Out, O, ll:dom, Literal, graph:meta).

% JSON-LD serialization format is not yet supported.
write_meta_error(Hash, rdf(unsupported_format(media(application/'ld+json',[]),_Content))) :- !,
  write_meta_error_(Hash, 'JsonldNotYetSupported').

% RDF/XML parse error.
write_meta_error(Hash, sgml(sgml_parser(_Parser),_File,Line,Msg)) :- !,
  atom_number(Lex, Line),
  write_meta_error_(Hash, 'RdfParseError', write_error_15(Lex, Msg)).
write_error_15(Lex, Msg, Out, O) :-
  rdf_write_quad(Out, O, ll:line, literal(type(xsd:nonNegativeInteger,Lex)), graph:meta),
  rdf_write_quad(Out, O, ll:message, literal(type(xsd:string,Msg)), graph:meta).

% TBD: Not yet handled.
write_meta_error(Hash, E) :-
  format(atom(Lex), "~w", [E]),
  write_meta_quad(Hash, ll:error, literal(type(xsd:string,Lex)), graph:meta).

write_meta_error_(Hash, CName) :-
  write_meta_error_(Hash, CName, true).

write_meta_error_(Hash, CName, Goal_2) :-
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_error_(S, CName, Goal_2)).

write_meta_error_(S, CName, Goal_2, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, ll:error, O, graph:meta),
  rdf_global_id(error:CName, C),
  rdf_write_quad(Out, O, rdf:type, C, graph:meta),
  call(Goal_2, Out, O).



%! write_meta_http(+Hash:atom, +Metadata:list(dict)) is det,

write_meta_http(Hash, L) :-
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_http_list1(S, L)).
write_meta_http_list1(S, L, Out) :-
  rdf_bnode_iri(O),
  rdf_write_quad(Out, S, ll:http, O, graph:meta),
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
  % Some servers emit non-numeric status codes, so we cannot use
  % `xsd:positiveInteger' here.
  Status = Meta.status,
  (atom(Status) -> Lex = Status ; atom_number(Lex, Status)),
  rdf_write_quad(Out, Item, ll:status, literal(type(xsd:string,Lex)), graph:meta),
  rdf_write_quad(Out, Item, ll:url, literal(type(xsd:anyURI,Meta.uri)), graph:meta).
% TBD: Multiple values should emit a warning in `http/http_client2'.
write_meta_http_header(Out, Item, PLocal-Lexs) :-
  rdf_global_id(ll:PLocal, P),
  forall(
    member(Lex, Lexs),
    rdf_write_quad(Out, Item, P, literal(type(xsd:string,Lex)), graph:meta)
  ).



%! write_meta_now(+Hash:atom, +PLocal:atom) is det.

write_meta_now(Hash, PLocal) :-
  rdf_global_id(id:Hash, S),
  rdf_global_id(ll:PLocal, P),
  write_meta(Hash, write_meta_now_(S, P)).
write_meta_now_(S, P, Out) :-
  get_time(Now),
  format_time(atom(Lex), "%FT%T%:z", Now),
  rdf_write_quad(Out, S, P, literal(type(xsd:dateTime,Lex)), graph:meta).



%! write_meta_quad(+Hash:atom, +P:iri, +O:rdf_term, +G:rdf_graph) is det.

write_meta_quad(Hash, P, O, G) :-
  rdf_global_id(id:Hash, S),
  write_meta(Hash, {S,P,O,G}/[Out]>>rdf_write_quad(Out, S, P, O, G)).



%! write_meta_serialization_format(+Hash:atom, +MediaType:compound) is det.

write_meta_serialization_format(Hash, MediaType) :-
  dcg_with_output_to(atom(Lex), media_type(MediaType)),
  write_meta_quad(Hash, ll:serializationFormat, literal(type(xsd:string,Lex)), graph:meta).



%! write_meta_statements(+Hash:atom, +RdfMeta:dict) is det.

write_meta_statements(Hash, RdfMeta) :-
  maplist(
    atom_number,
    [Lex1,Lex2],
    [RdfMeta.number_of_quadruples,RdfMeta.number_of_triples]
  ),
  write_meta_quad(Hash, ll:quadruples, literal(type(xsd:nonNegativeInteger,Lex1)), graph:meta),
  write_meta_quad(Hash, ll:triples, literal(type(xsd:nonNegativeInteger,Lex2)), graph:meta).
