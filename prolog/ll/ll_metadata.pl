:- module(
  ll_metadata,
  [
    close_metadata/3,        % +Hash, +PLocal, +Stream
    write_message/3,         % +Kind, +Hash, +Error
    write_meta_archive/2,    % +Hash, +Filters
    write_meta_encoding/4,   % +Hash, ?GuessEncoding, ?HttpEncoding, ?XmlEncoding
    write_meta_entry/5,      % +ArchiveHash, +EntryName, +EntryHash, +Format, +Props
    write_meta_http/2,       % +Hash, +Metadata
    write_meta_now/2,        % +Hash, +PLocal
    write_meta_quad/3,       % +Hash, +PLocal, +Input
    write_meta_serialization_format/2, % +Hash, +MediaType
    write_meta_statements/2  % +Hash, +RdfMeta
  ]
).

/** <module> LOD Laundromat: Metadata assertions

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(zlib)).

:- use_module(library(call_ext)).
:- use_module(library(dcg)).
:- use_module(library(dict)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(media_type)).
:- use_module(library(stream_ext)).
:- use_module(library(semweb/rdf_export)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).
:- use_module(library(term_ext)).
:- use_module(library(xsd/xsd)).

:- discontiguous
    write_message/3.

:- maplist(rdf_register_prefix, [
     graph-'https://lodlaundromat.org/graph/',
     id-'https://lodlaundromat.org/id/',
     ll,
     rdf,
     xsd
   ]).

:- meta_predicate
    write_(+, +, 2),
    write_message_(+, +, +, 3),
    write_message_(+, +, 3, +, +).

:- rdf_meta
   rdf_write_(+, +, r, r, o),
   write_meta_quad(+, +, o).





%! close_metadata(+Hash:atom, +PLocal:atom, +Stream:stream) is det.

close_metadata(Hash, PLocal, Stream) :-
  call_cleanup(
    (
      stream_metadata(Stream, Meta),
      rdf_prefix_iri(id:Hash, S),
      rdf_prefix_iri(ll:PLocal, P),
      write_(meta, Hash, close_metadata_(S, P, Meta))
    ),
    close(Stream)
  ).
close_metadata_(S, P, Meta, Kind, Out) :-
  rdf_bnode_iri(O),
  rdf_write_(Kind, Out, S, P, O),
  rdf_write_(Kind, Out, O, rdf:type, ll:'StreamProperties'),
  rdf_write_(Kind, Out, O, ll:newline, str(Meta.newline)),
  rdf_write_(Kind, Out, O, ll:bytes, nonneg(Meta.bytes)),
  rdf_write_(Kind, Out, O, ll:characters, nonneg(Meta.characters)),
  rdf_write_(Kind, Out, O, ll:lines, nonneg(Meta.lines)).



%! write_message(+Kind:oneof([error,warning]), +Hash:atom, +Error:compound) is det,

% Archive errors.
%
% | *Code* | *Message*                                                         |
% |--------+-------------------------------------------------------------------|
% | 0      | Damaged 7-Zip archive                                             |
% | 25     | Invalid central directory signature                               |
% | ???    | Unrecognized archive format                                       |
% | ???    | Truncated input file (needed $(INT) bytes, only $(INT) available) |
% | ???    | Can\'t parse line $(INT)                                          |
write_message(Kind, Hash, error(archive_error(Code,Msg),_)) :- !,
  write_message_(Kind, Hash, 'ArchiveError', write_message_1(Code, Msg)).
write_message_1(Code, Msg, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:code, nonneg(Code)),
  rdf_write_(Kind, Out, O, ll:message, str(Msg)).

% HTTP error: ???
write_message(Kind, Hash, error(domain_error(http_encoding,identity),_)) :- !,
  write_message_(Kind, Hash, 'HttpEncodingIdentity').

% Set cookie error: ???
write_message(Kind, Hash, error(domain_error(set_cookie,Value),_)) :- !,
  write_message_(Kind, Hash, 'SetCookieError', write_message_2(Value)).
write_message_2(Value, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:value, str(Value)).

% Malformed URL
write_message(Kind, Hash, error(domain_error(url,Url),_)) :- !,
  write_message_(Kind, Hash, 'InvalidUrl', write_message_3(Url)).
write_message_3(Url, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:value, uri(Url)).

% Existence error: the HTTP reply is empty (no status line, no
% headers, and no body).
write_message(Kind, Hash, error(existence_error(http_reply,Uri),_)) :- !,
  write_message_(Kind, Hash, 'EmptyHttpReply', write_message_4(Uri)).
write_message_4(Uri, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:alias, uri(Uri)).

% Existence error: Turtle prefix is not declared.
write_message(Kind, Hash, error(existence_error(turtle_prefix,Alias),Stream)) :- !,
  stream_line_column_(Stream, Line, Column),
  write_message_(Kind, Hash, 'MissingTurtlePrefixDeclaration', write_message_6(Alias, Line, Column)).
write_message_6(Alias, Line, Column, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:alias, str(Alias)),
  write_message_stream_(Line, Column, Kind, Out, O).

% Existence error: IRI scheme is not registered with IANA.
write_message(Kind, Hash, error(existence_error(uri_scheme,Scheme),_)) :- !,
  write_message_(Kind, Hash, 'IllegalUriScheme', write_message_5(Scheme)).
write_message_5(Scheme, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:scheme, str(Scheme)).

write_message(Kind, Hash, error(http_error(cyclic_link_header,_),_)) :- !,
  write_message_(Kind, Hash, 'CyclicLinkHeader').

% HTTP maximum redirection sequence length exceeded.
write_message(Kind, Hash, error(http_error(max_redirect,_,_),_)) :- !,
  write_message_(Kind, Hash, 'HttpMaxRedirect').

% HTTP no content type header in reply.
write_message(Kind, Hash, error(http_error(no_content_type,_),_)) :- !,
  write_message_(Kind, Hash, 'HttpNoContentType').

% HTTP redirection loop detected.
write_message(Kind, Hash, error(http_error(redirect_loop,_),_)) :- !,
  write_message_(Kind, Hash, 'HttpRedirectLoop').

% HTTP error status code
write_message(Kind, Hash, error(http_error(status,_),_)) :- !,
  write_message_(Kind, Hash, 'HttpErrorStatus').

% I/O error
%
% Observed instances:
%   - Msg = 'Connection reset by peer'
%   - Msg = 'Inappropriate ioctl for device'
%   - Msg = 'Is a directory'
write_message(Kind, Hash, error(io_error(read,_Stream),context(_,Msg))) :- !,
  write_message_(Kind, Hash, 'ReadStreamError', write_message_8(Msg)).
write_message_8(Msg, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:message, str(Msg)).

% No non-binary encoding could be found.
write_message(Kind, Hash, error(no_encoding,_)) :-
  write_message_(Kind, Hash, 'NoEncoding').

% Representation error: Turtle character
write_message(Kind, Hash, error(representation_error(turtle_character),_)) :- !,
  write_message_(Kind, Hash, 'TurtleCharacter').

% Resource error: global stack
write_message(Kind, Hash, error(resource_error(stack),global)) :- !,
  write_message_(Kind, Hash, 'GlobalStack').

% Socket errors
write_message(Kind, Hash, error(socket_error(Msg),_)) :- !,
  message_class_name(Msg, CName),
  write_message_(Kind, Hash, CName, true).

message_class_name('Connection refused', 'ConnectionRefused').
message_class_name('Connection reset by peer', 'ConnectionResetByPeer').
message_class_name('Connection timed out', 'ConnectionTimedOut').
message_class_name('Host not found', 'HostNotFound').
message_class_name('Network is unreachable', 'NetworkIsUnreachable').
message_class_name('No Data', 'NoData').
message_class_name('No Recovery', 'NoRecovery').
message_class_name('No route to host', 'NoRouteToHost').
message_class_name('Try Again', 'TryAgain').

% SSL error: unexpected end of file
%
% Observed instances:
%   - Code = 'SSL_eof'
%     Library = ssl
%     Function = negotiate
%     Reason = 'Unexpected end-of-file'
write_message(Kind, Hash, error(ssl_error(Code,Library,Function,Reason),_)) :- !,
  write_message_(Kind, Hash, 'SslUnexpectedEof', write_message_10(Code, Library, Function, Reason)).
write_message_10(Code, Library, Function, Reason, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:code, str(Code)),
  rdf_write_(Kind, Out, O, ll:library, str(Library)),
  rdf_write_(Kind, Out, O, ll:function, str(Function)),
  rdf_write_(Kind, Out, O, ll:reason, str(Reason)).

% Syntax error (1/2)
write_message(Kind, Hash, error(syntax_error(grammar(Language,Source)),_)) :- !,
  write_message_(Kind, Hash, 'SyntaxError', write_message_11(Language,Source)).
write_message_11(Language, Source, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:language, str(Language)),
  rdf_write_(Kind, Out, O, ll:string, str(Source)).

% Syntax error (2/2)
write_message(Kind, Hash, error(syntax_error(grammar(Language,Expr,Source)),_)) :- !,
  write_message_(Kind, Hash, 'SyntaxError', write_message_13(Language,Expr,Source)).
write_message_13(Language, Expr, Source, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:language, str(Language)),
  rdf_write_(Kind, Out, O, ll:expression, str(Expr)),
  rdf_write_(Kind, Out, O, ll:string, str(Source)).

% RDF syntax error:
%   - Mag = 'end-of-line expected'
%   - Mag = 'End of statement expected'
%   - Mag = 'EOF in string'
%   - Mag = 'EOF in uriref'
%   - Mag = 'Expected ":"'
%   - Mag = 'Expected "]"'
%   - Mag = 'Expected ":" after "_"'
%   - Mag = 'Illegal \\-escape'
%   - Mag = 'Illegal character in uriref'
%   - Mag = 'Illegal control character in uriref'
%   - Mag = 'illegal escape'
%   - Mag = 'Illegal IRIREF'
%   - Mag = 'Invalid @base directive'
%   - Mag = 'Invalid @prefix directive'
%   - Mag = 'invalid node ID'
%   - Mag = 'newline in string'
%   - Mag = 'newline in uriref'
%   - Mag = 'PN_PREFIX expected'
%   - Mag = 'predicate expected'
%   - Mag = 'predicate not followed by whitespace'
%   - Mag = 'subject expected'
%   - Mag = 'subject not followed by whitespace'
%   - Mag = 'Unexpected "." (missing object)'
%   - Mag = 'Unexpected newline in short string'
%   - Mag = 'LANGTAG expected'
write_message(Kind, Hash, error(syntax_error(Msg),Stream)) :-
  atom(Msg), !,
  stream_line_column_(Stream, Line, Column),
  write_message_16(Kind, Hash, Msg, Line, Column).
write_message(Kind, Hash, error(syntax_error(Msg),_,Line,Column,_)) :-
  atom(Msg), !,
  write_message_16(Kind, Hash, Msg, Line, Column).
write_message_16(Kind, Hash, Msg, Line, Column) :-
  write_message_(Kind, Hash, 'RdfParseError', write_message_17(Msg, Line, Column)).
write_message_17(Msg, Line, Column, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:message, str(Msg)),
  write_message_stream_(Line, Column, Kind, Out, O).

% Syntax error: incorrect HTTP status code
write_message(Kind, Hash, error(type_error(http_status,_),_)) :- !,
  write_message_(Kind, Hash, 'IncorrectHttpStatusCode').

write_message(Kind, Hash, error(timeout_error(read,_Stream),_)) :- !,
  write_message_(Kind, Hash, 'HttpTimeout').

% RDF/XML and RDFa: Cannot parse as XML DOM
write_message(Kind, Hash, error(type_error(xml_dom,Dom))) :- !,
  write_message_(Kind, Hash, 'IllegalXmlDom', write_message_18(Dom)).
write_message_18(Dom, Kind, Out, O) :-
  rdf_literal_value(Literal, rdf:'XMLLiteral', Dom),
  rdf_write_(Kind, Out, O, ll:dom, Literal).

% URI parse error
write_message(Kind, Hash, error(uri_error(Code,Uri),_)) :- !,
  write_message_(Kind, Hash, 'UriError', write_message_19(Code, Uri)).
write_message_19(Code, Uri, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:code, nonneg(Code)),
  rdf_write_(Kind, Out, O, ll:content, str(Uri)).
write_message(Kind, Hash, error(uri_error(Code,Uri,Pos),_)) :- !,
  write_message_(Kind, Hash, 'UriSyntaxError', write_message_20(Code, Uri, Pos)).
write_message_20(Code, Uri, Pos, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:code, nonneg(Code)),
  rdf_write_(Kind, Out, O, ll:content, str(Uri)),
  rdf_write_(Kind, Out, O, ll:position, nonneg(Pos)).

% Illegal Unicode character ???
write_message(Kind, Hash, io_warning(Stream,'Illegal UTF-8 continuation')) :- !,
  write_message_(Kind, Hash, 'IllegalUtf8Continuation'),
  stream_line_column_(Stream, Line, Column),
  write_message_(Kind, Hash, 'IllegalUtf8Continuation', write_message_stream_(Line, Column)).

% Unicode something ???
write_message(Kind, Hash, io_warning(Stream,'Illegal UTF-8 start')) :- !,
  stream_line_column_(Stream, Line, Column),
  write_message_(Kind, Hash, 'IllegalUtf8Start', write_message_stream_(Line, Column)).

% RDF/XML: name
write_message(Kind, Hash, rdf(not_a_name(Name))) :- !,
  write_message_(Kind, Hash, 'NotAnXmlName', write_message_23(Name)).
write_message_23(Name, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:name, str(Name)).

% RDF/XML: multiple definitions ???
write_message(Kind, Hash, rdf(redefined_id(Iri))) :- !,
  write_message_(Kind, Hash, 'RdfRedefinedId', write_message_24(Iri)).
write_message_24(Iri, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:id, str(Iri)).

% RDF/XML parser error: unexpected tag
write_message(Kind, Hash, rdf(unexpected(Tag,_Parser))) :- !,
  write_message_(Kind, Hash, 'RdfXmlParseError', write_message_25(Tag)).
write_message_25(Tag, Kind, Out, O) :-
  with_output_to(string(String), write_term(Tag)),
  rdf_write_(Kind, Out, O, ll:tag, String).

% RDF/XML parser error: unparseable DOM.
write_message(Kind, Hash, rdf(unparsed(Dom))) :- !,
  write_message_(Kind, Hash, 'RdfXmlParseError', write_message_26(Dom)).
write_message_26(Dom, Kind, Out, O) :-
  rdf_literal_value(Literal, rdf:'XMLLiteral', Dom),
  rdf_write_(Kind, Out, O, ll:dom, Literal).

% JSON-LD serialization format is not yet supported.
write_message(
  Kind,
  Hash,
  error(rdf_error(unsupported_format,media(application/'ld+json',[])),_)
) :- !,
  write_message_(Kind, Hash, 'JsonldNotYetSupported').

% RDF syntax error: a language-tagged string where the language tag is
% missing.
write_message(Kind, Hash, error(rdf_error(missing_language_tag,Lex),_)) :- !,
  write_message_(Kind, Hash, 'MissingLanguageTag', write_message_21(Lex)).
write_message_21(Lex, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:lexicalFrom, str(Lex)).

% Not an RDF serialization format.
write_message(Kind, Hash, error(rdf_error(non_rdf_format,Content),_)) :- !,
  write_message_(Kind, Hash, 'NonRdfFormat', write_message_22(Content)).
write_message_22(Content, Kind, Out, O) :-
  string_phrase(xsd_encode_string, Content, EncodedContent),
  rdf_write_(Kind, Out, O, ll:content, str(EncodedContent)).

% RDF non-canonicity: a language-tagged string where the language tag is
% not in canonical form.
write_message(Kind, Hash, error(rdf_error(non_canonical_language_tag,LTag),_)) :- !,
  write_message_(Kind, Hash, 'NonCanonicalLanguageTag', write_message_28(LTag)).
write_message_28(LTag, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:languageTag, str(LTag)).

% RDF non-canonicicty: a lexical form that belongs to the lexical
% space, but is not canonical.
write_message(Kind, Hash, error(rdf_error(non_canonical_lexical_form,D,Lex1,Lex2),_)) :- !,
  write_message_(Kind, Hash, 'NonCanonicalLexicalForm', write_message_29(D, Lex1, Lex2)).
write_message_29(D, Lex1, Lex2, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:datatype, D),
  rdf_write_(Kind, Out, O, ll:lexicalForm, str(Lex1)),
  rdf_write_(Kind, Out, O, ll:canonicalLexicalForm, str(Lex2)).

% RDF lexical-to-value and value-to-lexical mappings.
write_message(Kind, Hash, error(unimplemented_lex2val(D,Lex),_)) :- !,
  write_message_(Kind, Hash, 'Lexical2ValueError', write_message_l2v(D, Lex)).
write_message(Kind, Hash, error(unimplemented_val2lex(D,Value),_)) :- !,
  write_message_(Kind, Hash, 'Value2LexicalError', write_message_v2l(D, Value)).
write_message_l2v(D, Lex, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:datatypeIri, uri(D)),
  rdf_write_(Kind, Out, O, ll:lexicalForm, str(Lex)).
write_message_v2l(D, Value, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:datatypeIri, uri(D)),
  format(string(String), write_term(Value)),
  string_phrase(xsd_encode_string, String, EncodedString),
  rdf_write_(Kind, Out, O, ll:value, EncodedString).

% RDF/XML parse error.
write_message(Kind, Hash, sgml(sgml_parser(_Parser),_File,Line,Msg)) :- !,
  write_message_(Kind, Hash, 'RdfParseError', write_message_27(Line, Msg)).
write_message_27(Line, Msg, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:line, nonneg(Line)),
  rdf_write_(Kind, Out, O, ll:message, str(Msg)).

% TBD: Not yet handled.
write_message(Kind, Hash, E) :-
  rdf_prefix_iri(id:Hash, S),
  with_output_to(string(String), write_term(E)),
  write_(Kind, Hash, write_message_30(S, String)).
write_message_30(S, String, Kind, Out) :-
  rdf_write_(Kind, Out, S, ll:error, String).

stream_line_column_(stream(_,Line,Column,_), Line, Column) :- !.
stream_line_column_(Stream, Line, Column) :-
  stream_line_column(Stream, Line, Column).

write_message_(Kind, Hash, CName) :-
  write_message_(Kind, Hash, CName, true).

write_message_(Kind, Hash, CName, Goal_3) :-
  rdf_prefix_iri(id:Hash, S),
  write_(Kind, Hash, write_message_(S, CName, Goal_3)).

write_message_(S, CName, Goal_3, Kind, Out) :-
  rdf_bnode_iri(O),
  rdf_write_(Kind, Out, S, ll:error, O),
  rdf_prefix_iri(ll:CName, C),
  rdf_write_(Kind, Out, O, rdf:type, C),
  call(Goal_3, Kind, Out, O).

write_message_stream_(Line, Column, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:line, nonneg(Line)),
  rdf_write_(Kind, Out, O, ll:column, nonneg(Column)).



%! write_meta_archive(+Hash:atom, +Filters:list(atom)) is det.

write_meta_archive(_, []) :- !.
write_meta_archive(Hash, Filters) :-
  rdf_prefix_iri(id:Hash, S),
  write_(meta, Hash, write_meta_archive_1(S, Filters)).
write_meta_archive_1(S, Filters, Kind, Out) :-
  rdf_bnode_iri(O),
  (   Filters == []
  ->  true
  ;   rdf_write_(Kind, Out, S, ll:filter, O),
      rdf_write_(Kind, Out, O, rdf:type, ll:'Filters'),
      write_meta_archive_2(O, Filters, Kind, Out)
  ).
write_meta_archive_2(Node, [Filter|Filters], Kind, Out) :-
  rdf_write_(Kind, Out, Node, rdf:first, str(Filter)),
  (   Filters == []
  ->  rdf_equal(Next, rdf:nil)
  ;   rdf_bnode_iri(Next),
      write_meta_archive_2(Node, Filters, Kind, Out)
  ),
  rdf_write_(Kind, Out, Node, rdf:next, Next).



%! write_meta_encoding(+Hash:atom, ?GuessEncoding:atom, ?HttpEncoding:atom, ?XmlEncoding:atom) is det.

write_meta_encoding(Hash, GuessEnc, HttpEnc, XmlEnc) :-
  rdf_prefix_iri(id:Hash, S),
  write_(meta, Hash, write_meta_encoding_(S, GuessEnc, HttpEnc, XmlEnc)).
write_meta_encoding_(S, GuessEnc, HttpEnc, XmlEnc, Kind, Out) :-
  (   var(GuessEnc)
  ->  true
  ;   rdf_write_(Kind, Out, S, ll:uchardet, str(GuessEnc))
  ),
  (   var(HttpEnc)
  ->  true
  ;   rdf_write_(Kind, Out, S, ll:httpEncoding, str(HttpEnc))
  ),
  (   var(XmlEnc)
  ->  true
  ;   rdf_write_(Kind, Out, S, ll:xmlEncoding, str(XmlEnc))
  ).



%! write_meta_entry(+ArchiveHash:atom, +EntryName:atom, +EntryHash:atom, +Format:atom, +Props:list(compound)) is det.

write_meta_entry(ArchiveHash, EntryName, EntryHash, Format, Props) :-
  rdf_prefix_iri(id:ArchiveHash, Archive),
  rdf_prefix_iri(id:EntryHash, Entry),
  write_(meta, ArchiveHash, write_meta_entry_archive_(Archive, Entry)),
  write_(meta, EntryHash, write_meta_entry_entry_(Archive, EntryName, Entry, Format, Props)).
write_meta_entry_archive_(Archive, Entry, Kind, Out) :-
  rdf_write_(Kind, Out, Archive, rdf:type, ll:'Archive'),
  rdf_write_(Kind, Out, Archive, ll:entry, Entry).
write_meta_entry_entry_(Archive, EntryName, Entry, Format, Props, Kind, Out) :-
  memberchk(mtime(MTime), Props),
  memberchk(permissions(Permissions), Props),
  memberchk(size(Size), Props),
  rdf_write_(Kind, Out, Entry, rdf:type, ll:'Entry'),
  rdf_write_(Kind, Out, Entry, ll:archive, Archive),
  rdf_write_(Kind, Out, Entry, ll:format, str(Format)),
  rdf_write_(Kind, Out, Entry, ll:mtime, MTime),
  rdf_write_(Kind, Out, Entry, ll:name, str(EntryName)),
  rdf_write_(Kind, Out, Entry, ll:permissions, positive_integer(Permissions)),
  rdf_write_(Kind, Out, Entry, ll:size, nonneg(Size)).



%! write_meta_http(+Hash:atom, +Metadata:list(dict)) is det,

write_meta_http(Hash, L) :-
  rdf_prefix_iri(id:Hash, S),
  write_(meta, Hash, write_meta_http_list_1(S, L)).

write_meta_http_list_1(S, L, Kind, Out) :-
  rdf_bnode_iri(O),
  rdf_write_(Kind, Out, S, ll:http_replies, O),
  rdf_write_(Kind, Out, O, rdf:type, ll:'HttpReplySequence'),
  write_meta_http_list_2(O, L, Kind, Out).

write_meta_http_list_2(Node, [H|T], Kind, Out) :-
  rdf_bnode_iri(First),
  rdf_write_(Kind, Out, Node, rdf:first, First),
  rdf_write_(Kind, Out, First, rdf:type, ll:'HttpReply'),
  write_meta_http_item_(First, H, Kind, Out),
  (   T == []
  ->  rdf_equal(Next, rdf:nil)
  ;   rdf_bnode_iri(Next),
      write_meta_http_list_2(Next, T, Kind, Out)
  ),
  rdf_write_(Kind, Out, Node, rdf:next, Next).

write_meta_http_item_(Item, Meta, Kind, Out) :-
  dict_pairs(Meta.headers, Pairs),
  (   Pairs == []
  ->  true
  ;   rdf_bnode_iri(Headers),
      rdf_write_(Kind, Out, Item, ll:headers, Headers),
      rdf_write_(Kind, Out, Headers, rdf:type, ll:'HttpHeaders'),
      maplist(write_meta_http_header_(Kind, Out, Headers), Pairs)
  ),
  % Some servers emit non-numeric status codes, so we cannot use
  % `xsd:positiveInteger' here.
  Status = Meta.status,
  ensure_atom(Status, Lex),
  rdf_write_(Kind, Out, Item, ll:status, str(Lex)),
  rdf_write_(Kind, Out, Item, ll:uri, uri(Meta.uri)).

% TBD: Multiple values should emit a warning in `http/http_client2'.
write_meta_http_header_(Kind, Out, Headers, PLocal-Lexs) :-
  rdf_prefix_iri(ll:PLocal, P),
  forall(
    member(Lex, Lexs),
    rdf_write_(Kind, Out, Headers, P, str(Lex))
  ).



%! write_meta_now(+Hash:atom, +PLocal:atom) is det.

write_meta_now(Hash, PLocal) :-
  get_time(Now),
  format_time(atom(Lex), "%FT%T%:z", Now),
  write_meta_quad(Hash, PLocal, literal(type(xsd:dateTime,Lex))).



%! write_meta_quad(+Hash:atom, +PLocal:atom, +Input:term) is det.

write_meta_quad(Hash, PLocal, O) :-
  rdf_prefix_iri(id:Hash, S),
  rdf_prefix_iri(ll:PLocal, P),
  write_(meta, Hash, write_meta_quad_(S, P, O)).
write_meta_quad_(S, P, O, Kind, Out) :-
  rdf_write_(Kind, Out, S, P, O).



%! write_meta_serialization_format(+Hash:atom, +MediaType:compound) is det.

write_meta_serialization_format(Hash, MediaType) :-
  dcg_with_output_to(string(String), media_type(MediaType)),
  write_meta_quad(Hash, serializationFormat, String).



%! write_meta_statements(+Hash:atom, +Meta:dict) is det.

write_meta_statements(Hash, Meta) :-
  rdf_prefix_iri(id:Hash, S),
  write_(meta, Hash, write_meta_statements_(S, Meta)).
write_meta_statements_(S, Meta, Kind, Out) :-
  rdf_write_(Kind, Out, S, ll:quadruples, nonneg(Meta.number_of_quadruples)),
  rdf_write_(Kind, Out, S, ll:triples, nonneg(Meta.number_of_triples)).





% HELPERS %

%! ensure_atom(+Atomic:atomic, -Atom:atom) is det.

ensure_atom(N, Atom) :-
  number(N), !,
  atom_number(Atom, N).
ensure_atom(Atom, Atom).



%! rdf_write_(+Kind:oneof([error,meta,warning]), +Out:stream, +S:rdf_nonliteral, +P:iri, +O:term) is det.

rdf_write_(Kind, Out, S, P, O) :-
  rdf_prefix_iri(graph:Kind, G),
  catch(rdf_write_quad(Out, S, P, O, G), E, true),
  % TBD: Some metadata cannot be written, e.g., strange PDF file
  % content peeked as non-RDF content.
  (   var(E)
  ->  true
  ;   debug(ll(debug), "~w ~w", [S,P]),
      rdf_write_quad(Out, S, P, str('OMG!'), G)
  ).



%! write_(+Kind:oneof([error,meta,warning]), +Hash:atom, :Goal_2) is det.

write_(Kind, Hash, Goal_2) :-
  file_name_extension(Kind, 'nq.gz', Local),
  hash_file(Hash, Local, File),
  setup_call_cleanup(
    gzopen(File, append, Out),
    call(Goal_2, Kind, Out),
    close(Out)
  ).
