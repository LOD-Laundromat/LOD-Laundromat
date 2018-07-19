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
:- use_module(library(yall)).

:- use_module(library(call_ext)).
:- use_module(library(dcg)).
:- use_module(library(dict)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(media_type)).
:- use_module(library(stream_ext)).
:- use_module(library(semweb/rdf_export)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).

:- discontiguous
    write_message/3.

:- maplist(rdf_register_prefix, [
     error-'https://lodlaundromat.org/error/def/',
     graph-'https://lodlaundromat.org/graph/',
     http-'https://lodlaundromat.org/http/def/',
     id-'https://lodlaundromat.org/id/',
     ll-'https://lodlaundromat.org/def/',
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
  rdf_write_(Kind, Out, O, ll:newline, str(Meta.newline)),
  rdf_write_(Kind, Out, O, ll:bytes, nonneg(Meta.bytes)),
  rdf_write_(Kind, Out, O, ll:characters, nonneg(Meta.characters)),
  rdf_write_(Kind, Out, O, ll:lines, nonneg(Meta.lines)).



%! write_message(+Kind:oneof([error,warning]), +Hash:atom, +Error:compound) is det,

% Archive errors.
%
% Observes instances:
%   - Msg = 'Invalid central directory signature'
%   - Msg = 'Missing type keyword in mtree specification'
%   - Msg = 'Unrecognized archive format'
%   - memberchk(Code, [22,25,1001])
%     Msg = 'Truncated input file (needed INETEGER bytes, only INTEGER available)'
%   - Msg = 'Can\'t parse line INTEGER'
write_message(Kind, Hash, error(archive_error(Code,Msg),_Context)) :- !,
  write_message_(Kind, Hash, 'ArchiveError', write_message_1(Code, Msg)).
write_message_1(Code, Msg, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:code, positive_integer(Code)),
  rdf_write_(Kind, Out, O, ll:message, str(Msg)).

% HTTP error: ???
write_message(Kind, Hash, error(domain_error(http_encoding,identity),_Context)) :- !,
  write_message_(Kind, Hash, 'HttpEncodingIdentity').

% Set cookie error: ???
write_message(Kind, Hash, error(domain_error(set_cookie,Value),_Context)) :- !,
  write_message_(Kind, Hash, 'SetCookieError', write_message_2(Value)).
write_message_2(Value, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:value, str(Value)).

% Malformed URL
write_message(Kind, Hash, error(domain_error(url,Url),_Context)) :- !,
  write_message_(Kind, Hash, 'InvalidUrl', write_message_3(Url)).
write_message_3(Url, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:value, uri(Url)).

% Existence error: the HTTP reply is empty (no status line, no
% headers, and no body).
write_message(Kind, Hash, error(existence_error(http_reply,Url),_Context)) :- !,
  write_message_(Kind, Hash, 'EmptyHttpReply', write_message_4(Url)).
write_message_4(Url, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:alias, uri(Url)).

% Existence error: URI scheme is not registered with IANA.
write_message(Kind, Hash, error(existence_error(uri_scheme,Scheme),_Context)) :- !,
  write_message_(Kind, Hash, 'IllegalUriScheme', write_message_5(Scheme)).
write_message_5(Scheme, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:scheme, str(Scheme)).

% Existence error: Turtle prefix is not declared.
write_message(Kind, Hash, error(existence_error(turtle_prefix,Alias),stream(Line,Column,_))) :- !,
  write_message_(Kind, Hash, 'MissingTurtlePrefixDeclaration', write_message_6(Alias, Line, Column)).
write_message_6(Alias, Line, Column, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:alias, str(Alias)),
  write_message_stream(Line, Column, Kind, Out, O).

% HTTP maximum redirection sequence length exceeded.
write_message(Kind, Hash, error(http_error(max_redirect,_Length,_Urls),_Context)) :- !,
  write_message_(Kind, Hash, 'HttpMaxRedirect').

% HTTP no content type header in reply.
write_message(Kind, Hash, error(http_error(no_content_type,_Uri),_Context)) :- !,
  write_message_(Kind, Hash, 'HttpNoContentType').

% HTTP redirection loop detected.
write_message(Kind, Hash, error(http_error(redirect_loop,_Urls),_Context)) :- !,
  write_message_(Kind, Hash, 'HttpRedirectLoop').

% HTTP error status code
write_message(Kind, Hash, error(http_error(status,Status),_Context)) :- !,
  write_message_(Kind, Hash, 'HttpErrorStatus', write_message_7(Status)).
write_message_7(Status, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:code, positive_integer(Status)).

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
write_message(Kind, Hash, error(no_encoding,_Context)) :-
  write_message_(Kind, Hash, 'NoEncoding').

% Representation error: Turtle character
write_message(Kind, Hash, error(representation_error(turtle_character),_)) :- !,
  write_message_(Kind, Hash, 'TurtleCharacter').

% Resource error: global stack
write_message(Kind, Hash, error(resource_error(stack),global)) :- !,
  write_message_(Kind, Hash, 'GlobalStack').

% Socket errors:
%   - Msg = 'Connection refused'
%   - Msg = 'Connection reset by peer'
%   - Msg = 'Connection timed out'
%   - Msg = 'Host not found'
%   - Msg = 'Network is unreachable'
%   - Msg = 'No Data'
%   - Msg = 'No Recovery'
%   - Msg = 'No route to host'
%   - Msg = 'Try Again'
write_message(Kind, Hash, error(socket_error(Msg),_Context)) :- !,
  write_message_(Kind, Hash, 'SocketError', write_message_9(Msg)).
write_message_9(Msg, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:message, str(Msg)).

% SSL error: unexpected end of file
%
% Observed instances:
%   - Code = 'SSL_eof'
%     Library = ssl
%     Function = negotiate
%     Reason = 'Unexpected end-of-file'
write_message(Kind, Hash, error(ssl_error(Code,Library,Function,Reason),_Context)) :- !,
  write_message_(Kind, Hash, 'SslUnexpectedEof', write_message_10(Code, Library, Function, Reason)).
write_message_10(Code, Library, Function, Reason, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:code, str(Code)),
  rdf_write_(Kind, Out, O, ll:library, str(Library)),
  rdf_write_(Kind, Out, O, ll:function, str(Function)),
  rdf_write_(Kind, Out, O, ll:reason, str(Reason)).

% Syntax error: HTTP parameter
write_message(Kind, Hash, error(syntax_error(http_parameter(Param)),_)) :- !,
  write_message_(Kind, Hash, 'HttpParameterParseError', write_message_11(Param)).
write_message_11(Param, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:parameter, str(Param)).

% Syntax error: inccorrect HTTP status code
write_message(Kind, Hash, error(syntax_error(http_status(Status)),_Context)) :- !,
  ensure_atom(Status, Lex),
  write_message_(Kind, Hash, 'IncorrectHttpStatusCode', write_message_12(Lex)).
write_message_12(Lex, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:status, str(Lex)).

% Syntax error: incorrect URI authority component.
write_message(Kind, Hash, error(syntax_error(uri_authority(Auth)),_Context)) :- !,
  write_message_(Kind, Hash, 'IncorrectUriAuthority', write_message_13(Auth)).
write_message_13(Auth, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:authority, str(Auth)).

% Syntax error: incorrect URI path component.
write_message(Kind, Hash, error(syntax_error(uri_path(Path)),_Context)) :- !,
  write_message_(Kind, Hash, 'IncorrectUriPath', write_message_14(Path)).
write_message_14(Path, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:path, str(Path)).

% Syntax error: incorrect URI scheme component.
write_message(Kind, Hash, error(syntax_error(uri_scheme(Scheme)),_Context)) :- !,
  write_message_(Kind, Hash, 'IncorrectUriScheme', write_message_15(Scheme)).
write_message_15(Scheme, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:scheme, str(Scheme)).

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
write_message(Kind, Hash, error(syntax_error(Msg),Stream0)) :- !,
  (Stream0 = stream(Stream,_,_,_) -> true ; Stream = Stream0),
  stream_line_column(Stream, Line, Column),
  write_message_16(Kind, Hash, Msg, Line, Column).
write_message(Kind, Hash, error(syntax_error(Msg),_Stream,Line,Column,_)) :- !,
  write_message_16(Kind, Hash, Msg, Line, Column).
write_message_16(Kind, Hash, Msg, Line, Column) :-
  write_message_(Kind, Hash, 'RdfParseError', write_message_17(Msg, Line, Column)).
write_message_17(Msg, Line, Column, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:message, str(Msg)),
  write_message_stream(Line, Column, Kind, Out, O).

write_message(Kind, Hash, error(timeout_error(read,_Stream),_Context)) :- !,
  write_message_(Kind, Hash, 'HttpTimeout').

% RDF/XML and RDFa: Cannot parse as XML DOM
write_message(Kind, Hash, error(type_error(xml_dom,Dom))) :- !,
  write_message_(Kind, Hash, 'IllegalXmlDom', write_message_18(Dom)).
write_message_18(Dom, Kind, Out, O) :-
  rdf_literal_value(Literal, rdf:'XMLLiteral', Dom),
  rdf_write_(Kind, Out, O, ll:dom, Literal).

% Illegal Unicode character ???
write_message(Kind, Hash, io_warning(Stream,'Illegal UTF-8 continuation')) :- !,
  write_message_(Kind, Hash, 'IllegalUtf8Continuation'),
  stream_line_column(Stream, Line, Column),
  write_message_(Kind, Hash, 'IllegalUtf8Continuation', write_message_stream(Line, Column)).

% Unicode something ???
write_message(Kind, Hash, io_warning(Stream,'Illegal UTF-8 start')) :- !,
  stream_line_column(Stream, Line, Column),
  write_message_(Kind, Hash, 'IllegalUtf8Start', write_message_stream(Line, Column)).

% RDF syntax error: the lexical form does not occur in the lexical
% space of the indicated datatype.
write_message(Kind, Hash, rdf(incorrect_lexical_form(D,Lex))) :- !,
  write_message_(Kind, Hash, 'CannotMapLexicalForm', write_message_20(D, Lex)).
write_message_20(D, Lex, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:datatype, D),
  rdf_write_(Kind, Out, O, ll:lexicalForm, str(Lex)).

% RDF syntax error: a language-tagged string where the language tag is
% missing.
write_message(Kind, Hash, rdf(missing_language_tag(LTag))) :- !,
  write_message_(Kind, Hash, 'MissingLanguageTag', write_message_21(LTag)).
write_message_21(LTag, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:languageTag, str(LTag)).

% Not an RDF serialization format.
write_message(Kind, Hash, error(rdf(non_rdf_format,Str),_Context)) :- !,
  write_message_(Kind, Hash, 'NonRdfFormat', write_message_22(Str)).
write_message_22(Str, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:content, str(Str)).

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
  format(string(String), "~k", [Tag]),
  rdf_write_(Kind, Out, O, ll:tag, String).

% RDF/XML parser error: unparseable DOM.
write_message(Kind, Hash, rdf(unparsed(Dom))) :- !,
  write_message_(Kind, Hash, 'RdfXmlParseError', write_message_26(Dom)).
write_message_26(Dom, Kind, Out, O) :-
  rdf_literal_value(Literal, rdf:'XMLLiteral', Dom),
  rdf_write_(Kind, Out, O, ll:dom, Literal).

% JSON-LD serialization format is not yet supported.
write_message(Kind, Hash, rdf(unsupported_format(media(application/'ld+json',[]),_Content))) :- !,
  write_message_(Kind, Hash, 'JsonldNotYetSupported').

% RDF/XML parse error.
write_message(Kind, Hash, sgml(sgml_parser(_Parser),_File,Line,Msg)) :- !,
  write_message_(Kind, Hash, 'RdfParseError', write_message_27(Line, Msg)).
write_message_27(Line, Msg, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:line, nonneg(Line)),
  rdf_write_(Kind, Out, O, ll:message, str(Msg)).

% RDF non-canonicity: a language-tagged string where the language tag is
% not in canonical form.
write_message(Kind, Hash, rdf(non_canonical_language_tag(LTag))) :- !,
  write_message_(Kind, Hash, 'NonCanonicalLanguageTag', write_message_28(LTag)).
write_message_28(LTag, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:languageTag, str(LTag)).

% RDF non-canonicicty: a lexical form that belongs to the lexical
% space, but is not canonical.
write_message(Kind, Hash, rdf(non_canonical_lexical_form(D,Lex1,Lex2))) :- !,
  write_message_(Kind, Hash, 'NonCanonicalLexicalForm', write_message_29(D, Lex1, Lex2)).
write_message_29(D, Lex1, Lex2, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:datatype, D),
  rdf_write_(Kind, Out, O, ll:lexicalForm, str(Lex1)),
  rdf_write_(Kind, Out, O, ll:canonicalLexicalForm, str(Lex2)).

% TBD: Not yet handled.
write_message(Kind, Hash, E) :-
  rdf_prefix_iri(id:Hash, S),
  format(string(String), "~k", [E]),
  write_(Kind, Hash, write_message_30(S, String)).
write_message_30(S, String, Kind, Out) :-
  rdf_write_(Kind, Out, S, ll:error, str(String)).

write_message_(Kind, Hash, CName) :-
  write_message_(Kind, Hash, CName, true).

write_message_(Kind, Hash, CName, Goal_3) :-
  rdf_prefix_iri(id:Hash, S),
  write_(Kind, Hash, write_message_(S, CName, Goal_3)).

write_message_(S, CName, Goal_3, Kind, Out) :-
  rdf_bnode_iri(O),
  rdf_write_(Kind, Out, S, ll:error, O),
  rdf_prefix_iri(error:CName, C),
  rdf_write_(Kind, Out, O, rdf:type, C),
  call(Goal_3, Kind, Out, O).

write_message_stream(Line, Column, Kind, Out, O) :-
  rdf_write_(Kind, Out, O, ll:line, nonneg(Line)),
  rdf_write_(Kind, Out, O, ll:column, nonneg(Column)).



%! write_meta_archive(+Hash:atom, +Filters:list(atom)) is det.

write_meta_archive(_, []) :- !.
write_meta_archive(Hash, L) :-
  rdf_prefix_iri(id:Hash, S),
  write_(meta, Hash, write_meta_archive_1(S, L)).
write_meta_archive_1(S, L, Kind, Out) :-
  rdf_bnode_iri(O),
  (   L == []
  ->  true
  ;   rdf_write_(Kind, Out, S, ll:filter, O),
      write_meta_archive_2(O, L, Kind, Out)
  ).
write_meta_archive_2(Node, [H|T], Kind, Out) :-
  rdf_write_(Kind, Out, Node, rdf:first, str(H)),
  (   T == []
  ->  rdf_equal(Next, rdf:nil)
  ;   rdf_bnode_iri(Next),
      write_meta_archive_2(Node, T, Kind, Out)
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
  write_(meta, ArchiveHash, write_meta_archive_(Archive, Entry)),
  write_(meta, EntryHash, write_meta_entry_(Archive, EntryName, Entry, Format, Props)).
write_meta_archive_(Archive, Entry, Kind, Out) :-
  rdf_write_(Kind, Out, Archive, rdf:type, ll:'Archive'),
  rdf_write_(Kind, Out, Archive, ll:entry, Entry).
write_meta_entry_(Archive, EntryName, Entry, Format, Props, Kind, Out) :-
  memberchk(mtime(MTime), Props),
  memberchk(permissions(Permissions), Props),
  memberchk(size(Size), Props),
  rdf_write_(Kind, Out, Entry, rdf:type, ll:'Entry'),
  rdf_write_(Kind, Out, Entry, ll:archive, Archive),
  rdf_write_(Kind, Out, Entry, ll:format, str(Format)),
  rdf_write_(Kind, Out, Entry, ll:mtime, MTime),
  rdf_write_(Kind, Out, Entry, ll:name, str(EntryName)),
  rdf_write_(Kind, Out, Entry, ll:permissions, positive_integer(Permissions)),
  rdf_write_(Kind, Out, Entry, ll:size, positive_integer(Size)).



%! write_meta_http(+Hash:atom, +Metadata:list(dict)) is det,

write_meta_http(Hash, L) :-
  rdf_prefix_iri(id:Hash, S),
  write_(meta, Hash, write_meta_http_list1(S, L)).
write_meta_http_list1(S, L, Kind, Out) :-
  rdf_bnode_iri(O),
  rdf_write_(Kind, Out, S, ll:http, O),
  write_meta_http_list2(O, L, Kind, Out).
write_meta_http_list2(Node, [H|T], Kind, Out) :-
  rdf_bnode_iri(First),
  rdf_write_(Kind, Out, Node, rdf:first, First),
  write_meta_http_item(First, H, Kind, Out),
  (   T == []
  ->  rdf_equal(Next, rdf:nil)
  ;   rdf_bnode_iri(Next),
      write_meta_http_list2(Next, T, Kind, Out)
  ),
  rdf_write_(Kind, Out, Node, rdf:next, Next).
write_meta_http_item(Item, Meta, Kind, Out) :-
  dict_pairs(Meta.headers, Pairs),
  maplist(write_meta_http_header(Out, Item, Kind), Pairs),
  % Some servers emit non-numeric status codes, so we cannot use
  % `xsd:positiveInteger' here.
  Status = Meta.status,
  (atom(Status) -> Lex = Status ; atom_number(Lex, Status)),
  rdf_write_(Kind, Out, Item, ll:status, str(Lex)),
  rdf_write_(Kind, Out, Item, ll:url, uri(Meta.uri)).
% TBD: Multiple values should emit a warning in `http/http_client2'.
write_meta_http_header(Out, Item, Kind, PLocal-Lexs) :-
  rdf_prefix_iri(ll:PLocal, P),
  forall(
    member(Lex, Lexs),
    rdf_write_(Kind, Out, Item, P, str(Lex))
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
  write_(meta, Hash, {S,P,O}/[Kind,Out]>>rdf_write_(Kind, Out, S, P, O)).



%! write_meta_serialization_format(+Hash:atom, +MediaType:compound) is det.

write_meta_serialization_format(Hash, MediaType) :-
  dcg_with_output_to(string(String), media_type(MediaType)),
  write_meta_quad(Hash, serializationFormat, str(String)).



%! write_meta_statements(+Hash:atom, +Meta:dict) is det.

write_meta_statements(Hash, Meta) :-
  rdf_prefix_iri(id:Hash, S),
  write_(meta, Hash, write_meta_statements_(S, Meta)).
write_meta_statements_(S, Meta, Kind, Out) :-
  rdf_write_(Kind, Out, S, ll:quadruples, nonneg(Meta.number_of_quadruples)),
  rdf_write_(Kind, Out, S, ll:triples, nonneg(Meta.number_of_triples)).





% HELPERS %

%! ensure_atom(+Term:term, -Atom:atom) is det.

ensure_atom(Atom, Atom) :-
  atom(Atom), !.
ensure_atom(N, Atom) :-
  number(N), !,
  atom_number(Atom, N).



%! rdf_write_(+Kind:oneof([error,meta,warning]), +Out:stream, +S:rdf_nonliteral, +P:iri, +O:term) is det.

rdf_write_(Kind, Out, S, P, O0) :-
  rdf_prefix_iri(graph:Kind, G),
  rdf_create_term(O0, O),
  rdf_write_quad(Out, S, P, O, G).



%! write_(+Kind:oneof([error,meta,warning]), +Hash:atom, :Goal_2) is det.

write_(Kind, Hash, Goal_2) :-
  file_name_extension(Kind, nq, Local),
  hash_file(Hash, Local, File),
  setup_call_cleanup(
    open(File, append, Out),
    call(Goal_2, Kind, Out),
    close(Out)
  ).
