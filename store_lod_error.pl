:- module(
  store_lod_error,
  [
    store_lod_error/3 % +Datadoc:url
                      % +Kind:oneof([exception,warning])
                      % +ErrorTerm:compound
  ]
).

/** <module> Store LOD exception

Stores error term denoting exceptions in a LOD format.

@author Wouter Beek
@version 2014/09
*/

:- use_module(library(semweb/rdf_db)).
:- use_module(library(uri)).

:- use_module(generics(atom_ext)).
:- use_module(xml(xml_dom)).

:- use_module(lwm(noRdf_store)).

:- rdf_register_prefix(error, 'http://lodlaundromat.org/error/ontology/').
:- rdf_register_prefix(httpo, 'http://lodlaundromat.org/http/ontology/').
:- rdf_register_prefix(ll, 'http://lodlaundromat.org/resource/').
:- rdf_register_prefix(llo, 'http://lodlaundromat.org/ontology/').



% Archive error
store_lod_error(Datadoc, Kind, error(archive_error(Code,_),_)):- !,
  (   Code == 2
  ->  InstanceName = missingTypeKeywordInMtreeSpec
  ;   true
  ),
  store_triple(Datadoc, llo-Kind, error-InstanceName).

% Encoding: character
store_lod_error(Datadoc, Kind, error(type_error(character,Char),context(_Pred,_Var))):- !,
  rdf_bnode(BNode),
  store_triple(Datadoc, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'EncodingError'),
  store_triple(BNode, error-object, literal(type(xsd-integer,Char))).

% Existence error: directory
store_lod_error(
  Datadoc,
  Kind,
  error(existence_error(directory,Directory),context(_Pred,Message))
):- !,
  (   Message == 'File exists'
  ->  ClassName = 'DirectoryExistenceError'
  ;   fail
  ),
  rdf_bnode(BNode),
  store_triple(Datadoc, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-ClassName),
  uri_file_name(Uri, Directory),
  store_triple(BNode, error-object, literal(type(xsd-anyURI,Uri))).

% Existence error: file
store_lod_error(
  Datadoc,
  Kind,
  error(existence_error(file,File),context(_Pred,Message))
):- !,
  (   Message == 'Directory not empty'
  ->  ClassName = 'DirectoryNotEmpty'
  ;   Message == 'No such file or directory'
  ->  ClassName = 'FileExistenceError'
  ;   fail
  ),
  rdf_bnode(BNode),
  store_triple(Datadoc, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-ClassName),
  uri_file_name(Uri, File),
  store_triple(BNode, error-object, literal(type(xsd-anyURI,Uri))).

% Existence error: source sink?
store_lod_error(
  Datadoc,
  Kind,
  error(existence_error(source_sink,Path),context(_Pred,Message))
):- !,
  (   Message == 'Is a directory'
  ->  ClassName = 'IsADirectoryError'
  ;   fail
  ), !,
  rdf_bnode(BNode),
  store_triple(Datadoc, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-ClassName),
  uri_file_name(Uri, Path),
  store_triple(BNode, error-object, literal(type(xsd-anyURI,Uri))).


% HTTP status
store_lod_error(Datadoc, Kind, error(http_status(Status),_)):- !,
  (   between(400, 599, Status)
  ->  store_triple(Datadoc, llo-Kind, httpo-Status)
  ;   true
  ),
  store_triple(Datadoc, llo-httpStatus, httpo-Status).

% IO error: read
store_lod_error(
  Datadoc,
  Kind,
  error(io_error(read,_Stream),context(_Pred,Message))
):- !,
  (   Message == 'Connection reset by peer'
  ->  InstanceName = connectionResetByPeer
  ;   Message == 'Inappropriate ioctl for device'
  ->  InstanceName = notATypewriter
  ;   Message = 'Is a directory'
  ->  InstanceName = isADirectory
  ;   fail
  ),
  store_triple(Datadoc, llo-Kind, error-InstanceName).

% IO error: write
store_lod_error(
  Datadoc,
  Kind,
  error(io_error(write,_Stream),context(_Pred,Message))
):- !,
  (   Message == 'Encoding cannot represent character'
  ->  InstanceName = encodingError
  ;   fail
  ),
  store_triple(Datadoc, llo-Kind, error-InstanceName).

% IO warning
store_lod_error(Datadoc, Kind, io_warning(_Stream,Message)):- !,
  (   Message == 'Illegal UTF-8 continuation'
  ->  InstanceName = illegalUtf8Continuation
  ;   Message == 'Illegal UTF-8 start'
  ->  InstanceName = illegalUtf8Start
  ;   fail
  ),
  store_triple(Datadoc, llo-Kind, error-InstanceName).

% Malformed URL
store_lod_error(Datadoc, Kind, error(domain_error(url,Url),_)):- !,
  rdf_bnode(BNode),
  store_triple(BNode, rdf-type, error-'MalformedUrl'),
  store_triple(BNode, error-object, literal(type(xsd-anyURI,Url))),
  store_triple(Datadoc, llo-Kind, BNode).

% No RDF
store_lod_error(Datadoc, _Kind, error(no_rdf(_File))):- !,
  store_triple(Datadoc, llo-serializationFormat, error-unrecognizedFormat).

% Permission error: redirect
store_lod_error(
  Datadoc,
  Kind,
  error(permission_error(Action0,Type0,Object),context(_,Message1))
):- !,
  rdf_bnode(BNode),
  store_triple(Datadoc, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'PermissionError'),
  atom_truncate(Message1, 500, Message2),
  store_triple(BNode, error-message, literal(type(xsd-string,Message2))),

  % Action
  (   Action0 == redirect
  ->  Action = redirectAction
  ;   true
  ),
  store_triple(BNode, error-action, error-Action),

  % Object
  store_triple(BNode, error-object, Object),

  % Type
  (   Type0 == http
  ->  ObjectClass = 'HttpUri'
  ;   true
  ),
  store_triple(Object, error-object, error-ObjectClass).

% SGML parser
store_lod_error(
  Datadoc,
  Kind,
  sgml(sgml_parser(_Parser),_File,Line,Message1)
):- !,
  rdf_bnode(BNode),
  store_triple(Datadoc, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'SgmlParserError'),
  store_triple(
    BNode,
    error-sourceLine,
    literal(type(xsd-nonNegativeInteger,Line))
  ),
  atom_truncate(Message1, 500, Message2),
  store_triple(BNode, error-message, literal(type(xsd-string,Message2))).

% Socket error
store_lod_error(Datadoc, Kind, error(socket_error(Message),_)):- !,
  (   Message == 'Connection timed out'
  ->  InstanceName = connectionTimedOut
  ;   Message == 'Connection refused'
  ->  InstanceName = connectionRefused
  ;   Message == 'No Data'
  ->  InstanceName = noData
  ;   Message == 'No route to host'
  ->  InstanceName = noRouteToHost
  ;   Message == 'Host not found'
  ->  InstanceName = hostNotFound
  ;   Message == 'Try Again'
  ->  InstanceName = tryAgain
  ;   fail
  ),
  store_triple(Datadoc, llo-Kind, error-InstanceName).

% SSL error: SSL verify
store_lod_error(Datadoc, Kind, error(ssl_error(ssl_verify),_)):- !,
  store_triple(Datadoc, llo-Kind, error-sslError).

% Syntax error
store_lod_error(
  Datadoc,
  Kind,
  error(syntax_error(Message1),stream(_Stream,Line,Column,Character))
):- !,
  rdf_bnode(BNode),
  store_triple(Datadoc, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'SyntaxError'),
  store_position(BNode, Line, Column, Character),
  atom_truncate(Message1, 500, Message2),
  store_triple(BNode, error-message, literal(type(xsd-string,Message2))).

% Timeout error: read
store_lod_error(
  Datadoc,
  Kind,
  error(timeout_error(read,_Stream),context(_Pred,_))
):- !,
  store_triple(Datadoc, llo-Kind, error-readTimeoutError).

% Turtle: undefined prefix
store_lod_error(
  Datadoc,
  Kind,
  error(
    existence_error(turtle_prefix,Prefix),
    stream(_Stream,Line,Column,CharacterNumber)
  )
):- !,
  rdf_bnode(BNode),
  store_triple(Datadoc, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'MissingTurtlePrefixDefintion'),
  store_triple(BNode, error-prefix, literal(type(xsd-string,Prefix))),
  store_position(BNode, Line, Column, CharacterNumber).

% RDF/XML: multiple definitions
store_lod_error(Datadoc, Kind, rdf(redefined_id(Uri))):- !,
  rdf_bnode(BNode),
  store_triple(Datadoc, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'RedefinedRdfId'),
  store_triple(BNode, error-object, Uri).

% RDF/XML: name
store_lod_error(Datadoc, Kind, rdf(not_a_name(XmlName))):- !,
  rdf_bnode(BNode),
  store_triple(Datadoc, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'XmlNameError'),
  store_triple(BNode, error-object, literal(type(xsd-string,XmlName))).

% RDF/XML: unparsable
store_lod_error(Datadoc, Kind, rdf(unparsed(DOM))):- !,
  rdf_bnode(BNode),
  store_triple(Datadoc, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'RdfXmlParserError'),
  xml_dom_to_atom([], DOM, Atom1),
  atom_truncate(Atom1, 500, Atom2),
  store_triple(BNode, error-dom, literal(type(xsd-string,Atom2))).

% RDFa: Processing Instruction
% @tbd This seems to be incorrectly identified as an error by the RDFa parser.
store_lod_error(Datadoc, Kind, error(type_error(xml_dom, pi(_PI)),_)):- !,
  store_triple(Datadoc, error-Kind, error-processingInstruction).

% DEB
store_lod_error(Datadoc, Kind, Error):-
  gtrace,
  store_lod_error(Datadoc, Kind, Error).



% Helpers

%! store_position(
%!   +Resource:or([bnode,iri]),
%!   +Line:nonneg,
%!   +Column:nonneg,
%!   +Character:nonneg
%! ) is det.

store_position(Resource, Line, Column, Character):-
  rdf_bnode(BNode),
  store_triple(Resource, error-streamPosition, BNode),
  store_triple(BNode, rdf-type, error-'StreamPosition'),
  store_triple(BNode, error-line, literal(type(xsd-nonNegativeInteger,Line))),
  store_triple(
    BNode,
    error-linePosition,
    literal(type(xsd-nonNegativeInteger,Column))
  ),
  store_triple(
    BNode,
    error-character,
    literal(type(xsd-nonNegativeInteger,Character))
  ).

