:- module(
  store_lod_error,
  [
    store_lod_error/3 % +Md5:atom
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

:- use_module(plDcg(dcg_content)).
:- use_module(plDcg(dcg_generic)).

:- use_module(lwm(noRdf_store)).

:- rdf_register_prefix(error, 'http://lodlaundromat.org/error/ontology/').
:- rdf_register_prefix(httpo, 'http://lodlaundromat.org/http/ontology/').
:- rdf_register_prefix(ll,    'http://lodlaundromat.org/resource/').
:- rdf_register_prefix(llo,   'http://lodlaundromat.org/ontology/').


% Archive error
store_lod_error(Md5, Kind, error(archive_error(Code,_))):-
  (   Code == 2
  ->  InstanceName = missingTypeKeywordInMtreeSpec
  ;   true
  ),
  store_triple(ll-Md5, llo-Kind, error-InstanceName).

% Existence error: file
store_lod_error(
  Md5,
  Kind,
  error(existence_error(file,File),context(_Pred,Message))
):-
  (   Message == 'Directory not empty'
  ->  ClassName = 'DirectoryNotEmpty'
  ;   Message == 'No such file or directory'
  ->  ClassName = 'FileExistenceError'
  ;   fail
  ), !,
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-ClassName),
  uri_file_name(Uri, File),
  store_triple(BNode, error-object, Uri).

% HTTP status
store_lod_error(Md5, Kind, error(http_status(Status),_)):- !,
  (   between(400, 599, Status)
  ->  store_triple(ll-Md5, llo-Kind, httpo-Status)
  ;   true
  ),
  store_triple(ll-Md5, llo-httpStatus, httpo-Status).

% IO error
store_lod_error(
  Md5,
  Kind,
  error(io_error(read,_Stream),context(_Pred,Message))
):-
  (   Message == 'Connection reset by peer'
  ->  InstanceName = connectionResetByPeer
  ;   Message == 'Inappropriate ioctl for device'
  ->  InstanceName = notATypewriter
  ;   fail
  ),
  store_triple(ll-Md5, llo-Kind, error-InstanceName).

% IO warning
store_lod_error(Md5, Kind, io_warning(_Stream,Message)):-
  (   Message == 'Illegal UTF-8 continuation'
  ->  InstanceName = illegalUtf8Continuation
  ;   Message == 'Illegal UTF-8 start'
  ->  InstanceName = illegalUtf8Start
  ;   fail
  ),
  store_triple(ll-Md5, llo-Kind, llo-InstanceName).

% No RDF
store_lod_error(Md5, _Kind, error(no_rdf(_File))):- !,
  store_triple(ll-Md5, llo-serializationFormat, llo-unrecognizedFormat).

% Permission error: redirect
store_lod_error(
  Md5,
  Kind,
  error(permission_error(Action0,Type0,Object),context(_,Message))
):- !,
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'PermissionError'),
  store_triple(BNode, error-message, Message),

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
store_lod_error(Md5, Kind, sgml(sgml_parser(_Parser),_File,Line,Message)):- !,
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'SgmlParserWarning'),
  store_triple(BNode, error-sourceLine, literal(type(xsd-integer,Line))),
  store_triple(BNode, error-message, literal(type(xsd-string,Message))).

% Socket error
store_lod_error(Md5, Kind, error(socket_error(Message),_)):-
  (   Message == 'Connection timed out'
  ->  InstanceName = connectionTimedOut
  ;   Message == 'Connection refused'
  ->  InstanceName = connectionRefused
  ;   Message == 'No data'
  ->  InstanceName = noData
  ;   Message == 'No route to host'
  ->  InstanceName = noRouteToHost
  ;   Message == 'Host not found'
  ->  InstanceName = hostNotFound
  ;   Message == 'Try Again'
  ->  InstanceName = tryAgain
  ;   fail
  ), !,
  store_triple(ll-Md5, llo-Kind, llo-InstanceName).

% SSL error: SSL verify
store_lod_error(Md5, Kind, error(ssl_error(ssl_verify),_)):- !,
  store_triple(ll-Md5, llo-Kind, error-sslError).

% Syntax error
store_lod_error(
  Md5,
  Kind,
  error(syntax_error(Message),stream(_Stream,Line,Column,Character))
):- !,
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'SyntaxError'),
  store_position(BNode, Line, Column, Character),
  store_triple(BNode, error-message, literal(type(xsd-string,Message))).

% Timeout error: read
store_lod_error(
  Md5,
  Kind,
  error(timeout_error(read,_Stream),context(_Pred,_))
):- !,
  store_triple(ll-Md5, llo-Kind, llo-readTimeoutError).

% Turtle: undefined prefix
store_lod_error(
  Md5,
  Kind,
  error(
    existence_error(turtle_prefix,Prefix),
    stream(_Stream,Line,Column,CharacterNumber)
  )
):- !,
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'MissingTurtlePrefixDefintion'),
  store_triple(BNode, error-prefix, literal(type(xsd-string,Prefix))),
  store_position(BNode, Line, Column, CharacterNumber).

% RDF/XML: multiple definitions
store_lod_error(Md5, Kind, rdf(redefined_id(Uri))):- !,
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error:redefinedRdfId),
  store_triple(BNode, error-object, Uri).

% RDF/XML: unparsable
store_lod_error(Md5, Kind, rdf(unparsed(DOM))):- !,
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-Kind, BNode),
  store_triple(BNode, rdf-type, error-'RdfXmlParserError'),
  xml_dom_to_atom([], DOM, Atom1),
  atom_truncate(Atom1, 1000, Atom2),
  store_triple(BNode, error-dom, literal(type(xsd-string,Atom2))).

% DEB
store_lod_error(Md5, Kind, Error):-
  gtrace,
  store_lod_error(Md5, Kind, Error).



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
  store_triple(BNode, error-line, literal(type(xsd-integer,Line))),
  store_triple(BNode, error-linePosition, literal(type(xsd-integer,Column))),
  store_triple(BNode, error-character, literal(type(xsd-integer,Character))).

