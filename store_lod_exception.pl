:- module(
  store_lod_exception,
  [
    store_lod_exception/2 % +Md5:atom
                          % +ErrorTerm:compound
  ]
).

/** <module> Store LOD exception

Stores error term denoting exceptions in a LOD format.

@author Wouter Beek
@version 2014/09
*/



% DEB
store_lod_exception(Md5, Error):-
  gtrace,
  store_lod_exception(Md5, Error).


/*
% Archive error.
store_error(Md5, error(archive_error(_,Message),_)):-
  error_code(Var, Message), !,
  store_triple(ll-Md5, llo-exception, error-Var),
  store_triple(error-Var, rdfs-subClassOf, error-'ArchiveException').
% Archive error: TBD
store_error(Md5, error(archive_error(Code,Message),_)):- !,
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-exception, BNode),
  store_triple(BNode, rdf-type, error-'ArchiveException'),
  store_triple(BNode, rdfs-comment, literal(type(xsd-string,Message))),
  store_triple(BNode, error-code, literal(type(xsd-integer,Code))).
% Existence error.
store_error(Md5, error(existence_error(Kind1,Obj),context(_Pred,Message))):-
  error_code(Var, Message), !,
  store_triple(ll-Md5, llo-exception, error-Var),
  
% Existence error: TBD
store_error(Md5, error(existence_error(Kind1,Obj),context(_Pred,Message))):- !,
  dcg_phrase(capitalize, Kind1, Kind2),
  atom_concat(Kind2, 'ExistenceError', Class),
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-exception, BNode),
  store_triple(BNode, rdf-type, error-Class),
  store_triple(BNode, rdfs-comment, literal(type(xsd-string,Message))),
  store_triple(BNode, error-object, literal(type(xsd-string,Obj))).
% HTTP status.
store_error(Md5, error(http_status(Status),_)):- !,
  (   between(400, 599, Status)
  ->  store_triple(ll-Md5, llo-exception, http-Status)
  ;   true
  ),
  store_triple(ll-Md5, llo-httpStatus, http-Status).
store_error(Md5, error(io_error(read,_),context(_,'Inappropriate ioctl for device'))):- !,
  store_triple(ll-Md5, llo-exception, literal(type(xsd-string,'Inappropriate ioctl for device'))).
% IO error.
store_error(Md5, error(io_error(read,_Stream),context(_Predicate,Message))):-
  error_code(VariableName, _, Message), !,
  store_triple(ll-Md5, llo-exception, error-VariableName).
% No RDF Media Type.
store_error(Md5, error(no_rdf(_))):- !,
  store_triple(ll-Md5, llo-serializationFormat, llo-unrecognizedFormat).
% Permission error.
store_error(Md5, error(permission_error(redirect,_,Url),_)):- !,
  % It is not allowed to perform Action on the object Term
  % that is of the given Type.
  rdf_bnode(BNode),
  store_triple(ll-Md5, llo-exception, BNode),
  store_triple(BNode, rdf-type, error-permissionError),
  store_triple(BNode, error-action, error-redirectionLoop),
  store_triple(BNode, error-object, Url).
% Socket error.
store_error(Md5, error(error_code(Message), _)):-
  error_code(VariableName, _, Message), !,
  store_triple(ll-Md5, llo-exception, error-VariableName).
% Socket error: TBD.
store_error(Md5, error(error_code(Undefined), _)):- !,
  format(atom(LexExpr), 'error_code(~a)', [Undefined]),
  store_triple(ll-Md5, llo-exception, literal(type(xsd-string,LexExpr))).
% SSL verification error.
store_error(Md5, error(ssl_error(ssl_verify), _)):- !,
  store_triple(ll-Md5, llo-exception, error-sslError).
% Read timeout error.
store_error(Md5, error(timeout_error(read,_),_)):- !,
  store_triple(ll-Md5, llo-exception, error-readTimeoutError).
*/
