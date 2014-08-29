:- module(
  schema_error,
  [
    schema_error/1 % +Graph:atom
  ]
).

/** <module> Exception schema

Asserts the exception schema.

@author Wouter Beek
@version 2014/08
*/

:- use_module(library(semweb/rdf_db)).

:- use_module(plRdf(rdfs_build)).
:- use_module(plRdf(rdfs_build2)).
:- use_module(plRdf_owl(owl_build)).

:- rdf_register_prefix(error, 'http://lodlaundromat.org/error/ontology/').
:- rdf_register_prefix(http, 'http://lodlaundromat.org/http/ontology/').
:- rdf_register_prefix(tcp, 'http://lodlaundromat.org/tcp/ontology/').



schema_error(Graph):-
  % error:'Error'
  rdfs_assert_class(
    error:'Error',
    rdfs:'Resource',
    error,
    'An recognized irregularity in program execution.',
    Graph
  ),
  
  % error:Exception
  rdfs_assert_class(
    error:'Exception',
    error:'Error',
    exception,
    'An error that blocks program execution.',
    Graph
  ),
  
  % error:HttpException
  rdfs_assert_class(
    error:'HttpException',
    error:'Exception',
    'HTTP exception',
    'An exception that is emitted as part of an HTTP reply.',
    Graph
  ),
  rdfs_assert_subclass(http:'3xx', error:'HttpException', Graph),
  rdfs_assert_subclass(http:'4xx', error:'HttpException', Graph),
  
  % error:TcpException
  rdfs_assert_class(
    error:'TcpException',
    error:'Exception',
    'TCP exception',
    'An exception that is emitted in TCP communication.',
    Graph
  ),
  owl_assert_class_equivalence(
    error:'TcpException',
    tcp:'TcpException',
    Graph
  ),
  
  % error:Warning
  rdfs_assert_class(
    error:'Warning',
    error:'Error',
    warning,
    'An error that does not block program execution.',
    Graph
  ),
  
  % error:ParserWarning
  rdfs_assert_class(
    error:'ParserWarning',
    error:'Warning',
    'parser warning',
    'A warning that is emitted by a parser.',
    Graph
  ),
  
  % error:ParserWarning
  rdfs_assert_class(
    error:'SgmlParserWarning',
    error:'ParserWarning',
    'SGML parser warning',
    'A warning that is emitted by an SGML parser.',
    Graph
  ),
  
  % error:message
  rdfs_assert_property(
    error:message,
    error:'Error',
    xsd:string,
    'error message',
    'Error message for the human programmer.',
    Graph
  ),
  
  % error:sourceLine
  rdfs_assert_property(
    error:sourceLine,
    error:'ParserWarning',
    xsd:integer,
    'source line',
    'The line number in the original source file for which \c
     a parser warning was thrown.',
    Graph
  ),
  
  % error:readTimeoutException
  rdfs_assert_instance(
    error:readTimeoutException,
    error:'Exception',
    'read timeout error',
    'Timeout occurs while reading',
    Graph
  ),
  
  % error:sslError
  rdfs_assert_instance(
    error:sslError,
    error:'Exception',
    'SSL error',
    'SSL error',
    Graph
  ).

