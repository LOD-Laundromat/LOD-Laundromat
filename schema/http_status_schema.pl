:- module(
  http_status_schema,
  [
    assert_http_status_schema/1 % +Graph:atom
  ]
).

/** <module> HTTP Status Schema

Asserts the schema of HTTP statuses.

@author Wouter Beek
@version 2014/08
*/

:- use_module(library(semweb/rdf_db)).

:- use_module(plRdf(rdfs_build)).
:- use_module(plRdf(rdfs_build2)).
:- use_module(plRdf_owl(owl_build)).
:- use_module(plRdf_term(rdf_datatype)).
:- use_module(plRdf_term(rdf_string)).

:- rdf_register_prefix(
     http,
     'http://lodlaundromat.org/http-status/ontology/'
   ).
:- rdf_register_prefix('http-w3c', 'http://www.w3.org/2011/http#').

:- meta_predicate(rdfs_assert_status(r,r,r,+,+,+,+)).



assert_http_status_schema(Graph):-
  % http:Status
  rdfs_assert_class(
    http:'Status',
    rdfs:'Class',
    'HTTP status',
    'A status that is returned by an HTTP server, \c
     indicating whether an HTTP request was handled successfully or not.',
    Graph
  ),
  owl_assert_class_equivalence(
    http:'Status',
    'http-w3c':'StatusCode',
    Graph
  ),
  
  % http:1xx
  rdfs_assert_class(
    http:'1xx',
    http:'Status',
    'Informational',
    'An HTTP status that is informational.',
    Graph
  ),
  rdfs_assert_status(
    http:100,
    http:'1xx',
    'Continue',
    'Indicates that the initial part of a request has been received \c
     and has not yet been rejected by the server. \c
     The server intends to send a final response after the request \c
     has been fully received and acted upon.',
    100,
    Graph
  ),
  rdfs_assert_status(
    http:101,
    http:'1xx',
    'Switching Protocols',
    'Indicates that the server understands and is willing to comply \c
     with the client\'s request, via the Upgrade header field \c
     for a change in the application protocol being used on this connection.',
    101,
    Graph
  ),
  
  % http:2xx
  rdfs_assert_class(
    http:'2xx',
    http:'Status',
    'Successful',
    'An HTTP status that is successful.',
    Graph
  ),
  rdfs_assert_status(
    http:200,
    http:'2xx',
    'OK',
    'Indicates that the request has succeeded.',
    200,
    Graph
  ),
  rdfs_assert_status(
    http:201,
    http:'2xx',
    'Created',
    'Indicates that the request has been fulfilled and has resulted in \c
     one or more new resources being created.',
    201,
    Graph
  ),
  rdfs_assert_status(
    http:202,
    http:'2xx',
    'Accepted',
    'Indicates that the request has been accepted for processing, \c
     but the processing has not been completed.',
    202,
    Graph
  ),
  rdfs_assert_status(
    http:203,
    http:'2xx',
    'Non-Authoritative Information',
    'Indicates that the request was successful but the enclosed payload \c
     has been modified from that of the origin server\'s 200 (OK) response \c
     by a transforming proxy.',
    203,
    Graph
  ),
  rdfs_assert_status(
    http:204,
    http:'2xx',
    'No Content',
    'Indicates that the server has successfully fulfilled the request \c
     and that there is no additional content to send in \c
     the response payload body.',
    204,
    Graph
  ),
  rdfs_assert_status(
    http:205,
    http:'2xx',
    'Reset Content',
    'Indicates that the server has fulfilled the request and desires that \c
     the user agent reset the "document view", which caused the request \c
     to be sent, to its original state as received from the origin server.',
    205,
    Graph
  ),
  
  % http:3xx
  rdfs_assert_class(
    http:'3xx',
    http:'Status',
    'Redirection',
    'An HTTP status that is a redirection.',
    Graph
  ),
  rdfs_assert_status(
    http:300,
    http:'3xx',
    'Multiple Choices',
    'Indicates that the target resource has more than one representation, \c
     each with its own more specific identifier, \c
     and information about the alternatives is being provided so that \c
     the user (or user agent) can select a preferred representation \c
     by redirecting its request to one or more of those identifiers.',
    300,
    Graph
  ),
  rdfs_assert_status(
    http:301,
    http:'3xx',
    'Moved Permanently',
    'Indicates that the target resource has been assigned \c
     a new permanent URI and any future references to this resource \c
     ought to use one of the enclosed URIs.',
    301,
    Graph
  ),
  rdfs_assert_status(
    http:302,
    http:'3xx',
    'Found',
    'Indicates that the target resource resides temporarily under \c
     a different URI.',
    302,
    Graph
  ),
  rdfs_assert_status(
    http:303,
    http:'3xx',
    'See Other',
    'Indicates that the server is redirecting the user agent to \c
     a different resource, as indicated by a URI in \c
     the Location header field, which is intended to provide \c
     an indirect response to the original request.',
    303,
    Graph
  ),
  rdfs_assert_status(
    http:305,
    http:'3xx',
    'Use Proxy',
    'The requested resource MUST be accessed through the proxy given by \c
     the Location field.',
    305,
    Graph,
    'http://tools.ietf.org/html/rfc2616'
  ),
  rdfs_assert_status(
    http:306,
    http:'3xx',
    'Unused',
    'Subsequent requests should use the specified proxy.',
    306,
    Graph,
    'https://tools.ietf.org/html/draft-cohen-http-305-306-responses-00'
  ),
  rdfs_assert_status(
    http:307,
    http:'3xx',
    'Temporary Redirect',
    'Indicates that the target resource resides temporarily \c
     under a different URI and the user agent MUST NOT change \c
     the request method if it performs an automatic redirection to that URI.',
    307,
    Graph
  ),
  rdfs_assert_status(
    http:307,
    http:'3xx',
    'Temporary Redirect',
    'Indicates that the target resource resides temporarily \c
     under a different URI and the user agent MUST NOT change \c
     the request method if it performs an automatic redirection to that URI.',
    307,
    Graph
  ),
  rdfs_assert_status(
    http:308,
    http:'3xx',
    'Permanent Redirect',
    'The request, and all future requests should be repeated \c
     using another URI.',
    308,
    Graph,
    'http://tools.ietf.org/html/rfc7238'
  ),
  
  % http:4xx
  rdfs_assert_class(
    http:'4xx',
    http:'Status',
    'Client Error',
    'An HTTP status that is unsuccessful due to a client error.',
    Graph
  ),
  rdfs_assert_subclass(http:'4xx', exception:'Exception', Graph),
  rdfs_assert_status(
    http:400,
    http:'4xx',
    'Bad Request',
    'Indicates that the server cannot or will not process the request \c
     due to something that is perceived to be a client error.',
    400,
    Graph
  ),
  rdfs_assert_status(
    http:402,
    http:'4xx',
    'Payment Required',
    'Reserved for future use.',
    402,
    Graph
  ),
  rdfs_assert_status(
    http:403,
    http:'4xx',
    'Forbidden',
    'Indicates that the server understood the request \c
     but refuses to authorize it.',
    403,
    Graph
  ),
  rdfs_assert_status(
    http:404,
    http:'4xx',
    'Not Found',
    'Indicates that the origin server did not find a current representation \c
     for the target resource or is not willing to disclose that one exists.',
    404,
    Graph
  ),
  rdfs_assert_status(
    http:405,
    http:'4xx',
    'Method Not Allowed',
    'Indicates that the method received in the request-line is known \c
     by the origin server but not supported by the target resource.',
    405,
    Graph
  ),
  rdfs_assert_status(
    http:406,
    http:'4xx',
    'Not Acceptable',
    'Indicates that the target resource does not have a current \c
     representation that would be acceptable to the user agent, \c
     according to the proactive negotiation header fields received \c
     in the request, and the server is unwilling to supply \c
     a default representation.',
    406,
    Graph
  ),
  rdfs_assert_status(
    http:408,
    http:'4xx',
    'Request Timeout',
    'Indicates that the server did not receive a complete request message \c
     within the time that it was prepared to wait.',
    408,
    Graph
  ),
  rdfs_assert_status(
    http:409,
    http:'4xx',
    'Conflict',
    'Indicates that the request could not be completed due to a conflict \c
     with the current state of the target resource.',
    409,
    Graph
  ),
  rdfs_assert_status(
    http:410,
    http:'4xx',
    'Gone',
    'Indicates that access to the target resource is no longer available \c
     at the origin server and that this condition is likely to be permanent.',
    410,
    Graph
  ),
  rdfs_assert_status(
    http:411,
    http:'4xx',
    'Length Required',
    'Indicates that the server refuses to accept the request \c
     without a defined Content-Length.',
    411,
    Graph
  ),
  rdfs_assert_status(
    http:413,
    http:'4xx',
    'Payload Too Large',
    'Indicates that the server is refusing to process a request \c
     because the request payload is larger than the server is willing \c
     or able to process.',
    413,
    Graph
  ),
  rdfs_assert_status(
    http:414,
    http:'4xx',
    'URI Too Long',
    'Indicates that the server is refusing to service the request \c
     because the request-target is longer than the server is willing \c
     to interpret.',
    414,
    Graph
  ),
  rdfs_assert_status(
    http:415,
    http:'4xx',
    'Unsupported Media Type',
    'Indicates that the origin server is refusing to service the request \c
     because the payload is in a format not supported by this method \c
     on the target resource.',
    415,
    Graph
  ),
  rdfs_assert_status(
    http:417,
    http:'4xx',
    'Expectation Failed',
    'Indicates that the expectation given in the request\'s Expect \c
     header field could not be met by at least one of the inbound servers.',
    417,
    Graph
  ),
  rdfs_assert_status(
    http:426,
    http:'4xx',
    'Upgrade Required',
    'Indicates that the server refuses to perform the request \c
     using the current protocol but might be willing to do so after \c
     the client upgrades to a different protocol.',
    426,
    Graph
  ),
  
  % http:5xx
  rdfs_assert_class(
    http:'5xx',
    http:'Status',
    'Server Error',
    'An HTTP status that is unsuccessful due to a server error.',
    Graph
  ),
  rdfs_assert_subclass(http:'5xx', exception:'Exception', Graph),
  rdfs_assert_status(
    http:501,
    http:'5xx',
    'Not Implemented',
    'Indicates that the server does not support the functionality required \c
     to fulfill the request.',
    501,
    Graph
  ),
  rdfs_assert_status(
    http:502,
    http:'5xx',
    'Bad Gateway',
    'Indicates that the server, while acting as a gateway or proxy, \c
     received an invalid response from an inbound server it accessed \c
     while attempting to fulfill the request.',
    502,
    Graph
  ),
  rdfs_assert_status(
    http:503,
    http:'5xx',
    'Service Unavailable',
    'Indicates that the server is currently unable to handle the request \c
     due to a temporary overload or scheduled maintenance, \c
     which will likely be alleviated after some delay.',
    503,
    Graph
  ),
  rdfs_assert_status(
    http:504,
    http:'5xx',
    'Gateway Timeout',
    'Indicates that the server, while acting as a gateway or proxy, \c
     did not receive a timely response from an upstream server \c
     it needed to access in order to complete the request.',
    504,
    Graph
  ),
  rdfs_assert_status(
    http:505,
    http:'5xx',
    'HTTP Version Not Supported',
    'Indicates that the server does not support, or refuses to support, \c
     the major version of HTTP that was used in the request message.',
    505,
    Graph
  ),
  
  % http:statusCode
  rdfs_assert_property(
    http:statusCode,
    http:'Status',
    xsd:integer,
    'HTTP status code',
    'The numeric indicator of an HTTP status.',
    Graph
  ),
  owl_assert_class_equivalence(
    http:statusCode,
    'http-w3c':statusCodeNumber,
    Graph
  ),
  
  % http:reasonPhrase
  rdfs_assert_property(
    http:reasonPhrase,
    http:'Status',
    xsd:string,
    'HTTP reason phrase',
    'A natural language message describing an HTTP status.',
    Graph
  ).



% Helpers.

rdfs_assert_status(Uri, Class, ReasonPhrase, Code, Comment, Graph):-
  rdfs_assert_status(
    Uri,
    Class,
    ReasonPhrase,
    Code,
    Comment,
    Graph,
    'http://tools.ietf.org/html/rfc7231'
  ).

rdfs_assert_status(Uri, Class, ReasonPhrase, Code, Comment, Graph, Def):-
  rdfs_assert_instance(Uri, Class, ReasonPhrase, Comment, Graph),
  rdf_assert_datatype(Uri, http:statusCode, Code, xsd:int, Graph),
  rdf_assert_string(Uri, http:reasonPhrase, ReasonPhrase, Graph),
  rdf_assert(Uri, rdfs:isDefinedBy, Def, Graph).

