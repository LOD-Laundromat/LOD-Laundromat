:- module(ll_download, [ll_download/0]).

/** <module> LOD Laundromat: Download

@author Wouter Beek
@version 2017/09-2017/12
*/

:- use_module(library(debug)).

:- use_module(library(http/http_client2)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(sw/rdf_media_type)).

ll_download :-
  % precondition
  (   debugging(ll(Hash,_))
  ->  hash_url(Hash, Url)
  ;   start_seed(Seed),
      Hash = Seed.hash,
      Url = Seed.url
  ),
  debug(ll(_,download), "┌─> downloading ~a ~a", [Hash,Url]),
  write_meta_now(Hash, downloadBegin),
  write_meta_quad(Hash, def:url, literal(type(xsd:anyURI,Url)), graph:meta),
  % operation
  catch(download_url(Hash, Url, MediaType), E, true),
  % postcondition
  write_meta_now(Hash, downloadEnd),
  failure_success(Hash, downloaded, MediaType, E),
  gtrace,
  debug(ll(_,download), "└─< downloaded ~a ~a", [Hash,Url]).

download_url(Hash, Uri, MediaType) :-
  hash_file(Hash, dirty, File),
  setup_call_cleanup(
    open(File, write, Out, [type(binary)]),
    download_stream(Hash, Uri, Out, MediaType),
    close_metadata(Hash, downloadWritten, Out)
  ).

download_stream(Hash, Uri, Out, MediaType) :-
  findall(RdfMediaType, rdf_media_type(RdfMediaType), RdfMediaTypes),
  http_open2(Uri, In, [accept(RdfMediaTypes),metadata(HttpMetas)]),
  ignore(http_metadata_content_type(HttpMetas, MediaType)),
  write_meta_http(Hash, HttpMetas),
  HttpMetas = [HttpMeta|_],
  between(200, 299, HttpMeta.status),
  call_cleanup(
    copy_stream_data(In, Out),
    close_metadata(Hash, downloadRead, In)
  ).
