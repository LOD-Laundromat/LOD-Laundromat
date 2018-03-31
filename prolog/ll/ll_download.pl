:- module(ll_download, [ll_download/0]).

/** <module> LOD Laundromat: Download

@author Wouter Beek
@version 2017/09-2017/12
*/

:- use_module(library(debug)).

:- use_module(library(http/http_client2)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(sw/rdf_media_type)).





ll_download :-
  % precondition
  stale_seed(Seed),
  debug(ll(download), "┌─> downloading ~a", [Seed.url]),
  write_meta_now(Seed.hash, downloadBegin),
  % operation
  catch(download_url(Seed.hash, Seed.url), E, true),
  % postcondition
  write_meta_now(Seed.hash, downloadEnd),
  (   % download succeeded
      var(E)
  ->  touch_hash_file(Seed.hash, downloaded),
      debug(ll(download), "└─< downloaded ~a", [Seed.url])
  ;   % download failed
      write_meta_error(Seed.hash, E),
      debug(ll(download), "└─< download failed ~a", [Seed.url])
  ).

download_url(Hash, Uri) :-
  hash_file(Hash, dirty, File),
  setup_call_cleanup(
    open(File, write, Out, [type(binary)]),
    download_stream(Hash, Uri, Out),
    close_metadata(Out, WriteMeta)
  ),
  write_meta_stream(Hash, downloadWritten, WriteMeta).

download_stream(Hash, Uri, Out) :-
  findall(RdfMediaType, rdf_media_type(RdfMediaType), RdfMediaTypes),
  http_open2(Uri, In, [accept(RdfMediaTypes),metadata(HttpMetas)]),
  write_meta_http(Hash, HttpMetas),
  HttpMetas = [HttpMeta|_],
  between(200, 299, HttpMeta.status),
  call_cleanup(
    copy_stream_data(In, Out),
    close_metadata(In, ReadMeta)
  ),
  write_meta_stream(Hash, downloadRead, ReadMeta).
