:- module(ll_download, [ll_download/0]).

/** <module> LOD Laundromat: Download

@author Wouter Beek
@version 2017/09-2017/10
*/

:- use_module(library(dcg/dcg_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(hash_stream)).
:- use_module(library(http/http_client2)).
:- use_module(library(http/rfc7231)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_seedlist)).





ll_download :-
  stale_seed(Uri, Hash1),
  debug(ll(download), "┌─> downloading ~a", [Uri]),
  get_time(Begin),
  md5(Hash1-Begin, Hash2),
  seed_store(Hash2{parent: Hash1, status: downloading}),
  seed_merge(Hash1{children: [Hash2]}),
  ll_download1(Hash2, Uri, HttpMeta, ContentMeta),
  get_time(End),
  Dict = Hash2{http: HttpMeta, timestamp: Begin-End},
  (   % download failed
      var(ContentMeta)
  ->  seed_merge(Dict),
      debug(ll(download), "└─< download failed ~a", [Uri])
  ;   % download succeeded
      merge_dicts([Dict,ContentMeta,Hash2{status: filed}], Dict0),
      seed_merge(Dict0),
      debug(ll(download), "└─< downloaded ~a", [Uri])
  ).

ll_download1(Hash2, Uri, HttpMeta, ContentMeta) :-
  hash_file(Hash2, dirty, File1),
  setup_call_cleanup(
    open(File1, write, Out1, [type(binary)]),
    ll_download2(Uri, Out1, HttpMeta, ContentMeta),
    close(Out1)
  ).

ll_download2(CurrentUri, Out, HttpMeta, ContentMeta) :-
  findall(MediaType, rdf_media_type(MediaType), MediaTypes),
  atom_phrase(accept(MediaTypes), Accept),
  http_open2(
    CurrentUri,
    In,
    [metadata(HttpMeta),next(NextUri),request_header('Accept'=Accept)]
  ),
  (var(NextUri) -> true ; add_seed(NextUri)),
  call_cleanup(
    ll_download3(In, Out, HttpMeta, ContentMeta),
    close(In)
  ).

ll_download3(In1, Out, HttpMeta, ContentMeta) :-
  HttpMeta = [Dict|_],
  _{status: Status} :< Dict,
  between(200, 299, Status),
  stream_pair(In1, In2, _),
  setup_call_cleanup(
    open_hash_stream(In2, In3, [algorithm(md5),close_parent(false)]),
    (
      copy_stream_data(In3, Out),
      stream_meta(In3, ContentMeta)
    ),
    close(In3)
  ).
