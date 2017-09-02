:- module(ll_download, [ll_download/0]).

/** <module> LOD Laundromat: Download

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(hash_ext)).
:- use_module(library(hash_stream)).
:- use_module(library(http/http_client2), []).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_seedlist)).



ll_download :-
  with_mutex(download, (
    seed(Seed),
    Hash{relative: false, status: added, uri: Uri} :< Seed,
    seed_merge(Hash{status: downloading})
  )),
  ll_download1(Hash, Uri, HttpMeta, ContentMeta),
  with_mutex(download,
    seed_merge(Hash{content: ContentMeta, http: HttpMeta, status: filed})
  ).

ll_download1(Hash, Uri, HttpMeta, ContentMeta) :-
  hash_file(Hash, dirty, File1),
  setup_call_cleanup(
    open(File1, write, Out1, [type(binary)]),
    ll_download2(Uri, Out1, HttpMeta, ContentMeta),
    close(Out1)
  ).

ll_download2(Uri, Out, HttpMeta, ContentMeta) :-
  setup_call_cleanup(
    http_client2:http_open2(Uri, In, [], HttpMeta),
    ll_download3(In, Out, ContentMeta),
    close(In)
  ).

ll_download3(In1, Out, ContentMeta) :-
  setup_call_cleanup(
    open_hash_stream(In1, In2, [algorithm(md5),close_parent(false)]),
    (
      copy_stream_data(In2, Out),
      content_meta(In2, ContentMeta)
    ),
    close(In2)
  ).
