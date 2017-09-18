:- module(ll_download, [ll_download/0]).

/** <module> LOD Laundromat: Download

Eligibility for downloading: _{relative: false, status: added, uri: Uri}

If downloading fails: _{http: HttpMeta, status: failed}

If downloading succeeds: _{content: ContentMeta, http: HttpMeta, status:filed}

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(hash_stream)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_seedlist)).



ll_download :-
  with_mutex(ll_download, (
    seed(Seed),
    Hash{relative: false, status: added, uri: Uri} :< Seed,
    seed_merge(Hash{status: downloading})
  )),
  ll_download1(Hash, Uri, HttpMeta, ContentMeta),
  (   % download failed
      var(ContentMeta)
  ->  with_mutex(ll_download, seed_merge(Hash{http: HttpMeta, status: failed}))
  ;   % download succeeded
      with_mutex(ll_download,
        seed_merge(Hash{content: ContentMeta, http: HttpMeta, status: filed})
      )
  ).

ll_download1(Hash, Uri, HttpMeta, ContentMeta) :-
  hash_file(Hash, dirty, File1),
  setup_call_cleanup(
    open(File1, write, Out1, [type(binary)]),
    ll_download2(Uri, Out1, HttpMeta, ContentMeta),
    close(Out1)
  ).

ll_download2(Uri, Out, HttpMeta, ContentMeta) :-
  rdf_http_open(Uri, In, HttpMeta),
  (ll_download3(In, Out, HttpMeta, ContentMeta) -> close(In) ; true).

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
