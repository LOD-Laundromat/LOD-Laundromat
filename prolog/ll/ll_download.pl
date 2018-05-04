:- module(ll_download, [ll_download/0,ll_download/1]).

/** <module> LOD Laundromat: Download

@author Wouter Beek
@version 2017/09-2017/12
*/

:- use_module(library(error)).

:- use_module(library(debug_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(sw/rdf_media_type)).

ll_download :-
  % precondition
  start_seed(Seed),
  _{hash: Hash, url: Uri} :< Seed,
  ll_download(Hash, Uri).

ll_download(Uri) :-
  md5(Uri, Hash),
  ll_download(Hash, Uri).

ll_download(Hash, Uri) :-
  % preparation
  indent_debug(1, ll(_,download), "> downloading ~a ~a", [Hash,Uri]),
  write_meta_now(Hash, downloadBegin),
  write_meta_quad(Hash, ll:url, literal(type(xsd:anyURI,Uri)), graph:meta),
  % operation
  thread_create(download_url(Hash, Uri), Id, [alias(Hash)]),
  thread_join(Id, Status),
  % postcondition
  write_meta_now(Hash, downloadEnd),
  write_meta_status(Hash, Status),
  indent_debug(-1, ll(_,download), "< downloaded ~a ~a", [Hash,Uri]).

download_url(Hash, Uri) :-
  hash_file(Hash, compressed, File),
  setup_call_cleanup(
    open(File, write, Out, [type(binary)]),
    download_stream(Hash, Uri, Out, MediaType, Status, FinalUri),
    close_metadata(Hash, downloadWritten, Out)
  ),
  % End task or finish with an error, depending on the HTTP status
  % code.
  (   between(200, 299, Status)
  ->  write_task_memory(Hash, base_uri, FinalUri),
      (   var(MediaType)
      ->  true
      ;   write_task_memory(Hash, http_media_type, MediaType)
      ),
      end_task(Hash, downloaded)
  ;   % cleanup
      hash_file(Hash, compressed, File),
      delete_file(File),
      (   % Correct HTTP error status code
          between(400, 599, Status)
      ->  throw(error(http_error(status,Status),ll_download))
      ;   % Incorrect HTTP status code
          syntax_error(http_status(Status))
      )
  ).

download_stream(Hash, Uri, Out, MediaType, Status, FinalUri) :-
  findall(RdfMediaType, rdf_media_type(RdfMediaType), RdfMediaTypes),
  http_open2(Uri, In, [accept(RdfMediaTypes),metadata(HttpMetas)]),
  ignore(http_metadata_content_type(HttpMetas, MediaType)),
  write_meta_http(Hash, HttpMetas),
  http_metadata_final_uri(HttpMetas, FinalUri),
  http_metadata_status(HttpMetas, Status),
  call_cleanup(
    copy_stream_data(In, Out),
    close_metadata(Hash, downloadRead, In)
  ).
