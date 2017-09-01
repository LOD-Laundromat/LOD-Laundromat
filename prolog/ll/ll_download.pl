:- module(ll_download, [ll_download/0,ll_download/2]).

/** <module> LOD Laundromat: Download

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(hash_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(http/json)).
:- use_module(library(ll/ll_seedlist)).
:- use_module(library(stream_ext)).
:- use_module(library(zlib)).

ll_download :-
  with_mutex(download, (
    rocks(seedlist, Hash, Dict),
    _{relative: false, status: added, uri: Uri} :< Dict,
    rocks_merge(seedlist, Hash, _{status: downloading})
  )),
  ll_download(Hash, Uri),
  with_mutex(download, rocks_merge(seedlist, Hash, _{status: downloaded})).

ll_download(Hash, Uri) :-
  % Download the data.
  hash_file('/home/wbeek/data/ll', Hash, dirty, File1),
  setup_call_cleanup(
    open(File1, write, Out1, [type(binary)]),
    ll_download(Uri, Out1, Meta),
    close(Out1)
  ),
  % Store the metadata.
  hash_file('/home/wbeek/data/ll', Hash, 'meta.json.gz', File2),
  maplist(writeln, Meta),
  setup_call_cleanup(
    gzopen(File2, write, Out2),
    json_write_dict(Out2, Meta),
    close(Out2)
  ).

ll_download(Uri, Out, Meta) :-
  setup_call_cleanup(
    http_open_ll(Uri, In, Meta),
    copy_stream_data(In, Out),
    close(In)
  ).

http_open_ll(Uri, In, Meta) :-
  http_client2:http_open2(Uri, In, [], Meta),
  Meta = [Dict|_],
  ignore(metadata_content_type([Dict], MediaType)),
  http_client2:check_empty_body(MediaType, In).
