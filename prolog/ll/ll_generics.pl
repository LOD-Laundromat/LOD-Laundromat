:- module(
  ll_generics,
  [
    content_meta/2,    % +In, -Meta
    hash_entry_hash/3, % +Hash1, +Entry, -Hash2
    hash_file/3,       % +Hash, +Local, -File
    rdf_http_open/3,   % +Uri, -In, -HttpMeta
    seed_base_uri/2    % +Seed, -BaseUri
  ]
).

/** <module> LOD Laundromat: Generics

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(dcg/dcg_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(hash_stream)).
:- use_module(library(http/rfc7231)).
:- use_module(library(ll/ll_seedlist)).



%! content_meta(+In:stream, -Meta:dict) is det.

content_meta(In, Meta) :-
  stream_property(In, position(Position)),
  stream_position_data(byte_count, Position, NumberOfBytes),
  stream_position_data(char_count, Position, NumberOfChars),
  stream_position_data(line_count, Position, NumberOfLines),
  stream_property(In, newline(Newline)),
  stream_hash(In, Hash),
  Meta = content{
    hash: Hash,
    newline: Newline,
    number_of_bytes: NumberOfBytes,
    number_of_chars: NumberOfChars,
    number_of_lines: NumberOfLines
  }.



%! hash_entry_hash(+Hash1:atom, +Entry:atom, -Hash2:atom) is det.

hash_entry_hash(Hash1, Entry, Hash2) :-
  md5(Hash1-Entry, Hash2).



%! hash_file(+Hash:atom, +Local:atom, -File:atom) is det.

hash_file(Hash, Local, File) :-
  hash_file('/home/wbeek/data/ll', Hash, Local, File).



%! rdf_http_open(+Uri:atom, -In:stream, -HttpMeta:dict) is det.

rdf_http_open(Uri, In, HttpMeta) :-
  findall(MediaType, rdf_media_type(MediaType), MediaTypes),
  atom_phrase(accept(MediaTypes), Accept),
  http_client2:http_open2(Uri, In, [request_header('Accept'=Accept)], HttpMeta).



%! rdf_media_type(?MediaType:compound) is nondet.

rdf_media_type(media(application/'json-ld',[])).
rdf_media_type(media(application/'rdf+xml',[])).
rdf_media_type(media(text/turtle,[])).
rdf_media_type(media(application/'n-triples',[])).
rdf_media_type(media(application/trig,[])).
rdf_media_type(media(application/'n-quads',[])).



%! seed_base_uri(+Seed:dict, -BaseUri:atom) is det.

seed_base_uri(Seed, BaseUri) :-
  _{uri: BaseUri} :< Seed, !.
seed_base_uri(Seed1, BaseUri) :-
  _{parent: Parent} :< Seed1,
  seed(Parent, Seed2),
  seed_base_uri(Seed2, BaseUri).
