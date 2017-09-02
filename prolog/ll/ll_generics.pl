:- module(
  ll_generics,
  [
    content_meta/2,   % +In, -Meta
    hash_entry_hash/3 % +Hash1, +Entry, -Hash2
  ]
).

/** <module> LOD Laundromat: Generics

@author Wouter Beek
@version 2017/08
*/

:- use_module(library(hash_ext)).
:- use_module(library(hash_stream)).



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
