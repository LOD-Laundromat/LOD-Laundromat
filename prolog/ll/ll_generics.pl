:- module(
  ll_generics,
  [
    call_loop/1,        % :Goal_0
    delete_empty_directories/0,
    hash_directory/2,   % +Hash, -Directory
    hash_entry_hash/3,  % +Hash1, +Entry, -Hash2
    hash_file/3,        % +Hash, +Local, -File
    rdf_media_type/1,   % ?MediaType:compound
    seed_base_uri/2,    % +Seed, -BaseUri
    stream_meta/2,      % +In, -Meta
    uri_hash/2,         % +Uri, -Hash
    uri_last_modified/2 % +Uri, -LastModified
  ]
).
:- reexport(library(debug)).

/** <module> LOD Laundromat: Generics

@author Wouter Beek
@version 2017/09-2017/10
*/

:- use_module(library(conf_ext)).
:- use_module(library(date)).
:- use_module(library(dcg/dcg_ext)).
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(hash_stream)).
:- use_module(library(http/http_client2)).
:- use_module(library(ll/ll_seedlist)).
:- use_module(library(settings)).
:- use_module(library(uri)).

:- initialization
   conf_json(Dict),
   get_dict('data-directory', Dict, Dir),
   create_directory(Dir),
   set_setting(data_directory, Dir).

:- meta_predicate
    call_loop(0),
    running_loop(0).

:- setting(data_directory, atom, .,
           "The directory where data files are stored.").





%! call_loop(:Goal_0) is det.

call_loop(Mod:Goal_0) :-
  thread_create(running_loop(Mod:Goal_0), _, [alias(Goal_0),detached(true)]).



%! delete_empty_directories is det.

delete_empty_directories :-
  setting(data_directory, Root),
  forall(
    directory_path(Root, Path),
    (is_empty_directory(Path) -> delete_directory(Path) ; true)
  ).



%! hash_directory(+Hash:atom, -Directory:atom) is det.

hash_directory(Hash, Dir) :-
  setting(data_directory, Root),
  hash_directory(Root, Hash, Dir).



%! hash_entry_hash(+Hash1:atom, +Entry:atom, -Hash2:atom) is det.

hash_entry_hash(Hash1, Entry, Hash2) :-
  md5(Hash1-Entry, Hash2).



%! hash_file(+Hash:atom, +Local:atom, -File:atom) is det.

hash_file(Hash, Local, File) :-
  setting(data_directory, Root),
  hash_file(Root, Hash, Local, File).



%! rdf_media_type(?MediaType:compound) is nondet.

rdf_media_type(media(application/'json-ld',[])).
rdf_media_type(media(application/'rdf+xml',[])).
rdf_media_type(media(text/turtle,[])).
rdf_media_type(media(application/'n-triples',[])).
rdf_media_type(media(application/trig,[])).
rdf_media_type(media(application/'n-quads',[])).



%! running_loop(:Goal_0) is det.

running_loop(Goal_0) :-
  Goal_0, !,
  running_loop(Goal_0).
running_loop(Goal_0) :-
  sleep(1),
  running_loop(Goal_0).



%! seed_base_uri(+Seed:dict, -BaseUri:atom) is det.

seed_base_uri(Seed, BaseUri) :-
  _{uri: BaseUri} :< Seed, !.
seed_base_uri(Seed1, BaseUri) :-
  _{parent: Parent} :< Seed1,
  seed(Parent, Seed2),
  seed_base_uri(Seed2, BaseUri).



%! stream_meta(+In:stream, -Meta:dict) is det.

stream_meta(In, Meta) :-
  stream_property(In, position(Position)),
  stream_position_data(byte_count, Position, NumberOfBytes),
  stream_position_data(char_count, Position, NumberOfChars),
  stream_position_data(line_count, Position, NumberOfLines),
  stream_property(In, newline(Newline)),
  stream_hash(In, Hash),
  Meta = Hash{
    newline: Newline,
    number_of_bytes: NumberOfBytes,
    number_of_chars: NumberOfChars,
    number_of_lines: NumberOfLines
  }.



%! uri_hash(+Uri:atom, -Hash:atom) is det.

uri_hash(Uri1, Hash) :-
  uri_normalized(Uri1, Uri2),
  md5(Uri2, Hash).



%! uri_last_modified(+Uri:atom, -LastModified:integer) is semidet.

uri_last_modified(Uri, LastModified) :-
  http_head2(Uri, [metadata(Meta)]),
  Meta = [Dict|_],
  _{'last-modified': [LastModifiedAtom]} :< Dict.headers,
  parse_time(LastModifiedAtom, rfc_1123, Timestamp),
  LastModified is float_integer_part(Timestamp).
