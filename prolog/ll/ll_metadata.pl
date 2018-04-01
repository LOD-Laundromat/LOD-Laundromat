:- module(
  ll_metadata,
  [
    close_metadata/3,      % +Hash, +PLocal, +Out
    write_meta/2,          % +Hash, :Goal_1
    write_meta_archive/2,  % +Hash, +Metadata
    write_meta_encoding/2, % +Hash, +Encodings
    write_meta_error/2,    % +Hash, +Error
    write_meta_http/2,     % +Hash, +Metadata
    write_meta_now/2,      % +Hash, +PLocal
    write_meta_triple/3,   % +Hash, ?P, ?O
    write_meta_triple/4    % +Hash, ?S, ?P, ?O
  ]
).

/** <module> LOD Laundromat: Metadata assertions

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(yall)).

:- use_module(library(dict)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(stream_ext)).
:- use_module(library(sw/rdf_export)).
:- use_module(library(sw/rdf_prefix)).
:- use_module(library(sw/rdf_term)).

:- maplist(rdf_assert_prefix, [
     def-'https://lodlaundromat.org/def/',
     id-'https://lodlaundromat.org/id/'
   ]).

:- meta_predicate
    write_meta(+, 1).

:- rdf_meta
   rdf_meta_triple(+, r, o),
   rdf_meta_triple(+, r, r, o).





%! close_metadata(+Hash:atom, +PLocal:atom, +Out:stream) is det.

close_metadata(Hash, PLocal, Out) :-
  stream_metadata(Out, Meta),
  rdf_global_id(id:Hash, S),
  rdf_global_id(def:PLocal, P),
  write_meta(Hash, write_meta_stream_(S, P, Meta)),
  close(Out).

write_meta_stream_(S, P, Meta, Out) :-
  rdf_bnode_iri(O),
  rdf_write_triple(Out, S, P, O),
  rdf_write_triple(Out, O, def:newline, literal(type(xsd:string,Meta.newline))),
  maplist(
    atom_number,
    [NumBytes,NumChars,NumLines],
    [Meta.bytes,Meta.characters,Meta.lines]
  ),
  rdf_write_triple(Out, O, def:bytes, literal(type(xsd:string,NumBytes))),
  rdf_write_triple(Out, O, def:characters, literal(type(xsd:string,NumChars))),
  rdf_write_triple(Out, O, def:lines, literal(type(xsd:string,NumLines))).



%! write_meta(+Hash:atom, :Goal_1) is det.

write_meta(Hash, Goal_1) :-
  hash_file(Hash, 'meta.nt', File),
  setup_call_cleanup(
    open(File, append, Out),
    call(Goal_1, Out),
    close(Out)
  ).



%! write_meta_archive(+Hash:atom, +Metadata:list(dict)) is det.

write_meta_archive(Hash, L) :-
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_archive_list(S, L)).

write_meta_archive_list(S, L, Out) :-
  rdf_bnode_iri(O),
  rdf_write_triple(Out, S, def:archive, O),
  write_meta_archive_list(O, L, Out).

write_meta_archive_list(Node, [H|T], Out) :-
  rdf_bnode_iri(First),
  rdf_write_triple(Out, Node, rdf:first, First),
  write_meta_archive_item(First, H, Out),
  (   T == []
  ->  rdf_equal(Next, rdf:type)
  ;   rdf_bnode_iri(Next),
      write_meta_http_list(Next, T, Out)
  ),
  rdf_write_triple(Out, Node, rdf:next, Next).

write_meta_archive_item(Item, Meta, Out) :-
  atomic_list_concat(Meta.filters, ' ', Filters),
  maplist(
    atom_number,
    [Mtime,Permissions,Size],
    [Meta.mtime,Meta.permissions,Meta.size]
  ),
  rdf_write_triple(Out, Item, def:filetype, literal(type(xsd:string,Meta.filetype))),
  rdf_write_triple(Out, Item, def:filter, literal(type(xsd:string,Filters))),
  rdf_write_triple(Out, Item, def:format, literal(type(xsd:string,Meta.format))),
  rdf_write_triple(Out, Item, def:mtime, literal(type(xsd:float,Mtime))),
  rdf_write_triple(Out, Item, def:name, literal(type(xsd:string,Meta.name))),
  rdf_write_triple(Out, Item, def:permissions, literal(type(xsd:positiveInteger,Permissions))),
  rdf_write_triple(Out, Item, def:permissions, literal(type(xsd:float,Size))).



%! write_meta_encoding(+Hash:atom, +Encodings:list(atom)) is det.

write_meta_encoding(Hash, Encodings) :-
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_encoding(S, Encodings)).

write_meta_encoding(S, [GuessEnc,HttpEnc,XmlEnc,Enc], Out) :-
  rdf_write_triple(Out, S, def:encoding, literal(type(xsd:string,Enc))),
  (var(GuessEnc) -> true ; rdf_write_triple(Out, S, def:uchardet, literal(type(xsd:string,GuessEnc)))),
  (var(HttpEnc) -> true ; rdf_write_triple(Out, S, def:httpEncoding, literal(type(xsd:string,HttpEnc)))),
  (var(XmlEnc) -> true ; rdf_write_triple(Out, S, def:xmlEncoding, literal(type(xsd:string,XmlEnc)))).



%! write_meta_error(+Hash:atom, +Error:compound) is det,

write_meta_error(Hash, E) :-
  gtrace,
  print_message(informational, dummy(Hash,E)).



%! write_meta_http(+Hash:atom, +Metadata:list(dict)) is det,

write_meta_http(Hash, L) :-
  rdf_global_id(id:Hash, S),
  write_meta(Hash, write_meta_http_list(S, L)).

write_meta_http_list(S, L, Out) :-
  rdf_bnode_iri(O),
  rdf_write_triple(Out, S, def:http, O),
  write_meta_http_list(O, L, Out).

write_meta_http_list(Node, [H|T], Out) :-
  rdf_bnode_iri(First),
  rdf_write_triple(Out, Node, rdf:first, First),
  write_meta_http_item(First, H, Out),
  (   T == []
  ->  rdf_equal(Next, rdf:nil)
  ;   rdf_bnode_iri(Next),
      write_meta_http_list(Next, T, Out)
  ),
  rdf_write_triple(Out, Node, rdf:next, Next).

write_meta_http_item(Item, Meta, Out) :-
  dict_pairs(Meta.headers, Pairs),
  maplist(write_meta_http_header(Out, Item), Pairs),
  atom_number(Lex, Meta.status),
  rdf_write_triple(Out, Item, def:status, literal(type(xsd:positiveInteger,Lex))),
  rdf_write_triple(Out, Item, def:url, literal(type(xsd:anyURI,Meta.uri))).

% TBD: Multiple values should emit a warning in `http/http_client2'.
write_meta_http_header(Out, Item, PLocal-[Lex|_]) :-
  rdf_global_id(def:PLocal, P),
  rdf_write_triple(Out, Item, P, literal(type(xsd:string,Lex))).



%! write_meta_now(+Hash:atom, +PLocal:atom) is det.

write_meta_now(Hash, PLocal) :-
  rdf_global_id(id:Hash, S),
  rdf_global_id(def:PLocal, P),
  write_meta(Hash, write_meta_now_(S, P)).

write_meta_now_(S, P, Out) :-
  get_time(Now),
  format_time(atom(Lex), "%FT%T%:z", Now),
  rdf_write_triple(Out, S, P, literal(type(xsd:dateTime,Lex))).



%! write_meta_triple(+Hash:atom, +P:iri, +O:rdf_term) is det.
%! write_meta_triple(+Hash:atom, +S:rdf_nonliteral, +P:iri, +O:rdf_term) is det.

write_meta_triple(Hash, P, O) :-
  rdf_global_id(id:Hash, S),
  write_meta_triple(Hash, S, P, O).


write_meta_triple(Hash, S, P, O) :-
  write_meta(Hash, {S,P,O}/[Out]>>rdf_write_triple(Out, S, P, O)).
