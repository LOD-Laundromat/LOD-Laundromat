:- module(
  clean,
  [
    clean/0,
    clean_uri/1, % +Uri
    seed_uri/1   % -Uri
  ]
).

/** <module> LOD Laundromat

@author Wouter Beek
@version 2017/04
*/

:- use_module(library(apply)).
:- use_module(library(archive)).
:- use_module(library(ckan_api)).
:- use_module(library(debug)).
:- use_module(library(http/http_header)).
:- use_module(library(http/http_open)).
:- use_module(library(lists)).
:- use_module(library(md5)).
:- use_module(library(rdf)).
:- use_module(library(semweb/rdf_guess)).
:- use_module(library(semweb/rdf_http_plugin)).
:- use_module(library(semweb/rdf_ntriples)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(semweb/rdfa)).
:- use_module(library(semweb/turtle)).
:- use_module(library(zlib)).

:- use_module(library(file_ext)).
:- use_module(library(uri/uri_ext)).

:- debug(clean).

:- dynamic
    format/2.

:- rdf_register_prefix(bnode, 'https://lodlaundromat.org/.well-known/genid/').





%! clean is det.

clean :-
  clean_uri('http://resource.geolba.ac.at/GeologicUnit/export/GeologicUnit.rdf').



%! clean_uri(+BaseUri) is det.

clean_uri(BaseUri) :-
  md5_hash(BaseUri, Hash, []),
  hash_to_dir(Hash, Dir),
  download(BaseUri, Dir, File, HttpMediaType),
  forall(
    unpack_file(File, EntryFile),
    clean_uri(BaseUri, Hash, HttpMediaType, Dir, EntryFile)
  ).

unpack_file(File, EntryFile) :-
  findall(format(Format), archive_format(Format, true), Opts),
  archive_open(File, read, Arch, [filter(all)|Opts]),
  archive_data_stream(Arch, In, [meta_data(L)]),
  unpack_file_entry(In, File, EntryFile, L).

% @tbd multiple entries
unpack_file_entry(In, File, File, [_]) :- !,
  close(In).

clean_uri(BaseUri, Hash, HttpMediaType, Dir, File1) :-
  ignore(uri_media_type(BaseUri, ExtMediaType)),
  rdf_global_id(bnode:Hash, BNodePrefix),
  directory_file_path(Dir, clean, File2),
  setup_call_cleanup(
    (
      open(File1, read, In),
      open(File2, write, Out)
    ),
    (
      rdf_guess(In, MediaTypes),
      choose_media_type(MediaTypes, HttpMediaType, ExtMediaType, MediaType),
      clean_stream(In, Out, BNodePrefix, BaseUri, MediaType)
    ),
    (
      close(In),
      close(Out)
    )
  ).

choose_media_type([MediaType], _, _, MediaType) :- !.
choose_media_type(MediaTypes, HttpMediaType, ExtMediaType, MediaType) :-
  member(MediaType, MediaTypes),
  member(MediaType0, [HttpMediaType,ExtMediaType]),
  generalization(MediaType, MediaType0), !.

clean_stream(In, Out, BNodePrefix, BaseUri, media(application/'n-quads',_)) :- !,
  rdf_process_ntriples(
    In,
    write_clean_tuples(Out),
    [anon_prefix(BNodePrefix),base_uri(BaseUri)]
  ).
clean_stream(In, Out, BNodePrefix, BaseUri, media(application/'n-triples',_)) :- !,
  rdf_process_ntriples(
    In,
    write_clean_tuples(Out),
    [anon_prefix(BNodePrefix),base_uri(BaseUri)]
  ).
clean_stream(In, Out, _BNodePrefix, BaseUri, media(application/'rdf+xml',_)) :- !,
  % @tbd blank nodes
  process_rdf(In, write_clean_tuples(Out), [base_uri(BaseUri)]).
clean_stream(In, Out, BNodePrefix, BaseUri, media(application/trig,_)) :- !,
  rdf_process_turtle(
    In,
    write_clean_tuples(Out),
    [
      anon_prefix(BNodePrefix),
      base_uri(BaseUri),
      format(trig),
      resources(iri)
    ]
  ).
clean_stream(In, Out, BNodePrefix, BaseUri, media(application/'xhtml+xml',_)) :- !,
  read_rdfa(In, Triples, [anon_prefix(BNodePrefix),base(BaseUri)]),
  maplist(write_clean_tuple(Out), Triples).
clean_stream(In, Out, BNodePrefix, BaseUri, media(text/turtle,_)) :- !,
  rdf_process_turtle(
    In,
    write_clean_tuples(Out),
    [
      anon_prefix(BNodePrefix),
      base_uri(BaseUri),
      format(turtle),
      resources(iri)
    ]
  ).



%! download(+Uri, +Dir, -File, -MediaType) is det.

download(Uri, Dir, File1, MediaType) :-
  create_directory(Dir),
  directory_file_path(Dir, dirty, File1),
  setup_call_cleanup(
    (
      open(File1, write, Out1, [type(binary)]),
      open_uri(Uri, In, Headers)
    ),
    (
      copy_stream_data(In, Out1),
      stream_property(Out1, position(Pos)),
      stream_position_data(byte_count, Pos, NumBytes),
      stream_position_data(char_count, Pos, NumChars),
      stream_position_data(line_count, Pos, NumLines),
      stream_property(Out1, newline(Newline))
    ),
    (
      close(In),
      close(Out1)
    )
  ),
  (   memberchk(content_type(ContentType), Headers)
  ->  http_parse_header_value(content_type, ContentType, MediaType)
  ;   true
  ),
  directory_file_path(Dir, log, File2),
  setup_call_cleanup(
    open(File2, write, Out2),
    maplist(
      write_term0(Out2),
      [
        uri(Uri),
        byte_count(NumBytes),
        char_count(NumChars),
        line_count(NumLines),
        newline(Newline)
      | Headers
      ]
    ),
    close(Out2)
  ).

open_uri(Uri, In, []) :-
  uri_components(Uri, uri_components(file,_,_,_,_)), !,
  uri_file_name(Uri, File),
  open(File, read, In, [type(binary)]).
open_uri(Uri, In, Headers) :-
  rdf_http_plugin:rdf_accept_header_value(_, Accept),
  http_open(Uri, In, [headers(Headers),request_header(accept,Accept)]).

write_term0(Out, Header) :-
  write_canonical(Out, Header),
  write(Out, ' .\n').



%! seed_uri(-Uri) is nondet.

seed_uri(Uri) :-
  ckan_site_uri(Site),
  ckan_resource(Site, Res),
  atom_string(Format, Res.format),
  (rdf_format(Format) -> atom_string(Uri, Res.url)).





% HELPERS %

%! archive_formats(?Format, ?Active) is nondet.

archive_format('7zip', true).
archive_format(ar, true).
archive_format(cab, true).
archive_format(cpio, true).
archive_format(empty, true).
archive_format(gnutar, true).
archive_format(iso9660, true).
archive_format(lha, true).
archive_format(mtree, false).
archive_format(rar, true).
archive_format(raw, true).
archive_format(tar, true).
archive_format(xar, true).
archive_format(zip, true).



%! clean_graph(+G1, -G2) is det.

clean_graph(G1, G3) :-
  rdf11:post_graph(G2, G1),
  (G2 == user -> G3 = default ; G3 = G2).



%! clean_object(+O1, -O2) is semidet.

clean_object(O1, O2) :-
  rdf11:legacy_literal_components(O1, D, Lex1, LTag1), !,
  (   rdf_equal(rdf:'HTML', D)
  ->  rdf11:write_xml_literal(html, Lex1, Lex2)
  ;   rdf_equal(rdf:'XMLLiteral', D)
  ->  rdf11:write_xml_literal(xml, Lex1, Lex2)
  ;   Lex2 = Lex1
  ),
  catch(
    (
      rdf11:post_object(O2, O1),
      rdf11:pre_object(O2, O3),
      rdf11:legacy_literal_components(O3, D, Lex3, LTag3)
    ),
    E,
    true
  ),
  % Warn for a non-canonical lexical form.
  (   Lex2 \== Lex3
  ->  print_message(warning, non_canonical_lexical_form(D,Lex2,Lex3))
  ;   true
  ),
  % Warn for a non-canonical language tag.
  (   ground(LTag1),
      LTag1 \== LTag3
  ->  print_message(warning, non_canonical_language_tag(LTag1))
  ;   true
  ),
  % Warn and fail for an incorrect lexical form.
  (var(E) -> true ; print_message(warning, E), fail).
clean_object(O, O).



%! clean_tuple(+Tuple, -Quad) is det.

clean_tuple(rdf(S,P,O1,G1), rdf(S,P,O2,G2)) :- !,
  clean_graph(G1, G2),
  clean_object(O1, O2),
  (rdf_is_subject(S) -> true ; debug(clean, "BUGGY S ~w", [S]), fail),
  (rdf_is_predicate(P) -> true ; debug(clean, "BUGGY P ~w", [P]), fail),
  (rdf_is_object(O2) -> true ; debug(clean, "BUGGY O ~w", [O2]), fail),
  (rdf_is_graph(G2) -> true ; debug(clean, "BUGGY G ~w", [G2]), fail).
clean_tuple(rdf(S,P,O), Quad) :-
  rdf_default_graph(G),
  clean_tuple(rdf(S,P,O,G), Quad).



%! generalization(+MediaType1, +MediaType2) is semidet.

generalization(MediaType, MediaType) :- !.
generalization(MediaType1, MediaType3) :-
  generalization0(MediaType1, MediaType2),
  generalization(MediaType2, MediaType3).

generalization0(
  media(application/trig,Params),
  media(text/turtle,Params)
).
generalization0(
  media(application/'n-quads',Params),
  media(application/'n-triples',Params)
).
generalization0(
  media(text/turtle,Params),
  media(application/'n-triples',Params)
).



%! hash_to_dir(+Hash, -Dir) is det.

hash_to_dir(Hash, Dir) :-
  atom_codes(Hash, Cs),
  append([H1,H2], T, Cs),
  atom_codes(Dir1, [H1,H2]),
  atom_codes(Dir2, T),
  atomic_list_concat(['',scratch,wbeek,ll,Dir1,Dir2], /, Dir).



%! rdf_format(?Format) is nondet.

rdf_format('RDF').
rdf_format('SPARQL').



%! rdf_is_graph(+G) is semidet.

rdf_is_graph(default) :- !.
rdf_is_graph(G) :-
  rdf_is_iri(G).



%! uri_media_type(+Uri, -MediaType) is det.

uri_media_type(Uri, MediaType) :-
  uri_file_extensions(Uri, Exts),
  member(Ext1, Exts),
  (   media_type_ext(MediaType, Ext1)
  ;   alt_ext(Ext1, Ext2),
      media_type_ext(MediaType, Ext2)
  ), !.

media_type_ext(media(application/'ld+json',[]), jsonld).
media_type_ext(media(application/'n-quads',[]), nq).
media_type_ext(media(application/'n-triples',[]), nt).
media_type_ext(media(application/'rdf+xml',[]), rdf).
media_type_ext(media(application/trig,[]), trig).
media_type_ext(media(application/'xhtml+xml',[]), xhtml).
media_type_ext(media(text/html,[]), html).
media_type_ext(media(text/turtle,[]), ttl).

alt_ext(json, jsonld).
alt_ext(nquads, nq).
alt_ext(ntriples, nt).
alt_ext(n3, ttl).
alt_ext(turtle, ttl).



%! write_clean_tuple(+Out, +Tuple) is det.

write_clean_tuple(Out, Tuple) :-
  clean_tuple(Tuple, rdf(S,P,O,_)), !,
  write_ntriple0(Out, S, P, O),
  write_eot(Out).
% always succeeds
write_clean_tuple(_, _).

write_clean_tuples(Out, Tuples, _) :-
  maplist(write_clean_tuple(Out), Tuples).

write_ntriple0(Out, S, P, O) :-
  write_subject(Out, S),
  write(Out, ' '),
  write_predicate(Out, P),
  write(Out, ' '),
  write_object(Out, O),
  write(Out, ' ').

write_eot(Out) :-
  write(Out, '.\n').

write_graph(Out, G) :-
  write_iri(Out, G).

write_iri(Out, Iri) :-
  turtle:turtle_write_uri(Out, Iri).

write_predicate(Out, P) :-
  write_iri(Out, P).

write_object(Out, S) :-
  write_subject(Out, S), !.
write_object(Out, Lit) :-
  write_literal(Out, Lit).

write_subject(Out, Iri) :-
  rdf_is_iri(Iri), !,
  write_iri(Out, Iri).

write_literal(Out, Val^^D) :- !,
  rdf_lexical_form(Val^^D, Lex^^D),
  turtle:turtle_write_quoted_string(Out, Lex),
  write(Out, '^^'),
  write_iri(Out, D).
write_literal(Out, Lex@LTag) :- !,
  turtle:turtle_write_quoted_string(Out, Lex),
  write(Out, '@'),
  write(Out, LTag).
write_literal(Out, V) :-
  rdf_equal(xsd:string, D),
  write_literal(Out, V^^D).
