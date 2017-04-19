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
:- use_module(library(date_time/date_time)).
:- use_module(library(debug)).
:- use_module(library(dict_ext)).
:- use_module(library(http/http_cookie)).
:- use_module(library(http/http_header)).
:- use_module(library(http/http_open)).
:- use_module(library(http/http_ssl_plugin)).
:- use_module(library(http/json)).
:- use_module(library(lists)).
:- use_module(library(md5)).
:- use_module(library(rdf)).
:- use_module(library(semweb/rdf_guess)).
:- use_module(library(semweb/rdf_http_plugin)).
:- use_module(library(semweb/rdf_ntriples)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(semweb/rdfa)).
:- use_module(library(semweb/turtle)).
:- use_module(library(ssl)).
:- use_module(library(zlib)).

:- use_module(library(file_ext)).
:- use_module(library(uri/uri_ext)).

:- debug(clean).

:- dynamic
    seed_uri0/1.

seed_uri0('http://resource.geolba.ac.at/PoolParty/sparql/GeologicUnit').
seed_uri0('http://resource.geolba.ac.at/PoolParty/sparql/GeologicTimeScale').
seed_uri0('http://resource.geolba.ac.at/PoolParty/sparql/lithology').
seed_uri0('http://resource.geolba.ac.at/PoolParty/sparql/tectonicunit').
seed_uri0('http://resource.geolba.ac.at/PoolParty/sparql/structure').
seed_uri0('http://resource.geolba.ac.at/GeologicUnit/export/GeologicUnit.rdf').
seed_uri0('http://resource.geolba.ac.at/structure/export/structure.rdf').
seed_uri0('http://resource.geolba.ac.at/GeologicTimeScale/export/GeologicTimeScale.rdf').
seed_uri0('http://resource.geolba.ac.at/lithology/export/lithology.rdf').
seed_uri0('http://resource.geolba.ac.at/tectonicunit/export/tectonicunit.rdf').
seed_uri0('http://www.ogdcockpit.eu/Spezial:Semantische_Suche/-5B-5BKategorie:Metadaten-5D-5D/-3F%3DBezeichnung-23/-3FQuelle/-3FVersion/-3FFunktion/-3FID/-3FOGD-2DKurzname/-3FCKAN-20Feld/-3FAnzahl/-3FDefinition/-3FErl%C3%A4uterung/-3FBeispiel/-3FON-20A-202270:2010/-3FON-2FEN-2FISO-2019115:2003/-3FRDF-20property/-3FDefinition-20Englisch/format%3Drdf/sort%3DID/mainlabel%3DBezeichnung/offset%3D0').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A44%3A47.494Z/at-liquidados-por-mes.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A22%3A34.434Z/at-liquidados-por-uf.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A46%3A28.132Z/at-cnae-20.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A49%3A13.936Z/at-cnae-95.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A43%3A30.177Z/at-por-cbo.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A40%3A48.622Z/at-por-cid.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A29%3A39.082Z/at-por-idade.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A42%3A15.568Z/at-por-mes.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A12%3A14.117Z/at-por-parte-do-corpo-atingida.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-08-21T19%3A25%3A26.316Z/at-por-uf.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-09-28T20%3A10%3A17.798Z/cr-cred-mensal-pais.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-09-25T20%3A47%3A01.531Z/cr-cred-conc-uf.rdf').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-09-25T20%3A27%3A54.336Z/cr-cred-grupo-especies.rdf').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-09-30T21%3A23%3A47.099Z/cr-emissao-faixa-valor-e-especies.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-09-30T18%3A11%3A41.182Z/cr-emissao-especies-sexo.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-10-01T20%3A50%3A25.227Z/cr-emissao-meio-pagamento.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-09-10T14%3A16%3A50.035Z/sp-conclusivo-uf.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-08-28T20%3A32%3A59.413Z/sp-nao-conclusivo-uf.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-08-28T20%3A30%3A15.019Z/sp-exames-especialidade.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-09-10T21%3A11%3A08.419Z/sp-examestipouf.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-09-10T21%3A39%3A07.854Z/sp-examesnormaisgrupoesp.ttl').
seed_uri0('http://www.portaldocidadao.tce.sp.gov.br/api_rdf_municipios').
seed_uri0('http://www.portaldocidadao.tce.sp.gov.br/api_rdf_orgaos').
seed_uri0('http://api.comprasnet.gov.br/sicaf/v1/consulta/fornecedores.rdf?uf=RN').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2000.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2001.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2002.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2003.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2004.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2005.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2006.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2007.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2008.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2009.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2010.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2000.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2012.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2013.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2014.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2015.zip').
seed_uri0('http://www1.siop.planejamento.gov.br/downloads/rdf/loa2016.zip').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-09-11T18%3A09%3A51.119Z/sp-reabilitacaoporuf.ttl').
seed_uri0('http://dadosabertos.dataprev.gov.br/storage/f/2015-09-10T17%3A50%3A30.109Z/sp-servicosocialuf.ttl').
seed_uri0('https://data.sazp.sk/dataset/sk-ld-inspire-bio-geographical-regions').
seed_uri0('https://data.sazp.sk/dataset/sk-ld-inspire-species-distribution').
seed_uri0('https://data.sazp.sk/dataset/sk-ld-environmental-burdens-contaminated-sites').
seed_uri0('https://data.sazp.sk/dataset').
seed_uri0('https://data.sazp.sk/dataset/sk-ld-inspire-protected-sites').
seed_uri0('https://data.sazp.sk/dataset/sk-ld-inspire-corine-land-cover').

:- nodebug(http(_)).

:- public
    ssl_verify/5.

ssl_verify(_SSL, _ProblemCertificate, _AllCertificates, _FirstCertificate, _Error).

:- rdf_register_prefix(bnode, 'https://lodlaundromat.org/.well-known/genid/').





%! clean is det.

clean :-
  forall(
    seed_uri0(Uri),
    clean_uri(Uri)
  ).



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
      Count = count(1),
      call_statistics(
        clean_stream(Count, In, Out, BNodePrefix, BaseUri, MediaType),
        walltime,
        Walltime
      ),
      arg(1, Count, NumTriples),
      stream_metadata(In, Out, Walltime, StreamDict),
      MediaType = media(Super/Sub,_),
      format(string(MediaType0), "~a/~a", [Super,Sub]),
      Dict = _{
        number_of_triples: NumTriples,
        rdf_media_type: MediaType0,
        stream: StreamDict
      }
    ),
    (
      close(In),
      close(Out)
    )
  ),
  write_json(Dir, 'clean.json', Dict).

choose_media_type([MediaType], _, _, MediaType) :- !.
choose_media_type(MediaTypes, HttpMediaType, ExtMediaType, MediaType) :-
  member(MediaType, MediaTypes),
  member(MediaType0, [HttpMediaType,ExtMediaType]),
  generalization(MediaType, MediaType0), !.

% N-Quads
clean_stream(Count, In, Out, BNodePrefix, BaseUri, media(application/'n-quads',_)) :- !,
  rdf_process_ntriples(
    In,
    write_clean_tuples(Count, Out),
    [anon_prefix(BNodePrefix),base_uri(BaseUri)]
  ).
% N-Triples
clean_stream(Count, In, Out, BNodePrefix, BaseUri, media(application/'n-triples',_)) :- !,
  rdf_process_ntriples(
    In,
    write_clean_tuples(Count, Out),
    [anon_prefix(BNodePrefix),base_uri(BaseUri)]
  ).
% RDF/XML
clean_stream(Count, In, Out, _BNodePrefix, BaseUri, media(application/'rdf+xml',_)) :- !,
  % @tbd blank nodes
  process_rdf(In, write_clean_tuples(Count, Out), [base_uri(BaseUri)]).
% TriG
clean_stream(Count, In, Out, BNodePrefix, BaseUri, media(application/trig,_)) :- !,
  rdf_process_turtle(
    In,
    write_clean_tuples(Count, Out),
    [
      anon_prefix(BNodePrefix),
      base_uri(BaseUri),
      format(trig),
      resources(iri)
    ]
  ).
% Turtle
clean_stream(Count, In, Out, BNodePrefix, BaseUri, media(text/turtle,_)) :- !,
  rdf_process_turtle(
    In,
    write_clean_tuples(Count, Out),
    [
      anon_prefix(BNodePrefix),
      base_uri(BaseUri),
      format(turtle),
      resources(iri)
    ]
  ).
% RDFa
clean_stream(Count, In, Out, BNodePrefix, BaseUri, MediaType) :-
  memberchk(MediaType, [media(application/'xhtml+xml',_),media(text/html,_)]), !,
  read_rdfa(In, Triples, [anon_prefix(BNodePrefix),base(BaseUri)]),
  maplist(write_clean_tuple(Count, Out), Triples).
% unsupported Media Type
clean_stream(_, _, _, _, _, MediaType) :-
  print_message(warning, unsupported_media_type(MediaType)).



%! download(+Uri, +Dir, -File, -HttpMediaType) is det.

download(Uri, Dir, File, HttpMediaType) :-
  create_directory(Dir),
  directory_file_path(Dir, dirty, File),
  setup_call_cleanup(
    (
      open(File, write, Out, [type(binary)]),
      open_uri(Uri, In, HttpDicts)
    ),
    (   var(In)
    ->  Dict = _{http: HttpDicts}
    ;   call_statistics(copy_stream_data(In, Out), walltime, Walltime),
        stream_metadata(In, Out, Walltime, StreamDict),
        Dict = _{http: HttpDicts, stream: StreamDict}
    ),
    (
      close(In),
      close(Out)
    )
  ),
  (   HttpDicts = [HttpDict|_],
      get_dict(content_type, HttpDict.headers, ContentType)
  ->  http_parse_header_value(content_type, ContentType, HttpMediaType)
  ;   true
  ),
  write_json(Dir, 'download.json', Dict).

open_uri(Uri, In, []) :-
  uri_components(Uri, uri_components(file,_,_,_,_)), !,
  uri_file_name(Uri, File),
  open(File, read, In, [type(binary)]).
open_uri(Uri, In2, Dicts2) :-
  open_uri1(Uri, In2, 1, [], Dicts1),
  reverse(Dicts1, Dicts2).

open_uri1(Uri, In2, NumRetries, Visited, [Dict|Dicts]) :-
  rdf_http_plugin:rdf_accept_header_value(_, Accept),
  call_statistics(
    http_open(
      Uri,
      In1,
      [
        authenticate(false),
        cert_verify_hook(cert_accept_any),
        header(location,Location),
        headers(Headers),
        redirect(false),
        request_header(accept,Accept),
        status_code(Status),
        version(Major-Minor)
      ]
    ),
    walltime,
    Walltime
  ),
  maplist(term_to_pair, Headers, Pairs),
  dict_pairs(HeadersDict, Pairs),
  format(string(Version), "~d.~d", [Major,Minor]),
  Dict = _{
    '@id': Uri,
    headers: HeadersDict,
    status: Status,
    version: Version,
    walltime: Walltime
  },
  open_uri2(Uri, In1, Location, Status, NumRetries, Visited, In2, Dicts).

term_to_pair(Term, Key-Val2) :-
  Term =.. [Key,Val1],
  with_output_to(string(Val2), write_canonical(Val1)).

% authentication error
open_uri2(_, In, _, Status, _, _, _, []) :-
  Status =:= 401,
  close(In),
  print_message(warning, http_error_code(Status)).
% non-authentication error
open_uri2(Uri, In1, _, Status, NumRetries1, Visited, In2, Dicts) :-
  between(400, 599, Status), !,
  NumRetries2 is NumRetries1 + 1,
  (   NumRetries2 >= 5
  ->  close(In1),
      Dicts = [],
      print_message(warning, http_error_code(Status))
  ;   open_uri1(Uri, In2, NumRetries2, Visited, Dicts)
  ).
% redirect
open_uri2(Uri1, In1, Location, Status, NumRetries, Visited1, In2, Dicts) :-
  between(300, 399, Status), !,
  close(In1),
  uri_resolve(Location, Uri1, Uri2),
  Visited2 = [Uri2|Visited1],
  (   length(Visited2, NumVisited),
      NumVisited >= 5
  ->  close(In1),
      Dicts = [],
      print_message(warning, http_max_redirect(5,Uri2))
  ;   include(==(Uri2), Visited2, Visited3),
      length(Visited3, NumRepeats),
      NumRepeats >= 2
  ->  close(In1),
      Dicts = [],
      print_message(warning, http_redirect_loop(Uri2))
  ;   open_uri1(Uri2, In2, NumRetries, Visited2, Dicts)
  ).
% succes
open_uri2(_, In, _, _, _, [], In, []).



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



%! stream_metadata(+Stream, -Dict) is det.

stream_metadata(Stream, Dict) :-
  stream_property(Stream, position(Pos)),
  stream_position_data(byte_count, Pos, NumBytes),
  stream_position_data(char_count, Pos, NumChars),
  stream_position_data(line_count, Pos, NumLines),
  stream_property(Stream, newline(Newline)),
  Dict = _{
    byte_count: NumBytes,
    char_count: NumChars,
    line_count: NumLines,
    newline: Newline
  }.



%! stream_metadata(+In, +Out, +Walltime, -Dict) is det.

stream_metadata(In, Out, Walltime, Dict) :-
  stream_metadata(In, InDict),
  stream_metadata(Out, OutDict),
  Dict = _{
    in: InDict,
    out: OutDict,
    walltime: Walltime
  }.



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



%! write_clean_tuple(+Count, +Out, +Tuple) is det.

write_clean_tuple(Count, Out, Tuple) :-
  clean_tuple(Tuple, rdf(S,P,O,_)), !,
  write_ntriple0(Out, S, P, O),
  write_eot(Out),
  arg(1, Count, N1),
  N2 is N1 + 1,
  nb_setarg(1, Count, N2).
% always succeeds
write_clean_tuple(_, _, _).

write_clean_tuples(Count, Out, Tuples, _) :-
  maplist(write_clean_tuple(Count, Out), Tuples).

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



%! write_json(+Dir, +Local, +Dict) is det.

write_json(Dir, Local, Dict) :-
  directory_file_path(Dir, Local, File),
  setup_call_cleanup(
    open(File, write, Out),
    json_write_dict(Out, Dict),
    close(Out)
  ).
