:- module(debug_seed, [parse_unfinished/0,run/0]).

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(pairs)).
:- use_module(library(settings)).
:- use_module(library(yall)).
:- use_module(library(zlib)).

:- use_module(library(archive_ext)).
:- use_module(library(conf_ext)).
:- use_module(library(dict)).
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(media_type)).
:- use_module(library(semweb/ldfs)).
:- use_module(library(semweb/rdf_clean)).
:- use_module(library(semweb/rdf_deref)).
:- use_module(library(semweb/rdf_export)).
:- use_module(library(semweb/rdf_guess)).
:- use_module(library(semweb/rdf_media_type)).
:- use_module(library(semweb/rdf_term)).
:- use_module(library(xml_ext)).

:- debug(error).
%:- debug(stats).
%:- debug(warning).

:- initialization
   set_setting(rdf_term:bnode_prefix_scheme, https),
   set_setting(rdf_term:bnode_prefix_authority, 'lodlaundromat.org'),
   % This is needed for `http://spraakbanken.gu.se/rdf/saldom.rdf'
   % which has >8M triples in one single RDF/XML description.
   set_prolog_flag(stack_limit, 4 000 000 000).

:- multifile
    user:message_hook/3.

user:message_hook(E, Kind, _) :-
  debug(Kind, "~w", [E]).

seed('http://www.gutenberg.org/feeds/catalog.rdf.bz2').

%seed('http://compling.hss.ntu.edu.sg/omw/all+xml.zip', 'wns/wikt/wn-wikt-ems.tab').
seed(Uri, data) :-
  seed(Uri).

parse_unfinished :-
  findall(
    Size-Hash,
    (
      ldfs_file('', false, Dir, Hash, 'data.nq.gz', _),
      ldfs_file('', false, Dir, Hash, 'dirty.gz', File),
      size_file(File, Size)
    ),
    Pairs1
  ),
  sort(1, @=<, Pairs1, Pairs2),
  pairs_values(Pairs2, Hashes),
  maplist(
    [Hash]>>(
      format("[BEGIN] ~a\n", [Hash]),
      ldfs_file('', false, _, Hash, 'dirty.gz', File),
      parse(File),
      delete_file(File),
      format("[END] ~a\n", [Hash])
    ),
    Hashes
  ),
  delete_file(File),
  format("done!\n").

run :-
  findall(
    Id,
    (
      seed(Uri, EntryName),
      format(atom(Alias), "~a ~a", [Uri,EntryName]),
      thread_create(download(Uri, EntryName), Id, [alias(Alias)])
    ),
    Ids
  ),
  forall(
    member(Id, Ids),
    (
      thread_join(Id, Status),
      format("~w ~w\n", [Id,Status])
    )
  ).

run(Uri) :-
  run(Uri, data).

run(Uri, EntryName) :-
  download(Uri, EntryName).

download(Uri, EntryName) :-
  findall(RdfMediaType, rdf_media_type(RdfMediaType), RdfMediaTypes),
  Options = [accept(RdfMediaTypes),
             final_uri(FinalUri),
             maximum_number_of_hops(10),
             metadata(HttpMetas),
             status(Status)],
  http_open2(Uri, In, Options),
  ignore(http_metadata_content_type(HttpMetas, HttpMediaType)),
  assertion(between(200, 299, Status)),
  call_cleanup(
    decompress(In, FinalUri, EntryName, HttpMediaType),
    close(In)
  ).

decompress(In, FinalUri, EntryName, HttpMediaType) :-
  setup_call_cleanup(
    archive_open(In, Arch),
    decompress_archive(Arch, FinalUri, EntryName, HttpMediaType),
    archive_close(Arch)
  ).

decompress_archive(Arch, FinalUri, EntryName, HttpMediaType) :-
  repeat,
  archive_next_header(Arch, EntryName0),
  (EntryName == EntryName0 -> ! ; fail),
  md5(FinalUri-EntryName, Hash),
  conf_json(Conf),
  directory_file_path(Conf.'data-directory', Hash, File),
  setup_call_cleanup(
    open(File, write, Out),
    setup_call_cleanup(
      archive_open_entry(Arch, In),
      copy_stream_data(In, Out),
      close(In)
    ),
    close(Out)
  ),
  recode(File, FinalUri, HttpMediaType).

recode(FromFile, FinalUri, HttpMediaType) :-
  ignore(guess_file_encoding(FromFile, GuessEnc)),
  ignore(media_type_encoding(HttpMediaType, HttpEnc)),
  ignore(xml_file_encoding(FromFile, XmlEnc)),
  choose_encoding(GuessEnc, HttpEnc, XmlEnc, Enc),
  recode_file(FromFile, Enc),
  parse(FromFile, FinalUri).

parse(FromFile) :-
  parse(FromFile, 'https://example.com/').

parse(FromFile, FinalUri) :-
  peek_file(FromFile, 10 000, Content),
  (   rdf_guess_string(Content, MediaType)
  ->  parse(FromFile, FinalUri, MediaType)
  ;   print_message(warning, non_rdf(Content))
  ).

parse(FromFile, FinalUri, MediaType) :-
  md5(FinalUri, Hash),
  rdf_bnode_iri(Hash, bnode, BNodePrefix),
  Options = [base_uri(FinalUri),
             bnode_prefix(BNodePrefix),
             media_type(MediaType)],
  file_name_extension(FromFile, gz, ToFile),
  State = _{number_of_quadruples: 0, number_of_triples: 0},
  setup_call_cleanup(
    (
      open(FromFile, read, In),
      gzopen(ToFile, write, Out)
    ),
    rdf_deref_stream(FinalUri, In, clean_tuples(State, Out), Options),
    maplist(close, [In,Out])
  ),
  _{number_of_quadruples: Quads, number_of_triples: Triples} :< State,
  debug(stats, "~D quadruples & ~D triples", [Quads,Triples]).

% 1. XML header
choose_encoding(_, _, XmlEnc, XmlEnc) :-
  ground(XmlEnc), !.
% 2. HTTP Content-Type header
choose_encoding(_, HttpEnc, _, HttpEnc) :-
  ground(HttpEnc), !.
% 3. Guessed encoding
choose_encoding(GuessEnc, _, _, GuessEnc) :-
  ground(GuessEnc),
  GuessEnc \== octet, !.
% 4. No encoding or binary (`octet').
choose_encoding(_, _, _, _) :-
  throw(error(no_encoding,ll_recode)).

clean_tuples(State, Out, BNodePrefix, Tuples, _) :-
  maplist(clean_tuple(State, Out, BNodePrefix), Tuples).

% triple
clean_tuple(State, Out, BNodePrefix, rdf(S0,P0,O0)) :- !,
  (   rdf_clean_triple(BNodePrefix, rdf(S0,P0,O0), rdf(S,P,O))
  ->  rdf_write_triple(Out, BNodePrefix, S, P, O)
  ;   print_message(warning, rdf(S0,P0,O0))
  ),
  nb_increment_dict(State, number_of_triples, N),
  debug(stats, "~D triples", [N]).
% triple (legacy)
clean_tuple(State, Out, BNodePrefix, rdf(S,P,O,user)) :- !,
  clean_tuple(State, Out, BNodePrefix, rdf(S,P,O)).
% quadruple
clean_tuple(State, Out, BNodePrefix, rdf(S0,P0,O0,G0)) :-
  (   rdf_clean_quad(BNodePrefix, rdf(S0,P0,O0,G0), rdf(S,P,O,G))
  ->  rdf_write_quad(Out, BNodePrefix, S, P, O, G)
  ;   print_message(warning, rdf(S0,P0,O0,G0))
  ),
  nb_increment_dict(State, number_of_quadruples, N),
  debug(stats, "~D quadruples", [N]).
