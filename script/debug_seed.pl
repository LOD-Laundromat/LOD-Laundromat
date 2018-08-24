:- module(debug_seed, [parse_unfinished/0,run/0]).

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(pairs)).
:- use_module(library(settings)).
:- use_module(library(yall)).
:- use_module(library(zlib)).

:- use_module(library(archive_ext)).
:- use_module(library(conf_ext)).
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

seed('http://download.bio2rdf.org/files/release/4/pubmed/medline15n0647.nq.gz').
seed('http://download.bio2rdf.org/files/release/4/pubmed/medline15n1131.nq.gz').
seed('http://download.bio2rdf.org/files/release/4/pubmed/medline15n0497.nq.gz').

seed('http://compling.hss.ntu.edu.sg/omw/all+xml.zip', 'wns/wikt/wn-wikt-ems.tab').
seed('http://compling.hss.ntu.edu.sg/omw/all+xml.zip', 'wns/dan/LICENSE').
seed('https://github.com/UniversalDependencies/UD_English/archive/master.zip', 'UD_English-EWT-master/not-to-release/sources/answers/20111107233110AAmgsVx_ans.xml.conllu').
seed('https://github.com/UniversalDependencies/UD_English/archive/master.zip', 'UD_English-EWT-master/not-to-release/sources/answers/20111108103957AAcF3iZ_ans.xml.conllu').
seed('https://github.com/UniversalDependencies/UD_English/archive/master.zip', 'UD_English-EWT-master/not-to-release/sources/reviews/245928.xml.conllu').
seed('https://github.com/UniversalDependencies/UD_English/archive/master.zip', 'UD_English-EWT-master/not-to-release/sources/reviews/334388.xml.conllu').
seed('https://github.com/UniversalDependencies/UD_English/archive/master.zip', 'UD_English-EWT-master/not-to-release/sources/answers/20111108103158AARnMLC_ans.xml.conllu').
seed('https://github.com/UniversalDependencies/UD_English/archive/master.zip', 'UD_English-EWT-master/not-to-release/sources/reviews/173758.xml.conllu').
seed('https://opendata.oorlogsbronnen.nl/dataset/5f9cca9d-ffaf-401f-a12c-1c1593e06328/resource/5bac0f31-d546-4e8d-82de-3aae2faafd6e/download/data.tar.gz', 'data/output_87001.xml').
seed('https://opendata.oorlogsbronnen.nl/dataset/5f9cca9d-ffaf-401f-a12c-1c1593e06328/resource/5bac0f31-d546-4e8d-82de-3aae2faafd6e/download/data.tar.gz', 'data/output_63001.xml').
seed('http://www.anc.org/MASC/download/MASC-3.0.0.tgz', 'MASC-3.0.0/data/written/email/spam/ucb15.hdr').
seed('http://www.anc.org/MASC/download/MASC-3.0.0.tgz', 'MASC-3.0.0/data/written/email/spam/ucb20-nc.xml').
seed('http://www.anc.org/MASC/download/MASC-3.0.0.tgz', 'MASC-3.0.0/data/written/email/w3c/lists-034-10082707-s.xml').
seed('http://www.anc.org/MASC/download/MASC-3.0.0.tgz', 'MASC-3.0.0/data/written/newspaper/wsj/wsj_0173-mpqa.xml').
seed('http://www.anc.org/MASC/download/MASC-3.0.0.tgz', 'MASC-3.0.0/data/written/email/spam/ucb54-s.xml').
seed('http://www.anc.org/MASC/download/MASC-3.0.0.tgz', 'MASC-3.0.0/data/written/email/enron/211401-vc.xml').
seed('http://www.anc.org/MASC/download/MASC-1.0.3.zip', 'MASC-1.0.3/original-annotations/mpqa_opinion/20000815_AFP_ARB.0084.IBM-HA-NEW/gatesentences.mpqa.2.0').
seed('http://www.anc.org/MASC/download/MASC-1.0.3.zip', 'MASC-1.0.3/data/spoken/sw2071-ms98-a-trans.anc').
seed('http://www.anc.org/MASC/download/MASC-1.0.3.zip', 'MASC-1.0.3/original-annotations/mpqa_opinion/sw2014-UTF16-ms98-a-trans/.svn/entries').
seed('http://www.anc.org/MASC/download/MASC-1.0.3.zip', 'MASC-1.0.3/original-annotations/mpqa_opinion/110CYL067/gateman.mpqa.lre.2.0').
seed('http://compling.hss.ntu.edu.sg/omw/wns/bul+xml.zip', 'bul/wn-bul-lmf.xml').
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
  recode_file(Enc, FromFile),
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
  Options = [base_uri(FinalUri),bnode_prefix(BNodePrefix),media_type(MediaType)],
  file_name_extension(FromFile, gz, ToFile),
  setup_call_cleanup(
    (
      open(FromFile, read, In),
      gzopen(ToFile, write, Out)
    ),
    rdf_deref_stream(FinalUri, In, clean_tuples(Out), Options),
    maplist(close, [In,Out])
  ).

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

clean_tuples(Out, BNodePrefix, Tuples, _) :-
  maplist(clean_tuple(Out, BNodePrefix), Tuples).

clean_tuple(Out, BNodePrefix, rdf(S0,P0,O0)) :- !,
  (   rdf_clean_triple(BNodePrefix, rdf(S0,P0,O0), rdf(S,P,O))
  ->  rdf_write_triple(Out, BNodePrefix, S, P, O)
  ;   print_message(warning, rdf(S0,P0,O0))
  ).
clean_tuple(Out, BNodePrefix, rdf(S,P,O,user)) :- !,
  clean_tuple(Out, BNodePrefix, rdf(S,P,O)).
clean_tuple(Out, BNodePrefix, rdf(S0,P0,O0,G0)) :-
  (   rdf_clean_quad(BNodePrefix, rdf(S0,P0,O0,G0), rdf(S,P,O,G))
  ->  rdf_write_quad(Out, BNodePrefix, S, P, O, G)
  ;   print_message(warning, rdf(S0,P0,O0,G0))
  ).
