:- module(
  lclean,
  [
    clean/0,
    clean/1,           % +Hash
    clean_iri/1,       % +Iri
    reset/1,           % +Hash
    reset_and_clean/1, % +Hash
    thread_seed/2      % ?Alias, ?Hash
  ]
).

/** <module> LOD Laundromat cleaning

@author Wouter Beek
@version 2016/03
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(error)).
:- use_module(library(filesex)).
:- use_module(library(gen/gen_ntuples)).
:- use_module(library(hash_ext)).
:- use_module(library(http/json)).
:- use_module(library(jsonld/jsonld_metadata)).
:- use_module(library(jsonld/jsonld_read)).
:- use_module(library(os/dir_ext)).
:- use_module(library(os/file_ext)).
:- use_module(library(os/gnu_wc)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/process_ext)).
:- use_module(library(os/thread_ext)).
:- use_module(library(pl/pl_term)).
:- use_module(library(print_ext)).
:- use_module(library(prolog_stack)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_ext)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(rdf/rdf_prefix)).
:- use_module(library(rdf/rdf_print)).
:- use_module(library(semweb/rdf11)). % Operators.
:- use_module(library(string_ext)).
:- use_module(library(uri/uri_ext)).
:- use_module(library(zlib)).

:- use_module(cpack('LOD-Laundromat'/lfs)).
:- use_module(cpack('LOD-Laundromat'/lhdt)).
:- use_module(cpack('LOD-Laundromat'/seedlist)).

:- meta_predicate
    rdf_store_messages(+, +, 0, -).

:- rdf_meta
   rdf_store(+, r, r, o),
   rdf_store_messages(+, r, :, -).

:- dynamic
    currently_debugging0/1,
    thread_seed0/2.

%%%%currently_debugging0('07ecaef4979684bfa4507ecc28b54461').





%! clean is det.
% Clean an -- arbitrarily chosen - seed.

clean :-
  clean(_, _).


%! clean(+Hash) is det.
% Clean a specific seed from the seedlist.
% Does not re-clean documents.
%
% @throws existence_error If the seed is not in the seedlist.

clean(Hash) :-
  lfile_lhash(File, data, nquads, Hash),
  exists_file(File), !,
  msg_notification("Already cleaned ~a", [Hash]).
clean(Hash) :-
  clean(Hash, _).


%! clean(?Hash, ?Iri) is det.

clean(Hash, Iri) :-
  begin_seed(Hash, Iri),
  thread_name(Alias),
  thread_seed_update(Alias, Hash),
  debug(lclean, "~a is cleaning ~a ~a", [Alias,Hash,Iri]),
  clean_inner(Hash, Iri),
  debug(lclean, "~a has cleaned ~a ~a", [Alias,Hash,Iri]),
  end_seed(Hash).

clean_inner(Hash, Iri) :-
  ldir_lhash(Dir, Hash),
  ldoc_lhash(Doc, data, Hash),
  with_mutex(lfs, make_directory_path(Dir)),
  ldir_lfile(Dir, data, nquads, DataFile),
  ldir_lfile(Dir, meta, ntriples, MetaFile),
  ldir_lfile(Dir, warn, ntriples, WarnFile),
  setup_call_cleanup(
    (
      gzopen(MetaFile, write, MetaSink, [format(gzip)]),
      gzopen(WarnFile, write, WarnSink, [format(gzip)])
    ),
    (
      State = _{meta: MetaSink, warn: WarnSink, warns: 0},
      CleanOpts = [compress(gzip),metadata(M1),relative_to(Dir),sort_dir(Dir),warn(WarnSink)],
      currently_debugging(Hash, Iri),
      rdf_store_messages(State, Doc, rdf_clean(Iri, DataFile, CleanOpts), M2),
      (var(M1) -> M = M2 ; M = M1),
      (var(M) -> format(user_output, "~w~n", [Hash]) ; true), %DEB?
      rdf_store_metadata(State, Doc, M)
    ),
    (
      close(WarnSink),
      file_lines(WarnFile, NumWarns),
      rdf_store(MetaSink, Doc, llo:number_of_warnings, NumWarns^^xsd:nonNegativeInteger),
      close(MetaSink)
    )
  ).
  %absolute_file_name('dirty.gz', DirtyTo, Opts),
  %call_collect_messages(rdf_download_to_file(Iri, DirtyTo, [compress(gzip)])),

currently_debugging(Hash, Iri) :-
  currently_debugging0(Hash), !,
  ansi_format(user_output, [bold], "~a ~a", [Hash,Iri]),
  gtrace. %DEB
currently_debugging(_, _).



%! clean_iri(+Iri) is det.
% Clean a specific IRI, achieved by circumvents the seedlist.
% For debugging purposes only.

clean_iri(I1) :-
  iri_normalized(I1, I2),
  md5(I2, Hash),
  clean(Hash, I2).



%! reset(+Hash) is det.

reset(Hash) :-
  % Step 1: Unload the RDF data and metadata from memory.
  lrdf_unload(Hash),

  % Step 2: Remove the data and metadata files from disk.
  ldir_lhash(Dir, Hash),
  with_mutex(lfs, (
    (exists_directory(Dir) -> delete_directory_and_contents(Dir) ; true)
  )),

  % Step 3: Reset the seed in the seedlist.
  reset_seed(Hash).



%! reset_and_clean(+Hash) is det.

reset_and_clean(Hash) :-
  reset(Hash),
  clean(Hash).



%! thread_seed(?Alias, ?Hash) is nondet.

thread_seed(Alias, Hash) :-
  thread_seed0(Alias, Hash).



%! thread_seed_update(+Hash) is det.
%! thread_seed_update(+Alias, +Hash) is det.

thread_seed_update(Hash) :-
  thread_name(Alias),
  thread_seed_update(Alias, Hash).


thread_seed_update(Alias, Hash) :-
  with_mutex(thread_seed, (
    retractall(thread_seed0(Alias, _)),
    assert(thread_seed0(Alias, Hash))
  )).





% HELPERS %

%! rdf_store_messages(+State, +Doc, :Goal_0, -Metadata) is det.
% Run Goal, unify Result with `true`, `false` or `exception(Error)`
% and messages with a list of generated error and warning messages.
% Each message is a term `message(Term,Kind,Lines)`.

rdf_store_messages(State, Doc, Goal_0, M) :-
  asserta((
    user:thread_message_hook(Term,Kind,_) :-
      error_kind(Kind),
      rdf_store_warning(State.warn, Doc, Term)
  )),
  catch(Goal_0, E, true),
  (   var(E)
  ->  End0 = true
  ;   E = error(existence_error(open_any2,M),_)
  ->  End0 = "No stream"
  ;   End0 = E,
      msg_warning("[FAILED] ~w~n", [End0]),
      M = _{}
  ),
  with_output_to(string(End), write_term(End0)),
  % @bug RDF prefix expansion does not work for `llo:end’ here.
  rdf_equal(llo:end, P),
  rdf_store(State.meta, Doc, P, End^^xsd:string).


error_kind(warning).
error_kind(error).


% Archive error
rdf_store_warning(Out, Doc, error(archive_error(Code,_),_)) :-
  (   Code == 2
  ->  Name = missing_type_keyword_in_mtree_spec
  ;   Code == 25
  ->  Name = invalid_central_directory_signature
  ), !,
  rdf_global_id(llo:Name, O),
  rdf_store(Out, Doc, llo:arhive_error, O).
% Encoding: character
rdf_store_warning(Out, Doc, error(type_error(character,Char),context(_,_))) :- !,
  rdf_store(Out, Doc, llo:character_encoding_error, Char^^xsd:integer).
% Existence: directory
rdf_store_warning(Out, Doc, error(existence_error(directory,Dir),context(_,'File exists'))) :- !,
  uri_file_name(Uri, Dir),
  rdf_store(Out, Doc, llo:directory_existence_error, Uri^^xsd:anyURI).
% Existence: file
rdf_store_warning(Out, Doc, error(existence_error(file,File),context(_,Msg))) :-
  (   Msg == 'Directory not empty'
  ->  Name = directory_not_empty
  ;   Msg == 'No such file or directory'
  ->  Name = file_existence_error
  ), !,
  rdf_global_id(llo:Name, P),
  uri_file_name(Uri, File),
  rdf_store(Out, Doc, P, Uri^^xsd:anyURI).
% Existence: source sink?
rdf_store_warning(Out, Doc, error(existence_error(source_sink,Path),context(_,'Is a directory'))) :- !,
  uri_file_name(Uri, Path),
  rdf_store(Out, Doc, llo:is_a_directory_error, Uri^^xsd:anyURI).
% HTTP status
rdf_store_warning(Out, Doc, error(http_status(Status),_)) :-
  (between(400, 499, Status) ; between(500, 599, Status)), !,
  rdf_store(Out, Doc, llo:http_error, Status^^xsd:positiveInteger).
% IO: read
rdf_store_warning(Out, Doc, error(io_error(read,_),context(_,Msg))) :-
  (   Msg == 'Connection reset by peer'
  ->  Name = connection_reset_by_peer
  ;   Msg == 'Inappropriate ioctl for device'
  ->  Name = not_a_typewriter
  ;   Msg = 'Is a directory'
  ->  Name = is_a_directory
  ), !,
  rdf_global_id(llo:Name, O),
  rdf_store(Out, Doc, llo:io_read_error, O).
% IO: write
rdf_store_warning(Out, Doc, error(io_error(write,_),context(_,'Encoding cannot represent character'))) :- !,
  rdf_store(Out, Doc, llo:io_write_error, llo:encoding_error).
% IO warning
rdf_store_warning(Out, Doc, io_warning(_,Msg)) :-
  (   Msg == 'Illegal UTF-8 continuation'
  ->  Name = illegal_utf8_continuation
  ;   Msg == 'Illegal UTF-8 start'
  ->  Name = illegal_utf8_start
  ),
  rdf_global_id(llo:Name, O),
  rdf_store(Out, Doc, llo:io_warning, O).
% Malformed URL
rdf_store_warning(Out, Doc, error(domain_error(url,Url),_)) :- !,
  rdf_store(Out, Doc, llo:malformed_url, Url^^xsd:anyURI).
% No RDF
rdf_store_warning(Out, Doc, error(no_rdf(_))) :- !,
  rdf_store(Out, Doc, llo:rdf_serialization_format, llo:unrecognized_format).
% Permission: redirect
rdf_store_warning(Out, Doc, error(permission_error(redirect,http,Object),context(_,Msg1))) :- !,
  atom_truncate(Msg1, 500, Msg2),
  format(string(String), "[~a] ~a", [Object,Msg2]),
  rdf_store(Out, Doc, llo:http_redirect_permission_error, String^^xsd:string).
% SGML parser
rdf_store_warning(Out, Doc, sgml(sgml_parser(_),_,Line,Msg1)) :- !,
  atom_truncate(Msg1, 500, Msg2),
  format(string(String), "[~w] ~a", [Line,Msg2]),
  rdf_store(Out, Doc, llo:sgml_parser_error, String^^xsd:string).
% Socket error
rdf_store_warning(Out, Doc, error(socket_error(Msg),_)) :-
  (   Msg == 'Connection timed out'
  ->  Name = connection_timed_out
  ;   Msg == 'Connection refused'
  ->  Name = connection_refused
  ;   Msg == 'No Data'
  ->  Name = no_data
  ;   Msg == 'No route to host'
  ->  Name = no_route_to_host
  ;   Msg == 'Host not found'
  ->  Name = host_not_found
  ;   Msg == 'Try Again'
  ->  Name = try_again
  ), !,
  rdf_global_id(llo:Name, O),
  rdf_store(Out, Doc, llo:socket_error, O).
% SSL: SSL verify
rdf_store_warning(Out, Doc, error(ssl_error(ssl_verify),_)) :- !,
  rdf_store(Out, Doc, llo:ssl_error, llo:ssl_verify).
% Syntax error
rdf_store_warning(Out, Doc, error(syntax_error(Msg1),stream(_,Line,Col,Char))) :- !,
  atom_truncate(Msg1, 500, Msg2),
  format(string(String), "[~w:~w:~w] ~a", [Line,Col,Char,Msg2]),
  rdf_store(Out, Doc, llo:syntax_error, String^^xsd:string).
% Timeout: read
rdf_store_warning(Out, Doc, error(timeout_error(read,_),context(_,_))) :- !,
  rdf_store(Out, Doc, llo:timeout_error, llo:read).
% Turtle: undefined prefix
rdf_store_warning(Out, Doc, error(existence_error(turtle_prefix,Prefix), stream(_,Line,Col,Char))) :- !,
  format(string(String), "[~w:~w:~w] ~a", [Line,Col,Char,Prefix]),
  rdf_store(Out, Doc, llo:missing_turtle_prefix_defintion, String^^xsd:string).
% RDF/XML: multiple definitions
rdf_store_warning(Out, Doc, rdf(redefined_id(Uri))) :- !,
  rdf_store(Out, Doc, llo:redefined_rdf_id, Uri^^xsd:anyURI).
% RDF/XML: name
rdf_store_warning(Out, Doc, rdf(not_a_name(XmlName))) :- !,
  rdf_store(Out, Doc, llo:xml_name_error, XmlName^^xsd:string).
% RDF/XML: unparsable
rdf_store_warning(Out, Doc, rdf(unparsed(Dom))) :- !,
  rdf11:in_xml_literal(xml, Dom, A1),
  atom_truncate(A1, 500, A2),
  rdf_store(Out, Doc, llo:rdf_xml_parser_error, A2^^xsd:string).
% Non-canonical lexical form.
rdf_store_warning(Out, Doc, non_canonical_lexical_form(D1,Lex)) :- !,
  abbr_iri(D1, D2),
  atom_concat(noncanonical_, D2, Name),
  rdf_global_id(llo:Name, P),
  rdf_store(Out, Doc, P, Lex^^xsd:string).
% Unhandled error term.
rdf_store_warning(Out, _, Term) :-
  format(Out, "~w~n", [Term]).


%! rdf_store_metadata(+State, +Doc, +M) is det.

rdf_store_metadata(State, Doc1, M) :-
  jsonld_metadata(M, Jsonld1),
  atom_string(Doc1, Doc2),
  Jsonld2 = Jsonld1.put(_{'@id': Doc2}),
  (debugging(wm(low)) -> json_write_dict(user_output, Jsonld2) ; true),
  forall(jsonld_tuple(Jsonld2, rdf(Doc,P,O)), (
    (debugging(wm(low)) -> with_output_to(user_output, rdf_print_triple(Doc, P, O)) ; true),
    rdf_store(State.meta, Doc, P, O)
  )).


%! rdf_store(+Out, +S, +P, +O) is det.

rdf_store(Out, S, P, O) :-
  with_output_to(Out, gen_ntriple(S, P, O)).
