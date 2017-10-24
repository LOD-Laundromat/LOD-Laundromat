:- module(
  clean,
  [
    clean_uri/1 % +Uri
  ]
).

/** <module> LOD Laundromat

@tbd Assert end state / exception in `meta' i.o. `warn'.

HASH := MD5(URI + " " + ENTRY-PATH)

Archive:

```
HASH/
  meta.[hdt,nt.gz]
  source → source.data
  warn.log.gz
```

Entry:

```
HASH/
  data → data.[hdt,nt]
  meta.[hdt,nt.gz]
  warn.log.gz
```

@author Wouter Beek
@version 2017/04-2017/05
*/

:- use_module(library(apply)).
:- use_module(library(archive)).
:- use_module(library(check_installation), []).
:- use_module(library(debug)).
:- use_module(library(dict_ext)).
:- use_module(library(http/http_cookie)).
:- use_module(library(http/http_header)).
:- use_module(library(http/http_open)).
:- use_module(library(file_ext)).
:- use_module(library(lists)).
:- use_module(library(md5)).
:- use_module(library(option)).
:- use_module(library(rdf)).
:- use_module(library(semweb/rdf_api)).
:- use_module(library(semweb/rdf_export)).
:- use_module(library(semweb/rdf_guess)).
:- use_module(library(semweb/rdf_http_plugin)).
:- use_module(library(semweb/rdf_ntriples)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(semweb/rdfa)).
:- use_module(library(semweb/turtle)).
:- use_module(library(uuid)).
:- use_module(library(xsd/xsd_number)).
:- use_module(library(zlib)).

:- debug(clean).

:- meta_predicate
    store_warnings(+, 0).

:- rdf_register_prefix(bnode, 'https://lodlaundromat.org/.well-known/genid/', [force(true)]).
:- rdf_register_prefix(llh, 'https://lodlaundromat.org/http/').
:- rdf_register_prefix(llo, 'https://lodlaundromat.org/ontology/').
:- rdf_register_prefix(llr, 'https://lodlaundromat.org/resource/').
:- rdf_register_prefix(void, 'http://rdfs.org/ns/void#').





%! clean_uri(+Uri) is det.

clean_uri(Uri) :-
  md5(Uri, Hash),
  debug(clean, "Starting: ~a ~a", [Hash,Uri]),
  store_warnings(Hash, download_uri(Uri, Hash, ArchFile)),
  % Two cases: (1) download failed, (2) download succeeded.
  (   var(ArchFile)
  ->  hash_file(Hash, source, TmpFile),
      % Delete the source file.
      delete_file(TmpFile)
  ;   atomic_list_concat([a,Hash], :, Alias),
      call_in_thread(
        Alias,
        unpack_file(Uri, Hash, ArchFile)
      )
  ),
  finish_meta_and_warn(Hash),
  debug(clean, "Ended: ~a ~a", [Hash,Uri]).



%! clean_file(+Uri, +Hash, +MediaTypes, +EntryFile) is det.

clean_file(Uri, Hash, MediaTypes2, EntryFile) :-
  rdf_global_id(bnode:Hash, BNodePrefix),
  hash_file(Hash, data, DataFileTmp),
  (   setup_call_cleanup(
        (
          rdf_open(EntryFile, In, [media_type(MediaType)]),
          open(DataFileTmp, write, Out)
        ),
        (
          Count = count(0),
          call_statistics(
            clean_stream(Count, In, Out, BNodePrefix, Uri, MediaType),
            walltime,
            Walltime
          ),
          arg(1, Count, NumTriples),
          stream_metadata(In, Out, Walltime, StreamDict),
          MediaType = media(Super/Sub,_),
          format(string(MediaType0), "~a/~a", [Super,Sub])
        ),
        (
          close(In),
          close(Out)
        )
      )
  ->  rename_file(DataFileTmp, nt, DataFile),
      finish_ntriples_file(DataFile),
      hash_file(Hash, 'meta.nt', MetaFile),
      setup_call_cleanup(
        open(MetaFile, append, MetaOut),
        (
          rdf_global_id(llr:Hash, Entry),
          write_ntriple(MetaOut, Entry, rdf:type, llo:'Cleaning'),
          write_ntriple(MetaOut, Entry, void:triples,
                        NumTriples^^xsd:nonNegativeInteger),
          write_ntriple(MetaOut, Entry, llo:rdfMediaType, MediaType0^^xsd:string),
          write_stream_metadata(MetaOut, Entry, StreamDict)
        ),
        close(MetaOut)
      )
  ;   delete_file(DataFileTmp),
      print_message(warning, non_rdf)
  ).

% N-Quads
clean_stream(Count, In, Out, BNodePrefix, Uri, media(application/'n-quads',_)) :- !,
  rdf_process_ntriples(
    In,
    write_clean_tuples(Count, Out),
    [anon_prefix(BNodePrefix),base_uri(Uri)]
  ).
% N-Triples
clean_stream(Count, In, Out, BNodePrefix, Uri, media(application/'n-triples',_)) :- !,
  rdf_process_ntriples(
    In,
    write_clean_tuples(Count, Out),
    [anon_prefix(BNodePrefix),base_uri(Uri)]
  ).
% RDF/XML
clean_stream(Count, In, Out, _BNodePrefix, Uri, media(application/'rdf+xml',_)) :- !,
  % @tbd blank nodes
  process_rdf(In, write_clean_tuples(Count, Out), [base_uri(Uri)]).
% TriG
clean_stream(Count, In, Out, BNodePrefix, Uri, media(application/trig,_)) :- !,
  rdf_process_turtle(
    In,
    write_clean_tuples(Count, Out),
    [
      anon_prefix(BNodePrefix),
      base_uri(Uri),
      format(trig),
      resources(iri)
    ]
  ).
% Turtle
clean_stream(Count, In, Out, BNodePrefix, Uri, media(text/turtle,_)) :- !,
  rdf_process_turtle(
    In,
    write_clean_tuples(Count, Out),
    [
      anon_prefix(BNodePrefix),
      base_uri(Uri),
      format(turtle),
      resources(iri)
    ]
  ).
% RDFa
clean_stream(Count, In, Out, BNodePrefix, Uri, MT) :-
  memberchk(
    MT,
    [media(application/'xhtml+xml',_),media(text/html,_)]
  ), !,
  read_rdfa(In, Triples, [anon_prefix(BNodePrefix),base(Uri)]),
  maplist(write_clean_tuple(Count, Out), Triples).
% unsupported Media Type
clean_stream(_, _, _, _, _, MT) :-
  print_message(warning, unsupported_media_type(MT)).



%! download_uri(+Uri, +Hash, -File) is det.
%
% Step 1: Download archive

download_uri(Uri, Hash, File) :-
  hash_file(Hash, source, FileTmp),
  catch(
    setup_call_cleanup(
      (
        open(FileTmp, write, Out, [type(binary)]),
        rdf_open(Uri, In, [metadata(HttpMetadata)])
      ),
      (   var(In)
      ->  true
      ;   call_statistics(copy_stream_data(In, Out), walltime, Walltime),
          stream_metadata(In, Out, Walltime, StreamMetadata)
      ),
      (
        close(In),
        close(Out)
      )
    ),
    E,
    true
  ),
  (var(E) -> rename_file(FileTmp, data, File) ; true),
  hash_file(Hash, 'meta.nt', MetaFile),
  setup_call_cleanup(
    open(MetaFile, append, MetaOut),
    (
      rdf_global_id(llr:Hash, Source),
      write_ntriple(MetaOut, Source, rdf:type, llo:'Download'),
      write_ntriple(MetaOut, Source, llo:uri, Uri^^xsd:anyURI),
      (   var(StreamMetadata)
      ->  true
      ;   write_stream_metadata(MetaOut, Source, StreamMetadata)
      ),
      (   var(HttpMetadata)
      ->  true
      ;   write_http_metadata(MetaOut, Source, HttpMetadata)
      )
    ),
    close(MetaOut)
  ).



%! unpack_file(+Uri, +Hash, +File) is det.
%
% Step 2: Unpack archive file into entry files

unpack_file(Uri, Hash, File) :-
  findall(format(Format), archive_format(Format, true), Opts),
  setup_call_cleanup(
    archive_open(File, read, Arch, [filter(all)|Opts]),
    forall(
      store_warnings(Hash, archive_data_stream(Arch, In, [meta_data(Dicts)])),
      unpack_stream(Uri, Hash, File, In, Dicts)
    ),
    archive_close(Arch)
  ),
  % Delete the archive file.
  delete_file(File).



%! unpack_stream(+Uri, +Hash, +File, +In, +Dicts) is det.

% raw data
unpack_stream(Uri, Hash, File, In, [_]) :- !,
  close(In),
  store_warnings(Hash, clean_file(Uri, Hash, File)).
% more unpacking
unpack_stream(Uri, Hash0, _, In0, [Dict0,_]) :-
  _{
    filetype: file,
    filters: Filters,
    format: Format,
    mtime: MTime,
    name: EntryName,
    permissions: Permissions,
    size: Size
  } :< Dict0,
  atomic_list_concat([Uri,EntryName], ' ', Name),
  md5_hash(Name, Hash, []),
  hash_file(Hash, source, DataFileTmp),
  setup_call_cleanup(
    open(DataFileTmp, write, DataOut, [type(binary)]),
    (
      call_statistics(copy_stream_data(In0, DataOut), walltime, Walltime),
      stream_metadata(In0, DataOut, Walltime, StreamDict)
    ),
    close(DataOut)
  ),
  close(In0),
  rename_file(DataFileTmp, data, DataFile),
  hash_file(Hash, 'meta.nt', MetaFile),
  setup_call_cleanup(
    open(MetaFile, append, MetaOut),
    (
      rdf_global_id(llr:Hash, S),
      write_ntriple(MetaOut, S, rdf:type, llo:'Unpack'),
      write_filters_metadata(MetaOut, S, Filters),
      write_ntriple(MetaOut, S, llo:format, Format^^xsd:string),
      write_ntriple(MetaOut, S, llo:mtime, MTime^^xsd:float),
      write_ntriple(MetaOut, S, llo:name, EntryName^^xsd:string),
      rdf_global_id(llr:Hash0, S0),
      write_ntriple(MetaOut, S, llo:parent, S0),
      write_ntriple(MetaOut, S, llo:permissions,
                    Permissions^^xsd:nonNegativeInteger),
      write_ntriple(MetaOut, S, llo:size, Size^^xsd:float),
      write_stream_metadata(MetaOut, S, StreamDict)
    ),
    close(MetaOut)
  ),
  uri_file_name(EntryNameUri, EntryName),
  uri_media_type(EntryNameUri, ExtMT)
  atomic_list_concat([e,Hash], :, Alias),
  call_in_thread(Alias, unpack_file(Uri, Hash, DataFile)),
  finish_meta_and_warn(Hash).





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



%! finish_meta(+Hash) is det.

finish_meta(Hash) :-
  hash_file(Hash, 'meta.nt', NtFile),
  hash_file(Hash, 'meta.hdt', HdtFile),
  hdt:hdt_create_from_file(HdtFile, NtFile, []).



%! finish_meta_and_warn(+Hash) is det.

finish_meta_and_warn(Hash) :-
  finish_meta(Hash),
  finish_warn(Hash).



%! finish_warn(+Hash) is det.

finish_warn(Hash) :-
  hash_file(Hash, 'warn.log', WarnFile),
  wc(WarnFile, NumLines),
  (   NumLines =:= 0,
      exists_file(WarnFile)
  ->  true
  ;   compress_file(WarnFile)
  ),
  delete_file(WarnFile).



%! rdf_membership_property(+N, -P) is det.

rdf_membership_property(N, P) :-
  atom_concat(bnode, N, Local),
  rdf_global_id(rdf:Local, P).



%! rename_file(+File1, +Ext, -File2) is det.

rename_file(File1, Ext, File2) :-
  file_name_extension(File1, Ext, File2),
  rename_file(File1, File2).



%! store_warnings(+Hash, :Goal_0) is det.

store_warnings(Hash, Goal_0) :-
  hash_file(Hash, 'warn.log', File),
  setup_call_cleanup(
    open(File, append, Out),
    (
      asserta((
        user:thread_message_hook(E,Kind,_) :-
          check_installation:error_kind(Kind),
          write_error(Out, E)
      )),
      (catch(call(Goal_0), E, true) *-> true ; E = fail(Goal_0)),
      (var(E) -> true ; write_error(Out, E))
    ),
    close(Out)
  ).

write_error(Out, E) :-
  error_term(E, ETerm),
  with_output_to(Out, write_canonical(ETerm)),
  nl(Out).

error_term(error(ETerm,_), ETerm) :- !.
error_term(ETerm, ETerm).



%! stream_metadata(+In, +Out, +Walltime, -Dict) is det.

stream_metadata(In, Out, Walltime, Dict) :-
  stream_metadata(In, InDict),
  stream_metadata(Out, OutDict),
  Dict = _{
    in: InDict,
    out: OutDict,
    walltime: Walltime
  }.



%! write_clean_tuple(+Count, +Out, +Tuple) is det.

write_clean_tuple(Count, Out, Tuple) :-
  rdf_clean_tuple(Tuple, rdf(S,P,O,_)), !,
  rdf_ext:write_ntriple_part0(Out, S, P, O),
  write_eot(Out),
  arg(1, Count, N1),
  N2 is N1 + 1,
  nb_setarg(1, Count, N2).
% always succeeds
write_clean_tuple(_, _, _).

write_clean_tuples(Count, Out, Tuples, _) :-
  maplist(write_clean_tuple(Count, Out), Tuples).



%! write_filters_metadata(+Out, +S, +Filters) is det.

write_filters_metadata(Out, S, Filters) :-
  write_filters_metadata(Out, S, 1, Filters).

write_filters_metadata(_, _, _, []) :- !.
write_filters_metadata(Out, S, N1, [H|T]) :-
  rdf_membership_property(N1, P),
  write_ntriple(Out, S, P, H^^xsd:string),
  N2 is N1 + 1,
  write_filters_metadata(Out, S, N2, T).



%! write_http_metadata(+Out, +S, +Dicts) is det.

write_http_metadata(Out, S, Dicts) :-
  write_http_metadata(Out, S, 1, Dicts).

write_http_metadata(_, _, _, []) :- !.
write_http_metadata(Out, S, N1, [H|T]) :-
  _{
    headers: Dict,
    status: Status,
    uri: Uri,
    version: Version,
    walltime: Walltime
  } :< H,
  rdf_membership_property(N1, P),
  rdf_create_well_known_iri(BNode),
  write_ntriple(Out, S, P, BNode),
  write_ntriple(Out, BNode, rdf:type, llo:'HttpRequest'),
  write_ntriple(Out, BNode, llo:status, Status^^xsd:nonNegativeInteger),
  write_ntriple(Out, BNode, llo:uri, Uri^^xsd:anyURI),
  write_ntriple(Out, BNode, llo:version, Version^^xsd:string),
  write_ntriple(Out, BNode, llo:walltime, Walltime^^xsd:float),
  write_http_headers_metadata(Out, BNode, Dict),
  N2 is N1 + 1,
  write_http_metadata(Out, S, N2, T).

write_http_headers_metadata(Out, S, Dict) :-
  dict_pairs(Dict, _, Pairs),
  maplist(write_http_header_metadata(Out, S), Pairs).

write_http_header_metadata(Out, S, Key-Val) :-
  rdf_global_id(llh:Key, P),
  write_ntriple(Out, S, P, Val^^xsd:string).



%! write_stream_metadata(+Out, +S, +Dict) is det.

write_stream_metadata(Out, S, Dict) :-
  _{
    in: InDict,
    out: OutDict,
    walltime: Walltime
  } :< Dict,
  rdf_create_well_known_iri(InStream),
  write_ntriple(Out, S, llo:inputStream, InStream),
  write_iostream_metadata(Out, InStream, InDict),
  rdf_create_well_known_iri(OutStream),
  write_ntriple(Out, S, llo:outputStream, OutStream),
  write_iostream_metadata(Out, OutStream, OutDict),
  write_ntriple(Out, S, llo:walltime, Walltime^^xsd:float).

write_iostream_metadata(Out, Stream, Dict) :-
  _{
    byte_count: NumBytes,
    char_count: NumChars,
    line_count: NumLines,
    newline: Newline
  } :< Dict,
  write_ntriple(Out, Stream, llo:numberOfBytes,
                NumBytes^^xsd:nonNegativeInteger),
  write_ntriple(Out, Stream, llo:numberOfCharacters,
                NumChars^^xsd:nonNegativeInteger),
  write_ntriple(Out, Stream, llo:numberOfLines,
                NumLines^^xsd:nonNegativeInteger),
  write_ntriple(Out, Stream, llo:newline, Newline^^xsd:string).
