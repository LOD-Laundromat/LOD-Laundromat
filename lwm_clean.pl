:- module(
  lwm_clean,
  [
    lwm_clean/1, % +Datadoc:iri
    lwm_clean_loop/3 % +Category:atom
                     % ?Min:nonneg
                     % ?Max:nonneg
  ]
).

/** <module> LOD Washing Machine: cleaning

The cleaning process performed by the LOD Washing Machine.

@author Wouter Beek
@version 2014/03-2014/06, 2014/08-2014/09, 2015/01-2015/02
*/

:- use_module(library(apply)).
:- use_module(library(option)).
:- use_module(library(semweb/rdf_db), except([rdf_node/1])). % Format `xml`.
:- use_module(library(semweb/rdf_ntriples)). % Formats `ntriples` and `nquads`.
:- use_module(library(semweb/rdfa)). % Format `rdfa`
:- use_module(library(semweb/turtle)). % Formats `turtle` and `trig`.
:- use_module(library(zlib)).

:- use_module(generics(list_ext)).
:- use_module(generics(print_ext)).
:- use_module(generics(sort_ext)).
:- use_module(os(archive_ext)).
:- use_module(os(file_ext)).
:- use_module(os(file_gnu)).
:- use_module(pl(pl_log)).

:- use_module(plRdf(management/rdf_file_db)).
:- use_module(plRdf(management/rdf_guess_format)).
:- use_module(plRdf(syntax/ctriples/ctriples_write_generics)).
:- use_module(plRdf(syntax/ctriples/ctriples_write_graph)).
:- use_module(plRdf(syntax/ctriples/ctriples_write_triples)).

:- use_module(lwm(lwm_debug_message)).
:- use_module(lwm(lwm_store_triple)).
:- use_module(lwm(md5)).
:- use_module(lwm(noRdf_store)).
:- use_module(lwm(query/lwm_sparql_enum)).
:- use_module(lwm(query/lwm_sparql_query)).

:- dynamic(debug:debug_md5/2).
:- multifile(debug:debug_md5/2).

:- thread_local(datadump/1).
:- thread_local(has_quadruples/1).





%! lwm_clean_loop(+Category:atom, ?Min:nonneg, ?Max:nonneg) is det.

lwm_clean_loop(Category, Min, Max):-
  % Pick a new source to process.
  % If some exception is thrown here, the catch/3 makes it
  % silently fail. This way, the unpacking thread is able
  % to wait in case a SPARQL endpoint is temporarily down.
  catch(
    with_mutex(lwm_endpoint_access, (
      % Do not process dirty data documents that do not conform
      % to the given minimum and/or maximum file size constraints.
      datadoc_enum_unpacked(Min, Max, Datadoc, UnpackedSize),
      % Tell the triple store we are now going to clean this MD5.
      store_start_clean(Datadoc)
    )),
    Exception,
    var(Exception)
  ),
  lwm_clean(Category, Datadoc, UnpackedSize),
  % Intermittent loop.
  lwm_clean_loop(Category, Min, Max).
% Done for now. Check whether there are new jobs in one seconds.
lwm_clean_loop(Category, Min, Max):-
  sleep(5),
  lwm_debug_message(lwm_idle_loop(Category)), % DEB
  lwm_clean_loop(Category, Min, Max).



%! lwm_clean(+Datadoc:iri) is det.

lwm_clean(Datadoc):-
  % Tell the triple store we are now going to clean this MD5.
  store_start_clean(Datadoc),

  datadoc_unpacked_size(Datadoc, UnpackedSize),
  lwm_clean(clean_any, Datadoc, UnpackedSize).

%! lwm_clean(+Category:atom, +Datadoc:iri, +UnpackedSize:nonneg) is det.

lwm_clean(Category, Datadoc, UnpackedSize):-
  % We sometimes need the MD5 of the data document.
  rdf_global_id(ll:Md5, Datadoc),

  % DEB
  (   debug:debug_md5(Md5, clean)
  ->  gtrace %DEB
  ;   true
  ),

  % DEB: *start* cleaning a specific data document.
  lwm_debug_message(
    lwm_progress(Category),
    lwm_start(Category,Md5,Datadoc,Source,UnpackedSize)
  ),

  run_collect_messages(
    clean_md5(Category, Md5, Datadoc),
    Status,
    Warnings1
  ),
  (Status == false -> gtrace ; true), %DEB

  % Store the number of warnings.
  length(Warnings1, NumberOfWarnings),
  store_triple(
    Datadoc,
    llo-number_of_warnings,
    literal(type(xsd-nonNegativeInteger,NumberOfWarnings))
  ),

  % Store warnings and status as metadata.
  store_exception(Datadoc, Status),
  lwm_setting:setting(max_number_of_warnings, MaxWarnings),
  list_truncate(Warnings1, MaxWarnings, Warnings2),
  maplist(store_warning(Datadoc), Warnings),
  store_end_clean(Md5, Datadoc),

  % DEB: *end* cleaning a specific data document.
  lwm_debug_message(
    lwm_progress(Category),
    lwm_end(Category,Md5,Source,Status,Warnings)
  ).



%! clean_md5(+Category:atom, +Md5:atom, +Datadoc:iri) is det.

clean_md5(Category, Md5, Datadoc):-
  % Construct the file name belonging to the given MD5.
  md5_directory(Md5, Md5Dir),
  absolute_file_name(dirty, DirtyFile, [access(read),relative_to(Md5Dir)]),

  % Retrieve the content type, if it was previously determined.
  % Stays uninstantiated in case no content type is set.
  ignore(datadoc_content_type(Datadoc, ContentType)),

  % Clean the data document in an RDF transaction.
  setup_call_cleanup(
    open(DirtyFile, read, DirtyIn),
    (
      rdf_transaction(
        clean_datastream(
          Category,
          Md5,
          Datadoc,
          DirtyFile,
          DirtyIn,
          ContentType,
          VoidUris
        ),
        _,
        [snapshot(true)]
      ),
      store_stream(Datadoc, DirtyIn)
    ),
    close(DirtyIn)
  ),

  % Keep the old/dirty file around in compressed form,
  % or throw it away.
  (   lwm_settings:setting(keep_old_datadoc, true)
  ->  archive_create(DirtyFile, _)
  ;   true
  ),
  delete_file(DirtyFile),

  % Add the new VoID URLs to the LOD Basket.
  with_mutex(lwm_endpoint_access,
    maplist(store_seedpoint, VoidUris)
  ).

%! clean_datastream(
%!   +Category:atom,
%!   +Md5:atom,
%!   +Datadoc:iri,
%!   +DirtyFile:atom,
%!   +In:stream,
%!   +ContentType:atom,
%!   -VoidUris:ordset(uri)
%! ) is det.

clean_datastream(
  Category,
  Md5,
  Datadoc,
  DirtyFile,
  DirtyIn,
  ContentType,
  VoidUris
):-
  % Guess the RDF serialization format,
  % using the content type and the file extension as suggestions.
  ignore(datadoc_file_extension(Datadoc, FileExtension)),
  rdf_guess_format(Datadoc, DirtyIn, FileExtension, ContentType, Format),

  rdf_serialization(_, Format, _, Uri),
  store_triple(Datadoc, llo-serializationFormat, Uri),

  % Load all triples by parsing the data document
  % according to the guessed RDF serialization format.
  md5_base_url(Md5, Base),
  Options1 = [
      base_uri(Base),
      format(Format),
      graph(user),
      register_namespaces(false),
      silent(true)
  ],

  % Add options that are specific to XML/RDF and RDFa.
  merge_options([max_errors(-1),syntax(style)], Options1, Options2),

  % Prepare the file name.
  file_directory_name(DirtyFile, Dir),

  md5_bnode_base(Md5, BaseComponents),
  Options3 = [bnode_base(BaseComponents),number_of_triples(NumberOfTriples)],

  retractall(datadump/1),
  directory_file_path(Dir, unsorted, UnsortedFile),
  (   Format == rdfa
  ->  rdf_load(stream(DirtyIn), Options2),

      % Save the data in a cleaned format.
      setup_call_cleanup(
        open(UnsortedFile, write, UnsortedOut),
        ctriples_write_graph(UnsortedOut, _NoGraph, Options3),
        close(UnsortedOut)
      ),

      % Make sure any VoID datadumps are added to the LOD Basket.
      forall(
        rdf_has(_, void:dataDump, VoidUrl),
        assert(datadump(VoidUrl))
      )
  ;   setup_call_cleanup(
        ctriples_write_begin(State, BNodePrefix, Options3),
        setup_call_cleanup(
          open(UnsortedFile, write, UnsortedOut),
          clean_triples(Format, DirtyIn, UnsortedOut, State, BNodePrefix, Options2),
          close(UnsortedOut)
        ),
        ctriples_write_end(State, Options3)
      )
  ),
  % Collect datadump locations.
  findall(
    VoidUrl,
    datadump(VoidUrl),
    VoidUris
  ),

  % Establish the file name extension.
  (   retract(has_quadruples(true))
  ->  Ext = nq
  ;   Ext = nt
  ),

  % Sort file.
  directory_file_path(Dir, sorted, SortedFile),
  buffer_size_file(UnsortedFile, BufferSize),
  (   BufferSize > 4.5 * (1024 ** 3)
  ->  Threads = 4
  ;   BufferSize > 2.5 * (1024 ** 3)
  ->  Threads = 2
  ;   Threads = 1
  ),
  gnu_sort(
    UnsortedFile,
    [
      buffer_size(BufferSize),
      duplicates(false),
      output(SortedFile),
      parallel(Threads),
      temporary_directory('/ssd/lodlaundromat/tmp'),
      utf8(true)
    ]
  ),
  file_lines(SortedFile, NumberOfUniqueTriples),
  delete_file(UnsortedFile),

  % Compress file.
  atomic_list_concat([clean,Ext,gz], '.', LocalName),
  directory_file_path(Dir, LocalName, CleanFile),
  setup_call_cleanup(
    gzopen(CleanFile, write, CleanOut),
    setup_call_cleanup(
      open(SortedFile, read, SortedIn),
      copy_stream_data(SortedIn, CleanOut),
      close(SortedIn)
    ),
    close(CleanOut)
  ),
  delete_file(SortedFile),

  % Store statistics about the number of (duplicate) triples.
  store_number_of_triples(
    Category,
    Datadoc,
    NumberOfTriples,
    NumberOfUniqueTriples
  ).

clean_triples(xml, In, Out, State, BNodePrefix, Options):- !,
  process_rdf(
    In,
    clean_streamed_triples(Out, State, BNodePrefix),
    Options
  ).
clean_triples(Format, In, Out, State, BNodePrefix, Options1):-
  memberchk(Format, [nquads,ntriples]), !,
  merge_options([anon_prefix(BNodePrefix)], Options1, Options2),
  rdf_process_ntriples(
    In,
    clean_streamed_triples(Out, State, BNodePrefix),
    Options2
  ).
clean_triples(Format, In, Out, State, BNodePrefix, Options1):-
  memberchk(Format, [trig,turtle]), !,
  merge_options([anon_prefix(BNodePrefix)], Options1, Options2),
  rdf_process_turtle(
    In,
    clean_streamed_triples(Out, State, BNodePrefix),
    Options2
  ).
clean_triples(Format, In, Out, State, BNodePrefix, Options):-
  gtrace, %DEB
  clean_triples(Format, In, Out, State, BNodePrefix, Options).





% HELPERS %

%! clean_streamed_triples(
%!   +Out:stream,
%!   +State:compound,
%!   +BNodePrefix:atom,
%!   +Triples:compound,
%!   +LinePosition:compound
%! ) is det.

clean_streamed_triples(Out, State, BNodePrefix, Triples0, Graph0):-
  graph_without_line(Graph0, Graph),
  maplist(fix_triple(Graph), Triples0, Triples),
  maplist(ctriples_write_triple(Out, State, BNodePrefix), Triples).

%! graph_without_line(+WonkyGraph:compound, -Graph:atom) is det.
% Remove file line numbers from the graph name.

graph_without_line(Graph:_, Graph):- !.
graph_without_line(Graph, Graph).

%! fix_triple(
%!   +Graph:atom,
%!   +WonkyStatement:compound,
%!   -Statement:compound
%! ) is det.
% 

fix_triple(Graph, rdf(S,P,O), Triple):- !,
  (   is_named_graph(Graph)
  ->  set_has_quadruples,
      Triple = rdf(S,P,O,Graph)
  ;   Triple = rdf(S,P,O)
  ).
fix_triple(Graph, rdf(S,P,O,G0), Triple):-
  (   graph_without_line(G0, G),
      is_named_graph(G)
  ->  set_has_quadruples,
      Triple = rdf(S,P,O,G)
  ;   is_named_graph(Graph)
  ->  set_has_quadruples,
      Triple = rdf(S,P,O,Graph)
  ;   Triple = rdf(S,P,O)
  ).

%! is_named_graph(+Graph:atom) is semidet.
% Succeeds for all and only named graphs.

is_named_graph(Graph):-
  ground(Graph),
  Graph \== user.

%! set_has_quadruples is det.
% Store the fact that a quadruple occurred in the parser stream
% as a thread-local global Prolog fact.

set_has_quadruples:-
  has_quadruples(true), !.
set_has_quadruples:-
  assert(has_quadruples(true)).



%! rdf_guess_format(
%!   +Datadoc:uri,
%!   +In:stream,
%!   +FileExtension:atom,
%!   +ContentType:atom,
%!   -Format:atom
%! ) is semidet.

rdf_guess_format(_, In, FileExtension, ContentType, Format):-
  rdf_guess_format(In, FileExtension, ContentType, Format), !.
rdf_guess_format(Datadoc, _, _, _, _):-
  datadoc_source(Datadoc, Source),
  throw(error(no_rdf(Source))).

