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
@version 2014/03-2014/06, 2014/08-2014/09, 2015/01
*/

:- use_module(library(apply)).
:- use_module(library(option)).
:- use_module(library(semweb/rdf_db), except([rdf_node/1])).
:- use_module(library(semweb/rdf_ntriples)).
:- use_module(library(semweb/turtle)).
:- use_module(library(zlib)).

:- use_module(generics(list_ext)).
:- use_module(generics(print_ext)).
:- use_module(generics(sort_ext)).
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





%! lwm_clean_loop(+Category:atom, ?Min:nonneg, ?Max:nonneg) is det.

lwm_clean_loop(Category, Min, Max):-
  % Pick a new source to process.
  % If some exception is thrown here, the catch/3 makes it
  % silently fail. This way, the unpacking thread is able
  % to wait in case a SPARQL endpoint is temporarily down.
  catch(
    with_mutex(lod_washing_machine, (
      % Do not process dirty data documents that do not conform
      % to the given minimum and/or maximum file size constraints.
      datadoc_enum_unpacked(Min, Max, Datadoc, UnpackedSize)
    )),
    Exception,
    var(Exception)
  ),
  lwm_clean_loop(Category, Datadoc, UnpackedSize),
  % Intermittent loop.
  lwm_clean_loop(Category, Min, Max).
% Done for now. Check whether there are new jobs in one seconds.
lwm_clean_loop(Category, Min, Max):-
  sleep(1),
  lwm_debug_message(lwm_idle_loop(Category)), % DEB
  lwm_clean_loop(Category, Min, Max).



%! lwm_clean(+Datadoc:iri) is det.

lwm_clean(Datadoc):-
  datadoc_unpacked_size(Datadoc, UnpackedSize),
  lwm_clean(clean_any, Datadoc, UnpackedSize).

%! lwm_clean(+Category:atom, +Datadoc:iri, +UnpackedSize:nonneg) is det.

lwm_clean(Category, Datadoc, UnpackedSize):-
  % Tell the triple store we are now going to clean this MD5.
  store_start_clean(Datadoc),
  
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
  % @tbd Virtuoso gives 413 HTTP status code when sending too many warnings.
  list_truncate(Warnings1, 100, Warnings2),

  % DEB: *end* cleaning a specific data document.
  lwm_debug_message(
    lwm_progress(Category),
    lwm_end(Category,Md5,Source,Status,Warnings2)
  ),

  % Store warnings and status as metadata.
  store_exception(Datadoc, Status),
  maplist(store_warning(Datadoc), Warnings2),
  store_end_clean(Md5, Datadoc),

  %%%%% Make sure the unpacking threads do not create a pending pool
  %%%%% that is (much) too big.
  %%%%flag(number_of_pending_md5s, Id, Id - 1),
  
  true.



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
    open(DirtyFile, read, In),
    (
      rdf_transaction(
        clean_datastream(
          Category,
          Md5,
          Datadoc,
          DirtyFile,
          In,
          ContentType,
          VoidUrls
        ),
        _,
        [snapshot(true)]
      ),
      store_stream(Datadoc, In)
    ),
    close(In)
  ),

  % Keep the old/dirty file around in compressed form,
  % or throw it away.
  %%%%archive_create(DirtyFile, _),
  delete_file(DirtyFile),

  % Add the new VoID URLs to the LOD Basket.
  with_mutex(store_new_url, (
    absolute_file_name(data('url.txt'), File, [access(append)]),
    setup_call_cleanup(
      open(File, append, Out),
      maplist(writeln(Out), VoidUrls),
      close(Out)
    )
  )).


%! clean_datastream(
%!   +Category:atom,
%!   +Md5:atom,
%!   +Datadoc:iri,
%!   +File:atom,
%!   +In:stream,
%!   +ContentType:atom,
%!   -VoidUrls:ordset(uri)
%! ) is det.

clean_datastream(
  Category,
  Md5,
  Datadoc,
  File,
  In,
  ContentType,
  VoidUrls
):-
  % Guess the RDF serialization format,
  % using the content type and the file extension as suggestions.
  ignore(datadoc_file_extension(Datadoc, FileExtension)),
  rdf_guess_format(Datadoc, In, FileExtension, ContentType, Format),

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
  file_directory_name(File, Dir),

  md5_bnode_base(Md5, BaseComponents),
  Options3 = [
    bnode_base(BaseComponents),
    format(CFormat),
    number_of_triples(NumberOfTriples)
  ],

  retractall(datadump/1),
  directory_file_path(Dir, unsorted, UnsortedFile),
  (   Format == rdfa
  ->  rdf_load(stream(In), Options2),

      % Save the data in a cleaned format.
      setup_call_cleanup(
        open(UnsortedFile, write, Out),
        ctriples_write_graph(Out, _NoGraph, Options3),
        close(Out)
      ),

      % Make sure any VoID datadumps are added to the LOD Basket.
      forall(
        rdf_has(_, void:dataDump, VoidUrl),
        assert(datadump(VoidUrl))
      )
  ;   setup_call_cleanup(
        ctriples_write_begin(State, BNodePrefix, Options3),
        setup_call_cleanup(
          open(UnsortedFile, write, Out),
          clean_triples(Format, In, Out, State, BNodePrefix, Options2),
          close(Out)
        ),
        ctriples_write_end(State, Options3)
      )
  ),
  % Collect datadump locations.
  findall(
    VoidUrl,
    datadump(VoidUrl),
    VoidUrls
  ),
gtrace,

  % Establish the file name extension.
  (   CFormat == triples
  ->  Ext = nt
  ;   CFormat == quads
  ->  Ext = nq
  ),

  % Sort file.
  directory_file_path(Dir, sorted, SortedFile),
  gnu_sort(UnsortedFile, [duplicates(false),output(SortedFile)]),
  file_lines(SortedFile, NumberOfUniqueTriples),
  writeln(NumberOfUniqueTriples), %@tbd

  % Compress file.
  atomic_list_concat([clean,Ext,gz], LocalName),
  directory_file_path(Dir, LocalName, CleanFile),
  setup_call_cleanup(
    gzopen(CleanFile, write, Out),
    setup_call_cleanup(
      open(SortedFile, read, In),
      copy_stream_data(In, Out),
      close(In)
    ),
    close(Out)
  ),

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
clean_triples(Format, In, Out, State, BNodePrefix, Options):-
  memberchk(Format, [nquads,ntriples]), !,
  rdf_process_ntriples(
    In,
    clean_streamed_triples(Out, State, BNodePrefix),
    Options
  ).
clean_triples(Format, In, Out, State, BNodePrefix, Options):-
  memberchk(Format, [trig,turtle]), !,
  rdf_process_turtle(
    In,
    clean_streamed_triples(Out, State, BNodePrefix),
    Options
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
fix_triple(Graph, rdf(S,P,O), Triple):- !,
  (   is_named_graph(Graph)
  ->  Triple = rdf(S,P,O,Graph)
  ;   Triple = rdf(S,P,O)
  ).
fix_triple(Graph, rdf(S,P,O,G0), Triple):-
  (   graph_without_line(G0, G),
      is_named_graph(G)
  ->  Triple = rdf(S,P,O,G)
  ;   is_named_graph(Graph)
  ->  Triple = rdf(S,P,O,Graph)
  ;   Triple = rdf(S,P,O)
  ).

is_named_graph(Graph):-
  ground(Graph),
  Graph \== user.

graph_without_line(Graph:_, Graph):- !.
graph_without_line(Graph, Graph).



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

