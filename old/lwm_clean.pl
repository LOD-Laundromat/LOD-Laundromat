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
@version 2016/01
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(option)).
:- use_module(library(os/io)).
:- use_module(library(rdf/rdf__io)).
:- use_module(library(zlib)).

:- use_module(lwm_debug_message).
:- use_module(lwm_store_triple).
:- use_module(noRdf_store).

:- dynamic
    debug:debug_md5/2.

:- multifile
    debug:debug_md5/2.

:- thread_local
   datadump/1,
   has_quadruples/1.





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
  sleep(100),
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
    clean_md5(Category, Md5, Datadoc, DirtyFile),
    Status,
    Warnings1
  ),

  % Store the number of warnings.
  length(Warnings1, NumberOfWarnings),
  store_triple(
    Datadoc,
    llo-number_of_warnings,
    literal(type(xsd-nonNegativeInteger,NumberOfWarnings))
  ),

  % Store warnings and status as metadata.
  store_exception(Datadoc, Status),
  lwm_settings:setting(max_number_of_warnings, MaxWarnings),
  list_truncate(Warnings1, MaxWarnings, Warnings2),
  maplist(store_warning(Datadoc), Warnings2),
  store_end_clean(Md5, Datadoc),

  % Keep the old/dirty file around in compressed form,
  % or throw it away.
  (   ground(DirtyFile),
      exists_file(DirtyFile)
  ->  (   lwm_settings:setting(keep_old_datadoc, true)
      ->  archive_create(DirtyFile, _)
      ;   true
      ),
      delete_file(DirtyFile)
  ;   true
  ),

  % DEB: *end* cleaning a specific data document.
  lwm_debug_message(
    lwm_progress(Category),
    lwm_end(Category,Md5,Source,Status,Warnings2)
  ).



%! clean_md5(+Category:atom, +Md5:atom, +Datadoc:iri, -DirtyFile:atom) is det.

clean_md5(Category, Md5, Datadoc, DirtyFile):-
  % Construct the file name belonging to the given MD5.
  md5_directory(Md5, Md5Dir),
  absolute_file_name(dirty, DirtyFile, [access(read),relative_to(Md5Dir)]),

  % Retrieve the content type, if it was previously determined.
  % Stays uninstantiated in case no content type is set.
  ignore((
    datadoc_content_type(Datadoc, ContentType0),
    atom_phrase('Content-Type'(ContentType), ContentType0)
  )),

  % Clean the data document in an RDF transaction.
  call_on_stream(
    DirtyFile,
    {Category,Md5,Datadoc,DirtyFile,ContentType,VoidUris}/[In,Meta,Meta]>>(
      q_snap(
        clean_datastream(
          Category,
          Md5,
          Datadoc,
          DirtyFile,
          In,
          ContentType,
          VoidUris
        )
      ),
      store_stream(Datadoc, DirtyIn)
    )
  ),

  % Add the new VoID URIs to the LOD Basket.
  list_script(
    store_seedpoint,
    VoidUris,
    [message("LWM Seedpoint"),with_mutex(lwm_endpoint_access)]
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

  rdf_serialization_resource(Uri, Format),
  store_triple(Datadoc, llo-serializationFormat, Uri),

  % Load all triples by parsing the data document
  % according to the guessed RDF serialization format.
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

  Options3 = [base_iri(BaseIri),number_of_triples(NumberOfTriples)],

  retractall(datadump/1),
  directory_file_path(Dir, cleaning, CleaningFile),
  (   Format == rdfa
  ->  rdf_load(stream(DirtyIn), Options2),

      % Save the data in a cleaned format.
      call_to_stream(CleaningFile, ctriples_write_graph(_NoGraph, Options3)),

      % Make sure any VoID datadumps are added to the LOD Basket.
      forall(
        rdf_has(_, void:dataDump, VoidUrl),
        assert(datadump(VoidUrl))
      )
  ;   setup_call_cleanup(
        ctriples_write_begin(State, BNodePrefix, Options3),
        call_to_stream(
	  CleaningFile,
          rdf_clean:clean_triples(
            Format,
            DirtyIn,
            State,
            BNodePrefix,
            Options2
          )
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
  ->  Ext = nq,
      StatementsType = quadruples
  ;   Ext = nt,
      StatementsType = triples
  ),
  store_triple(
    Datadoc,
    llo-statementsType,
    literal(type(xsd-string,StatementsType))
  ),

  % Sort file.
  rdf_clean:sort_file(CleaningFile, '/ssd/lodlaundromat/tmp'),
  source_numlines(CleaningFile, NumberOfUniqueTriples),

  % Compress file.
  rdf_clean:compress_file(Dir, Ext, CleaningFile),
  delete_file(CleaningFile),

  % Store statistics about the number of (duplicate) triples.
  store_number_of_triples(
    Datadoc,
    NumberOfTriples,
    NumberOfUniqueTriples
  ),

  % Run the callback function.
  (   lwm_settings:setting(post_processing, true)
  ->  process_create(
        '/home/lodlaundromat/bin/wm-callback.sh',
        [file(Dir)],
        []
      )
  ;   true
  ).




% HELPERS %

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
