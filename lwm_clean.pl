:- module(
  lwm_clean,
  [
    lwm_clean_loop/0
  ]
).

/** <module> LOD Washing Machine: cleaning

The cleaning process performed by the LOD Washing Machine.

@author Wouter Beek
@version 2014/03-2014/06, 2014/08
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(http/http_client)).
:- use_module(library(option)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(uri)).
:- use_module(library(zlib)).

:- use_module(os(archive_ext)).
:- use_module(pl(pl_log)).

:- use_module(plRdf_ser(ctriples_write)).
:- use_module(plRdf_ser(rdf_file_db)).
:- use_module(plRdf_ser(rdf_guess_format)).

:- use_module(lwm(lwm_basket)).
:- use_module(lwm(lwm_messages)).
:- use_module(lwm(lwm_settings)).
:- use_module(lwm(lwm_sparql_query)).
:- use_module(lwm(lwm_store_triple)).
:- use_module(lwm(md5)).
:- use_module(lwm(noRdf_store)).

:- dynamic(debug:debug_md5/1).
:- multifile(debug:debug_md5/1).



lwm_clean_loop:-
  % Pick a new source to process.
  catch(pick_unpacked(Md5), Exception, var(Exception)),

  % DEB
  (debug:debug_md5(Md5) -> gtrace ; true),

  % Process the URL we picked.
  lwm_clean(Md5),

  % Intermittent loop.
  lwm_clean_loop.
% Done for now. Check whether there are new jobs in one seconds.
lwm_clean_loop:-
  sleep(1),
  print_message(warning, lwm_idle_loop(clean)),
  lwm_clean_loop.

%! lwm_clean(+Md5:atom) is det.

lwm_clean(Md5):-
  print_message(informational, lwm_start(clean,Md5,Source)),

  run_collect_messages(
    clean_md5(Md5),
    Status,
    Warnings
  ),

  store_exception(Md5, Status),
  maplist(store_warning(Md5), Warnings),

  store_end_clean(Md5),
  print_message(informational, lwm_end(clean,Md5,Source,Status,Warnings)).


%! clean_md5(+Md5:atom) is det.

clean_md5(Md5):-
  % Construct the file name belonging to the given MD5.
  md5_directory(Md5, Md5Dir),
  absolute_file_name(dirty, DirtyFile, [access(read),relative_to(Md5Dir)]),

  % Retrieve the content type, if it was previously determined.
  md5_content_type(Md5, ContentType),

  % Clean the data document in an RDF transaction.
  setup_call_cleanup(
    open(DirtyFile, read, Read),
    (
      rdf_transaction(
        clean_datastream(Md5, DirtyFile, Read, ContentType, VoidUrls),
        _,
        [snapshot(true)]
      ),
      store_stream(Md5,Read)
    ),
    close(Read)
  ),

  % Keep the old/dirty file around in compressed form,
  % or throw it away.
  %%%%archive_create(DirtyFile, _),
  delete_file(DirtyFile),

  % Add the new VoID URLs to the LOD Basket.
  maplist(send_to_basket, VoidUrls).


%! clean_datastream(
%!   +Md5:atom,
%!   +File:atom,
%!   +Read:blob,
%!   +ContentType:atom,
%!   -VoidUrls:ordset(url)
%! ) is det.

clean_datastream(Md5, File, Read, ContentType, VoidUrls):-
  % Guess the RDF serialization format,
  % using the content type and the file extension as suggestions.
  ignore(md5_file_extension(Md5, FileExtension)),
  rdf_guess_format_md5(Md5, Read, FileExtension, ContentType, Format),
  rdf_serialization(_, _, Format, _, Uri),
  store_triple(ll-Md5, llo-serializationFormat, Uri),

  % Load all triples by parsing the data document
  % according to the guessed RDF serialization format.
  md5_base_url(Md5, Base),
  Options1 =
      [base_uri(Base),format(Format),graph(user),register_namespaces(false)],

  % Add options that are specific to the RDFa serialization format.
  (
    Format == rdfa
  ->
    merge_options([max_errors(-1),syntax(style)], Options1, Options2)
  ;
    Options2 = Options1
  ),

  rdf_load(stream(Read), Options2),

  % In between loading and saving the data,
  % we count the number of triples, including the number of duplicates.
  aggregate_all(
    count,
    rdf(_, _, _, _),
    TIn
  ),

  % Save the data in a cleaned format.
  save_data_to_file(Md5, File, TOut),

  % Store statistics about the number of (duplicate) triples.
  store_number_of_triples(Md5, TIn, TOut),

  % Make sure any VoID datadumps are added to the LOD Basket.
  find_void_datasets(VoidUrls).



% Helpers

%! clean_file_name(+File:atom, +Format:oneof([quads,triples])) is det.

clean_file_name(_, triples):- !.
clean_file_name(File1, quads):-
  file_directory_name(File1, Dir),
  directory_file_path(Dir, 'clean.nq.gz', File2),
  rename_file(File1, File2).


%! find_void_datasets(-VoidUrls:ordset(url)) is det.

find_void_datasets(Urls):-
  aggregate_all(
    set(Url),
    rdf(_, void:dataDump, Url),
    Urls
  ),
  print_message(informational, found_void(Urls)).


%! rdf_guess_format_md5(
%!   +Md5:atom,
%!   +Read:blob,
%!   +FileExtension:atom,
%!   +ContentType:atom,
%!   -Format:atom
%! ) is semidet.

rdf_guess_format_md5(_, Read, FileExtension, ContentType, Format):-
  rdf_guess_format(Read, FileExtension, ContentType, Format), !.
rdf_guess_format_md5(Md5, _, _, _, _):-
  md5_source(Md5, Source),
  throw(error(no_rdf(Source))).


%! save_data_to_file(+Md5:atom, +File:atom, -NumberOfTriples:nonneg) is det.

save_data_to_file(Md5, File, NumberOfTriples):-
  file_directory_name(File, Dir),
  directory_file_path(Dir, 'clean.nt.gz', CleanFile),
  md5_bnode_base(Md5, BaseComponents),
  setup_call_cleanup(
    gzopen(CleanFile, write, Write),
    ctriples_write(
      Write,
      [
        bnode_base(BaseComponents),
        format(Format),
        number_of_triples(NumberOfTriples)
      ]
    ),
    close(Write)
  ),
  % Fix the file name, if needed.
  clean_file_name(CleanFile, Format).


%! send_to_basket(+Url:url) is det.

send_to_basket(Url):-
  ll_scheme(Scheme),
  ll_authority(Authority),
  uri_components(
    BasketLocation,
    uri_components(Scheme,Authority,basket,_,_)
  ),
  http_post(BasketLocation, atom(Url), Reply, []),
  writeln(Reply).



% Messages

:- multifile(prolog:message//1).

prolog:message(found_void([])) --> !, [].
prolog:message(found_void([H|T])) -->
  ['    [VoID] Found: ',H,nl],
  prolog:message(found_void(T)).

