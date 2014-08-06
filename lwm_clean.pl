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
:- use_module(library(semweb/rdf_db)).
:- use_module(library(zlib)).

:- use_module(generics(flag_ext)).
:- use_module(pl(pl_log)).
:- use_module(void(void_db)). % XML namespace.

:- use_module(plRdf_ser(rdf_detect)).
:- use_module(plRdf_ser(rdf_ntriples_write)).
:- use_module(plRdf_term(rdf_literal)).

:- use_module(lwm(lod_basket)).
:- use_module(lwm(lwm_generics)).
:- use_module(lwm(lwm_store_triple)).
:- use_module(lwm(noRdf_store)).



lwm_clean_loop:-
  % Pick a new source to process.
  catch(pick_unpacked(Md5), Exception, var(Exception)),

  % Process the URL we picked.
  lwm_clean(Md5),

  % Intermittent loop.
  flag(number_of_idle_loops, _, 0),
  lwm_clean_loop.
% Done for now. Check whether there are new jobs in one seconds.
lwm_clean_loop:-
  sleep(1),
  print_message(warning, idle_loop(lwm_clean)),
  lwm_clean_loop.


%! lwm_clean(+Md5:atom) is det.

lwm_clean(Md5):-
  print_message(informational, lwm_start(clean,Md5,Source)),

  run_collect_messages(
    clean_md5(Md5),
    Status,
    Messages
  ),

  store_status(Md5, Status),
  maplist(store_message(Md5), Messages),

  store_end_clean(Md5),
  print_message(informational, lwm_end(clean,Md5,Source,Status,Messages)).


%! clean_md5(+Md5:atom) is det.

clean_md5(Md5):-
  % Construct the file name belonging to the given MD5.
  md5_to_dir(Md5, Md5Dir),
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

  % Remove the old file.
  % @tbd This is where a compressed copy of the dirty file could be kept.
  delete_file(DirtyFile),

  % Add the new VoID URLs to the LOD Basket.
  maplist(add_to_basket, VoidUrls).


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
  ignore(lwm_file_extension(Md5, FileExtension)),
  rdf_guess_format_md5(Md5, Read, FileExtension, ContentType, Format),
  store_triple(ll-Md5, ll-serialization_format,
      literal(type(xsd-string,Format))),

  % Load all triples by parsing the data document
  % according to the guessed RDF serialization format.
  lwm_base(Md5, Base),
  rdf_load(
    stream(Read),
    [base_uri(Base),format(Format),register_namespaces(false)]
  ),

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

%! find_void_datasets(-VoidUrls:ordset(url)) is det.

find_void_datasets(Urls):-
  aggregate_all(
    set(Url),
    rdf(_, void:dataDump, Url),
    Urls
  ),
  print_message(informational, found_void(Urls)).


%! md5_content_type(+Md5:atom, -ContentType:atom) is det.

md5_content_type(Md5, ContentType):-
  lwm_sparql_select([ll], [content_type],
      [rdf(var(md5res),ll:md5,literal(type(xsd:string,Md5))),
       rdf(var(md5res),ll:content_type,var(content_type))],
      [[Literal]], [limit(1)]), !,
  rdf_literal(Literal, ContentType, _).
md5_content_type(_, _).


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
  lwm_source(Md5, Source),
  throw(error(no_rdf(Source))).


%! save_data_to_file(+Md5:atom, +File:atom, -NumberOfTriples:nonneg) is det.

save_data_to_file(Md5, File, NumberOfTriples):-
  file_directory_name(File, Dir),
  directory_file_path(Dir, 'clean.nt.gz', Path),
  lwm_bnode_base(Md5, BNodeBase),
  setup_call_cleanup(
    gzopen(Path, write, Write),
    rdf_ntriples_write(
      Write,
      [bnode_base(BNodeBase),number_of_triples(NumberOfTriples)]
    ),
    close(Write)
  ).



% Messages

:- multifile(prolog:message//1).

prolog:message(found_void([])) --> !, [].
prolog:message(found_void([H|T])) -->
  ['    [VoID] Found: ',H,nl],
  prolog:message(found_void(T)).

prolog:message(idle_loop(Kind)) -->
  {flag(number_of_idle_loops(Kind), X, X + 1)},
  ['IDLE ~D'-[X]].

