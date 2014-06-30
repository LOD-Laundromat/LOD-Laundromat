:- module(
  lwm_metadata,
  [
    run_metadata_scrape/0
  ]
).

/** <module> LOD Washing Machine Metadata

LOD Washing Machine companion for saving metadata separately.

@author Wouter Beek
@version 2014/06
*/

:- use_module(library(apply)).
:- use_module(library(filesex)).
:- use_module(library(zlib)).

:- use_module(dcg(dcg_generic)).
:- use_module(os(unpack)).

:- use_module(plRdf(rdf_metadata)).
:- use_module(plRdf_ser(rdf_ntriples_write)).
:- use_module(plRdf_term(rdf_literal)).

:- use_module(lwm(lwm_generics)).
:- use_module(lwm(noRdf_store)).



run_metadata_scrape:-
  thread_create(metadata_scrape_loop, _, []).

metadata_scrape_loop:-
  % Pick a new source to process.
  catch(get_datadoc(Md5), E, writeln(E)),

  % Process the URL we picked.
  metadata_scrape(Md5),

  % Intermittent loop.
  metadata_scrape_loop.
% Done for now. Check whether there are new jobs in one minute.
metadata_scrape_loop:-
  sleep(1),
  metadata_scrape_loop.


%! get_datadoc(-Md5:atom) is det.

get_datadoc(Md5):-
  catch(
    lwm_sparql_select([ll], [md5],
        [rdf(var(md5res),ll:end,var(end)),
         rdf(var(md5res),ll:triples,var(triples)),
         rdf(var(md5res),ll:md5,var(md5))],
        [[Literal]], [limit(1)]),
    error(socket_error('Connection refused'),_),
    fail
  ),
  rdf_literal(Literal, Md5, _).


%! metadata_scrape(+Md5:atom) is det.

metadata_scrape(Md5):-
  md5_to_dir(Md5, Md5Dir),
  directory_file_path(Md5Dir, 'clean.nt.gz', ReadFile),
  directory_file_path(Md5Dir, 'metadata.nt.gz', WriteFile),
  setup_call_cleanup(
    gzopen(WriteFile, write, Write),
    setup_call_cleanup(
      unpack(ReadFile, Read, _),
      save_metadata_triples(Read, Write, NumberOfTriples),
      close(Read)
    ),
    close(Write)
  ),
  store_triple(ll-Md5, ll-number_of_meta_triples,
      literal(type(xsd-integer,NumberOfTriples)), lwm),
  post_rdf_triples(Md5).


%! save_metadata_triples(
%!   +Read:blob,
%!   +Write:blob,
%!   -NumberOfTriples:nonneg
%! ) is det.

save_metadata_triples(Read, _, NumberOfTriples):-
  at_end_of_stream(Read), !,
  flag(lwm_metadata, NumberOfTriples, 0).
save_metadata_triples(Read, Write):-
  read_line_to_codes(Read, Codes),
  phrase(triple_terms(S, P, O), Codes),
  (
    rdf_metadata(S, P, O)
  ->
    rdf_write_ntriple(S, P, O, _),
    flag(lwm_metadata, NumberOfTriples, NumberOfTriples + 1)
  ;
    true
  ),
  save_metadata_triples(Read, Write).


%! triple_term(-RdfTerm:list(code)) is det.

triple_term([]) -->
  ` `, !.
triple_term([H|T]):-
  [H],
  triple_term(T).


%! triple_terms(
%!   -Subject:or([bnode,iri]),
%!   -Predicate:iri,
%!   -Object:or([bnode,iri,literal])
%! )// is det.

triple_terms(S2, P2, O2) -->
  triple_term(S1),
  triple_term(P1),
  dcg_rest(O1),
  {maplist(atom_codes, [S2,P2,O2], [S1,P1,O1])}.

