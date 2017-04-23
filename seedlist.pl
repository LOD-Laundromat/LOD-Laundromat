:- module(
  seedlist,
  [
    add_seed/1,      % +Uri
    end_seed/1,      % +Hash
    init_seedlist/0,
    seed/5,          % ?Hash, ?Uri, ?Added, ?Started, ?Ended
    start_seed/2     % -Hash, -Uri
  ]
).

/** <module> Seedlist

@author Wouter Beek
@version 2017/04
*/

:- use_module(library(ckan_api)).
:- use_module(library(debug)).
:- use_module(library(md5)).
:- use_module(library(persistency)).
:- use_module(library(semweb/rdf11)).
:- use_module(library(uri)).

:- use_module(ll_api).

:- debug(seedlist).

:- initialization(db_attach('seedlist.data', [])).

:- persistent
   seed(hash:atom, uri:atom, added:float, started:float, ended:float).





%! add_seed(+Uri) is det.

add_seed(Uri0) :-
  uri_normalized(Uri0, Uri),
  md5_hash(Uri, Hash, []),
  with_mutex(seedlist, (
    (   seed(Hash, Uri, _, _, _)
    ->  true
    ;   get_time(Now),
        assert_seed(Hash, Uri, Now, 0.0, 0.0),
        debug(seedlist, "Seedpoint added: ~a", [Uri])
    )
  )).



%! end_seed(+Hash) is semidet.

end_seed(Hash) :-
  with_mutex(seedlist, (
    retract_seed(Hash, Uri, Added, Started, 0.0),
    get_time(Ended),
    assert_seed(Hash, Uri, Added, Started, Ended)
  )).



%! init_seedlist is det.

init_seedlist :-
  forall(
    (
      ckan(S, ckan:format, Format^^xsd:string, File),
      rdf_format(Format)
    ),
    (
      ckan(S, ckan:url, Uri^^_, File),
      add_seed(Uri)
    )
  ).

rdf_format("api/sparql").
rdf_format("application/n-triples").
rdf_format("application/rdf xml").
rdf_format("application/trig").
rdf_format("application/x-nquads").
rdf_format("application/x-ntriples").
rdf_format("application/x-trig").
rdf_format("example/n3").
rdf_format("example/ntriples").
rdf_format("example/rdf+xml").
rdf_format("example/rdfa").
rdf_format("example/turtle").
rdf_format("GZIP::NQUADS").
rdf_format("gzip::nquads").
rdf_format("HTML+RDFa").
rdf_format("linked data").
rdf_format("mapping/owl").
rdf_format("mapping/RDFS").
rdf_format("meta/owl").
rdf_format("meta/rdf-schema").
rdf_format("meta/void").
rdf_format("n-quads").
rdf_format("n-triples").
rdf_format("N-Triples").
rdf_format("OWL").
rdf_format("owl, ontology, meta/owl").
rdf_format("rdf").
rdf_format("RDF").
rdf_format("rdf-turtle").
rdf_format("rdf/turtle").
rdf_format("rdf-xml").
rdf_format("RDFa").
rdf_format("SPARQL").
rdf_format("Sparql-query").
rdf_format("SPARQL web form").
rdf_format("text/n3").
rdf_format("text/rdf+n3").
rdf_format("text/rdf+ttl").
rdf_format("text/turtle").
rdf_format("ttl").



%! start_seed(-Hash, -Uri) is semidet.

start_seed(Hash, Uri) :-
  with_mutex(seedlist, (
    retract_seed(Hash, Uri, Added, 0.0, 0.0),
    get_time(Started),
    assert_seed(Hash, Uri, Added, Started, 0.0)
  )).
