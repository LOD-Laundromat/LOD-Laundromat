:- module(
  run_singlethread,
  [
    run_singlethread/0
  ]
).

/** <module> Run single-threaded

Initializes the downloading and cleaning of LOD in a single-threaded process.

See module [run_multithread] for the threaded version of this module.

@author Wouter Beek
@version 2014/03-2014/06, 2014/08
*/

:- use_module(library(uri)).

:- use_module(lwm(lwm_clean)).
:- use_module(lwm(lwm_init)). % Initialization.
:- use_module(lwm(lwm_settings)).
:- use_module(lwm(lwm_unpack)).
:- use_module(lwm(md5)).
:- use_module(lwm(noRdf_store)).

:- use_module(xsd(xsd_dateTime_ext)).



%! run_singlethread .

run_singlethread:-
  run_singlethread(15, 1).

%! run_singlethread(
%!   +NumberOfUnpackLoops:nonneg,
%!   +NumberOfCleanLoops:nonneg
%! ) .

run_singlethread(NumberOfUnpackLoops, NumberOfCleanLoops):-
  % Make sure the LOD Washing Machine version object is sent to
  % the LOD Laundromat Endpoint.
  init_lwm_version,
  forall(
    between(1, NumberOfUnpackLoops, _),
    thread_create(lwm_unpack_loop, _, [detached(true)])
  ),
  forall(
    between(1, NumberOfCleanLoops, _),
    thread_create(lwm_clean_loop, _, [detached(true)])
  ).


%! init_lwm_version is det.
% Initializes the current version of the LOD Washing Machine
% at the LOD Laundromat Endpoint.
%
% A LOD Washing Machine version is represented by a resource with
%  a version number,
%  a start dataTime,
%  and a data directory.

init_lwm_version:-
  lwm_version_object(Version),
  
  % IS-A.
  store_triple(Version, rdf-type, ll-'Version'),
  
  % Label.
  lwm_version_number(VersionNumber),
  format(atom(Label), 'LOD Washing Machine version ~D.', [VersionNumber]),
  store_triple(Version, rdfs-label, literal(type(xsd-string,Label))),
  
  % dataTime.
  get_dateTime(Now),
  store_triple(Version, ll-start, literal(type(xsd-dateTime,Now))),
  
  % Version directory.
  lwm_version_directory(VersionDir),
  uri_file_name(VersionUri, VersionDir),
  store_triple(Version, ll-datadir, VersionUri),
  
  % Send SPARQL Update request.
  post_rdf_triples.

