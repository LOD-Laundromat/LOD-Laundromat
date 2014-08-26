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

:- use_module(plXsd_datetime(xsd_dateTime_ext)).

:- use_module(lwm(lwm_clean)).
:- use_module(lwm(lwm_init)). % Initialization.
:- use_module(lwm(lwm_settings)).
:- use_module(lwm(lwm_unpack)).
:- use_module(lwm(md5)).
:- use_module(lwm(noRdf_store)).



%! run_singlethread .

run_singlethread:-
  run_singlethread(1, 1).

%! run_singlethread(
%!   +NumberOfUnpackLoops:nonneg,
%!   +NumberOfCleanLoops:nonneg
%! ) .

run_singlethread(NumberOfUnpackLoops, NumberOfCleanLoops):-
  forall(
    between(1, NumberOfUnpackLoops, _),
    thread_create(lwm_unpack_loop, _, [detached(true)])
  ),
  forall(
    between(1, NumberOfCleanLoops, _),
    thread_create(lwm_clean_loop, _, [detached(true)])
  ).

