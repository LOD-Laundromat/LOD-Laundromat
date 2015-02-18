:- module(
  debug_datadoc,
  [
    debug_datadoc/1 % +Datadoc:iri
  ]
).

/** <module> LOD Washing Machine: Debug Tools: Datadoc

Debug specific data documents.

@author Wouter Beek
@version 2015/01
*/

:- use_module(lwm(lwm_clean)).
:- use_module(lwm(lwm_reset)).
:- use_module(lwm(lwm_unpack)).





%! debug_datadoc(+Datadoc:iri) is det.

debug_datadoc(Datadoc):-
  reset_datadoc(Datadoc),
  lwm_unpack(Datadoc),
  lwm_clean(Datadoc).

