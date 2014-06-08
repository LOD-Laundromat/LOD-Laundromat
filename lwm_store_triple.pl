:- module(
  lwm_store_triple,
  [
    store_archive_entries/3, % +Url:url
                             % +Md5:atom
                             % +Coordinates:list(list(nonneg))
    store_http/2, % +Md5:atom
                  % +HttpReplyHeaders:list(nvpair)
    store_message/2, % +Md5:atom
                     % +Message:compound
    store_status/2, % +Md5:atom
                    % +Exception:compound
    store_url/4 % +Md5:atom
                % +Url:url
                % +Coordinate:list(nonneg)
                % +TimeAdded:nonneg
  ]
).

/** <module> LOD Washing Machine: store triples

Temporarily store triples into the noRDF store.
Whenever a dirty item has been fully cleaned,
the stored triples are sent in a SPARQL Update request
(see module [noRdf_store].

@author Wouter Beek
@version 2014/04-2014/06
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(lists)).
:- use_module(library(semweb/rdf_db)).

:- use_module(pl(pl_log)).
:- use_module(xsd(xsd_dateTime_ext)).

:- use_module(lwm(lwm_generics)).
:- use_module(lwm(noRdf_store)).



%! store_archive_entries(+Url, +Md5:atom, +Coordinates:list(list(nonneg))) is det.

store_archive_entries(_, _, []):- !.
store_archive_entries(Url, Md5, Coords):-
  store_triple(Md5, rdf:type, ap:'Archive', ap),
  maplist(store_archive_entry(Url, Md5), Coords).

store_archive_entry(Url, FromMd5, Coord):-
  url_to_md5(Url, Coord, ToMd5),
  store_triple(FromMd5, ap:contains_entry, ToMd5, ap).


%! store_http(+Md5:atom, +HttpReplyHeaders:list(nvpair)) is det.

store_http(Md5, NVPairs):-
  maplist(store_nvpair(Md5), NVPairs).

store_nvpair(S, NVPair):-
  NVPair =.. [P,O],
  store_triple(S, P, O, ap).


%! store_location_properties(+Url1:url, +Location:dict, -Url2:url) is det.

store_location_properties(Url1, Location, Url2):-
  (
    Data1 = Location.get(data),
    exclude(filter, Data1, Data2),
    last(Data2, ArchiveEntry)
  ->
    Name = ArchiveEntry.get(name),
    atomic_list_concat([Url1,Name], '/', Url2),
    store_triple(Url1, ap:archive_contains, Url2, ap),
    ignore(store_triple(Url2, ap:format,
        literal(type(xsd:string,ArchiveEntry.get(format))), ap)),
    ignore(store_triple(Url2, ap:size,
        literal(type(xsd:integer,ArchiveEntry.get(size))), ap)),
    store_triple(Url2, rdf:type, ap:'LOD-URL', ap)
  ;
    Url2 = Url1
  ),
  ignore(store_triple(Url2, ap:http_content_type,
      literal(type(xsd:string,Location.get(content_type))), ap)),
  ignore(store_triple(Url2, ap:http_content_length,
      literal(type(xsd:integer,Location.get(content_length))), ap)),
  ignore(store_triple(Url2, ap:http_last_modified,
      literal(type(xsd:string,Location.get(last_modified))), ap)),
  store_triple(Url2, ap:url, literal(type(xsd:string,Location.get(url))), ap),

  (
    location_base(Location, Base),
    file_name_extension(_, Ext, Base),
    Ext \== ''
  ->
    store_triple(Url2, ap:file_extension, literal(type(xsd:string,Ext)), ap)
  ;
    true
  ).
filter(filter(_)).


%! store_message(+Md5:atom, +Message:compound) is det.

store_message(Md5, Message):-
  with_output_to(atom(String), write_canonical_blobs(Message)),
  store_triple(Md5, ap:message, literal(type(xsd:string,String)), ap).


%! store_number_of_triples(
%!   +Url:url,
%!   +Path:atom,
%!   +ReadTriples:nonneg,
%!   +WrittenTriples:nonneg
%! ) is det.

store_number_of_triples(Url, Path, TIn, TOut):-
  store_triple(Url, ap:triples, literal(type(xsd:integer,TOut)), ap),
  TDup is TIn - TOut,
  store_triple(Url, ap:duplicates, literal(type(xsd:integer,TDup)), ap),
  print_message(informational, rdf_ntriples_written(Path,TDup,TOut)).


%! store_status(+Md5:atom, +Exception:compound) is det.

store_status(_, false):- !.
store_status(_, true):- !.
store_status(Md5, exception(Error)):-
  with_output_to(atom(String), write_canonical_blobs(Error)),
  store_triple(Md5, ap:exception, literal(type(xsd:string,String)), ap).


%! store_stream_properties(+Url:url, +Stream:stream) is det.

store_stream_properties(Url, Stream):-
  stream_property(Stream, position(Position)),
  forall(
    stream_position_data(Field, Position, Value),
    (
      atomic_list_concat([stream,Field], '_', Name),
      rdf_global_id(ap:Name, Predicate),
      store_triple(Url, Predicate, literal(type(xsd:integer,Value)), ap)
    )
  ).


%! store_url(+Md5:atom, +Url:url, Coordinate:list(nonneg), +TimeAdded:nonneg) is det.
% Store the given URL plus coordinate as one of the items
% that is cleaned by the LOD Washing Machine.
%
% This includes the following properties:
%   - URL
%   - Coordinate
%   - LOD Washing Machine version
%   - Start datetime of processing by the LOD Washing Machine.
%   - Datetime at which the URL was added to the LOD Basket.

store_url(Md5, Url, Coord, TimeAdded1):-
  store_triple(Md5, rdf:type, ap:'LOD-URL', ap),

  % URL + coordinate.
  store_triple(Md5, ap:url, Url, ap),
  forall(
    nth0(I, Coord, N),
    (
      atomic_list_concat([coord,I], '_', PName),
      rdf_global_id(ap:PName, P),
      store_triple(Md5, P, literal(type(xsd:integer,N)), ap)
    )
  ),

  % Datetime at which the URL was added to the LOD Basket.
  posix_timestamp_to_xsd_dateTime(TimeAdded1, TimeAdded2),
  store_triple(Md5, ap:time_added, literal(type(xsd:dateTime,TimeAdded2)),
      ap),

  % LOD Washing Machine version.
  lwm_version(Version),
  store_triple(Md5, ap:lwm_version, literal(type(xsd:integer,Version)), ap),

  % Start date of processing by the LOD Washing Machine.
  get_dateTime(DateTime),
  store_triple(Md5, ap:start_date, literal(type(xsd:dateTime,DateTime)), ap).


%! store_void_triples(+DataDocument:url) is det.

store_void_triples(DataDocument):-
  aggregate_all(
    set(P),
    (
      rdf_current_predicate(P),
      rdf_global_id(void:_, P)
    ),
    Ps
  ),
  forall(
    (
      member(P, Ps),
      rdf(S, P, O)
    ),
    store_triple(S, P, O, DataDocument)
  ).

