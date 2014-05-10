:- module(
  lod_laundry,
  [
    non_url_iris/5 % -NonUrlIris:ordset(iri)
                   % -NumberOfNonUrlIris:nonneg
                   % -UrlIris:orset(url)
                   % -NumberOfNonUrlIris:nonneg
                   % -PercentageOfNonUrlIris:between(0.0,1.0)
  ]
).

/** <module> LOD laundry

@author Wouter Beek
@version 2014/05
*/

:- use_module(library(aggregate)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_json)).
:- use_module(library(pairs)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(semweb/rdfs)).
:- use_module(library(url)).

:- use_module(html(html_table)).
:- use_module(math(math_ext)).
:- use_module(pl_web(html_pl_term)).
:- use_module(rdf_file(rdf_file_db)).
:- use_module(rdf_file(rdf_serial)).
:- use_module(rdf_term(rdf_datatype)).
:- use_module(rdf_term(rdf_string)).
:- use_module(server(app_ui)). % HTML style.
:- use_module(server(web_modules)). % Web module registration.
:- use_module(server(web_ui)).
:- use_module(xml(xml_namespace)).

:- xml_register_namespace(ap, 'http://www.wouterbeek.com/ap.owl#').
:- xml_register_namespace(ckan, 'http://www.wouterbeek.com/ckan#').

http:location(ll_web, root(ll), []).
:- http_handler(ll_web(.), ll_web_home, [prefix]).

user:web_module('LOD Laundry', ll_web_home).

%! rdf_triple(
%!   ?Subject:or([bnode,iri]),
%!   ?Predicate:iri,
%!   ?Object:or([bnode,iri,literal])
%! ) is nondet.
% Used to load triples from the messages logging file.

:- dynamic(rdf_triple/3).

:- dynamic(url_md5_translation/2).

:- multifile(prolog:message//1).

:- discontiguous(lod_url_property/3).

:- initialization(init_ll).

%! init_ll is det.
% Loads the triples from the messages log and the datahub scrape.

init_ll:-
  %init_datahub,
  init_messages,
  cache_url_md5_translations.

init_datahub:-
  rdf_graph(datahub), !.
init_datahub:-
  absolute_file_name(
    data('http/datahub.io/catalog'),
    File,
    [access(read),file_type(turtle)]
  ),
  rdf_load_any([graph(datahub)], File).

init_messages:-
  rdf_graph(messages), !.
init_messages:-
  absolute_file_name(
    data(messages),
    File,
    [access(read),file_errors(fail),file_type(turtle)]
  ),
  rdf_load_any([graph(messages)], File), !.
init_messages:-
  absolute_file_name(data('Output/messages.log'), FromFile, [access(read)]),
  setup_call_cleanup(
    ensure_loaded(FromFile),
    forall(
      rdf_triple(S, P, O),
      rdf_assert(S, P, O, messages)
    ),
    unload_file(FromFile)
  ),
  absolute_file_name(data(messages), ToFile, [access(write),file_type(turtle)]),
  rdf_save([format(turtle)], messages, ToFile).

cache_url_md5_translation(Url):-
  rdf_atom_md5(Url, 1, Md5),
  assert(url_md5_translation(Url, Md5)).

cache_url_md5_translations:-
  lod_url(Url),
  cache_url_md5_translation(Url),
  fail.
cache_url_md5_translations.


% Response to requesting a JSON description of all LOD URL.
ll_web_home(Request):-
  memberchk(path_info('all.json'), Request), !,
  aggregate_all(
    set(Url=Dict),
    (
      lod_url(Url),
      Url \== 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
      lod_url_dict(Url, Dict),
      print_message(informational, tick)
    ),
    NVPairs
  ),
  dict_create(Dict, all, NVPairs),
  reply_json(Dict, [cors(true)]).
% Response to requesting a JSON description of a single LOD URL.
ll_web_home(Request):-
  memberchk(path_info(Path), Request),
  file_name_extension(Md5, json, Path),
  url_md5_translation(Url, Md5), !,
  lod_url_dict(Url, Dict),
  reply_json(Dict).
% Generic response.
ll_web_home(_Request):-
  reply_html_page(
    app_style,
    title('LOD Laundry'),
    html([
      h1('LOD Laundry'),
      \lod_urls
    ])
  ).
% DEB
prolog:message(tick) -->
  {flag(flag_log, X, X + 1)},
  [X].


%! lod_url(?Url:url) is nondet.
% Enumerates the LOD URLs that have been washed.

lod_url(Url):-
  rdfs_individual_of(Url, ap:'LOD-URL').


%! lod_url_dict(+Url:url, -Dict:dict) is det.

lod_url_dict(Url, Dict):-
  findall(
    Name=Value,
    lod_url_property(Url, Name, Value),
    NVPairs
  ),
  url_md5_translation(Url, Md5),
  dict_create(Dict, Md5, NVPairs).


%! lod_urls// is det.
% Enumerates the washed LOD URLs.

lod_urls -->
  {
    aggregate_all(
      set([Url-InternalLink-Url]),
      (
        lod_url(Url),
        once(url_md5_translation(Url, Md5)),
        once(file_name_extension(Md5, json, File)),
        once(http_link_to_id(ll_web_home, path_postfix(File), InternalLink))
      ),
      Rows
    )
  },
  html_table(
    [header_column(true),header_row(true),indexed(true)],
    html('LOD files'),
    lod_laundry_cell,
    [['Url']|Rows]
  ).


%! lod_url_property(+Url:url, +Name:atom, -Value) is det.

%Archive.
lod_url_property(Url, archive_entry_size, Size):-
  once(rdf_datatype(Url, ap:size, Size, xsd:integer, messages)).
lod_url_property(Url, from_archive, Md5):-
  once((
    rdf(Archive, ap:archive_contains, Url),
    url_md5_translation(Archive, Md5)
  )).
lod_url_property(Url1, has_archive_entry, Md5s):-
  findall(
    Md5,
    (
      rdf(Url1, ap:archive_contains, Url2),
      once(url_md5_translation(Url2, Md5))
    ),
    Md5s
  ),
  Md5s \== [].

% Base IRI.
lod_url_property(Url, base_iri, Base):-
  rdf(Url, ap:base_iri, Base).

% RDF.
lod_url_property(Url, rdf, Dict):-
  findall(
    N-V,
    rdf_property(Url, N, V),
    Pairs
  ),
  Pairs \== [],
  dict_pairs(Dict, rdf, Pairs).

rdf_property(Url, duplicates, Duplicates):-
  once(rdf_datatype(Url, ap:duplicates, Duplicates, xsd:integer, messages)).
rdf_property(Url, triples, Triples):-
  once(rdf_datatype(Url, ap:triples, Triples, xsd:integer, messages)).
rdf_property(Url, serialization_format, Format):-
  once(rdf_string(Url, ap:serialization_format, Format, messages)).

% HTTP response.
lod_url_property(Url, http_repsonse, Dict):-
  findall(
    Name-Value,
    http_response_property(Url, Name, Value),
    Pairs
  ),
  Pairs \== [],
  dict_pairs(Dict, http_response, Pairs).

% File extension.
lod_url_property(Url, file_extension, FileExtension):-
  once(rdf_string(Url, ap:file_extension, FileExtension, messages)).

% MD5
lod_url_property(Url, md5, Md5):-
  once(url_md5_translation(Url, Md5)).

% Messages?
lod_url_property(Url, messages, Messages):-
  aggregate_all(
    set(Message2),
    (
      rdf_string(Url, ap:message, Message1, messages),
      abcd(Message1, Message2)
    ),
    Messages
  ),
  Messages \== [].

% Exceptions
lod_url_property(Url, exceptions, Dict):-
  findall(
    Kind-Exceptions,
    kind_exceptions(Url, Kind, Exceptions),
    Pairs1
  ),
  Pairs1 \== [],
  group_pairs_by_key(Pairs1, Pairs2),
  dict_pairs(Dict, exceptions, Pairs2).

kind_exceptions(Url, Kind, Exception):-
  once(rdf_string(Url, ap:exception, Atom, messages)),
  atom_to_term(Atom, Term, _),
  kind_exception(Term, Kind, Exception).

kind_exception(error(socket_error('Host not found'),_), tcp, 'Host not found').
kind_exception(error(socket_error('Try Again'),_), tcp, 'Try again?').
kind_exception(error(existence_error(url,_),context(_,status(Status,_))), http, Status).
%subkind_exception(

% Status?
lod_url_property(Url, status, Status):-
  once(rdf_string(Url, ap:status, Status, messages)).

% Stream?
lod_url_property(Url, stream, Dict):-
  findall(
    N-V,
    stream_property(Url, N, V),
    Pairs
  ),
  Pairs \== [],
  dict_pairs(Dict, stream, Pairs).

stream_property(Url, stream_byte_count, ByteCount):-
  once(rdf_datatype(Url, ap:stream_byte_count, ByteCount, xsd:integer, messages)).
stream_property(Url, stream_char_count, CharCount):-
  once(rdf_datatype(Url, ap:stream_char_count, CharCount, xsd:integer, messages)).
stream_property(Url, stream_line_count, LineCount):-
  once(rdf_datatype(Url, ap:stream_line_count, LineCount, xsd:integer, messages)).

% URL
lod_url_property(Url, url, Url).

http_response_property(Url, http_content_length, ContentLength):-
  once(rdf_datatype(Url, ap:http_content_length, ContentLength, xsd:integer, messages)).
http_response_property(Url, http_content_type, ContentType):-
  once(rdf_string(Url, ap:http_content_type, ContentType, messages)).
http_response_property(Url, last_modified, LastModified):-
  once(rdf_string(Url, ap:last_modified, LastModified, messages)).

abcd(M1, M2):-
  atom_to_term(M1, message(_,_,Lines), _),
  with_output_to(
    atom(M2),
    print_message_lines(current_output, '', Lines)
  ).


%! ckan_resources// is det.

ckan_resources -->
  {
    findall(
      Name-Row,
      (
        rdfs_individual_of(CkanResource, ckan:'Resource'),
        rdf_string(CkanResource, ckan:name, Name, datahub),
        rdf_string(CkanResource, ckan:url, Url, datahub),
        Row = [Url-Name]
      ),
      Pairs1
    ),
    keysort(Pairs1, Pairs2),
    pairs_values(Pairs2, Rows)
  },
  html_table(
    [header_column(true),header_row(true),indexed(true)],
    html('CKAN resources'),
    lod_laundry_cell,
    [['Name']|Rows]
  ).

lod_laundry_cell(Term) -->
  {
    nonvar(Term),
    Term = Name-InternalLink-ExternalLink
  }, !,
  html([
    \html_pl_term(InternalLink-Name),
    ' ',
    \external_link(ExternalLink)
  ]).
lod_laundry_cell(Term) -->
  html_pl_term(Term).



% STATISTICS %

%! non_url_iris(
%!   -NonUrlIris:ordset(iri),
%!   -NumberOfNonUrlIris:nonneg,
%!   -UrlIris:orset(url),
%!   -NumberOfNonUrlIris:nonneg,
%!   -PercentageOfNonUrlIris:between(0.0,1.0)
%! ) is det.

non_url_iris(NonUrlIris, NumberOfNonUrlIris, UrlIris, NumberOfUrlIris, Perc):-
  aggregate_all(
    set(NonUrlIri),
    (
      lod_url(NonUrlIri),
      url_iri(Url, NonUrlIri),
      Url \== NonUrlIri
    ),
    NonUrlIris
  ),
  length(NonUrlIris, NumberOfNonUrlIris),

  aggregate_all(
    set(UrlIri),
    (
      lod_url(UrlIri),
      url_iri(Url, UrlIri),
      Url == UrlIri
    ),
    UrlIris
  ),
  length(UrlIris, NumberOfUrlIris),

  percentage(NumberOfNonUrlIris, NumberOfUrlIris, Perc).

percentage(X, Y, Perc):-
  div_zero(X, Y, Perc).


number_of_urls(N):-
  aggregate_all(
    count,
    lod_url(_),
    N
  ).


%! http_status_codes(
%!   -Triples:triple(between(200,599),nonneg,between(0.0,1.0))
%! ) is det.

http_status_codes(Triples):-
  aggregate_all(
    set(Status-Url),
    (
      rdf_string(Url, ap:status, S, messages),
      atom_to_term(S, T, _),
      T = error(_,context(_,status(Status,_)))
    ),
    Pairs1
  ),
  group_pairs_by_key(Pairs1, Pairs2),
  pairs_keys_values(Pairs2, Keys, Values),
  maplist(length, Values, ValuesSize),
  pairs_keys_values(Pairs3, Keys, ValuesSize),
  number_of_urls(N),
  maplist(pair_to_triple(N), Pairs3, Triples).

pair_to_triple(N, X-Y, X-Y-Z):-
  percentage(Y, N, Z).

