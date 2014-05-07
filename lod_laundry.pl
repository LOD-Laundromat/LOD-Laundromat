:- module(lod_laundry, []).

/** <module> LOD laundry

@author Wouter Beek
@version 2014/05
*/

:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_json)).
:- use_module(library(pairs)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(semweb/rdfs)).

:- use_module(html(html_table)).
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
  absolute_file_name(data('Output/messages.log'), File, [access(read)]),
  setup_call_cleanup(
    ensure_loaded(File),
    forall(
      rdf_triple(S, P, O),
      rdf_assert(S, P, O, messages)
    ),
    unload_file(File)
  ).

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
  findall(
    Url=Dict,
    (
      lod_url(Url),
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
  rdfs_individual_of(Url, ap:'LOD-URL'),
  % @tbd
  Url \== 'http://aseg.cs.concordia.ca/secold/download/static/secold_v_001.tar'.


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
    findall(
      [
        Url-InternalLink-Url,
        ContentType,
        ContentLength,
        LastModified,
        TIn,
        TOut,
        Format
      ],
      (
        rdf_string(Url, ap:content_type, ContentType, messages),
        ignore(rdf_datatype(Url, ap:content_length,
            ContentLength, xsd:integer, messages)),
        ignore(rdf_string(Url, ap:last_modified, LastModified, messages)),
        ignore(rdf_datatype(Url, ap:triples_with_dups,
            TIn, xsd:integer, messages)),
        ignore(rdf_datatype(Url, ap:triples_without_dups,
            TOut, xsd:integer, messages)),
        ignore(rdf_string(Url, ap:format, Format, messages)),
        url_md5_translation(Url, Md5),
        file_name_extension(Md5, json, File),
        http_link_to_id(ll_web_home, path_postfix(File), InternalLink)
      ),
      Rows
    )
  },
  html_table(
    [header_column(true),header_row(true),indexed(true)],
    html('LOD files'),
    lod_laundry_cell,
    [[
      'Url',
      'Content Type',
      'Content Length',
      'Last Modified',
      'Triples with duplicates',
      'Triples without duplicates',
      'Format'
    ]|Rows]
  ).


%! lod_url_property(+Url:url, +Name:atom, -Value) is det.

lod_url_property(Url, base_iri, Base):-
  rdf(Url, ap:base_iri, Base).
lod_url_property(Url, content_length, ContentLength):-
  lod_property_content_length(Url, ContentLength).
lod_url_property(Url, content_type, Mime):-
  lod_property_content_type(Url, Mime).
lod_url_property(Url, duplicates, Duplicates):-
  rdf_datatype(Url, ap:triples_with_dups, TIn, xsd:integer, messages),
  rdf_datatype(Url, ap:triples_without_dups, TOut, xsd:integer, messages),
  Duplicates is TIn - TOut.
lod_url_property(Url, last_modified, LastModified):-
  rdf_string(Url, ap:last_modified, LastModified, messages).
lod_url_property(Url, md5, Md5):-
  url_md5_translation(Url, Md5).
lod_url_property(Url, messages, Messages):-
  findall(
    String,
    (
      rdf_string(Url, ap:message, Message1, messages),
      atom_to_term(Message1, message(Term,_,_), _),
      message_to_string(Term, String)
      %print_message_lines(atom(Message2), '', Lines)
    ),
    Messages
  ),
  Messages \== [].
lod_url_property(Url, status, Status3):-
  rdf_string(Url, ap:status, Status1, messages),
  atom_to_term(Status1, Status2, _),
  convert_status(Status2, Status3).
lod_url_property(Url, triples, Triples):-
  rdf_datatype(Url, ap:triples_without_dups, Triples, xsd:integer, messages).
lod_url_property(Url, url, Url).

lod_property_content_length(Url, ContentLength):-
  rdf_datatype(Url, ap:size, ContentLength, xsd:integer, messages), !.
lod_property_content_length(Url, ContentLength):-
  rdf_datatype(Url, ap:content_length, ContentLength, xsd:integer, messages).

lod_property_content_type(Url, Mime):-
  rdf_string(Url, ap:rdf_serialization_format, Format, messages), !,
  once(rdf_serialization(_, _, Format, [Mime|_], _)).
lod_property_content_type(Url, Mime):-
  rdf_string(Url, ap:content_type, Mime, messages).

convert_status(
  error(existence_error(url,_),context(_,status(Code,Description))),
  Status
):- !,
  format(atom(Status), 'HTTP error ~d: ~a', [Code,Description]).
convert_status(error(socket_error(Status),_), Status):- !.
convert_status(error(domain_error(sgml_option,anon_prefix(_)),_), no_rdfa):- !.
convert_status(Status, Status):-
  print_message(warning, verbatim(Status)).
% DEB
prolog:message(verbatim(Term)) -->
  {term_to_atom(Term, Atom)},
  [Atom].


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

