:- module(
  ll_sources,
  [
    ll_source/1, % +Source
    ll_source/2  % +Source, -Uri
  ]
).

/** <module> LOD Laundromat: sources for seedpoints

@author Wouter Beek
@version 2017/09-2017/12
*/

:- use_module(library(debug)).
:- use_module(library(http/ckan_api)).
:- use_module(library(http/json)).
:- use_module(library(http/http_client2)).
:- use_module(library(string_ext)).
:- use_module(library(zlib)).





%! ll_source(+Source:oneof([datahub])) is det.

ll_source(Source) :-
  file_name_extension(Source, 'tsv.gz', File),
  setup_call_cleanup(
    gzopen(File, write, Out),
    forall(
      (
        ckan_resource_loop('https://datahub.io', Dict),
        _{format: Format, url: Uri} :< Dict
      ),
      format(Out, "~a\t~a\n", [Uri,Format])
    ),
    close(Out)
  ).

ckan_resource_loop(Uri, Dict) :-
  catch(ckan_resource(Uri, Dict), E, true),
  (var(E) -> true ; print_message(warning, E), ckan_resource_loop(Uri, Dict)).



%! ll_source(+Source:atom, -Uri:atom) is nondet.

ll_source(Local, Uri) :-
  absolute_file_name(Local, File, [access(read),file_errors(fail)]), !,
  setup_call_cleanup(
    gzopen(File, read, In),
    (
      read_line_to_string(In, Line),
      split_string(Line, "\t", "", [Uri,Format]),
      rdf_format0(Format)
    ),
    close(In)
  ).
ll_source(datahub, Uri) :-
  ckan_resource('https://datahub.io', Dict),
  _{format: Format, url: Uri} :< Dict,
  debug(ll(sources), "~a\t~a", [Format,Uri]),
  rdf_format0(Format).
ll_source(lov, Uri) :-
  setup_call_cleanup(
    http_open2('http://lov.okfn.org/dataset/lov/api/v2/vocabulary/list', In),
    json_read_dict(In, Dicts, [value_string_as(atom)]),
    close(In)
  ),
  member(Dict, Dicts),
  _{uri: Uri} :< Dict.
