:- module(
  lod_laundry,
  [
    lwm/2, % +Request:list
           % +HtmlStyle:atom
    lwm_basket/2 % +Request:list
                 % +HtmlStyle:atom
  ]
).

/** <module> LOD laundry

@author Wouter Beek
@version 2014/05-2014/06
*/

:- use_module(library(aggregate)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(semweb/rdf_db)).

:- use_module(plHtml(html)).
:- use_module(plHtml(html_pl_term)).
:- use_module(plHtml(html_table)).

:- use_module(lwm(lod_urls)).

:- dynamic(http:location/3).
:- multifile(http:location/3).
   http:location(ll_web, root(ll), []).

:- dynamic(url_md5_translation/2).

:- initialization(cache_url_md5_translations).



% Response to requesting a JSON description of all LOD URL.
lwm(_Request, HtmlStyle):-
  reply_html_page(HtmlStyle, title('LOD Laundry'), html(\lod_urls)).

lwm_basket(Request, HtmlStyle):-
  gtrace,
  writeln(Request),
  reply_html_page(HtmlStyle, title('LOD Basket'), p(test)).


lod_urls -->
  {
    aggregate_all(
      set([Url-InternalLink-Url]),
      (
        lod_url(Url), %@tbd
        once(url_md5_translation(Url, Md5)),
        once(file_name_extension(Md5, json, File)),
        once(http_link_to_id(lwm, path_postfix(File), InternalLink))
      ),
      Rows
    )
  },
  html_table(
    [header_column(true),header_row(true),indexed(true)],
    html('Cleaned LOD files'),
    lod_laundry_cell,
    [['Url']|Rows]
  ).

lod_laundry_cell(Term) -->
  {
    nonvar(Term),
    Term = Name-InternalLink-ExternalLink
  }, !,
  html([
    \html_link(InternalLink-Name),
    ' ',
    \html_external_link(ExternalLink)
  ]).
lod_laundry_cell(Term) -->
  html_pl_term(plDev(.), Term).



% INITIALIZATION

cache_url_md5_translation(Url):-
  rdf_atom_md5(Url, 1, Md5),
  assert(url_md5_translation(Url, Md5)).

cache_url_md5_translations:-
  lod_url(Url),
  cache_url_md5_translation(Url),
  fail.
cache_url_md5_translations.

