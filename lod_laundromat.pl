:- module(
  lod_laundromat,
  [
    add_thread/0,
    clean/1       % +Seed
  ]
).

/* <module> LOD Laundromat

@author Wouter Beek
@version 2016/01-2016/02
*/

:- use_module(library(bs/bs)).
:- use_module(library(bs/bs_hamburger)).
:- use_module(library(debug)).
:- use_module(library(filesex)).
:- use_module(library(hash_ext)).
:- use_module(library(html/html_ext)).
:- use_module(library(http/html_head)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/js_write)).
:- use_module(library(lodapi/lodapi_generics)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/thread_counter)).
:- use_module(library(os/thread_ext)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_debug)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(rdf11/rdf11)). % Operators.

:- use_module(cpack('LOD-Laundromat'/lod_basket)).

:- http_handler(/, lod_laundromat, [priority(100)]).
:- http_handler(root(data), lod_laundromat_data, [prefix]).
:- http_handler(root(meta), lod_laundromat_meta, [prefix]).

:- html_resource(
     css(lod_laundromat),
     [order(true),requires([css('bootstrap-theme'),css('lod_laundromat.css')]),virtual(true)]
   ).
:- html_resource(
     js(lod_laundromat),
     [order(true),requires([js(bootstrap),js('lod_laundromat.js')]),virtual(true)]
   ).

:- multifile
    user:body//2,
    user:head//2.

user:body(lod_laundromat, Content) -->
  html([
    nav(class=[navbar,'navbar-default'],%'navbar-fixed-top'],
      div(class='container-fluid', [
        div(class='navbar-header', \bs_hamburger('#ll-collapse')),
        div([class=[collapse,'navbar-collapse'],id=ll_collapse,role=navigation], [
          ul(class=[nav,'navbar-nav'], []),
          span(style='float: right;', \triple_counter)
        ])
      ])
    )
  | Content
  ]).

user:head(lod_laundromat, Content) -->
  {http_absolute_location(img('lod_laundromat.svg'), Path)},
  html(
    head([
      meta(charset='utf-8', []),
      \html_meta_ie_latest,
      \html_meta_viewport,
      \html_meta_description("LOD Laundromat: Cleaning Other People's Dirty Data"),
      \html_meta_authors(['Laurens Rietveld','Wouter Beek']),
      \html_favicon(Path),
      \html_requires(js(lod_laundromat)),
      \html_requires(css(lod_laundromat)),
      \html_google_analytics
      %<link rel="stylesheet" href="/bower_components/github-fork-ribbon-css/gh-fork-ribbon.css">
      %<!-- HTML5 shim and Respond.js IE8 support of HTML5 elements and media queries -->
      %<!--[if lt IE 9]>
      %<script src="https://oss.maxcdn.com/libs/html5shiv/3.7.0/html5shiv.js"></script>
      %<script src="https://oss.maxcdn.com/libs/respond.js/1.4.2/respond.min.js"></script>
      %<link rel="stylesheet" href="/bower_components/github-fork-ribbon-css/gh-fork-ribbon.ie.css">
      %<![endif]-->
    | Content
    ])
  ).

html_google_analytics -->
  js_script({|javascript(_)||
(function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
(i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
})(window,document,'script','//www.google-analytics.com/analytics.js','ga');
ga('create', analyticsId, 'auto');
ga('send', 'pageview');
  |}).

lod_laundromat(_) :-
  reply_html_page(lod_laundromat,
    title("LOD Laundromat"),
    [
      \lod_laundromat,
      \lod_laundry_basket,
      \lod_wardrobe,
      \lod_analytics,
      \metadata_endpoint,
      \about,
      \js_script({|javascript(_)||
$(".devLink").click(function() { window.location = "/about"; });
$(".wardrobeLink").click(function() { window.location = "/wardrobe"; });
$(".laundryBasketLink").click(function() { window.location = "/basket"; });
$(".analysisLink").click(function() { window.location = "/visualizations"; });
$(".labelsLink").click(function() { window.location = "/sparql"; });
      |})
    ]
  ).

triple_counter -->
  html([span(class=counter, []),"triples and counting!"]).

see_more(Class) -->
  html(div(
    button([class=[btn,'btn-info','btn-lg',Class],type=button],
      span(class=[glyphicon,'glyphicon-chevron-right'], [])
    )
  )).

badge_image_content(Img, Content) -->
  html(
    div(class=container,
      div(class=row, [
        div(class='col-md-1', []),
        div(class='col-md-4', \ll_image(Img)),
        div([class='col-md-6',style='vertical-align: middle;'], Content),
        div(class='col-md-1', [])
      ])
    )
  ).

badge_content_image(Content, Img) -->
  html(
    div(class=container,
      div(class=row, [
        div(class='col-md-1', []),
        div([class='col-md-6',style='vertical-align: middle;'], Content),
        div(class='col-md-4', \ll_image(Img)),
        div(class='col-md-1', [])
      ])
    )
  ).

ll_image(Name) -->
  {
    file_name_extension(Name, png, Local),
    http_absolute_location(img(Local), Path),
    ll_image_alt(Name, Alt)
  },
  html(img([alt=Alt,src=Path], [])).

lod_laundromat -->
  badge_image_content(lod_laundromat, [
    h1("LOD Laundromat"),
    p("The LOD Laundromat provides access to all Linked Open Data (LOD) in the world.  It does so by crawling the LOD Cloud, and converting all its contents in a standards-compliant way (gzipped N-Quads), removing all data stains such as syntax errors, duplicate occurrences, and file-local blank nodes.")
  ]).

ll_image_alt(lod_laundromat, "LOD Laundromat logo.").
ll_image_alt(lod_laundry_basket, "LOD Laundry Basket logo.").
ll_image_alt(lod_wardrobe, "LOD Wardrobe logo.").
ll_image_alt(lod_analytics, "Various diagrams that show analytics.").
ll_image_alt(labels, "Various icons familiar from descriptions of how to wash clothes.").
ll_image_alt(laundry_line, "A laundry line.").

lod_laundry_basket -->
  badge_content_image([
    h1("LOD Laundry Basket"),
    p("The LOD Laundry Basket contains the URLs of dirty datasets that are waiting to be cleaned by the LOD Washing Machine.  You can add URLs to the LOD Laundry Basket for cleaning."),
    \see_more(laundryBasketLink)
  ], lod_laundry_basket
  ).

lod_wardrobe -->
  badge_image_content(lod_wardrobe, [
    h1("Wardrobe"),
    p("The LOD Wardrobe is where the cleaned data is stored.  You can download both the cleaned and the original version.  Each dataset contains a metadata describing the crawling and cleaning process.  This enumerates all the stains that were detected, including wrong HTTP headers, syntax errors, etc."),
    \see_more(wardrobeLink)
  ]).
    
lod_analytics -->
  badge_content_image([
    h1("Analytics"),
    p([
      "How much data did we clean?  How many ",
      del("socks"),
      " triples did we lose in the LOD Washing Machine?  Which RDF serialization formats did we come across?  This is where we show such LOD Analytics."
    ]),
    \see_more(analysisLink)
  ], lod_analytics).
    
metadata_endpoint -->
  badge_image_content(labels, [
    h1("SPARQL endpoint"),
    p("For an in-depth overview of the data cleaned by the LOD Laundromat, we provide a live SPARQL endpoint in which all metadata can be queried."),
    \see_more(labelsLink)
  ]).

about -->
  badge_content_image([
    h1("About"),
    p("This is great!  I think...  But what do you exactly do?  How can I use it?"),
    \see_more(devLink)
  ], laundry_line).



%! add_thread is det.
% Add a LOD Laundromat thread.

add_thread :-
  detached_thread(thread).

thread :-
  clean(Hash, Iri),
  debug(lod_laundromat(thread), "Cleaned ~a (~a)", [Hash,Iri]),
  thread.
thread :-
  M = 100,
  sleep(M),
  thread_name(Name),
  increment_thread_counter(lod_laundromat(idle), N),
  S is M * N,
  debug(lod_laundromat(idle), "Thread ~w is ~D sec. idle", [Name,S]),
  thread.


%! clean(+Seed) is det.
% Cleans either a seed from the seedlist API or an IRI.

clean(Seed) :-
  (   Seed = seed(Hash,Iri,_,_,_)
  ->  true
  ;   iri_normalized(Seed, Iri),
      md5(Iri, Hash)
  ),
  clean(Hash, Iri).


%! clean(+Hash, +Iri) is det.

clean(Hash, Iri) :-
  with_mutex(seedlist, (
    once(current_seed(seed(Hash, Iri, _, 0.0, 0.0))),
    begin_seed(Hash)
  )),
  clean0(Hash, Iri),
  with_mutex(seedlist, end_seed(Hash)).

clean0(Hash, Iri) :-
  document_name(Doc, Hash),
  % @tbd This should be a setting.
  %Dir1 = '/scratch/lodlab/crawls/13/',
  Dir1 = '/home/wbeek/Data/',
  document_path(Doc, Dir2),
  directory_file_path(Dir1, Dir2, Dir),
  make_directory_path(Dir),
  Opts = [access(write),relative_to(Dir)],
  absolute_file_name('dirty.gz', DirtyTo, Opts),
  absolute_file_name('data.nq.gz', DataTo, Opts),
  absolute_file_name('meta.nq.gz', MetaTo, Opts),
  rdf_download_to_file(Iri, DirtyTo, [compress(gzip)]),
  setup_call_cleanup(
    open_any2(MetaTo, append, Write, Close_0, [compress(gzip)]),
    with_output_to(Write,
      rdf_store_messages(Doc, (
        rdf_clean(Iri, DataTo, [compress(gzip),metadata(M)]),
        rdf_store_metadata(Doc, M)
      ))
    ),
    close_any2(Close_0)
  ).
