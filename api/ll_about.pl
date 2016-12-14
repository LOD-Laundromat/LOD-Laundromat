:- module(ll_about, []).

/** <module> LOD Laundromat: About page

@author Wouter Beek
@version 2016/02, 2016/08, 2016/12
*/

:- use_module(library(html/html_bibtex)).
:- use_module(library(html/html_ext)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_ext)).
:- use_module(library(http/rest)).

:- use_module(ll(style/ll_style)).

:- http_handler(ll(about), ll_about_handler, [prefix]).





ll_about_handler(Req) :-
  rest_method(Req, [get], ll_about_handler).


ll_about_handler(_, Method, MTs) :-
  http_is_get(Method),
  rest_media_type(MTs, ll_about_get).


ll_about_get(text/html) :-
  reply_html_page(
    ll([]),
    \cp_title(["About"]),
    article([
      \about_header,
      \about_code,
      \about_publications,
      \about_faq
    ])
  ).

about_header -->
  html(
    header(
      \ll_image_content(
        laundry_line,
        [
          h1("About"),
          p("What do you publish?  How do you do it?  And what can I do with it?")
        ]
      )
    )
  ).

about_code -->
  html(
    section([
      h2("Code"),
      p([
        "The LOD Laundromat is a crawling and cleaning infrastructure for Linked Open data.  It is Open Source and is available on ",
        \external_link('https://github.com/LOD-Laundromat/LOD-Laundromat.git', "GitHub"),
        ".  The ",
        \external_link('https://github.com/LOD-Laundromat', "GitHub organization"),
        " contains additional repositories for accessing LOD Laundromat data from within different programming languages and frameworks."
      ])
    ])
  ).

about_publications -->
  html(
    section([
      h2("Publications"),
      ul(li(\iswc2014))
    ])
  ).

iswc2014 -->
  html([
    span(class=authors,
      \bibtex_authors([
        "Wouter"-"Beek",
        "Laurens"-"Rietveld",
        "Hamid"-"Bazoobandi",
        "Jan"-"Wielemaker",
        "Stefan"-"Schlobach"
      ])
    ),
    ": ",
    span(class=title, "LOD Laundromat: A Uniform Way of Publishing Other People's Dirty Data"),
    ".  In: ",
    span(class=journal, "Proceedings of the International Semantic Web Conference"),
    " (",
    span(class=year, "2014"),
    ") ",
    \internal_link(
      pdf('LOD_Laundromat_-_A_Uniform_Way_of_Publishing_Other_Peoples_Dirty_Data.pdf'),
      \icon(download)
    ),
    div(class=well, [
      p("It is widely accepted that proper data publishing is difficult.  The majority of Linked Open Data (LOD) does not meet even a core set of data publishing guidelines.  Moreover, datasets that are clean at creation, can get stains over time.  As a result, the LOD cloud now contains a high level of dirty data that is difficult for humans to clean and for machines to process."),
      p([
        "Existing solutions for cleaning data (standards, guidelines, tools) are targeted towards human data creators, who can (and do) choose not to use them.  This paper presents the LOD Laundromat, which removes stains from data without any human intervention.  This fully automated approach is able to make very large amounts of LOD more easily available for further processing ",
        emph("right now"),
        "."
      ]),
      p("The LOD Laundromat is not a new dataset, but rather a uniform point of entry to a collection of cleaned siblings of existing datasets.  It provides researchers and application developers a wealth of data that is guaranteed to conform to a specified set of best practices, thereby greatly improving the chance of data actually being (re)used.")
    ])
  ]).

about_faq -->
  html(
    section([
      h2("FAQ"),
      dl([
        dt("In which formats do you republish the data?"),
        dd([
          "Firstly, we publish the crawled and cleaned data files in the ",
          \ll_link(wardrobe),
          " as gzipped and sorted N-Quads.  Secondly, we publish every dataset in the ",
          \ll_link(hdt),
          " format which allows the data to be queried by using HDT software.  Thirdly, for every data we provide a ",
          \ll_link(ldf),
          " API."
        ]),
        dt("In which formats do you publish the crawling and cleaning metadata?"),
        dd([
          "The crawling and cleaning metadata is published by using the ",
          \ll_link(prov),
          " and ",
          \ll_link(void),
          " vocabularies.  The metadata is accessible through our ",
          \ll_link(sparql),
          " as well as through file download."
        ]),
        dt("Why do you publish all Linked Open Data in one spot?  Should Linked Data not be distributed?"),
        dd("Using and finding Linked Data takes time and effort.  We are not yet at a point where all available Linked Data is clean, standard and easy to use.  Many datasets contain syntax errors, duplicates, or are difficult to find.  We offer one single download location for Linked Data, and publish the Linked Data in a consistent simple (sorted) N-Triple format, making it easy to use and compare datasets."),
        dt("Why gzipped N-Quads?"),
        dd("Canonical N-Quads is a relatively simple RDF serialization format that allows us to store locally interpretable statements.  In Turtle, XML/RDF and JSON-LD we need the header of the file or the JSON-LD context in order to interpret a statements.  This allows statements to be read from a stream without requiring an in-memory translation object.  Also, N-Quads allows a one-to-one mapping between RDF statements and file lines.  This makes it easy to split and concatenate files (on newlines) without introducing errors.  Gzip results in smaller downloads while still being easy to compress by most clients."),
        dt("How do I use LOD Laundromat data?"),
        dd([
          "You can query the LOD Laundromat Web Services.  But you can also use ",
          \ll_link(frank),
          ", a command-line tool that allows Triple Pattern Queries to be performed over the entire LOD Laundromat data collection.  If you are programming in ",
          \ll_link(java),
          ", ",
          \ll_link(nodejs),
          ", ",
          \ll_link(javascript),
          ", ",
          \ll_link(python),
          " or ",
          \ll_link(swipl),
          " then there are dedicated programming libraries that you can use."
        ])
      ])
    ])
  ).
