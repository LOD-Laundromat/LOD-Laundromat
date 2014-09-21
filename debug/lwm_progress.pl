:- module(
  lwm_progress,
  [
    lwm_progress/2 % +Request:list(nvpair)
                   % +HtmlStyle
  ]
).

/** <module> LOD Washing Machine: Progress

A Web-based debug tool for tracking the progress of the LOD Washing Machine.

@author Wouter Beek
@version 2014/09
*/

:- use_module(library(http/html_write)).
:- use_module(library(semweb/rdf_db)).

:- use_module(plHtml(html_pl_term)).

:- use_module(plRdf_term(rdf_literal)).

:- use_module(plTabular(rdf_html_table)).

:- use_module(lwm(lwm_settings)).
:- use_module(lwm(lwm_sparql_query)).



%! lwm_deb_progress(+Request:list(nvpair), +HtmlStyle)// is det.

lwm_deb_progress(_, HtmlStyle):-
  lle_version_graph(Graph),
  reply_html_page(
    HtmlStyle,
    title('LOD Laundromat'),
    html([
      \pending_table(Graph),
      \unpacking_table(Graph),
      \unpacked_table(Graph),
      \cleaning_table(Graph),
      \cleaned_table(Graph)
    ])
  ).


%! pending_table(+Graph:atom)// is det.

pending_table(Graph) -->
  {
    with_mutex(lod_washing_machine, (
      lwm_sparql_select(
        [llo],
        [md5],
        [
          rdf(var(md5),llo:added,var(added)),
          not([rdf(var(md5),llo:startUnpack,var(start))]),
        ],
        Rows,
        [limit(5),sparql_errors(fail)]
      )
    ))
  },
  progress_table(' pending data documents.', Rows).


%! unpacking_table(+Graph:atom)// is det.

unpacking_table(Graph) -->
  {
    with_mutex(lod_washing_machine, (
      lwm_sparql_select(
        [llo],
        [md5],
        [
          rdf(var(md5),llo:startUnpack,var(start)),
          not([rdf(var(md5),llo:endUnpack,var(clean))])
        ],
        Rows,
        [limit(5),sparql_errors(fail)]
      )
    ))
  },
  progress_table(' data documents are being unpacked.', Rows).


%! unpacked_table(+Graph:atom)// is det.

unpacked_table(Graph) -->
  {
    with_mutex(lod_washing_machine, (
      lwm_sparql_select(
        [llo],
        [md5],
        [
          rdf(var(md5),llo:endUnpack,var(start)),
          not([rdf(var(md5),llo:startClean,var(clean))]),
        ],
        Rows,
        [limit(5),sparql_errors(fail)]
      )
    ))
  },
  progress_table(' unpacked data documents.', Rows).


%! cleaning_table(+Graph:atom)// is det.

cleaning_table(Graph) -->
  {
    with_mutex(lod_washing_machine, (
      lwm_sparql_select(
        [llo],
        [md5],
        [
          rdf(var(md5),llo:startClean,var(start_clean)),
          not([rdf(var(md5),llo:endClean,var(end_clean))]),
        ],
        Rows,
        [limit(5),sparql_errors(fail)]
      )
    ))
  },
  progress_table(' data documents are being cleaned.', Rows).


%! cleaned_table(+Graph:atom)// is det.

cleaned_table(Graph) -->
  {
    with_mutex(lod_washing_machine, (
      lwm_sparql_select(
        [llo],
        [md5],
        [rdf(var(md5),llo:endClean,var(end_clean))],
        Rows,
        [limit(5),sparql_errors(fail)]
      )
    ))
  },
  progress_table(' cleaned data documents.', Rows).



% Helpers

%! progress_table(
%!   +CaptionPostfix:atom,
%!   +ColumnHeader:atom,
%!   +Rows:list(list(iri))
%! )// is det.

progress_table(CaptionPostfix, ColumnHeader, Rows) -->
  {length(Rows, Length)},
  rdf_html_table(
    ['Data document'],
    Rows,
    [
      header_column(true),
      header_row(true),
      indexed(true),
      maximum_number_of_rows(5)
    ]
  ).

