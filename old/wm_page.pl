wm_table -->
  {
    findall(
      Global-[Alias,Global,Iri],
      (
        current_wm(Alias),
        thread_statistics(Alias, global, Global),
        get_thread_seed(Alias, Iri)
      ),
      Pairs
    ),
    desc_pairs_values(Pairs, Rows)
  },
  html([
    h1("Washing Machines"),
    \table(
      tr([th("Washing Machine"),th("Global"),th("Seed")]),
      \html_maplist(tr, Rows)
    )
  ]).
