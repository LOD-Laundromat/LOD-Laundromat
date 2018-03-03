%! upload_dbpedia(+Version:atom) is det.

upload_dbpedia(Version) :-
  uri_comps(Uri, uri(http,'downloads.dbpedia.org',[Version],_,_)),
  atomic_list_concat([dbpedia,Version], -, Dataset),
  forall(html_index(Dataset, Uri), true),
  dataset_upload0(
    'dbpedia-2016-10',
    _{
      avatar: 'img/dbpedia.png',
      description: "DBpedia is a crowd-sourced community effort to extract structured information from Wikipedia and make this information available on the Web.  DBpedia allows you to ask sophisticated queries against Wikipedia, and to link the different data sets on the Web to Wikipedia data.  We hope that this work will make it easier for the huge amount of information in Wikipedia to be used in some new interesting ways.  Furthermore, it might inspire new mechanisms for navigating, linking, and improving the encyclopedia itself."
    }
  ).



%! html_index(+Dataset:atom, +Uri:atom) is det.

html_index(Dataset, Uri0) :-
  aggregate_all(set(Uri), html_index_uri(Uri0, Uri), Uris),
  concurrent_maplist(download_rdf0(Dataset), Uris).



%! html_index_uri(+Uri1:atom, -Uri2:atom) is nondet.

html_index_uri(Uri1, Uri2) :-
  uri_comps(Uri1, uri(Scheme,Authority,Segments1,_,_)),
  http_open2(Uri1, In, [accept(html)]),
  call_cleanup(
    (
      load_html(In, Dom, []),
      xpath(Dom, //a(@href), Uri1), %NONDET
      \+ memberchk(Uri1, ['../']),
      \+ uri_is_global(Uri1),
      atomic_list_concat(Segments, /, Uri1),
      append(Segments1, Segments, Segments2),
      uri_comps(Uri2, uri(Scheme,Authority,Segments2,_,_))
    ),
    close(In)
  ).
