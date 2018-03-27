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
