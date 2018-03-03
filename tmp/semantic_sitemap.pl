:- use_module(library(sgml)).
:- use_module(library(xpath)).

  (   MediaType = media(text/xml,_)
  ->  forall(
        semantic_sitemap_uri(In1, Uri2),
        (
          http_open2(Uri2, In2, [failure(-1)]),
          call_cleanup(
            download_from_uri(Dataset, Subdir, Uri2, In2),
            close(In2)
          )
        )
      )
semantic_sitemap_uri(In, Uri) :-
  call_cleanup(
    (
      load_xml(In, Dom, []),
      xpath(Dom, //'sc:dataDumpLocation'(normalize_space), Uri)
    ),
    close(In)
  ).
