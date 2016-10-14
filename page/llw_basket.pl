:- module(llw_basket, []).

/** <module> LOD Laundromat Web: Basket page

@author Wouter Beek
@version 2016/02, 2016/08-2016/09
*/

:- use_module(library(html/html_ext)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_ext)).

:- use_module(llw(html/llw_html)).


:- http_handler(llw(basket), basket_handler, [prefix]).





basket_handler(_) :-
  reply_html_page(
    llw([]),
    % @tbd <meta name="description" content="Contains the URLs of dirty datasets that are waiting to be cleaned by the LOD Washing Machine.">
    title("LOD Laundromat - Laundry Basket"),
    article([
      \basket_header,
      %\basket_dropbox,
      \basket_form
    ])
  ).


basket_header -->
  html(
    header(
      \llw_image_content(
        laundry_basket,
        [
          h1("Laundry Basket"),
          p("The LOD Laundry Basket contains the URLs of dirty datasets that are waiting to be cleaned by the LOD Laundromat.  You can also add your own URLs to the basket (see below).")
        ]
      )
    )
  ).


/*
basket_dropbox -->
  html(
    section([
      h2("Add your laundry through DropBox"),
      \note,
      div(id=dropboxBtn, []),
      div(class=submitStatusDropbox, []),
      \dropbox_plugin('1vvu25oej4wzu7w')
    ])
  ).
*/


note -->
  html(
    div(
      class=[label,'label-info'],
      "Note that this file will become publicly available!"
    )
  ).


basket_form -->
  html(
    section([
      h3("Submit Link"),
      p("Submit a URL that points to an online available Linked Data source.  The URL can point to plain data files or archives of data files.  The data files can be XML/RDF, Turtle, N-Triples, N-Quads, HTML/RDFa or Trig."),
      % @tbd Pressing ENTER should also send the text.
      \form(
        link_to_id(basket_handler),
        [
          \input_text(
            seed,
            [onkeydown='if (event.keyCode == 13) { storeUrl(); return false; }']
          ),
          % @tbd Add `storeUrl()’.
          \submit_button,
          div(class='submit-status', [])
        ]
      )
    ])
  ).


/*
basket_contents -->
  html(
    section([
      h3("Basket contents"),
      <div id="tableWrapper">
      <table cellpadding="0" cellspacing="0" border="0" class="display" id="laundryBasketTable">
      <thead>
      <tr>
      <th id="urlHeader">URL</th><th id="dateHeader">Date added</th><th id="statusHeader">Status</th><th> </th>
      </tr>
      </thead>
      </table>
      </div>
    ])
  ).
*/
