:- module(ll_guess, [ll_guess/0]).

/** <module> LOD Laundromat: Guess format

RDF/XML can be distinguished from Turtle-family, because it is not
possible to define valid RDF/XML without XML namespaces.  At the same
time, it is not possible to define a valid absolute Turtle-family IRI
(`<…>'-notation) with a valid `xmlns' declaration[?].

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(dcg/basics)).
:- use_module(library(lists)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_seedlist)).
:- use_module(library(ordsets)).
:- use_module(library(pio)).
:- use_module(library(semweb/rdf_db)).

:- meta_predicate
    lexical_form_codes(//, ?, ?).



ll_guess :-
  with_mutex(ll_guess, (
    seed(Seed),
    Hash{status: unarchived} :< Seed,
    seed_merge(Hash{status: guessing})
  )),
  hash_file(Hash, dirty, File),
  ll_guess(File, Format),
  with_mutex(ll_guess, seed_merge(Hash{format: Format, status: guessed})).

% JSON-LD
ll_guess(File, jsonld) :-
  phrase_from_file(jsonld, File), !.
% N-Quads, N-Triples, TriG, Turtle
ll_guess(File, Format) :-
  phrase_from_file(rdf_guess1([nquads,ntriples,trig,turtle], Formats), File),
  Formats = [Format|T],
  (T == [] -> true ; print_message(warning, uncertainty(File,Formats))).
ll_guess(File, Format) :-
  setup_call_cleanup(
    open(File, read, In),
    guess_sgml_type(In, Format),
    close(In)
  ).

jsonld -->
  json_optional_arrays,
  "{",
  blanks,
  json_string,
  blanks,
  ":".

json_optional_arrays -->
  "[", !,
  blanks,
  json_optional_arrays.
json_optional_arrays --> "".

% JSON strings start with a double quote.
json_string -->
  "\"",
  json_string_rest.

% JSON strings may contain escaped double quotes.
json_string_rest -->
  "\\\"", !,
  json_string_rest.
% An unescaped double quote ends a JSON string.
json_string_rest -->
  "\"", !.
% Skip other JSON string content.
json_string_rest -->
  [_],
  json_string_rest.

rdf_guess1([H], [H]) --> !.
rdf_guess1(L1, L3) -->
  rdf_guess2(L1, L2), !,
  rdf_guess1(L2, L3).

% skip blanks
rdf_guess2(L1, L2) -->
  blank, !,
  blanks,
  rdf_guess2(L1, L2).
% N-Quads, N-Triples, TriG, Turtle comment
rdf_guess2(L1, L2) -->
  comment, !,
  rdf_guess2(L1, L2).
% Turtle, TriG base or prefix declaration
rdf_guess2(L1, L2) -->
  ("@base" ; "base" ; "@prefix" ; "prefix"), !,
  {ord_subtract(L1, [nquads,ntriples], L2)}.
% TriG default graph
rdf_guess2(L1, L2) -->
  "{", !,
  {ord_subtract(L1, [nquads,ntriples,turtle], L2)}.
% N-Quads, N-Triples TriG, Turtle triple or quadruple
rdf_guess2(L1, L6) -->
  subject(L1, L2),
  turtle_blanks,
  predicate(L2, L3), !,
  turtle_blanks,
  object(L3, L4),
  turtle_blanks,
  (   % end of a triple
      "."
  ->  {L6 = L4}
  ;   % TriG, Turtle object list notation
      ";"
  ->  {ord_subtract(L4, [nquads,ntriples], L6)}
  ;   % TriG, Turtle predicate-object pairs list notation
      ","
  ->  {ord_subtract(L4, [nquads,ntriples], L6)}
  ;   % N-Quads end of a quadruple
      graph(L4, L5),
      turtle_blanks,
      "."
  ->  {ord_subtract(L5, [ntriples,trig,turtle], L6)}
  ).
% TriG, Turtle anonymous blank node
rdf_guess2(L1, L2) -->
  "[", !,
  {ord_subtract(L1, [nquads,ntriples], L2)}.
% TriG, Turtle collection
rdf_guess2(L1, L2) -->
  "(", !,
  {ord_subtract(L1, [nquads,ntriples], L2)}.
% TriG named graph
rdf_guess2(L1, L3) -->
  graph(L1, L2),
  turtle_blanks,
  "{",
  {ord_subtract(L2, [nquads,ntriples,turtle], L3)}.

bnode -->
  "_:",
  nonblanks.

comment -->
  "#",
  string(_),
  (eol ; eos).

eol --> "\n", !.
eol --> "\r\n".

graph(L1, L2) -->
  iriref(L1, L2).

% N-Quads, N-Triples, TriG, Turtle full IRI
iriref(L, L) -->
  "<", !,
  string(_),
  ">".
% TriG, Turtle prefixed IRI
iriref(L1, L2) -->
  iriref_prefix,
  ":",
  nonblanks,
  {ord_subtract(L1, [nquads,ntriples], L2)}.

iriref_prefix -->
  ":", !,
  {fail}.
iriref_prefix -->
  blank, !,
  {fail}.
iriref_prefix -->
  nonblank(Code), !,
  {char_code(Char, Code), format(user_output, "~a\n", [Char])},
  iriref_prefix.
iriref_prefix --> "".

% TriG, Turtle lexical form with triple single quotes
lexical_form(L1, L2) -->
  "'''", !,
  lexical_form_codes([0'',0'',0'']),
  {ord_subtract(L1, [nquads,ntriples], L2)}.
% TriG, Turtle lexical form with single single quotes
lexical_form(L1, L2) -->
  "'", !,
  lexical_form_codes([0'']),
  {ord_subtract(L1, [nquads,ntriples], L2)}.
% TriG, Turtle lexical form with triple double quotes
lexical_form(L1, L2) -->
  "\"\"\"", !,
  lexical_form_codes([0'",0'",0'"]), %"
  {ord_subtract(L1, [nquads,ntriples], L2)}.
% N-Quads, N-Triples, TriG, Turtle lexical form with single double
% quotes
lexical_form(L, L) -->
  "\"",
  lexical_form_codes([0'"]). %"

% Escaped single quote.
lexical_form_codes(End) -->
  "\\\'", !,
  lexical_form_codes(End).
% Escaped double quote.
lexical_form_codes(End) -->
  "\\\"", !,
  lexical_form_codes(End).
% End of string.
lexical_form_codes(End) -->
  End, !.
% Content.
lexical_form_codes(End) -->
  [_], !,
  lexical_form_codes(End).
% End of stream.
lexical_form_codes(_) --> "".

% TriG, Turtle abbreviated form for XSD boolean, decimal, double, and
% integer literals
literal(L1, L2) -->
  ("false" ; "true" ; "+" ; "-" ; "." ; digit(_)), !,
  {ord_subtract(L1, [nquads,ntriples], L2)}.
literal(L1, L3) -->
  lexical_form(L1, L2),
  turtle_blanks,
  (   "^^"
  ->  turtle_blanks,
      iriref(L2, L3)
  ;   "@"
  ->  ltag,
      {L3 = L2}
  ;   % TriG, Turtle abbreviated form for XSD string literals.
      {ord_subtract(L2, [nquads,ntriples], L3)}
  ).

ltag -->
  nonblanks.

nonblank -->
  nonblank(_).

nonblanks -->
  nonblanks(_).

object(L1, L2) -->
  iriref(L1, L2), !.
object(L, L) -->
  bnode, !.
object(L1, L2) -->
  literal(L1, L2).

predicate(L1, L2) -->
  iriref(L1, L2), !.
% TriG, Turtle abbreviation for `rdf:type'
predicate(L1, L2) -->
  "a",
  {ord_subtract(L1, [nquads,ntriples], L2)}.

subject(L1, L2) -->
  iriref(L1, L2), !.
subject(L, L) -->
  bnode.

% Turtle only allows horizontal tab and space, but we skip other blank
% characters as well, since they may appear in non-conforming
% documents without telling us anything about which Turtle subtype we
% are parsing.
turtle_blanks --> blank, !, turtle_blanks.
turtle_blanks --> comment, !, turtle_blanks.
turtle_blanks --> "".



%! guess_sgml_type(+In:stream, -Format:oneof([rdfa,rdfxml])) is semidet.
%
% Try to see whether the document is some form of HTML or XML, and in
% particular whether it is RDF/XML.  Notice that the latter cannot be
% done in all cases, since it is not obligatory for an RDF/XML
% document to have an `rdf:RDF' top level element.  In addition, when
% using a typed node just about anything can qualify for RDF [?].  The
% only real demand is that the XML document must use XML namespaces,
% because these are both required to define `<rdf:Description>' and a
% valid type IRI from a typed node.
%
% If the toplevel element is detected as HTML we assume that the
% document contains RDFa.

guess_sgml_type(In, Format) :-
  sgml_doctype(In, Dialect, DocType, Attrs),
  doc_content_type(Dialect, DocType, Attrs, Format).


%! sgml_doctype(+In:stream, -Dialect:atom, -DocType:atom,
%!              -Attrs:list(compound)) is semidet.
%
% Parse a _repositional_ stream and get the name of the first SGML
% element and demand that this element defines XML namespaces.
%
% This predicate fails if the document is illegal SGML before the
% first element.

sgml_doctype(In, Dialect, DocType, Attrs) :-
  setup_call_cleanup(
    make_parser(In, Parser, State),
    catch(
      sgml_parse(
        Parser,
        [
          call(begin, on_begin),
          call(cdata, on_cdata),
          max_errors(-1),
          source(In),
          syntax_errors(quiet)
        ]
      ),
      E,
      true
    ),
    clean_parser(In, Parser, State)
  ),
  nonvar(E),
  E = tag(Dialect, DocType, Attrs).

make_parser(In, Parser, state(Position)):-
  stream_property(In, position(Position)),
  new_sgml_parser(Parser, []).

clean_parser(In, Parser, state(Position)):-
  free_sgml_parser(Parser),
  set_stream_position(In, Position).

on_begin(Tag, Attrs, Parser) :-
  get_sgml_parser(Parser, dialect(Dialect)),
  throw(tag(Dialect, Tag, Attrs)).

on_cdata(_, _) :-
  throw(error(cdata)).


%! doc_content_type(+Dialect:atom, +Doctype:atom, +Attrs:list(compound),
%!                  -Format:atom) is det.

doc_content_type(_, html, _, rdfa) :- !.
doc_content_type(html, _, _, rdfa) :- !.
doc_content_type(xhtml, _, _, rdfa) :- !.
doc_content_type(html5, _, _, rdfa) :- !.
doc_content_type(xhtml5, _, _, rdfa) :- !.
doc_content_type(Dialect, Top,  Attrs, rdfxml) :-
  % Extract the namespace from the doctype.
  (   Dialect == sgml
  ->  LocalName = rdf
  ;   Dialect == xml
  ->  LocalName = 'RDF'
  ),
  atomic_list_concat([NS,LocalName], :, Top),
  % Look up the RDF namespace in the attributes list.
  atomic_list_concat([xmlns,NS], :, Attr),
  memberchk(Attr=RDFNS, Attrs),
  % Ensure it is indeed the RDF namespace.
  rdf_current_prefix(rdf, RDFNS).
