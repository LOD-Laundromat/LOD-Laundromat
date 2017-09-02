:- module(ll_guess, [ll_guess/0]).

/** <module> LOD Laundromat: Guess format

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(dcg/basics)).
:- use_module(library(lists)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_seedlist)).
:- use_module(library(ordsets)).
:- use_module(library(pio)).
:- use_module(library(semweb/rdf_guess), []).

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
  member(Format, Formats).
ll_guess(File, Format) :-
  setup_call_cleanup(
    open(File, read, In),
    rdf_guess:guess_sgml_type(In, MediaType),
    close(In)
  ),
  media_type_format(MediaType, Format).

media_type_format(media(application/'rdf+xml',[]), rdfxml).
media_type_format(media(application/'xhtml+xml',[]), rdfa).
media_type_format(media(text/html,[]), rdfa).

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
