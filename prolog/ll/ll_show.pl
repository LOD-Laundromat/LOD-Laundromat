:- module(
  ll_show,
  [
    export_uri/1, % +Uri
    show_uri/1    % +Uri
  ]
).

/** <module> LOD Laundromat: Show

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(atom_ext)).
:- use_module(library(date_time)).
:- use_module(library(dcg/dcg_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(graph/dot)).
:- use_module(library(http/http_generic)).
:- use_module(library(lists)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_seedlist)).
:- use_module(library(stream_ext)).
:- use_module(library(yall)).

:- debug(dot).





%! export_uri(+Uri:atom) is det.
%! export_uri(+Uri:atom, +Format:atom) is det.
%
% Exports the LOD Laundromat job for the given URI to a PDF file, or
% to a file in some other Format.

export_uri(Uri) :-
  export_uri(Uri, pdf).


export_uri(Uri, Format) :-
  uri_hash(Uri, Hash),
  file_name_extension(Hash, Format, File),
  seed(Hash, Seed),
  setup_call_cleanup(
    graphviz(dot, ProcIn, Format, ProcOut),
    seed2dot(ProcIn, Seed),
    close(ProcIn)
  ),
  setup_call_cleanup(
    open(File, write, Out),
    copy_stream_type(ProcOut, Out),
    close(Out)
  ).



%! show_uri(+Uri:atom) is det.
%! show_uri(+Uri:atom, +Program:atom) is det.
%
% Shows the LOD Laundromat job for the given URI in X11, or in some
% other Program.

show_uri(Uri) :-
  show_uri(Uri, x11).


show_uri(Uri, Program) :-
  uri_hash(Uri, Hash),
  seed(Hash, Seed),
  setup_call_cleanup(
    graphviz(dot, ProcIn, Program),
    seed2dot(ProcIn, Seed),
    close(ProcIn)
  ).





% GENERICS %

seed2dot(Out, Dict) :-
  dict_pairs(Dict, Hash, Pairs),
  format_debug(dot, Out, "digraph g {"),
  atomic_concat(n, Hash, Id),
  seed2dot(Out, Id, Pairs),
  format_debug(dot, Out, "}").

seed2dot_header(Pairs1, Header, Pairs2) :-
  selectchk(status-Status, Pairs1, Pairs2),
  (   % Seed status label
      atom(Status)
  ->  atom_string(Status, Header0)
  ;   % HTTP status code
      http_status_reason(Status, Reason),
      format(string(Header0), "HTTP status: ~d (~s)", [Status,Reason])
  ),
  format(string(Header), "<B>~s</B>", [Header0]).

seed2dot(Out, Id, Pairs1) :-
  seed2dot_header(Pairs1, Header, Pairs2),
  seed2dot_edges(Out, Id, Pairs2, Pairs3),
  aggregate_all(
    set(Attr),
    (
      member(Name-Value, Pairs3),
      seed2dot_attr(Name, Value, Attr)
    ),
    Attrs
  ),
  atomics_to_string([Header|Attrs], "<BR/>", Label),
  dot_node(Out, Id, [label(Label),shape(box)]).

seed2dot_edges(Out, Id, Pairs1, Pairs2) :-
  seed2dot_edges(Out, Id, Pairs1, [], Pairs2).

seed2dot_edges(_, _, [], Pairs3, Pairs3) :- !.
seed2dot_edges(Out, Id, [Name-Value|Pairs1], Pairs2, Pairs3) :-
  seed2dot_edge(Out, Id, Name, Value), !,
  seed2dot_edges(Out, Id, Pairs1, Pairs2, Pairs3).
seed2dot_edges(Out, Id, [Pair|Pairs1], Pairs2, Pairs3) :-
  seed2dot_edges(Out, Id, Pairs1, [Pair|Pairs2], Pairs3).

% archive, content, headers: Keys with a value that is a dictionary of
% properties that are displayed for the current node.
seed2dot_attr(Name1, Value1, Attr) :-
  memberchk(Name1, [archive,content,headers]),
  dict_pairs(Value1, Pairs),
  member(Name2-Value2, Pairs),
  seed2dot_attr(Name2, Value2, Attr).
% added
seed2dot_attr(added, Added, Attr) :-
  dt_label(Added, Label),
  format(string(Attr), "Added: ~s", [Label]).
% format
seed2dot_attr(format, Format, Attr) :-
  rdf_format(Format, Super, Sub),
  format(string(Attr), "RDF serialization format: ~a/~a", [Super,Sub]).
% headers
seed2dot_attr(headers, Headers, Attr) :-
  dict_pairs(Headers, Pairs),
  member(Name-Values, Pairs),
  member(Value, Values),
  http_header_name_label(Name, NameLabel),
  format(string(Attr), "~s: ~w", [NameLabel,Value]).
% newline
seed2dot_attr(newline, Atom, Attr) :-
  format(string(Attr), "Content/Newline: ~a", [Atom]).
% number of bytes
seed2dot_attr(number_of_bytes, N, Attr) :-
  format(string(Attr), "Content/Number of bytes: ~D", [N]).
% number of characters
seed2dot_attr(number_of_characters, N, Attr) :-
  format(string(Attr), "Content/Number of bytes: ~D", [N]).
% number of lines
seed2dot_attr(number_of_lines, N, Attr) :-
  format(string(Attr), "Content/Number of bytes: ~D", [N]).
% relative
seed2dot_attr(relative, Bool, Attr) :-
  bool_string(Bool, String),
  format(string(Attr), "URI/relative: ~s", [String]).
% uri
seed2dot_attr(uri, Uri, Attr) :-
  format(string(Attr), "URI: ~a", [Uri]).
% version
seed2dot_attr(version, Version, Attr) :-
  _{major: Major, minor: Minor} :< Version,
  format(string(Attr), "HTTP version: ~d.~d", [Major,Minor]).
% walltime
seed2dot_attr(walltime, Walltime, Attr) :-
  format(string(Attr), "Walltime: ~d mili-seconds", [Walltime]).

% dirty, parent: A single link to another node.
seed2dot_edge(Out, Id1, Name, Hash) :-
  memberchk(Name, [dirty,parent]),
  atomic_concat(n, Hash, Id2),
  dot_edge(Out, Id1, Id2, [label(Name)]).
% http: A linked list / sequence of nodes.
seed2dot_edge(Out, Id1, http, Dicts) :-
  maplist(dot_id, Dicts, Id2s),
  maplist(dict_pairs, Dicts, Pairss),
  maplist(seed2dot(Out), Id2s, Pairss),
  dot_linked_list(Out, Id2s, FirstId2-_LastId2),
  dot_edge(Out, Id1, FirstId2, [label("HTTP")]).
% children: Multiple links to other nodes
seed2dot_edge(Out, Id1, children, Hashes) :-
  maplist(atomic_concat(n), Hashes, Id2s),
  maplist(
    {Out,Id1}/[Id2]>>dot_edge(Out, Id1, Id2, [label("has child")]),
    Id2s
  ).

dot_linked_list(Out, [FirstId|Ids], FirstId-LastId) :-
  dot_linked_list_(Out, [FirstId|Ids], LastId).

dot_linked_list_(_, [LastId], LastId) :- !.
dot_linked_list_(Out, [Id1,Id2|T], LastId) :-
  dot_edge(Out, Id1, Id2),
  dot_linked_list_(Out, [Id2|T], LastId).

bool_string(false, "❌").
bool_string(true, "✓").

rdf_format(jsonld, application, 'json-ld').
rdf_format(nquads, application, 'n-quads').
rdf_format(ntriples, application, 'n-triples').
rdf_format(rdfa, application, 'xhtml+xml').
rdf_format(rdfxml, application, 'rdf+xml').
rdf_format(trig, application, trig).
rdf_format(turtle, text, turtle).

http_header_label(Key-Values, Label) :-
  atom_capitalize(Key, CKey),
  atomics_to_string(Values, "; ", Value),
  format(string(Label), "~s: ~s", [CKey,Value]).
