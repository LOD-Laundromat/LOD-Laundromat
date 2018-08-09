:- module(ll_parse, [ll_parse/0]).

/** <module> LOD Laundromat: Parsing

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).
:- use_module(library(settings)).
:- use_module(library(zlib)).

:- use_module(library(debug_ext)).
:- use_module(library(dict)).
:- use_module(library(file_ext)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(semweb/rdf_clean)).
:- use_module(library(semweb/rdf_deref)).
:- use_module(library(semweb/rdf_export)).
:- use_module(library(semweb/rdf_guess)).
:- use_module(library(semweb/rdf_term)).
:- use_module(library(string_ext)).
:- use_module(library(uri_ext)).

:- initialization
   set_setting(rdf_term:bnode_prefix_scheme, https),
   set_setting(rdf_term:bnode_prefix_authority, 'lodlaundromat.org').

% This is needed for `http://spraakbanken.gu.se/rdf/saldom.rdf' which
% has >8M triples in one single RDF/XML description.
:- set_prolog_flag(stack_limit, 3 000 000 000).

ll_parse :-
  % precondition
  with_mutex(ll_parse, (
    ldfs_file(Hash, false, recoded, TaskFile),
    delete_file(TaskFile)
  )),
  (debugging(ll(task,parse,Hash)) -> gtrace ; true),
  indent_debug(1, ll(task,parse), "> parsing ~a", [Hash]),
  write_meta_now(Hash, parseBegin),
  % operation
  thread_create(parse_file(Hash), Id, [alias(Hash)]),
  thread_join(Id, Status),
  % postcondition
  write_meta_now(Hash, parseEnd),
  handle_status(Hash, parsed, Status),
  % This is the last task: finish if true.
  (Status == true -> finish(Hash) ; true),
  indent_debug(-1, ll(task,parse), "< parsed ~a", [Hash]).

parse_file(Hash) :-
  maplist(hash_file(Hash), [dirty,'data.nq.gz'], [FromFile,ToFile]),
  % base URI
  read_task_memory(Hash, base_uri, BaseUri),
  % blank node-replacing well-known IRI prefix
  bnode_prefix([Hash], BNodePrefix),
  peek_file(FromFile, 10 000, String),
  (   % RDF serialization format Media Type
      rdf_guess_string(String, MediaType)
  ->  write_meta_serialization_format(Hash, MediaType),
      % counter
      RdfMeta = _{number_of_quadruples: 0, number_of_triples: 0},
      % Reserialize the RDF statements.
      setup_call_cleanup(
        (
          open(FromFile, read, In),
          gzopen(ToFile, write, Out)
        ),
        rdf_deref_stream(
          BaseUri,
          In,
          clean_tuples(RdfMeta, Out),
          [base_uri(BaseUri),bnode_prefix(BNodePrefix),media_type(MediaType)]
        ),
        maplist(close_metadata(Hash), [parseRead,parseWritten], [In,Out])
      ),
      % cleanup
      delete_file(FromFile),
      % metadata
      write_meta_statements(Hash, RdfMeta)
  ;   string_truncate(String, 1 000, Truncated),
      throw(error(rdf_error(non_rdf_format,Truncated),ll_parse))
  ).

bnode_prefix(Segments, BNodePrefix) :-
  setting(rdf_term:bnode_prefix_scheme, Scheme),
  setting(rdf_term:bnode_prefix_authority, Auth),
  uri_comps(BNodePrefix, uri(Scheme,Auth,['.well-known',genid|Segments],_,_)).

clean_tuples(Meta, Out, BNodePrefix, Tuples, _) :-
  maplist(clean_tuple(Meta, Out, BNodePrefix), Tuples).

% triple
clean_tuple(Meta, Out, BNodePrefix, rdf(S0,P0,O0)) :- !,
  (   rdf_clean_triple(BNodePrefix, rdf(S0,P0,O0), rdf(S,P,O))
  ->  rdf_write_triple(Out, BNodePrefix, S, P, O),
      nb_increment_dict(Meta, number_of_triples)
  ;   true
  ).
% quadruple with the default graph → actually a triple
clean_tuple(Meta, Out, BNodePrefix, rdf(S,P,O,user)) :- !,
  clean_tuple(Meta, Out, BNodePrefix, rdf(S,P,O)).
% quadruple
clean_tuple(Meta, Out, BNodePrefix, rdf(S0,P0,O0,G0)) :-
  (   rdf_clean_quad(BNodePrefix, rdf(S0,P0,O0,G0), rdf(S,P,O,G))
  ->  rdf_write_quad(Out, BNodePrefix, S, P, O, G),
      nb_increment_dict(Meta, number_of_quadruples)
  ;   true
  ).
