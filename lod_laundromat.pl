:- module(
  lod_laundromat,
  [
    ll_add_seed/1, % +Iri
    ll_add_old_seeds/0,
    ll_add_thread/0,
    ll_clean/1
  ]
).

/* <module> LOD Laundromat

@author Wouter Beek
@version 2016/01-2016/02
*/

:- use_module(library(apply)).
:- use_module(library(debug_ext)).
:- use_module(library(dict_ext)).
:- use_module(library(filesex)).
:- use_module(library(gui_tracer)).
:- use_module(library(hash_ext)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_header)).
:- use_module(library(http/http_json)).
:- use_module(library(http/http_receive)).
:- use_module(library(http/http_request)).
:- use_module(library(http/http_server)).
:- use_module(library(json_ext)).
:- use_module(library(jsonld/jsonld_metadata)).
:- use_module(library(jsonld/jsonld_read)).
:- use_module(library(lists)).
:- use_module(library(lodapi/lodapi_document)).
:- use_module(library(lodapi/lodapi_generics)).
:- use_module(library(lodapi/lodapi_metadata)).
:- use_module(library(os/archive_ext)).
:- use_module(library(os/gnu_sort)).
:- use_module(library(os/open_any2)).
:- use_module(library(os/thread_counter)).
:- use_module(library(os/thread_ext)).
:- use_module(library(persistency)).
:- use_module(library(rdf/rdf_clean)).
:- use_module(library(rdf/rdf_debug)).
:- use_module(library(rdf/rdf_load)).
:- use_module(library(rdf/rdf_print)).
:- use_module(library(rdf11/rdf11)). % Operators.
:- use_module(library(sparql/sparql_db)).
:- use_module(library(sparql/query/sparql_query)).

:- http_handler(root(seedlist), seedlist, []).

:- persistent seed(hash:atom, from:atom, added:float, started:float, ended:float).

:- sparql_register_endpoint(
     ll_endpoint,
     ['http://sparql.backend.lodlaundromat.org'],
     virtuoso
   ).

:- initialization(init_lod_laundromat).

init_lod_laundromat :-
  db_attach('seedlist.pl', [sync(flush)]),
  start_server([port(3000)]).

:- debug(http(parse)).
:- debug(rdf(clean)).
:- debug(sparql(_)).



%! ll_add_seed(+Iri) is det.

ll_add_seed(Iri) :-
  catch(
    http_post('http://localhost:3000/seedlist', json(_{from: Iri})),
    E,
    writeln(E)
  ).


%! ll_add_old_seeds is det.

ll_add_old_seeds :-
  absolute_file_name(seedlist, File, [access(write),file_type(prolog)]),
  Q = '\c
PREFIX llo: <http://lodlaundromat.org/ontology/>\n\c
SELECT ?url\n\c
WHERE {\n\c
  ?doc llo:url ?url\n\c
}\n',
  setup_call_cleanup(
    open(File, write, Write),
    forall(
      sparql_select(ll_endpoint, Q, Rows),
      forall(member([Url], Rows), ll_add_seed(Url))
    ),
    close(Write)
  ),
  sort_file(File),
  halt.


%! seedlist(+Request) is det.
% ```json
% {from: Iri}
% ```

seedlist(Req) :-
  http_method(Req, get), !,
  findall(
    _{added:Added, ended:Ended2, from:From, hash:Hash, started:Started2},
    (
      seed(Hash, From, Added, Started1, Ended1),
      maplist(var_to_null, [Started1,Ended1], [Started2,Ended2])
    ),
    Ds
  ),
  length(Ds, N),
  reply_json(_{seeds:Ds,size:N}, [status(200)]).
seedlist(Req) :-
  http_method(Req, post), !,
  http_output(Req, Out),
  catch(
    (
      http_read_json_dict(Req, Data),
      get_time(Now),
      iri_normalized(Data.from, From),
      md5(From, Hash),
      (   seed(Hash, _, _, _, _)
      ->  reply_json(_{}, [status(409)])
      ;   assert_seed(Hash, From, Now, 0.0, 0.0)
      )
    ),
    E,
    http_status_reply(bad_request(E), Out, [], _)
  ),
  reply_json(_{}, [status(201)]).

var_to_null(X, null) :- var(X), !.
var_to_null(X, X).

ll_add_thread :-
  detached_thread(ll_thread).

ll_thread :-
  with_mutex(ll_endpoint, (seed(Hash, From, _, 0.0, 0.0), ll_store_begin0(Hash))),
  document_name(Doc, Hash),
  ll_clean(Doc, From),
  with_mutex(ll_endpoint, ll_store_end0(Hash)),
  ll_thread.
ll_thread :-
  M = 100,
  sleep(M),
  thread_name(Name),
  increment_thread_counter(ll(idle), N),
  S is M * N,
  debug(ll(idle), "Thread ~w has been idle for ~D seconds.", [Name,S]),
  ll_thread.

ll_store_begin0(Hash) :-
  retract_seed(Hash, From, Added, 0.0, 0.0),
  get_time(Started),
  assert_seed(Hash, From, Added, Started, 0.0).

ll_store_end0(Hash) :-
  retract_seed(Hash, From, Added, Started, 0.0),
  get_time(Ended),
  assert_seed(Hash, From, Added, Started, Ended).

ll_clean(From0):-
  iri_normalized(From0, From),
  md5(From, Hash),
  document_name(Doc, Hash),
  ll_clean(Doc, From).

ll_clean(Doc, From) :-
  Dir1 = '/scratch',
  document_path(Doc, Dir2),
  directory_file_path(Dir1, Dir2, Dir),
  make_directory_path(Dir),
  Opts = [access(write),relative_to(Dir)],
  absolute_file_name('dirty.gz', DTo, Opts),
  absolute_file_name('clean.nq.gz', CTo, Opts),
  absolute_file_name('metadata.nq.gz', MTo, Opts),
  rdf_download_to_file(From, DTo, [compress(gzip)]),
  setup_call_cleanup(
    open_any2(MTo, append, Write, Close_0, [compress(gzip)]),
    with_output_to(Write,
      rdf_store_messages(Doc, (
        rdf_clean(From, CTo, [compress(gzip),metadata(M)]),
        rdf_store_metadata(Doc, M)
      ))
    ),
    close_any2(Close_0)
  ).

ll_clean_test(From, N):-
  (   var(N)
  ->  test_source(From)
  ;   findnsols(N, From, test_source(From), Froms),
      last(Froms, From)
  ),
  ll_clean(From).

test_source(Source):-
  document(Doc),
  metadata(Doc, llo:url, Source).
