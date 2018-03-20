:- module(
  ll_init,
  [
    clear_wardrobe/0
  ]
).

/** <module> Initializes a scraping activity

@author Wouter Beek
@version 2018
*/

:- use_module(library(debug)).
:- use_module(library(settings)).
:- use_module(library(zlib)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(tapir)).
:- use_module(library(thread_ext)).
:- use_module(library(write_ext)).

:- initialization
   init_ll.

lod_cloud_portray(Blob, Options) :-
  blob(Blob, Type),
  \+ atom(Blob),
  Type \== reserved_symbol,
  write_term('BLOB'(Type), Options).

:- setting(ll:data_directory, any, _,
           "The directory where data is temporarily stored and where error logs are kept.").
:- setting(ll:seedlist_authority, any, _,
           "The URI authority of the seedlist endpoint.").
:- setting(ll:seedlist_scheme, oneof([http,https]), https,
           "The URI scheme of the seedlist endpoint.").





%! clear_wardrobe is det.
%
% Delete all data currently stored in the wardrobe.

clear_wardrobe :-
  tapir:user_(Site, User),
  forall(
    dataset(Site, User, Dataset, _),
    dataset_delete(Site, User, Dataset)
  ).



%! init_ll is det.

init_ll :-
  % Count-by-one for thread aliases.
  flag(number_of_workers, _, 1),
  conf_json(Conf),
  _{authority: Auth, scheme: Scheme} :< Conf.seedlist,
  set_setting(ll:seedlist_authority, Auth),
  set_setting(ll:seedlist_scheme, Scheme),
  _{directory: Dir} :< Conf.data,
  create_directory(Dir),
  set_setting(ll:data_directory, Dir),
  (debugging(ll) -> true ; init_log(Dir)).

init_log(Dir) :-
  init_out_log(Dir),
  init_err_log(Dir).

init_out_log(Dir) :-
  directory_file_path(Dir, 'out.log', File),
  protocol(File).

init_err_log(Dir) :-
  directory_file_path(Dir, 'err.log.gz', File),
  gzopen(File, write, Out),
  asserta((
    user:message_hook(E1, Kind, _) :-
      memberchk(Kind, [error,warning]),
      thread_self_property(alias(Alias)),
      (   skip_error(E1)
      ->  true
      ;   replace_blobs(E1, E2),
          format(
            Out,
            "~a\t~W\n",
            [Alias,E2,[blobs(portray),portray_goal(lod_cloud_portray),quoted(true)]]
          )
      )
  )),
  asserta(user:at_halt(close(Out))).

% Do not record errors for datatype IRIs whose parser and generator
% are not standards-compliant: `rdf:HTML' and `rdf:XMLLiteral'.
skip_error(non_canonical_lexical_form('http://www.w3.org/1999/02/22-rdf-syntax-ns#HTML',_,_)).
skip_error(non_canonical_lexical_form('http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral',_,_)).
