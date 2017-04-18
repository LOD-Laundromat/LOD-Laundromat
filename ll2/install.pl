#!/usr/bin/env swipl

:- module(install, []).

/** <module> Install dependencies

Let's do this!

@author Wouter Beek
@author Jan Wielemaker
@version 2015/09-2015/10
*/

:- use_module(library(debug)).
:- use_module(library(error)).
:- use_module(library(filesex)).
:- use_module(library(git)).
:- use_module(library(lists)).
:- use_module(library(option)).
:- use_module(library(optparse)).
:- use_module(library(prolog_pack)).
:- use_module(library(uri)).

:- dynamic(author/2).
:- dynamic(download/1).
:- dynamic(home/1).
:- dynamic(maintainer/2).
:- dynamic(name/1).
:- dynamic(packager/2).
:- dynamic(requires/1).
:- dynamic(title/1).
:- dynamic(type/1).
:- dynamic(version/1).

:- meta_predicate(verbose(0,+,+)).

:- initialization(install).





%! install is det.

install:-
  OptSpec = [
    [ % Debug
      default(false),
      help('Whether debug messages are displayed.'),
      longflags([debug]),
      opt(debug),
      type(boolean)
    ],
    [ % Github.
      default(true),
      help('Whether Github should be used to install requirements.'),
      longflags([git,github]),
      opt(github),
      type(boolean)
    ],
    [ % Help.
      default(false),
      help('Display help about the install command.'),
      longflags([help]),
      opt(help),
      shortflags([h]),
      type(boolean)
    ],
    [ % Pack.
      default(true),
      help('Whether the Prolog Pack system is used for installing requirements.'),
      longflags([pack]),
      opt(pack),
      type(boolean)
    ]
  ],
  opt_arguments(OptSpec, Opts, _),

  % Process debug option.
  (option(debug(true), Opts) -> debug(install) ; true),
  
  % Process help option.
  (option(help(true), Opts) -> show_help(OptSpec) ; install(Opts)).


%! install(+Options:list(compound)) is det.

install(Opts):-
  source_file(install, File),
  file_directory_name(File, AppDir),
  clean_config,
  install(AppDir, AppDir, Opts),
  clean_meta,
  halt.


%! install(
%!   +ApplicationDirectory:atom,
%!   +LibraryDirectory:atom,
%!   +Options:list(compound)
%! ) is det.
% The application is installed inside the ApplicationDirectory.
% The LibraryDirectory may be different when installing dependencies.

% A `pack.pl` file.
install(AppDir, LibDir, Opts):-
  % The pack information is taken from the library directory.
  % This allows application requirements to be installed recursively.
  directory_file_path(LibDir, 'pack.pl', Info),
  access_file(Info, read), !,
  load_meta(Info),

  % Set this library's search path.
  name(Name),
  type(Type),
  store_search_path(Name, Type, LibDir),

  % Pull in the required libraries.
  forall(requires(LibName), install_required(AppDir, LibName, Opts)), !.
% No `pack.pl` file.
install(_, _, _).


%! install_required(
%!   +ApplicationDirectory:atom,
%!   +LibName:atom,
%!   +Options:list(compound)
%! ) is det.

% Install through the Prolog Pack system.
install_required(_, LibName, Opts):-
  option(pack(true), Opts),

  prolog_pack:query_pack_server(search(LibName), Result,[silent(true)]),
  Result = true(Matches),
  memberchk(pack(LibName,_,_,_,_), Matches), !,

  verbose(
    pack_install(LibName, [interactive(false),silent(true),upgrade(true)]),
    'Installing ~w',
    [LibName]
  ).
% Install through Github.
install_required(AppDir, LibName, Opts):-
  option(github(true), Opts),
  directory_file_path(AppDir, deps, DefaultInstallDir),
  option(directory(Install), Opts, DefaultInstallDir),

  % Remove the old version of the library.
  directory_file_path(Install, LibName, LibDir),
  make_directory_path(Install),
  (exists_directory(LibDir) -> delete_directory_and_contents(LibDir) ; true),

  % Git clone the new version of the library.
  atomic_list_concat(['',wouterbeek,LibName], /, Path),
  uri_components(Iri, uri_components(https,'github.com',Path,_,_)),
  verbose(
    git([clone,Iri,LibName], [directory(Install)]),
    'Installing ~w',
    [LibName]
  ),

  % Recurse.
  install(AppDir, LibDir, Opts), !.
% Oops!
install_required(_, LibName, _):-
  msg_warning('Installation of library ~w failed.', [LibName]).





% HELPERS %

%! clean_meta is det.

clean_meta:-
  retractall(author(_,_)),
  retractall(download(_)),
  retractall(home(_)),
  retractall(maintainer(_,_)),
  retractall(name(_)),
  retractall(packager(_,_)),
  retractall(requires(_)),
  retractall(title(_)),
  retractall(type(_)),
  retractall(version(_)).



%! clean_config

clean_config :-
  exists_file('config.pl'), !,
  delete_file('config.pl').
clean_config.



%! add_config(+Term)

add_config(Term) :-
  assertz(Term),
  setup_call_cleanup(
    open('config.pl', append, Out),
    format(Out, '~q.~n', [Term]),
    close(Out)
  ).



%! show_help(+OptionSpecification:list(compound)) is det.

show_help(OptSpec):-
  opt_help(OptSpec, Help),
  format(user_output, '~a\n', [Help]),
  halt.



%! load_meta(+File:atom) is det.

load_meta(File):-
  clean_meta,
  ensure_loaded(File).



%! store_search_path(
%!   +Name:atom,
%!   +Type:oneof([app,lib]),
%!   +Directory:atom
%! ) is det.

store_search_path(Name0, Type, Dir0):-
  must_be(oneof([app,lib]), Type),
  (   Type == app
  ->  Name = Name0,
      Dir = Dir0
  ;   Type == lib
  ->  Name = library,
      directory_file_path(Dir0, prolog, Dir)
  ),
  add_config(user:file_search_path(Name, Dir)),
  debug(install, 'Added file search path: ~w = ~w', [Name,Dir]).



%! msg_success(+Message:atom, +Arguments:list) is det.

msg_success(Msg, Args):-
  ansi_format([fg(green),bold], Msg, Args), nl.



%! msg_warning(+Message:atom, +Arguments:list) is det.

msg_warning(Msg, Args):-
  ansi_format([bold,fg(red)], Msg, Args), nl.



%! verbose(:Goal_0, +Format:atom, +Arguments:list) is det.

verbose(Goal_0, Format, Args):-
  ansi_format([], Format, Args),
  (   catch(Goal_0, Error, true)
  ->  (   var(Error)
      ->  msg_success('~`.t success~72|', [])
      ;   message_to_string(Error, String),
          msg_warning('~`.t ERROR: ~w~72|', [String])
      )
  ;   msg_warning('~`.t ERROR: (failed)~72|', [])
  ).
