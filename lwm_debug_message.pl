:- module(
  lwm_debug_message,
  [
    lwm_debug_message/1, % ?Topic:compound
    lwm_debug_message/2 % ?Topic:compound
                        % +Message:compound
  ]
).

/** <module> LOD Washing Machine: Debug Message

Prints debug messages for the LOD Washing Machine.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09
*/

:- use_module(library(apply)).
:- use_module(library(debug)).

:- use_module(lwm(lwm_sparql_query)).



%! lwm_debug_message(+Topic:compound) is det.
% @see lwm_debug_message/2

lwm_debug_message(Topic):-
  lwm_debug_message(Topic, Topic).


%! lwm_debug_message(Topic, Message) is det.
% `Topic` is a debug topic, specified in `library(debug)`.

% Do not print debug message.
lwm_debug_message(Topic, _):-
(Topic == lwm_idle_loop(clean_large) -> gtrace ; true),
  nonvar(Topic),
  debugging(Topic, false), !.

% C-Triples written.
lwm_debug_message(_, ctriples_written(_,0,_)):- !.
lwm_debug_message(Topic, ctriples_written(_,Triples,Duplicates)):-
  % Duplicates
  (   Duplicates == 0
  ->  DuplicatesString = ''
  ;   format(string(DuplicatesString), ' (~D duplicates)', [Duplicates])
  ),

  debug(Topic, '[+~D~s]', [Triples,DuplicatesString]).

% Idle loop.
lwm_debug_message(Topic, lwm_idle_loop(Category)):-
  % Every category has its own idle loop flag.
  atomic_list_concat([number_of_idle_loops,Category], '_', Flag),
  flag(Flag, X, X + 1),

  debug(Topic, '[IDLE] ~a ~D', [Category,X]).

% End a process.
lwm_debug_message(Topic, lwm_end(Category1,Md5,Source,Status,Warnings)):-
  % Category
  upcase_atom(Category1, Category2),

  % Status
  (   Status == true
  ->  true
  ;   Status == false
  ->  debug(Topic, '  [STATUS] FALSE', [])
  ;   debug(Topic, '  [STATUS] ~w', [Status])
  ),

  % Warnings
  maplist(warning(Topic), Warnings),

  debug(Topic, '[END ~a] ~w ~w', [Category2,Md5,Source]).

% Start a process.
lwm_debug_message(Topic, lwm_start(Category1,Md5,Source)):-
  upcase_atom(Category1, Category2),

  % File source: URL or archive
  md5_source(Md5, Source),

  % File size
  (   Category1 == unpack
  ->  SizeString = ""
  ;   md5_size(Md5, NumberOfGigabytes),
      format(string(SizeString), ' (~f GB)', [NumberOfGigabytes])
  ),

  debug(Topic, '[START ~a] ~w ~w~s', [Category2,Md5,Source,SizeString]).

% VoID description found
lwm_debug_message(Topic, void_found(Urls)):-
  maplist(void_found(Topic), Urls).



% Helpers

%! void_found(+Topic:compound, +Url:atom) is det.

void_found(Topic, Url):-
  debug(Topic, '  [VOID] ~a', [Url]).


%! warning(+Topic:compound, +Message:compound) is det.

warning(Topic, message(_,Kind,Lines)):-
  atomic_list_concat(Lines, '\n', Content),
  debug(Topic, '  [MESSAGE(~w)] ~a', [Kind,Content]).

