:- module(lwm_messages, []).

/** <module> LOD Washing Machine (LWM): messages

Print messages for the LOD Washing Machine.

@author Wouter Beek
@version 2014/06, 2014/08
*/

:- use_module(lwm(lwm_sparql_query)).

:- multifile(prolog:message/1).



% lwm_end(
%   +Mode:oneof([clean,unpack]),
%   +Md5:atom,
%   +Source,
%   +Status,
%   +Warnings:list
% )

prolog:message(lwm_end(Mode,Md5,Source,Status,Warnings)) -->
  status(Md5, Status),
  warnings(Warnings),
  ['[END '],
  lwm_mode(Mode),
  ['] [~w] [~w]'-[Md5,Source]].


% lwm_idle_loop(+Mode:oneof([clean,unpack]))

prolog:message(lwm_idle_loop(Mode)) -->
  {flag(number_of_idle_loops(Mode), X, X + 1)},
  ['['],
    ['IDLE '],
    lwm_mode(Mode),
  ['] ~D'-[X]].


% lwm_start(+Mode:oneof([clean,unpack]), +Md5:atom, +Source)

prolog:message(lwm_start(Mode,Md5,Source)) -->
  {md5_source(Md5, Source)},
  ['[START '],
  lwm_mode(Mode),
  ['] [~w] [~w]'-[Md5,Source]],
  lwm_start_mode_specific_suffix(Md5, Mode).



% Helpers.

lines([]) --> [].
lines([H|T]) -->
  [H],
  lines(T).

lwm_mode(clean) --> ['CLEAN'].
lwm_mode(unpack) --> ['UNPACK'].

lwm_start_mode_specific_suffix(Md5, clean) --> !,
  {md5_size(Md5, NumberOfGigabytes)},
  [' [~f]'-[NumberOfGigabytes]].
lwm_start_mode_specific_suffix(_, unpack) --> [].

warning(message(_,Kind,Lines)) -->
  ['    [MESSAGE(~w)] '-[Kind]],
  lines(Lines),
  [nl].

warnings([]) --> !, [].
warnings([H|T]) -->
  warning(H),
  warnings(T).

% @tbd Send an email whenever an MD5 fails.
status(_, false) --> !,
  ['    [STATUS] FALSE',nl].
status(_, true) --> !.
status(_, Status) -->
  ['    [STATUS] ~w'-[Status],nl].

