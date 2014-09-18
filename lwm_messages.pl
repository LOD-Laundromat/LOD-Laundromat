:- module(lwm_messages, []).

/** <module> LOD Washing Machine (LWM): messages

Print messages for the LOD Washing Machine.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09
*/

:- use_module(lwm(lwm_sparql_query)).

:- multifile(prolog:message/1).



% lwm_end(
%   +Category:atom,
%   +Md5:atom,
%   +Source,
%   +Status,
%   +Warnings:list
% )

prolog:message(lwm_end(Category,Md5,Source,Status,Warnings)) -->
  status(Status),
  warnings(Warnings),
  ['[END '],
  lwm_category(Category),
  ['] [~w] [~w]'-[Md5,Source]].


% lwm_idle_loop(+Category:atom)

prolog:message(lwm_idle_loop(Category)) -->
  {
    Flag =.. [number_of_idle_loops,Category],
    flag(Flag, X, X + 1)
  },
  ['['],
    ['IDLE '],
    lwm_category(Category),
  ['] ~D'-[X]].


% lwm_start(+Category:atom, +Md5:atom, +Source)

prolog:message(lwm_start(Category,Md5,Source)) -->
  {md5_source(Md5, Source)},
  ['[START '],
  lwm_category(Category),
  ['] [~w] [~w]'-[Md5,Source]],
  lwm_start_mode_specific_suffix(Md5, Category).



% Helpers.

lines([]) --> [].
lines([H|T]) -->
  [H],
  lines(T).

lwm_category(Category1) -->
  {upcase_atom(Category1, Category2)},
  [Category2].

lwm_start_mode_specific_suffix(_, unpack) --> !, [].
lwm_start_mode_specific_suffix(Md5, _) -->
  {md5_size(Md5, NumberOfGigabytes)},
  [' [~f GB]'-[NumberOfGigabytes]].

warning(message(_,Kind,Lines)) -->
  ['    [MESSAGE(~w)] '-[Kind]],
  lines(Lines),
  [nl].

warnings([]) --> !, [].
warnings([H|T]) -->
  warning(H),
  warnings(T).

% @tbd Send an email whenever an MD5 fails.
status(false) --> !,
  ['    [STATUS] FALSE',nl].
status(true) --> !.
status(Status) -->
  ['    [STATUS] ~w'-[Status],nl].

