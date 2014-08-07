:- module(lwm_messages, []).

/** <module> LOD Washing Machine (LWM): messages

Print messages for the LOD Washing Machine.

@author Wouter Beek
@version 2014/06, 2014/08
*/

:- use_module(ll_sparql(ll_sparql_query)).

:- multifile(prolog:message/1).



prolog:message(lwm_end(Mode,Md5,Source,Status,Messages)) -->
  status(Md5, Status),
  messages(Messages),
  ['[END '],
  lwm_mode(Mode),
  ['] [~w] [~w]'-[Md5,Source]].

prolog:message(lwm_start(Mode,Md5,Source)) -->
  {md5_source(Md5, Source)},
  ['[START '],
  lwm_mode(Mode),
  ['] [~w] [~w]'-[Md5,Source]],
  lwm_start_mode_specific_suffix(Md5, Mode).

lines([]) --> [].
lines([H|T]) -->
  [H],
  lines(T).

lwm_mode(clean) --> ['CLEAN'].
lwm_mode(metadata) --> ['METADATA'].
lwm_mode(unpack) --> ['UNPACK'].

lwm_start_mode_specific_suffix(Md5, clean) --> !,
  {md5_size(Md5, NumberOfGigabytes)},
  [' [~f]'-[NumberOfGigabytes]].
lwm_start_mode_specific_suffix(_, unpack) --> [].

message(message(_,Kind,Lines)) -->
  ['    [MESSAGE(~w)] '-[Kind]],
  lines(Lines),
  [nl].

messages([]) --> !, [].
messages([H|T]) -->
  message(H),
  messages(T).

% @tbd Send an email whenever an MD5 fails.
status(_, false) --> !,
  ['    [STATUS] FALSE',nl].
status(_, true) --> !.
status(_, Status) -->
  ['    [STATUS] ~w'-[Status],nl].

