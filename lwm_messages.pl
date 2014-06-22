:- module(lwm_messages, []).

/** <module> LOD Washing Machine: messages

Informational messages for the LOD Washing Machine.

@author Wouter Beek
@version 2014/04-2014/06
*/

:- multifile(prolog:message/1).



prolog:message(end(Mode,Md5,Status,Messages)) -->
  status(Status), [nl],
  messages(Messages), [nl],
  ['[END'],
  lwm_mode(Mode),
  ['] [~w]'-[Md5],nl].

prolog:message(found_void([])) --> !.
prolog:message(found_void([H|T])) -->
  ['[VoID] Found: ',H,nl],
  prolog:message(found_void(T)).

prolog:message(rdf_ntriples_written(TDup,TOut)) -->
  ['['],
    number_of_triples_written(TOut),
    number_of_duplicates_written(TDup),
    total_number_of_triples_written(TOut),
  [']'].

prolog:message(start(Mode,Md5)) -->
  ['[START'],
  lwm_mode(Mode),
  ['] [~w]'-[Md5]].



% Helpers

lines([]) --> [].
lines([H|T]) -->
  [H],
  lines(T).

lwm_mode(clean) --> ['CLEAN'].
lwm_mode(metadata) --> ['METADATA'].
lwm_mode(unpack) --> ['UNPACK'].

messages([]) --> !, [].
messages([message(_,Kind,Lines)|T]) -->
  ['  [~w] '-[Kind]],
  lines(Lines),
  [nl],
  messages(T).

number_of_duplicates_written(0) --> !, [].
number_of_duplicates_written(T) --> [' (~D dups)'-[T]].

number_of_triples_written(0) --> !, [].
number_of_triples_written(T) --> ['+~D'-[T]].

remote_file(remote(User,Machine,Path)) --> !,
  [User,'@',Machine,':',Path].
remote_file(File) -->
  [File].

status(false) --> !, ['false'].
status(true) --> !, ['true'].
status(exception(Error)) -->
  {print_message(error, Error)}.

total_number_of_triples_written(0) --> !, [].
total_number_of_triples_written(T) -->
  {
    with_mutex(
      number_of_triples_written,
      (
        flag(number_of_triples_written, All1, All1 + T),
        All2 is All1 + T
      )
    )
  },
  [' (~D tot)'-[All2]].

