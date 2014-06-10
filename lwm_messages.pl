:- module(lwm_messages, []).

/** <module> LOD Washing Machine: messages

Informational messages for the LOD Washing Machine.

@author Wouter Beek
@version 2014/04-2014/06
*/

:- multifile(prolog:message/1).



prolog:message(found_void_datadumps([])) --> !.
prolog:message(found_void_datadumps([H|T])) -->
  ['[VoID] Found: ',H,nl],
  prolog:message(found_void_datadumps(T)).

prolog:message(start_cleaning(X,Md5)) -->
  {flag(number_of_processed_files, X, X + 1)},
  ['[START ~D] [~w]'-[X,Md5]].

prolog:message(end_cleaning(X,Md5,Status,Messages)) -->
  status(Status),
  messages(Messages),
  ['[DONE ~D] [~w]'-[X,Md5]],
  [nl,nl].

prolog:message(rdf_ntriples_written(File,TDup,TOut)) -->
  ['['],
    number_of_triples_written(TOut),
    number_of_duplicates_written(TDup),
    total_number_of_triples_written(TOut),
  ['] ['],
    remote_file(File),
  [']'].

prolog:message(sent_to_endpoint(Endpoint,Reply)) -->
  ['[',Endpoint,']',nl,Reply,nl].

number_of_duplicates_written(0) --> !, [].
number_of_duplicates_written(T) --> [' (~D dups)'-[T]].

number_of_triples_written(0) --> !, [].
number_of_triples_written(T) --> ['+~D'-[T]].

status(false) --> !, ['false'].
status(true) --> !, ['true'].
status(exception(Error)) -->
  {print_message(error, Error)}.

messages([]) --> !, [].
messages([message(_,Kind,Lines)|T]) -->
  ['  [~w] '-[Kind]],
  lines(Lines),
  [nl],
  messages(T).

lines([]) --> [].
lines([H|T]) -->
  [H],
  lines(T).

remote_file(remote(User,Machine,Path)) --> !,
  [User,'@',Machine,':',Path].
remote_file(File) -->
  [File].

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

