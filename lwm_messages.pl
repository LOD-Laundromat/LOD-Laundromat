:- module(lwm_messages, []).

/** <module> LOD Washing Machine: messages

Informational messages for the LOD Washing Machine.

@author Wouter Beek
@version 2014/04-2014/06
*/

:- multifile(prolog:message/1).



prolog:message(found_void_lod_urls([])) --> !.
prolog:message(found_void_lod_urls([H|T])) -->
  ['[VoID] Found: ',H,nl],
  prolog:message(found_void_lod_urls(T)).

prolog:message(lod_download_start(X,Url)) -->
  {flag(number_of_processed_files, X, X + 1)},
  ['[START ~D] [~w]'-[X,Url]].

prolog:message(lod_downloaded_file(Url,X,Status,Messages)) -->
  prolog_status(Status, Url),
  prolog_messages(Messages),
  ['[DONE ~D]'-[X]],
  [nl,nl].

prolog:message(lod_skipped_file(_,true,_,_)) --> !, [].
prolog:message(lod_skipped_file(Url,false,_,_)) --> !,
  {report_failed(Url)},
  [].
prolog:message(lod_skipped_file(Url,_,Status,Messages)) -->
  {flag(number_of_skipped_files, X, X + 1)},
  ['[SKIP ~D] '-[X],nl],
  prolog_status(Status, Url),
  prolog_messages(Messages).

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

prolog_status(false, Url) --> !,
  {report_failed(Url)},
  [].
prolog_status(true, _) --> !, [].
prolog_status(exception(Error), _) -->
  {print_message(error, Error)}.

prolog_messages([]) --> !, [].
prolog_messages([message(_,Kind,Lines)|T]) -->
  ['  [~w] '-[Kind]],
  prolog_lines(Lines),
  [nl],
  prolog_messages(T).

prolog_lines([]) --> [].
prolog_lines([H|T]) -->
  [H],
  prolog_lines(T).

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

