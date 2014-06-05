:- module(
  download_lod_generics,
  [
    data_directory/1, % -DataDirectory:atom
    set_data_directory/1, % +DataDirectory:atom
    write_lod_url/2 % +Kind:oneof([failed,finished])
                    % +Dataset:atom
  ]
).

/** <module> Download LOD generics

Generic predicates that are used in the LOD download process.

@author Wouter Beek
@version 2014/04-2014/06
*/

:- use_module(generics(db_ext)).

:- db_add_novel(user:prolog_file_type(log, logging)).

:- dynamic(data_directory/1).



%! set_data_directory(+DataDirectory:atom) is det.

set_data_directory(DataDir):-
  % Assert the data directory.
  db_replace_novel(data_directory(DataDir), [e]).


%! write_lod_url(+Kind:oneof([failed,finished]), +Dataset:atom) is det.

write_lod_url(Kind, Url):-
  with_mutex(
    Kind,
    (
      data_directory(DataDir),
      absolute_file_name(
        Kind,
        File,
        [access(write),file_type(logging),relative_to(DataDir)]
      ),
      setup_call_cleanup(
        open(File, append, Stream),
        (
          Term =.. [Kind,Url],
          writeq(Stream, Term),
          write(Stream, '.'),
          nl(Stream)
        ),
        close(Stream)
      )
    )
  ).

