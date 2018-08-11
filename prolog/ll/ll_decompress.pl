:- module(ll_decompress, [ll_decompress/0]).

/** <module> LOD Laundromat: Decompress

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(archive)).

:- use_module(library(archive_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(dict)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_metadata)).

ll_decompress :-
  % precondition
  start_task(downloaded, Hash, State),
  (debugging(ll(offline)) -> gtrace ; true),
  indent_debug(1, ll(task,decompress), "> decompressing ~a", [Hash]),
  write_meta_now(Hash, decompressBegin),
  % operation
  catch(decompress_file(Hash, State), E, true),
  % postcondition
  write_meta_now(Hash, decompressEnd),
  (var(E) -> true ; write_message(error, Hash, E)),
  (   hash_file(Hash, dirty, File),
      exists_file(File)
  ->  end_task(Hash, decompressed, State)
  ;   % If there is no data, processing for this hash has finished.
      finish(Hash, State)
  ),
  indent_debug(-1, ll(task,decompress), "< decompressing ~a", [Hash]).



%! decompress_file(+Hash:atom, +State:dict) is det.

decompress_file(Hash, State) :-
  hash_file(Hash, compressed, File),
  setup_call_cleanup(
    open(File, read, In),
    decompress_file_stream(Hash, In, State),
    close_metadata(Hash, decompressRead, In)
  ),
  % Cleanup of the compressed file after decompression.
  delete_file(File).



%! decompress_file_stream(+Hash:atom, +In:stream, +State:dict) is det.

decompress_file_stream(Hash, In, State) :-
  setup_call_cleanup(
    (
      archive_open(In, Arch),
      indent_debug(1, ll(task,decompress), "> ~w OPEN ARCHIVE ~w", [In,Arch])
    ),
    decompress_archive(Hash, Arch, State),
    (
      indent_debug(-1, ll(task,decompress), "< ~w CLOSE ARCHIVE ~w", [In,Arch]),
      archive_close(Arch)
    )
  ).



%! decompress_archive(+Hash:atom, +Archive:blob, +State:dict) is det.

decompress_archive(Hash, Arch, State) :-
  archive_property(Arch, filter(Filters)),
  write_meta_archive(Hash, Filters),
  forall(
    (
      repeat,
      (archive_next_header(Arch, EntryName) -> true ; !, fail)
    ),
    (
      findall(Prop, archive_header_property(Arch, Prop), Props),
      decompress_entry(Hash, Arch, EntryName, Props, State)
    )
  ).



%! decompress_entry(+Hash:atom, +Archive:blob, +EntryName:atom,
%!                  +Properties:list(compound), +State:dict) is det.

decompress_entry(Hash, Arch, EntryName, Props, State) :-
  % Cases other than `file' (e.g., `directory') are not interesting.
  memberchk(filetype(Type), Props),
  (   Type == file
  ->  setup_call_cleanup(
        (
          archive_open_entry(Arch, In),
          indent_debug(1, ll(task,decompress), "> ~w OPEN ENTRY ‘~a’ ~w", [Arch,EntryName,In])
        ),
        decompress_file_entry(Hash, EntryName, Props, In, State),
        (
          indent_debug(-1, ll(task,decompress), "< ~w CLOSE ENTRY ‘~a’ ~w", [Arch,EntryName,In]),
          close(In)
        )
      )
  ;   true
  ).



%! decompress_file_entry(+Hash:atom, +EntryName:atom, Properties:list(compound),
%!                       +In:stream, +State:dict) is det.

decompress_file_entry(Hash, EntryName, Props, In, State1) :-
  memberchk(format(Format), Props),
  (   EntryName == data,
      Format == raw
  ->  % leaf node: archive entry
      hash_file(Hash, dirty, File),
      setup_call_cleanup(
        open(File, write, Out, [type(binary)]),
        copy_stream_data(In, Out),
        close(Out)
      )
  ;   % non-leaf node: archive, similar to a file that was just
      % `downloaded'.
      hash_entry_hash(Hash, EntryName, EntryHash),
      hash_file(EntryHash, compressed, File),
      setup_call_cleanup(
        open(File, write, Out, [type(binary)]),
        copy_stream_data(In, Out),
        close_metadata(Hash, decompressWrite, Out)
      ),
      % Copy task files for the decompressed entry.
      write_meta_entry(Hash, EntryName, EntryHash, Format, Props),
      % End this task from the perspective of the entry.
      dict_put(entry, State1, EntryName, State2),
      end_task(EntryHash, downloaded, State2)
  ).
