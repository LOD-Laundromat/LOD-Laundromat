:- module(ll_decompress, [ll_decompress/0]).

/** <module> LOD Laundromat: Decompress

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(archive)).

:- use_module(library(archive_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_metadata)).

ll_decompress :-
  % precondition
  with_mutex(ll_decompress, (
    ldfs_file(Hash, false, downloaded, TaskFile),
    delete_file(TaskFile)
  )),
  (debugging(ll(task,decompress,Hash)) -> gtrace ; true),
  indent_debug(1, ll(task,decompress), "> decompressing ~a", [Hash]),
  write_meta_now(Hash, decompressBegin),
  % operation
  catch(decompress_file(Hash), E, true),
  (var(E) -> true ; write_message(error, Hash, E), finish(Hash)),
  % postcondition
  write_meta_now(Hash, decompressEnd),
  (   hash_file(Hash, dirty, File),
      exists_file(File)
  ->  end_task(Hash, decompressed)
  ;   % If there is no data, processing for this hash has finished.
      finish(Hash)
  ),
  indent_debug(-1, ll(task,decompress), "< decompressing ~a", [Hash]).



%! decompress_file(+Hash:atom) is det.

decompress_file(Hash) :-
  hash_file(Hash, compressed, File),
  setup_call_cleanup(
    open(File, read, In),
    decompress_file_stream(Hash, In),
    close_metadata(Hash, decompressRead, In)
  ),
  % Cleanup of the compressed file after decompression.
  delete_file(File).



%! decompress_file_stream(+Hash:atom, +In:stream) is det.

decompress_file_stream(Hash, In) :-
  setup_call_cleanup(
    (
      archive_open(In, Arch),
      indent_debug(1, ll(task,decompress), "> ~w OPEN ARCHIVE ~w", [In,Arch])
    ),
    decompress_archive(Hash, Arch),
    (
      indent_debug(-1, ll(task,decompress), "< ~w CLOSE ARCHIVE ~w", [In,Arch]),
      archive_close(Arch)
    )
  ).



%! decompress_archive(+Hash:atom, +Archive:blob) is det.

decompress_archive(Hash, Arch) :-
  archive_property(Arch, filter(Filters)),
  write_meta_archive(Hash, Filters),
  forall(
    (
      repeat,
      (archive_next_header(Arch, EntryName) -> true ; !, fail)
    ),
    (
      findall(Prop, archive_header_property(Arch, Prop), Props),
      decompress_entry(Hash, Arch, EntryName, Props)
    )
  ).



%! decompress_entry(+Hash:atom, +Archive:blob, +EntryName:atom, +Properties:list(compound)) is det.
%! decompress_entry(+Hash:atom, +Archive:blob, +EntryName:atom, +Properties:list(compound), +Type:atom) is det.

decompress_entry(Hash, Arch, EntryName, Props) :-
  memberchk(filetype(file), Props), !,
  setup_call_cleanup(
    (
      archive_open_entry(Arch, In),
      indent_debug(1, ll(task,decompress), "> ~w OPEN ENTRY ‘~a’ ~w", [Arch,EntryName,In])
    ),
    decompress_file_entry(Hash, EntryName, Props, In),
    (
      indent_debug(-1, ll(task,decompress), "< ~w CLOSE ENTRY ‘~a’ ~w", [Arch,EntryName,In]),
      close(In)
    )
  ).
decompress_entry(_, _, _, _).



%! decompress_file_entry(+Hash:atom, +EntryName:atom, +Properties:list(compound), +In:stream) is det.
%! decompress_file_entry(+Hash:atom, +EntryName:atom, +Properties:list(compound), +In:stream, +Format:atom) is det.

decompress_file_entry(Hash, EntryName, Props, In) :-
  memberchk(format(Format), Props),
  decompress_file_entry(Hash, EntryName, Props, In, Format).


% leaf node: archive entry
decompress_file_entry(EntryHash, data, _, In, raw) :- !,
  hash_file(EntryHash, dirty, File),
  setup_call_cleanup(
    open(File, write, Out, [type(binary)]),
    copy_stream_data(In, Out),
    close(Out)
  ).
% non-leaf node: archive
decompress_file_entry(ArchHash, EntryName, Props, In, Format) :-
  hash_entry_hash(ArchHash, EntryName, EntryHash),
  hash_file(EntryHash, compressed, File),
  setup_call_cleanup(
    open(File, write, Out, [type(binary)]),
    copy_stream_data(In, Out),
    close_metadata(ArchHash, decompressWrite, Out)
  ),
  % Copy task files for the decompressed entry.
  copy_task_files(ArchHash, EntryHash),
  write_meta_entry(ArchHash, EntryName, EntryHash, Format, Props),
  % End this task from the perspective of the entry.
  end_task(EntryHash, downloaded).
