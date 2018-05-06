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
    find_hash_file(Hash, downloaded, TaskFile),
    delete_file(TaskFile)
  )),
  (debugging(ll(task,decompress,Hash)) -> gtrace ; true),
  indent_debug(1, ll(task,decompress), "> decompressing ~a", [Hash]),
  write_meta_now(Hash, decompressBegin),
  % operation
  catch(decompress_file(Hash), E, true),
  (var(E) -> true ; write_meta_error(Hash, E), finish(Hash)),
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
      (archive_next_header(Arch, Name) -> true ; !, fail)
    ),
    (
      findall(Prop, archive_header_property(Arch, Prop), Props),
      decompress_entry(Hash, Arch, Name, Props)
    )
  ).



%! decompress_entry(+Hash:atom, +Archive:blob, +Name:atom, +Properties:list(compound)) is det.
%! decompress_entry(+Hash:atom, +Archive:blob, +Name:atom, +Properties:list(compound),
%!                  +Type:atom) is det.

decompress_entry(Hash, Arch, Name, Props) :-
  memberchk(filetype(Type), Props),
  decompress_entry(Hash, Arch, Name, Props, Type).


decompress_entry(Hash, Arch, Name, Props, file) :-
  setup_call_cleanup(
    (
      archive_open_entry(Arch, In),
      indent_debug(1, ll(task,decompress), "> ~w OPEN ENTRY ‘~a’ ~w", [Arch,Name,In])
    ),
    decompress_file_entry(Hash, Name, Props, In),
    (
      indent_debug(-1, ll(task,decompress), "< ~w CLOSE ENTRY ‘~a’ ~w", [Arch,Name,In]),
      close(In)
    )
  ).
decompress_entry(_, _, _, _, _).



%! decompress_file_entry(+Hash:atom, +Name:atom, +Properties:list(compound), +In:stream) is det.
%! decompress_file_entry(+Hash:atom, +Name:atom, +Properties:list(compound), +In:stream,
%!                       +Format:atom) is det.

decompress_file_entry(Hash, Name, Props, In) :-
  memberchk(format(Format), Props),
  decompress_file_entry(Hash, Name, Props, In, Format).


% leaf node
decompress_file_entry(Hash, data, _, In, raw) :- !,
  decompress_file_entry_stream(Hash, In, dirty),
  write_meta_quad(Hash, rdf:type, ll:'Archive', graph:meta),
  end_task(Hash, decompressed).
% non-leaf node
decompress_file_entry(Hash1, Name, Props, In, Format) :-
  hash_entry_hash(Hash1, Name, Hash2),
  decompress_file_entry_stream(Hash2, In, compressed),
  % Copy task files for the decompressed entry.
  copy_task_files(Hash1, Hash2),
  % Store metadata.
  memberchk(mtime(MTime), Props),
  memberchk(permissions(Permissions), Props),
  memberchk(size(Size), Props),
  write_meta_entry(Hash1, Hash2, Format, MTime, Name, Permissions, Size),
  % End this task from the perspective of the entry.
  end_task(Hash2, downloaded).



%! decompress_file_entry_stream(+Hash:atom, +In:stream,
%!                              +Local:oneof([compressed,dirty])) is det.

decompress_file_entry_stream(Hash, In, Local) :-
  hash_file(Hash, Local, File),
  setup_call_cleanup(
    open(File, write, Out, [type(binary)]),
    copy_stream_data(In, Out),
    close_metadata(Hash, decompressWrite, Out)
  ).
