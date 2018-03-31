:- module(ll_decompress, [ll_decompress/0]).

/** <module> LOD Laundromat: Decompress

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(archive)).
:- use_module(library(zlib)).

:- use_module(library(archive_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(ll/ll_generics)).





ll_decompress :-
  gtrace,
  % precondition
  with_mutex(ll_decompress, (
    find_hash_file(downloaded, Hash, File),
    delete_file(File)
  )),
  % operation
  decompress_file(Hash).

% open dirty file
decompress_file(Hash) :-
  hash_file(Hash, dirty, File),
  setup_call_cleanup(
    open(File, read, In),
    decompress_stream(Hash, In),
    close_metadata(In, ReadMeta)
  ),
  write_meta_stream(Hash, decompressRead, ReadMeta).

% open archive
decompress_stream(Hash, In) :-
  setup_call_cleanup(
    (
      archive_open(In, Arch),
      indent_debug(1, ll(decompress), "> ~w OPEN ARCHIVE ~w", [In,Arch])
    ),
    decompress_archive(Hash, Arch),
    (
      indent_debug(-1, ll(decompress), "< ~w CLOSE ARCHIVE ~w", [Arch,In]),
      archive_close(Arch)
    )
  ).

% open entry
decompress_archive(Hash, Arch) :-
  archive_data_stream(Arch, In, [meta_data(ArchMetas)]), %NONDET
  write_meta_archive(Hash, ArchMetas),
  ArchMetas = [ArchMeta|_],
  indent_debug(1, ll(decompress), "> ~w OPEN ENTRY ~w ‘~a’", [Arch,In,ArchMeta.name]),
  call_cleanup(
    decompress_entry(Hash, ArchMeta.name, In),
    (
      indent_debug(-1, ll(decompress), "< ~w ‘~a’ CLOSE ENTRY ~w", [In,ArchMeta.name,Arch]),
      close(In)
    )
  ).

decompress_entry(Hash, data, In) :- !,
  hash_file(Hash, 'dirty.gz', File),
  setup_call_cleanup(
    gzopen(File, write, Out),
    copy_stream_data(In, Out),
    close_metadata(Out, WriteMeta)
  ),
  write_meta_stream(Hash, decompressWrite, WriteMeta),
  hash_file(Hash, dirty, File0),
  delete_file(File0),
  touch_hash_file(Hash, decompressed).
decompress_entry(Hash1, Entry, In) :-
  hash_entry_hash(Hash1, Entry, Hash2),
  hash_file(Hash2, dirty, File),
  setup_call_cleanup(
    open(File, write, Out),
    copy_stream_data(In, Out),
    close_metadata(Out, WriteMeta)
  ),
  write_meta(Hash1, hasEntry, Hash2),
  write_meta(Hash2, hasArchive, Hash1),
  write_meta_stream(Hash2, decompressWrite, WriteMeta),
  touch_hash_file(Hash1, downloaded).
