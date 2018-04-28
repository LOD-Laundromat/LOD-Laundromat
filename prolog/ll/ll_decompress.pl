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
:- use_module(library(ll/ll_metadata)).
:- use_module(library(sw/rdf_prefix)).

ll_decompress :-
  % precondition
  with_mutex(ll_decompress, (
    find_hash_file(downloaded, Hash, TaskFile),
    ignore(read_term_from_file(TaskFile, MediaType)),
    delete_file(TaskFile)
  )),
  indent_debug(1, ll(_,decompress), "> decompressing ~a", [Hash]),
  write_meta_now(Hash, decompressBegin),
  % operation
  catch(decompress_file(Hash), E, true),
  % postcondition
  write_meta_now(Hash, decompressEnd),
  (   var(E)
  ->  % Check whether there is any data to recode, or whether all data
      % is now stored under different hashes.
      hash_file(Hash, 'dirty.gz', DataFile),
      (   exists_file(DataFile)
      ->  end_task(Hash, decompressed, MediaType)
      ;   finish(Hash)
      )
  ;   write_meta_error(Hash, E),
      finish(Hash)
  ),
  indent_debug(1, ll(_,decompress), "> decompressing ~a", [Hash]).



% open dirty file
decompress_file(Hash) :-
  hash_file(Hash, dirty, File),
  setup_call_cleanup(
    open(File, read, In),
    decompress_stream(Hash, In),
    close_metadata(Hash, decompressRead, In)
  ).

% open archive
decompress_stream(Hash, In) :-
  setup_call_cleanup(
    (
      archive_open(In, Arch),
      indent_debug(1, ll(_,decompress), "> ~w OPEN ARCHIVE ~w", [In,Arch])
    ),
    decompress_archive(Hash, Arch),
    (
      indent_debug(-1, ll(_,decompress), "< ~w CLOSE ARCHIVE ~w", [Arch,In]),
      archive_close(Arch)
    )
  ).

% open entry
decompress_archive(Hash, Arch) :-
  archive_data_stream(Arch, In, [meta_data(ArchMetas)]), %NONDET
  write_meta_archive(Hash, ArchMetas),
  ArchMetas = [ArchMeta|_],
  indent_debug(1, ll(_,decompress), "> ~w OPEN ENTRY ~w ‘~a’", [Arch,In,ArchMeta.name]),
  call_cleanup(
    decompress_entry(Hash, ArchMeta.name, In),
    (
      indent_debug(-1, ll(_,decompress), "< ~w ‘~a’ CLOSE ENTRY ~w", [In,ArchMeta.name,Arch]),
      close(In)
    )
  ).

% leaf node
decompress_entry(Hash, data, In) :- !,
  hash_file(Hash, 'dirty.gz', File),
  setup_call_cleanup(
    gzopen(File, write, Out),
    copy_stream_data(In, Out),
    close_metadata(Hash, decompressWrite, Out)
  ),
  hash_file(Hash, dirty, File0),
  delete_file(File0).
% non-leaf node
decompress_entry(Hash1, Entry, In) :-
  hash_entry_hash(Hash1, Entry, Hash2),
  hash_file(Hash2, dirty, File),
  setup_call_cleanup(
    open(File, write, Out),
    copy_stream_data(In, Out),
    close_metadata(Hash2, decompressWrite, Out)
  ),
  write_meta_entry(Hash1, Hash2),
  touch_hash_file(Hash2, downloaded).
