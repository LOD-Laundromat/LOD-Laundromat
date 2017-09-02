:- module(ll_unarchive, [ll_unarchive/0]).

/** <module> LOD Laundromat: Unarchive

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(apply)).
:- use_module(library(archive)).
:- use_module(library(debug_ext)).
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(hash_stream)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_seedlist)).
:- use_module(library(zlib)).

ll_unarchive :-
  with_mutex(ll_unarchive, (
    seed(Seed),
    Hash1{status: filed} :< Seed,
    seed_merge(Hash1{status: unarchiving})
  )),
  hash_file(Hash1, dirty, File),
  findall(Entry, ll_unarchive1(Hash1, File, Entry), Entries),
  (   Entries == [data]
  ->  Dict = Hash1{status: unarchived}
  ;   maplist(hash_entry_hash(Hash1), Entries, Children),
      Dict = Hash1{children: Children, status: depleted}
  ),
  with_mutex(ll_unarchive, seed_merge(Dict)).

% open dirty file
ll_unarchive1(Hash1, File, Entry) :-
  setup_call_cleanup(
    open(File, read, In),
    ll_unarchive2(Hash1, In, Entry),
    close(In)
  ).

% open archive
ll_unarchive2(Hash1, In, Entry) :-
  findall(format(Format), stream_ext:archive_format(Format), Formats),
  setup_call_cleanup(
    (
      archive_open(In, Archive, [close_parent(false),filter(all)|Formats]),
      indent_debug(1, ll, "> ~w OPEN ARCHIVE ~w", [In,Archive])
    ),
    ll_unarchive3(Hash1, Archive, Entry),
    (
      indent_debug(-1, ll, "< ~w CLOSE ARCHIVE ~w", [Archive,In]),
      archive_close(Archive)
    )
  ).

% open entry
ll_unarchive3(Hash1, Archive, Entry) :-
  archive_data_stream(Archive, In, [meta_data(ArchiveMeta)]), %NONDET
  ArchiveMeta = [Dict|_],
  _{name: Entry} :< Dict,
  indent_debug(1, ll, "> ~w OPEN ENTRY ~w ‘~a’", [Archive,In,Entry]),
  call_cleanup(
    ll_unarchive4(Hash1, ArchiveMeta, Entry, In),
    (
      indent_debug(-1, ll, "< ~w ‘~a’ CLOSE ENTRY ~w", [In,Entry,Archive]),
      close(In)
    )
  ).

% recode entry stream
ll_unarchive4(Hash, _, data, In) :- !,
  ll_unarchive_data1(Hash, In).
ll_unarchive4(Hash, ArchiveMeta, Entry, In) :-
  ll_unarchive_entry1(Hash, ArchiveMeta, Entry, In).



% DATA %

ll_unarchive_data1(Hash, In1) :-
  hash_file(Hash, dirty, File1),
  uchardet_file(File1, FromEnc),
  setup_call_cleanup(
    recode_stream(FromEnc, In1, In2),
    ll_unarchive_data2(Hash, In1, In2),
    ll_unarchive_data_close(In1, In2)
  ).

ll_unarchive_data2(_, In, In) :- !.
ll_unarchive_data2(Hash, In1, In2) :-
  indent_debug(1, ll, "> ~w RECODE ~w", [In1,In2]),
  ll_unarchive_data3(Hash, In2).

ll_unarchive_data3(Hash, In) :-
  maplist(hash_file(Hash), [dirty,'dirty.gz'], [File1,File2]),
  setup_call_cleanup(
    gzopen(File2, write, Out, [format(gzip)]),
    copy_stream_data(In, Out),
    close(Out)
  ),
  delete_file(File1).

ll_unarchive_data_close(In, In) :- !.
ll_unarchive_data_close(In1, In2) :-
  indent_debug(-1, ll, "< ~w RECODE ~w", [In1,In2]),
  close(In2).



% ENTRY %

% open output file, whose name is based in `Entry'
ll_unarchive_entry1(Hash1, ArchiveMeta, Entry, In) :-
  hash_entry_hash(Hash1, Entry, Hash2),
  hash_file(Hash2, dirty, File),
  setup_call_cleanup(
    open(File, write, Out),
    ll_unarchive_entry2(Hash1, ArchiveMeta, Hash2, In, Out),
    close(Out)
  ).

% compute the hash over the recoded entry stream's content
% store the dirty entry file
ll_unarchive_entry2(Hash1, ArchiveMeta, Hash2, In1, Out) :-
  setup_call_cleanup(
    (
      open_hash_stream(In1, In2, [algorithm(md5),close_parent(false)]),
      indent_debug(1, ll, "> ~w CALCULATE HASH ~w", [In1,In2])
    ),
    (
      copy_stream_data(In2, Out),
      stream_meta(In2, ContentMeta)
    ),
    (
      indent_debug(-1, ll, "< ~w CALCULATE HASH ~w", [In2,In1]),
      close(In2)
    )
  ),
  seed_add(
    Hash2{
      archive: ArchiveMeta,
      content: ContentMeta,
      parent: Hash1,
      status: filed
    }
  ).
