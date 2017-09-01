:- module(ll_unarchive, [ll_unarchive/0]).

/** <module> LOD Laundromat: Unarchive

@author Wouter Beek
@version 2017/09
*/

:- use_module(library(archive)).
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(lists)).
:- use_module(library(ll/ll_seedlist)).
:- use_module(library(os_ext)).

ll_unarchive :-
  with_mutex(unarchive, (
    rocks(seedlist, Hash, Dict),
    _{status: downloaded} :< Dict,
    rocks_merge(seedlist, Hash, _{status:unarchiving})
  )),
  hash_file('/home/wbeek/data/ll', Hash1, dirty, File),
  uchardet_file(File, FromEnc),
  findall(Hash2, ll_unarchive1(Hash1, File, FromEnc, Hash2), Children),
  with_mutex(unarchive, (
    rocks_merge(seedlist, Hash, _{children: Children, status: unarchived})
  )).

% open dirty file
ll_unarchive1(Hash1, File, FromEnc, Hash2) :-
  setup_call_cleanup(
    open(File, read, In),
    ll_unarchive2(Hash1, In, FromEnc, Hash2),
    close(In)
  ).

% open archive
ll_unarchive2(Hash1, In, FromEnc, Hash2) :-
  findall(format(Format), archive_format(Format), Formats),
  setup_call_cleanup(
    archive_open(In, Archive, [close_parent(false),filter(all)|Formats]),
    ll_unarchive3(Hash1, Archive, FromEnc, Hash2),
    archive_close(Archive)
  ).

% open entry
ll_unarchive3(Hash1, Archive, FromEnc, Hash2) :-
  archive_data_stream(Archive, In, [meta_data(Meta1)]), %NONDET
  Meta1 = [Dict|_],
  _{name: Entry} :< Dict,
  reverse(Meta1, Meta2),
  md5(Hash1-Entry, Hash2),
  call_cleanup(
    ll_unarchive4(Hash1, In, FromEnc, Hash2),
    close(In)
  ).

% recode entry stream
ll_unarchive4(Hash1, In1, FromEnc, Hash2) :-
  setup_call_cleanup(
    recode_stream(FromEnc, In1, In2),
    ll_unarchive5(Hash1, Hash2, In2),
    close(In2)
  ).

% compute hash over recoded entry stream
ll_unarchive5(Hash1, Hash2, In1) :-
  setup_call_cleanup(
    open_hash(In1, In2, []),
    ll_unarchive6(Hash1, Hash2, In2),
    close(In2)
  ).

% store dirty entry file
ll_unarchive6(Hash1, Hash2, In) :-
  hash_file('/home/wbeek/data/ll', Hash2, dirty, File),
  setup_call_cleanup(
    open(File, write, Out),
    copy_stream_data(In, Out),
    close(Out)
  ),
  add_seed(Hash, downloaded, _{parent: Hash1}).
