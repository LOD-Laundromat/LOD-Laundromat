 :- module(ll_recode, [ll_recode/0]).

/** <module> LOD Laundromat: Recode

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).
:- use_module(library(ordsets)).
:- use_module(library(yall)).

:- use_module(library(debug_ext)).
:- use_module(library(file_ext)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(media_type)).
:- use_module(library(stream_ext)).
:- use_module(library(string_ext)).
:- use_module(library(xml_ext)).

ll_recode :-
  % precondition
  with_mutex(ll_recode, (
    ldfs_file(Hash, false, decompressed, TaskFile),
    delete_file(TaskFile)
  )),
  (debugging(ll(task,recode,Hash)) -> gtrace ; true),
  indent_debug(1, ll(task,recode), "> recoding ~a", [Hash]),
  write_meta_now(Hash, recodeBegin),
  % operation
  catch(recode_file(Hash), E, true),
  % postcondition
  write_meta_now(Hash, recodeEnd),
  handle_status(Hash, recoded, E),
  indent_debug(-1, ll(task,recode), "< recoding ~a", [Hash]).

recode_file(Hash) :-
  hash_file(Hash, dirty, File),
  ignore(guess_encoding_(Hash, File, GuessEnc)),
  ignore((
    read_task_memory(Hash, http_media_type, MediaType),
    media_type_encoding(MediaType, HttpEnc)
  )),
  ignore(xml_file_encoding(File, XmlEnc)),
  write_meta_encoding(Hash, GuessEnc, HttpEnc, XmlEnc),
  choose_encoding(GuessEnc, HttpEnc, XmlEnc, Enc),
  write_meta_quad(Hash, encoding, str(Enc)),
  recode_to_utf8(File, Enc).

guess_encoding_(Hash, File, Enc2) :-
  process_create(path(uchardet), [file(File)], [stdout(pipe(ProcOut))]),
  call_cleanup(
    (
      read_string(ProcOut, String1),
      string_strip(String1, "\n", String2),
      atom_string(Enc1, String2)
    ),
    close_metadata(Hash, recodeWrite, ProcOut)
  ),
  clean_encoding(Enc1, Enc2),
  Enc2 \== unknown.

% 1. XML header
choose_encoding(_, _, XmlEnc, XmlEnc) :-
  ground(XmlEnc), !.
% 2. HTTP Content-Type header
choose_encoding(_, HttpEnc, _, HttpEnc) :-
  ground(HttpEnc), !.
% 3. Guessed encoding
choose_encoding(GuessEnc, _, _, GuessEnc) :-
  ground(GuessEnc),
  GuessEnc \== octet.
% 4. No encoding or binary (`octet').
choose_encoding(_, _, _, _) :-
  throw(error(no_encoding,ll_recode)).

% unknown encoding
recode_to_utf8(_, Enc) :-
  var(Enc), !.
% already UTF-8
recode_to_utf8(_, Enc) :-
  generalize_encoding(Enc, utf8), !.
% must be recoded to UTF-8
recode_to_utf8(File, Enc) :-
  file_name_extension(File, tmp, TmpFile),
  process_create(
    path(iconv),
    ['-c','-f',Enc,'-t','utf-8','-o',file(TmpFile),file(File)],
    []
  ),
  rename_file(TmpFile, File).

generalize_encoding(Var, Var) :-
  var(Var), !.
generalize_encoding(ascii, utf8) :- !.
generalize_encoding(Enc, Enc).
