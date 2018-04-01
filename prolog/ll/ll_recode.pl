 :- module(ll_recode, [ll_recode/0]).

/** <module> LOD Laundromat: Recode

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).
:- use_module(library(ordsets)).
:- use_module(library(yall)).
:- use_module(library(zlib)).

:- use_module(library(debug_ext)).
:- use_module(library(file_ext)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(media_type)).
:- use_module(library(stream_ext)).
:- use_module(library(xml_ext)).





ll_recode :-
  % precondition
  with_mutex(ll_recode, (
    find_hash_file(decompressed, Hash, File),
    ignore(read_term_from_file(File, MediaType)),
    delete_file(File)
  )),
  debug(ll(recode), "┌> recoding ~a", [Hash]),
  write_meta_now(Hash, recodeBegin),
  % operation
  catch(recode_file(Hash, MediaType), E, true),
  % postcondition
  write_meta_now(Hash, recodeEnd),
  failure_success(Hash, recoded, _, E),
  debug(ll(decompress), "└─> recoding ~a", [Hash]).

recode_file(Hash, MediaType) :-
  hash_file(Hash, 'dirty.gz', File),
  ignore(xml_file_encoding(File, XmlEnc)),
  ignore(media_type_encoding(MediaType, HttpEnc)),
  guess_file_encoding(File, GuessEnc),
  choose_encoding(GuessEnc, HttpEnc, XmlEnc, Enc),
  warn_encoding(GuessEnc, HttpEnc, XmlEnc, Enc),
  recode_to_utf8(Hash, File, Enc).

% 1. XML header
choose_encoding(_, _, XmlEnc, XmlEnc) :-
  ground(XmlEnc), !.
% 2. HTTP Content-Type header
choose_encoding(_, HttpEnc, _, HttpEnc) :-
  ground(HttpEnc), !.
% 3. Guessed encoding
choose_encoding(GuessEnc, _, _, GuessEnc).

warn_encoding(X1, X2, X3, Z) :-
  maplist(generalize_encoding, [X1,X2,X3], [Y1,Y2,Y3]),
  include(ground, [Y1,Y2,Y3], L),
  list_to_ord_set(L, S),
  (   S = [_,_|_]
  ->  print_message(warning, unclear_encoding([X1,X2,X3],Z))
  ;   true
  ).

generalize_encoding(Var, Var) :-
  var(Var), !.
generalize_encoding(ascii, utf8) :- !.
generalize_encoding(Enc, Enc).

% unknown encoding
recode_to_utf8(_, _, Enc) :-
  var(Enc), !.
% already UTF-8
recode_to_utf8(_, _, Enc) :-
  generalize_encoding(Enc, utf8), !.
% must be recoded to UTF-8
recode_to_utf8(Hash, File1, Enc) :-
  file_name_extension(File1, tmp, File2),
  setup_call_cleanup(
    maplist(gzopen, [File1,File2], [read,write], [In,Out]),
    recode_stream(Enc, In, Out),
    maplist(close_metadata, [In,Out], Metas)
  ),
  maplist(write_meta_stream(Hash), [readRecode,writeRecode], Metas),
  rename_file(File1, File2).
