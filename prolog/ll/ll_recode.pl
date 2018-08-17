 :- module(ll_recode, [ll_recode/0]).

/** <module> LOD Laundromat: Recode

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(debug_ext)).
:- use_module(library(dict)).
:- use_module(library(file_ext)).
:- use_module(library(ll/ll_generics)).
:- use_module(library(ll/ll_metadata)).
:- use_module(library(media_type)).
:- use_module(library(xml_ext)).

ll_recode :-
  % precondition
  start_task(decompressed, Hash, State),
  (debugging(ll(offline,Hash)) -> gtrace ; true),
  indent_debug(1, ll(task,recode), "> recoding ~a", [Hash]),
  write_meta_now(Hash, recodeBegin),
  % operation
  catch(ll_recode(Hash, State), E, true),
  % postcondition
  write_meta_now(Hash, recodeEnd),
  handle_status(Hash, E, recoded, State),
  indent_debug(-1, ll(task,recode), "< recoding ~a", [Hash]).

ll_recode(Hash, State) :-
  hash_file(Hash, 'dirty.gz', File),
  ignore(guess_file_encoding(File, GuessEnc)),
  ignore((
    dict_get(media_type, State, MediaType),
    media_type_encoding(MediaType, HttpEnc)
  )),
  ignore(xml_file_encoding(File, XmlEnc)),
  write_meta_encoding(Hash, GuessEnc, HttpEnc, XmlEnc),
  choose_encoding(GuessEnc, HttpEnc, XmlEnc, Enc),
  write_meta_quad(Hash, encoding, str(Enc)),
  recode_file(Enc, File).

% 1. XML header
choose_encoding(_, _, XmlEnc, XmlEnc) :-
  ground(XmlEnc), !.
% 2. HTTP Content-Type header
choose_encoding(_, HttpEnc, _, HttpEnc) :-
  ground(HttpEnc), !.
% 3. Guessed encoding
choose_encoding(GuessEnc, _, _, GuessEnc) :-
  ground(GuessEnc),
  GuessEnc \== octet, !.
% 4. No encoding or binary (`octet').
choose_encoding(_, _, _, _) :-
  throw(error(no_encoding,ll_recode)).
