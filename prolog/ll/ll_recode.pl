:- module(ll_recode, [ll_recode/0]).

/** <module> LOD Laundromat: Recode

@author Wouter Beek
@version 2017-2018
*/

decompress_data1(Hash, In1) :-
  hash_file(Hash, dirty, File1),
  guess_file_encoding(File1, FromEnc),
  setup_call_cleanup(
    recode_stream(FromEnc, In1, In2),
    decompress_data2(Hash, In1, In2),
    decompress_data_close(In1, In2)
  ).

decompress_data2(_, In, In) :- !.
decompress_data2(Hash, In1, In2) :-
  indent_debug(1, ll_decompress, "> ~w RECODE ~w", [In1,In2]),
  decompress_data3(Hash, In2).

decompress_data3(Hash, In) :-

decompress_data_close(In, In) :- !.
decompress_data_close(In1, In2) :-
  indent_debug(-1, ll_decompress, "< ~w RECODE ~w", [In1,In2]),
  close(In2).
