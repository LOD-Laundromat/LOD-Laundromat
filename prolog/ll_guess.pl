:- module(ll_guess, [ll_guess/0]).

/** <module> LOD Laundromat: Guess format

@author Wouter Beek
@version 2017/09-2017/11
*/

:- use_module(library(semweb/rdf_api)).
:- use_module(library(semweb/rdf_guess)).

:- use_module(ll_generics).
:- use_module(ll_seedlist).

ll_guess :-
  with_mutex(ll_guess, (
    seed(Seed),
    Hash{status: unarchived} :< Seed,
    seed_merge(Hash{status: guessing})
  )),
  debug(ll(guess), "┌─> guessing (~a)", [Hash]),
  get_time(Begin),
  hash_file(Hash, dirty, File),
  (   rdf_guess_file(File, MediaType)
  ->  true
  ;   print_message(warning,ll_guess(Hash))
  ),
  (rdf_media_type_format(MediaType, Format) -> true ; gtrace),
  get_time(End),
  debug(ll(guess), "└─< guessed ~a", [Format]),
  seed_merge(Hash{format: Format, status: guessed, timestamp: Begin-End}).
