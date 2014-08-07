:- module(
  run_multithread,
  [
  ]
).

/** <module> Download LOD multithread

Multithreaded approach towards downloading LOD.

@author Wouter Beek
@tbd
@version 2014/03-2014/06
*/

:- use_module(library(lists)).
:- use_module(library(pair)).
:- use_module(library(thread)).
:- use_module(library(uri)).

:- use_module(lwm(download_lod)).



%! download_lod_authority(
%!   +DataDirectory:compound,
%!   +AuthorityPair:pair(atom,list(pair(atom)))
%! ) is det.
% Downloads the datasets at the given authority.
%
% An authority is represented as a pair of an authority name
% and a list of CKAN resources that -- according to the metadata --
% reside at that authority.

% Skip the first N authorities.
download_lod_authority(DataDir, _-Urls):-
  forall(
    member(Url, Urls),
    (
      % Add another LOD input to the pool.
      register_input(Url),
      download_lod_files(DataDir)
    )
  ).


download_lod_multithread:-
  % Process the resources by authority.
  % This avoids being blocked by servers that do not allow
  % multiple simultaneous requests.
  findall(
    Authority-Uri,
    (
      member(Uri, Uris3),
      uri_components(Uri, Components),
      uri_data(authority, Components, Authority)
    ),
    Pairs1
  ),
  group_pairs_by_key(Pairs1, Pairs2),
  
  % Construct the set of goals that will be distributed accross
  % the given number of treads.
  findall(
    download_lod_authority(DataDir, Pair),
    member(Pair, Pairs2),
    Goals
  ),


  % Run the goals in threads.
  % The number of threads can be given as an option.
  thread_create(run_goals_in_threads(N, Goals), _, []).


%! run_goals_in_threads(+NumberOfThreads:nonneg, :Goals) is det.

run_goals_in_threads(N, Goals):-
  N > 1, !,
  concurrent(N, Goals, []).
run_goals_in_threads(_, Goals):-
  maplist(call, Goals).

