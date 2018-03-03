:- module(
  ll_seedlist,
  [
    add_seed/2,       % +Source, +Seed
    clear_seedlist/0,
    seed/1,           % -Seed
    stale_seed/1      % -Seed
  ]
).

/** <module> LOD Laundromat: Seedlist

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(lists)).
:- use_module(library(pairs)).
:- use_module(library(settings)).

:- use_module(library(dcg)).
:- use_module(library(dict)).
:- use_module(library(hash_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(rocks_ext)).
:- use_module(library(sw/rdf_term)).
:- use_module(library(uri_ext)).

:- at_halt(maplist(rocks_close, [seedlist])).

:- initialization
   rocks_init(seedlist, [key(atom),merge(ll_seedlist:merge_dicts),value(term)]).

merge_dicts(partial, _, New, In, Out) :-
  merge_dicts(New, In, Out).
merge_dicts(full, _, Initial, Additions, Out) :-
  merge_dicts([Initial|Additions], Out).

:- setting(default_interval, float, 86400.0,
           "The default interval for recrawling.").





%! add_seed(+Source:atom, +Seed:dict) is det.
%
% Seed contains the following key/value pairs:
%
%   * description(string)
%   * documents(list(atom))
%   * image(atom)
%   * license(atom)
%   * name(atom)
%   * organization(dict)
%     * name(atom)
%     * image(atom)
%   * url(atom)

add_seed(Source, Seed1) :-
  Url{documents: Urls, name: DName0, organization: Org} :< Seed1,
  get_time(Now),
  % interval
  (   catch(http_metadata_last_modified(Url, LMod), _, fail)
  ->  Interval is Now - LMod
  ;   setting(default_interval, Interval)
  ),
  uri_hash(Url, Hash),
  (   % The URL has already been added to the seedlist.
      rocks_key(seedlist, Hash)
  ->  print_message(informational, existing_seed(Url,Hash))
  ;   (Org == null -> OName0 = Source ; _{name: OName0} :< Org),
      % Normalize for Triply names.
      maplist(triply_name, [OName0,DName0], [OName,DName]),
      % prefixes
      atomic_list_concat([OName,DName], /, Name),
      rdf_bnode_iri(Name, BNodePrefix),
      L1 = [
        added-Now,
        documents-Urls,
        interval-Interval,
        name-DName,
        organization-_{name: OName},
        prefixes-[bnode-BNodePrefix],
        processed-0.0,
        url-Url
      ],
      % license
      seed_license(Seed1, L1, L2),
      dict_pairs(Seed2, Hash, L2),
      format(string(Msg), "Added seed: ~a/~a", [OName0,DName0]),
      print_message(informational, Msg),
      rocks_put(seedlist, Hash, Seed2)
  ).

seed_license(Seed, T, L) :-
  _{license: License0} :< Seed,
  (   triply_license(License0, License)
  ->  L = [license-License|T]
  ;   print_message(warning, unrecognized_license(License0)),
      L = []
  ).
seed_license(_, T, T).



%! clear_seedlist is det.

clear_seedlist :-
  rocks_clear(seedlist).



%! seed(-Seed:dict) is nondet.

seed(Seed) :-
  rocks_value(seedlist, Seed).



%! stale_seed(-Seed:dict) is det.
%
% Gives the next stale seed for processing.

stale_seed(Seed) :-
  get_time(Now),
  with_mutex(seedlist, (
    rocks_value(seedlist, Seed),
    Hash{interval: Interval, processed: Processed} :< Seed,
    Processed + Interval < Now,
    rocks_merge(seedlist, Hash, Hash{processed: Now})
  )).





% HELPERS %

%! triply_license(?Url:atom, ?Label:string) is nondet.

triply_license('http://www.opendefinition.org/licenses/cc-zero', "CC0").
triply_license('http://creativecommons.org/licenses/by-nc/2.0/', "CC-BY-NC").
triply_license('http://reference.data.gov.uk/id/open-government-licence', "OGL").
triply_license('http://www.opendefinition.org/licenses/cc-by', "CC-BY").
triply_license('http://www.opendefinition.org/licenses/cc-by-sa', "CC-BY-SA").
triply_license('http://www.opendefinition.org/licenses/cc-zero', "CC0").
triply_license('http://www.opendefinition.org/licenses/gfdl', "GFDL").
triply_license('http://www.opendefinition.org/licenses/odc-by', "ODC-BY").
triply_license('http://www.opendefinition.org/licenses/odc-odbl', "ODC-ODBL").
triply_license('http://www.opendefinition.org/licenses/odc-pddl', "ODC-PDDL").



%! triply_name(+Name:atom, -TriplyName:atom) is det.
%
% Triply names can only contain alpha-numeric characters and hyphens.

triply_name(Name1, Name3) :-
  atom_phrase(triply_name_, Name1, Name2),
  atom_length(Name2, Length),
  (Length =< 40 -> Name3 = Name2 ; sub_atom(Name2, 0, 40, _, Name3)).

triply_name_, [Code] -->
  [Code],
  {code_type(Code, alnum)}, !,
  triply_name_.
triply_name_, "-" -->
  [_], !,
  triply_name_.
triply_name_--> "".
