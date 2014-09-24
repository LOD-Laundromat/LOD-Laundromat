:- module(
  md5,
  [
    md5_base_url/2, % +Md5:atom
                    % -Base:url
    md5_bnode_base/2, % +Md5:atom
                      % -BaseComponents:compound
    md5_directory/2 % +Md5:atom
                    % -Directory:atom
  ]
).

/** <module> MD5

MD5 support predicates.

@author Wouter Beek
@version 2014/06, 2014/08-2014/09
*/

:- use_module(library(filesex)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(uri)).

:- use_module(lwm(lwm_settings)).



%! md5_base_url(+Md5:atom, -Base:url) is det.
% The base URL that is used for the clean data document with the given MD5.

md5_base_url(Md5, Base):-
  ll_scheme(Scheme),
  ll_authority(Authority),
  atomic_list_concat(['',Md5], '/', Path1),
  atomic_concat(Path1, '#', Path2),
  uri_components(Base, uri_components(Scheme,Authority,Path2,_,_)).


%! md5_bnode_base(+Md5:atom, -BaseComponents:compound) is det.

md5_bnode_base(Md5, Scheme-Authority-Md5):-
  ll_scheme(Scheme),
  ll_authority(Authority).


%! md5_directory(+Md5:atom, -Directory:atom) is det.
% Returns the absolute directory of a specific MD5.

md5_directory(Md5, Dir):-
  % Retrieve the enclosing directory
  % for the current LOD Washing Machine version.
  lwm_version_directory(VersionDir),
  
  % Add the MD5 hash to the directory path.
  directory_file_path(VersionDir, Md5, Dir),
  make_directory_path(Dir).

