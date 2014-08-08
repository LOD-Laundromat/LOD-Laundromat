:- module(
  md5,
  [
    md5_base_url/2, % +Md5:atom
                    % -Base:url
    md5_bnode_base/2, % +Md5:atom
                      % -BaseComponents:compound
    md5_clean_url/2, % +Md5:atom
                     % -Location:url
    md5_directory/2 % +Md5:atom
                    % -Directory:atom
  ]
).

/** <module> MD5

MD5 support predicates.

@author Wouter Beek
@version 2014/06, 2014/08
*/

:- use_module(library(filesex)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(uri)).

:- use_module(lwm(lwm_settings)).



%! md5_base_url(+Md5:atom, -Base:url) is det.
% The base URL that is used for the clean data document with the given MD5.

md5_base_url(Md5, Base):-
  lwm_scheme(Scheme),
  lwm_authority(Authority),
  atomic_list_concat(['',Md5], '/', Path1),
  atomic_concat(Path1, '#', Path2),
  uri_components(Base, uri_components(Scheme,Authority,Path2,_,_)).


%! md5_bnode_base(+Md5:atom, -BaseComponents:compound) is det.

md5_bnode_base(Md5, Scheme-Authority-Md5):-
  lwm_scheme(Scheme),
  lwm_authority(Authority).


%! md5_clean_url(+Md5:atom, -Location:url) is det.

md5_clean_url(Md5, Location):-
  atomic_list_concat([Md5,'clean.nt.gz'], '/', Path),
  http_link_to_id(clean, path_postfix(Path), Location).


%! md5_directory(+Md5:atom, -Directory:atom) is det.

md5_directory(Md5, Md5Dir):-
  % Place data documents in the data subdirectory.
  absolute_file_name(data(.), DataDir, [access(write),file_type(directory)]),

  % Add the LOD Washing Machine version to the directory path.
  lwm_version(Version1),
  atom_number(Version2, Version1),
  directory_file_path(DataDir, Version2, VersionDir),
  make_directory_path(VersionDir),

  % Add the MD5 hash to the directory path.
  directory_file_path(VersionDir, Md5, Md5Dir),
  make_directory_path(Md5Dir).

