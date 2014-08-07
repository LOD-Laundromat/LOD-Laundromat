:- module(
  lwm_generics,
  [
    data_directory/1, % -DataDirectory:atom
    lwm_base/2, % +Md5:atom
                % -Base:triple(atom)
    lwm_bnode_base/2, % +Md5:atom
                      % -Base:triple(atom)
    lwm_datadoc_location/2, % +Md5:atom
                            % -Location:url
    lwm_version/1, % -Version:positive_integer
    md5_to_dir/2, % +Md5:atom
                  % -Md5Directory:atom
    set_data_directory/1 % +DataDirectory:atom
  ]
).

/** <module> Download LOD generics

Generic predicates that are used in the LOD download process.

Also contains Configuration settings for project LOD-Washing-Machine.

@author Wouter Beek
@version 2014/04-2014/06, 2014/08
*/

:- use_module(library(filesex)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(option)).
:- use_module(library(uri)).

:- use_module(generics(db_ext)).
:- use_module(generics(meta_ext)).

:- use_module(plSparql(sparql_api)).
:- use_module(plSparql(sparql_db)).

%! data_directory(?DataDirectory:atom) is semidet.

:- dynamic(data_directory/1).

%! ssl_verify(+SSL, +ProblemCert, +AllCerts, +FirstCert, +Error)
% Currently we accept all certificates.

:- public(ssl_verify/5).
   ssl_verify(_, _, _, _, _).



%! lwm_authortity(?Authortity:atom) is semidet.

lwm_authortity('lodlaundromat.org').


%! lwm_base(+Md5:atom, -Base:triple(atom)) is det.

lwm_base(Md5, Base):-
  lwm_scheme(Scheme),
  lwm_authortity(Authority),
  atomic_list_concat(['',Md5], '/', Path1),
  atomic_concat(Path1, '#', Path2),
  uri_components(Base, uri_components(Scheme,Authority,Path2,_,_)).


%! lwm_bnode_base(+Md5:atom, -Base:triple(atom)) is det.

lwm_bnode_base(Md5, Scheme-Authority-Md5):-
  lwm_scheme(Scheme),
  lwm_authortity(Authority).


lwm_datadoc_location(Md5, Location):-
  atomic_list_concat([Md5,'clean.nt.gz'], '/', Path),
  http_link_to_id(clean, path_postfix(Path), Location).


lwm_scheme(http).


lwm_version(11).


%! md5_to_dir(+Md5:atom, -Md5Directory:atom) is det.

md5_to_dir(Md5, Md5Dir):-
  % Place data documents in the data subdirectory.
  absolute_file_name(data(.), DataDir, [access(write),file_type(directory)]),
  
  % Add the LOD Washing Machine version to the directory path.
  lwm_version(Version),
  directory_file_path(DataDir, Version, VersionDir),
  make_directory_path(VersionDir),
  
  % Add the MD5 hash to the directory path.
  directory_file_path(VersionDir, Md5, Md5Dir),
  make_directory_path(Md5Dir).


%! set_data_directory(+DataDirectory:atom) is det.

set_data_directory(DataDir):-
  % Assert the data directory.
  db_replace(data_directory(DataDir), [e]).
