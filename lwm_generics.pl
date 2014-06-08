:- module(
  lwm_generics,
  [
    data_directory/1, % -DataDirectory:atom
    download_dirty/2, % +Url:url
                      % -DirtyFile:atom
    lwm_version/1, % -Version:positive_integer
    set_data_directory/1, % +DataDirectory:atom
    url_to_md5/3 % +Url:url
                 % +Coordinate:list(nonneg)
                 % -Md5:atom
  ]
).

/** <module> Download LOD generics

Generic predicates that are used in the LOD download process.

@author Wouter Beek
@version 2014/04-2014/06
*/

:- use_module(library(filesex)).
:- use_module(library(http/http_open)).
:- use_module(library(option)).
:- use_module(library(semweb/rdf_db)).

:- use_module(generics(db_ext)).
:- use_module(generics(uri_ext)).
:- use_module(http(http_download)).

%! data_directory(?DataDirectory:atom) is semidet.

:- dynamic(data_directory/1).

:- public(ssl_verify/5).



%! download_dirty(+Url:url, -DirtyFile:atom) is det.

download_dirty(Url, DirtyFile):-
  lod_accept_header_value(AcceptValue),
  url_nested_file(data(.), Url, DirtyDir),
  make_directory_path(DirtyDir),
  directory_file_path(DirtyDir, data, DirtyFile),
  download_to_file(
    Url,
    DirtyFile,
    [cert_verify_hook(ssl_verify),
     header(content_length, ContentLength),
     header(content_type, ContentType),
     header(last_modified, LastModified),
     request_header('Accept'=AcceptValue)],
  ).

  setup_call_cleanup(
    http_open_lod(
    ),
    (
      store_http(
        Md5,
        [content_length(ContentLength),
         content_type(ContentType),
         last_modified(LastModified)]
      ),
      clean_datastream(Url, Coord, Md5, Read)
    ),
    close(Read)
  ).

lod_accept_header_value(Value):-
  findall(
    Value,
    (
      lod_content_type(ContentType, Q),
      format(atom(Value), '~a; q=~1f', [ContentType,Q])
    ),
    Values
  ),
  atomic_list_concat(Values, ', ', Value).

% RDFa
lod_content_type('text/html',              0.3).
% N-Quads
lod_content_type('application/n-quads',    0.8).
% N-Triples
lod_content_type('application/n-triples',  0.8).
% RDF/XML
lod_content_type('application/rdf+xml',    0.7).
lod_content_type('text/rdf+xml',           0.7).
lod_content_type('application/xhtml+xml',  0.3).
lod_content_type('application/xml',        0.3).
lod_content_type('text/xml',               0.3).
lod_content_type('application/rss+xml',    0.5).
% Trig
lod_content_type('application/trig',       0.8).
lod_content_type('application/x-trig',     0.5).
% Turtle
lod_content_type('text/turtle',            0.9).
lod_content_type('application/x-turtle',   0.5).
lod_content_type('application/turtle',     0.5).
lod_content_type('application/rdf+turtle', 0.5).
% N3
lod_content_type('text/n3',                0.8).
lod_content_type('text/rdf+n3',            0.5).
% All
lod_content_type('*/*',                    0.1).

%! ssl_verify(+SSL, +ProblemCert, +AllCerts, +FirstCert, +Error)
% Currently we accept all certificates.

ssl_verify(_, _, _, _, _).


lwm_version(8).


%! set_data_directory(+DataDirectory:atom) is det.

set_data_directory(DataDir):-
  % Assert the data directory.
  db_replace_novel(data_directory(DataDir), [e]).


%! url_to_md5(+Url:url, +Coordinate:list(nonneg), -Md5:atom) is det.

url_to_md5(Url, Coord, Md5):-
  atomic_list_concat([Url|Coord], ' ', UrlCoord),
  rdf_atom_md5(UrlCoord, 1, Md5).

