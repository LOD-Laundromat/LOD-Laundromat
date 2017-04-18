:- module(
  clean,
  [
    clean/0,
    clean/1,          % +Uri
    seed/1            % -Uri
  ]
).

/** <module> LOD Laundromat

@author Wouter Beek
@version 2017/04
*/

:- use_module(library(apply)).
:- use_module(library(archive)).
:- use_module(library(ckan_api)).
:- use_module(library(file_ext)).
:- use_module(library(http/http_header)).
:- use_module(library(http/http_open)).
:- use_module(library(lists)).
:- use_module(library(md5)).
:- use_module(library(semweb/rdf_guess)).
:- use_module(library(semweb/rdf_http_plugin)).
:- use_module(library(uri/uri_ext)).
:- use_module(library(zlib)).

:- dynamic
    format/2.





%! clean is det.
%! clean(+Uri) is det.
%! clean(+Uri, +File) is det.

clean :-
  clean('http://resource.geolba.ac.at/GeologicUnit/export/GeologicUnit.rdf').


clean(Uri1) :-
  uri_normalized(Uri1, Uri2),
  md5_hash(Uri2, Hash, []),
  download(Uri2, Hash, MediaType),
  forall(
    unpack(Hash, File),
    clean(Uri, MediaType, File)
  ).


clean(Uri, MediaType, File) :-
  uri_comps(Uri, uri(_,_,Segments,_,_)),
  last(Segments, Local),
  file_name_extension(_, Ext, Local),
  setup_call_cleanup(
    open(File, read, In),
    (
      rdf_guess(In, MediaTypes),
      maplist(writeln, MediaTypes)
    ),
    close(In)
  ).



%! download(+Uri, -Hash, -MediaType) is det.

download(Uri, Hash, MediaType) :-
  hash_to_dir(Hash, Dir),
  create_directory(Dir),
  directory_file_path(Dir, clean, File1),
  rdf_http_plugin:rdf_accept_header_value(_, Accept),
  setup_call_cleanup(
    (
      open(File1, write, Out1, [type(binary)]),
      http_open(Uri, In, [headers(Headers),request_header(accept,Accept)])
    ),
    (
      copy_stream_data(In, Out1),
      stream_property(Out1, position(Pos)),
      stream_position_data(byte_count, Pos, NumBytes),
      stream_position_data(char_count, Pos, NumChars),
      stream_position_data(line_count, Pos, NumLines),
      stream_property(Out1, newline(Newline))
    ),
    (
      close(In),
      close(Out1)
    )
  ),
  (   memberchk(content_type(ContentType), Headers)
  ->  http_parse_header_value(content_type, ContentType, MediaType)
  ;   true
  ),
  directory_file_path(Dir, 'download.log.gz', File2),
  setup_call_cleanup(
    gzopen(File2, write, Out2),
    maplist(
      write_term0(Out2),
      [
        uri(Uri),
        byte_count(NumBytes),
        char_count(NumChars),
        line_count(NumLines),
        newline(Newline)
      | Headers
      ]
    ),
    close(Out2)
  ).

write_term0(Out, Header) :-
  write_canonical(Out, Header),
  write(Out, ' .\n').



%! unpack(+Hash, -File) is multi.

unpack(Hash, File2) :-
  hash_to_dir(Hash, Dir),
  directory_file_path(Dir, clean, File1),
  findall(format(Format), archive_format(Format, true), Opts),
  archive_open(File1, read, Arch, [filter(all)|Opts]),
  archive_data_stream(Arch, In, [meta_data(L)]),
  unpack_entry(In, File1, File2, L).

unpack_entry(In, File, File, [_]) :- !,
  close(In).



%! seed(-Uri) is nondet.

seed(Uri) :-
  ckan_site_uri(Site),
  ckan_resource(Site, Res),
  atom_string(Format, Res.format),
  (rdf_format(Format) -> atom_string(Uri, Res.url)).





% HELPERS %

%! archive_formats(?Format, ?Active) is nondet.

archive_format('7zip', true).
archive_format(ar, true).
archive_format(cab, true).
archive_format(cpio, true).
archive_format(empty, true).
archive_format(gnutar, true).
archive_format(iso9660, true).
archive_format(lha, true).
archive_format(mtree, false).
archive_format(rar, true).
archive_format(raw, true).
archive_format(tar, true).
archive_format(xar, true).
archive_format(zip, true).



%! hash_to_dir(+Hash, -Dir) is det.

hash_to_dir(Hash, Dir) :-
  atom_codes(Hash, Cs),
  append([H1,H2], T, Cs),
  atom_codes(Dir1, [H1,H2]),
  atom_codes(Dir2, T),
  atomic_list_concat(['',scratch,wbeek,ll,Dir1,Dir2], /, Dir).



%! rdf_format(?Format) is nondet.

rdf_format('RDF').
rdf_format('SPARQL').
