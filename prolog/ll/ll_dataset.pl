:- module(
  ll_dataset,
  [
    ll_dataset/1 % +Seed
  ]
).

/** <module> LOD Laundromat: Scrape an individual dataset

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(lists)).
:- use_module(library(process)).
:- use_module(library(readutil)).
:- use_module(library(settings)).
:- use_module(library(zlib)).

:- use_module(library(dict)).
:- use_module(library(file_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(media_type)).
:- use_module(library(tapir)).
:- use_module(library(uri_ext)).

:- use_module(library(ll/ll_document)).





%! ll_dataset(+Seed:dict) is det.

ll_dataset(Seed) :-
  _{name: DName} :< Seed.dataset,
  _{name: OName} :< Seed.organization,
  setting(ll_init:temporary_directory, Dir0),
  directory_file_path(Dir0, Seed.hash, Dir),
  create_directory(Dir),
  forall(
    member(Uri, Seed.documents),
    catch(
      ll_document(Seed.hash, Dir, Uri),
      E,
      print_message(warning, E)
    )
  ),
  directory_file_path(Dir, '*.trig.gz', Wildcard),
  expand_file_name(Wildcard, Files1),
  include(is_nonempty_file, Files1, Files2),
  (   % Do not upload empty datasets.
      Files2 == []
  ->  true
  ;   % Create the organization, unless it already exists.
      ignore(organization_create(_, OName, _{}, _)),
      ignore(dataset_create(OName, DName, _{}, _)),
      maplist(file_arg, Files2, T),
      setting(ll_init:script, Script),
      process_create(path(node), [Script,OName,DName|T], []),
      upload_image(Dir, Seed),
      upload_license(Seed),
      debug(ll, "DONE ~a ~a", [OName,DName])
  ),
  delete_directory_and_contents(Dir).

dataset_image(Dir, Seed, File) :-
  _{image: Url1} :< Seed,
  % We download the URL prior to determining whether it is an image,
  % because we may not be able to download the same image a second
  % time.
  downcase_atom(Url1, Url2),
  uri_file_extensions(Url2, Exts),
  once((
    member(Ext, Exts),
    media_type_extension(media(image/_,_), Ext)
  )),
  file_name_extension(avatar, Ext, Local),
  directory_file_path(Dir, Local, File),
  setup_call_cleanup(
    open(File, write, Out, [type(binary)]),
    (
      catch(http_open2(Url2, In1, [failure(404)]), _, fail),
      call_cleanup(
        copy_stream_data(In1, Out),
        close(In1)
      )
    ),
    close(Out)
  ), !,
  (   setup_call_cleanup(
        open(File, read, In2, [type(binary)]),
        is_image(In2),
        close(In2)
      )
  ->  true
  ;   print_message(warning, not_an_image(Url1))
  ).

file_arg(File, file(File)).

is_nonempty_file(File) :-
  setup_call_cleanup(
    gzopen(File, read, In),
    (
      read_line_to_codes(In, _Line1),
      read_line_to_codes(In, _Line2),
      \+ at_end_of_stream(In)
    ),
    close(In)
  ).

upload_image(Dir, Seed) :-
  dataset_image(Dir, Seed, Image), !,
  dataset_property(Seed.organization.name, Seed.dataset.name, avatar(Image), _).
upload_image(_, _).

upload_license(Seed) :-
  get_dict(license, Seed.dataset, License), !,
  dataset_property(Seed.organization.name, Seed.dataset.name, license(License), _).
upload_license(_).
