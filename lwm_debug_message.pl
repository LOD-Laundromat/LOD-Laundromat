:- module(
  lwm_debug_message,
  [
    document_name//1, % +Document:iri
    lwm_debug_message/1, % ?Topic:compound
    lwm_debug_message/2, % ?Topic:compound
                         % +Message:compound
    lwm_debugging/0
  ]
).

/** <module> LOD Washing Machine: Debug Message

Prints debug messages for the LOD Washing Machine.

@author Wouter Beek
@version 2015/11
*/

:- use_module(library(apply)).
:- use_module(library(dcg/dcg_content)).
:- use_module(library(debug)).
:- use_module(library(lodapi/lodapi_generics)).

:- discontiguous(lwm_debug_message/2).





%! document_name(+Document:iri)// is det.

document_name(Doc) -->
  {document_name(Doc, Name},
  atom(Name).
  



%! lwm_debug_message(+Topic:compound, +Message:compound) is det.
% `Topic` is a debug topic, specified in `library(debug)`.

% Idle loop.
lwm_debug_message(Topic, lwm_idle_loop(Category)):- !,
  % Every category has its own idle loop flag.
  atomic_list_concat([number_of_idle_loops,Category], '_', Flag),
  flag(Flag, X, X + 1),

  debug(Topic, "[IDLE] ~a ~D", [Category,X]).


% Do not print debug message.
lwm_debug_message(Topic, _):-
  nonvar(Topic),
  \+ debugging(Topic), !.


% C-Triples written.
lwm_debug_message(_, ctriples_written(_,0,_)):- !.
lwm_debug_message(
  Topic,
  ctriples_written(_,NumberOfUniqueTriples,NumberOfDuplicateTriples)
):-
  % Duplicates
  (   NumberOfDuplicateTriples =:= 0
  ->  DuplicatesString = ""
  ;   format(
        string(DuplicatesString),
        " (~D duplicates)",
        [NumberOfDuplicateTriples]
      )
  ),
  debug(Topic, "[+~D~s]", [NumberOfUniqueTriples,DuplicatesString]).



% End a process.
lwm_debug_message(Topic, lwm_end(Category1,Source,Status,_)):- !,
  % Category
  upcase_atom(Category1, Category2),

  % Status
  (   Status == true
  ->  true
  ;   Status == false
  ->  debug(Topic, "  [STATUS] FALSE", [])
  ;   debug(Topic, "  [STATUS] ~w", [Status])
  ),

  rdf_global_id(ll:Md5, Document),
  debug(Topic, "[END ~a] ~w ~w", [Category2,Md5,Source]).


% Start a process.
lwm_debug_message(Topic, lwm_start(unpack,Document,Source)):- !,
  lwm_start_generic(Topic, unpack, Document, Source, "").

lwm_debug_message(Topic, lwm_start(Category,Document,Source,Size)):- !,
  % `Size` is the number of bytes.
  NumberOfGigabytes is Size / (1024 ** 3),
  format(string(SizeString), " (~f GB)", [NumberOfGigabytes]),
  lwm_start_generic(Topic, Category, Document, Source, SizeString).

lwm_start_generic(Topic, Category1, Document, Source, SizeString):-
  % Category
  upcase_atom(Category1, Category2),

  % Document source: IRI or IRI+entry (for archives).
  datadoc_source(Document, Source),

  rdf_global_id(ll:Md5, Document),
  debug(Topic, '[START ~a] ~w ~w~s', [Category2,Md5,Source,SizeString]).


% VoID description found
lwm_debug_message(Topic, void_found(Urls)):-
  maplist(void_found(Topic), Urls).



%! lwm_debugging is semidet.
% Succeeds iff the LOD Washing Machine is currently in debugging mode.

lwm_debugging:-
  debugging(_), !.
lwm_debugging:-
  once(debug:debug_md5(_, _)).





% HELPERS %

%! void_found(+Topic:compound, +Url:atom) is det.

void_found(Topic, Url):-
  debug(Topic, '  [VOID] ~a', [Url]).
