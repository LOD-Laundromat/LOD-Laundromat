:- module(
  liri,
  [
    iri_liri/2, % +Iri, -Liri
    iri_liri/3  % +Base, +Iri, -Liri
  ]
).

/** <module> LOD Laundromat IRIs (LIRIs)

@author Wouter Beek
@version 2016/05
*/

:- use_module(library(uri)).

iri_liri(AbsoluteIri, Liri) :-
  uri_components(AbsoluteIri, uri_components(Scheme1,Authority1,Path1,Query1,Fragment1)),
  uri_authority_components(Authority1, uri_authority(User1,Password1,Host1,Port1)),
  liri_phrase(scheme, Scheme1, Scheme2),
  liri_phrase(user, User1, User2),
  liri_phrase(password, Password1, Password2),
  liri_phrase(host, Host1, Host2),
  liri_phrase(port, Port1, Port2),
  liri_phrase(path, Path1, Path2),
  liri_phrase(query, Query1, Query2),
  liri_phrase(fragment, Fragment1, Fragment2),
  uri_authority_components(Authority2, uri_authority(User2,Password2,Host2,Port2)),
  uri_components(Liri, uri_components(Scheme2,Authority2,Path2,Query2,Fragment2)).


iri_liri(BaseIri, RelativeIri, Liri) :-
  uri_resolve(RelativeIri, BaseIri, AbsoluteIri),
  iri_liri(AbsoluteIri, Liri).


% scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
scheme,  [C]   --> 'ALPHA'(C),       scheme0.
scheme0, [C]   --> 'ALPHA'(C),    !, scheme0.
scheme0, [C]   --> 'DIGIT'(_, C), !, scheme0.
scheme0, [0'+] --> "+",           !, scheme0.
scheme0, [0'-] --> "-",           !, scheme0.
scheme0, [0'.] --> ".",           !, scheme0.
scheme0        --> [].


% iuserinfo = *( iunreserved / pct-encoded / sub-delims / ":" )
user, [C]   --> iunreserved(C), !, user.
user, [C]   --> sub_delims(C),  !, user.
user, [0':] --> ":",            !, user.
user, [C]   --> pct_encoded(C), !, user.
user        --> [].


% ihost = IP-literal / IPv4address / ireg-name
%ihost --> ip_literal.
%ihost --> ipv4address.
ihost --> ireg_name.


% ipath-abempty = *( "/" isegment )
ipath_abempty, [0'/] --> "/", !, isegment, ipath_abempty.
ipath_abempty        --> [].


% ipath-absolute = "/" [ isegment-nz *( "/" isegment ) ]
ipath_absolute, [0'/]  --> "/", (isegment_nz -> isegments ; "").


% ipath-empty = 0<ipchar>
ipath_empty --> [].


% ipath-rootless = isegment-nz *( "/" isegment )
ipath_rootless --> isegment_nz, isegments.


% ireg-name = *( iunreserved / pct-encoded / sub-delims )
ireg_name, [C] --> iunreserved(C), !, ireg_name.
ireg_name, [C] --> sub_delims(C),  !, ireg_name.
ireg_name, [C] --> pct_encoded(C), !, ireg_name.
ireg_name --> [].


% port = *DIGIT
port, [C] --> 'DIGIT'(_, C), !, port.
port      --> [].


liri_phrase(_, X, X) :- var(X), !.
liri_phrase(Dcg_1, X, Z) :-
  atom_codes(X, Xs),
  phrase(deescape, Xs, Ys),
  Dcg_0 =.. [Dcg_1,Ys],
  phrase(Dcg_0, Ys, Zs),
  atom_codes(Z, Zs).


deescape, [C] -->
  "%",
  'HEXDIG'(H1),
  'HEXDIG'(H2), !,
  {C is H1 * 16 + H2},
  deescape.
deescape, [C] -->
  [C], !,
  deescape.
deescape --> [].


% ALPHA = %x41-5A / %x61-7A   ; A-Z / a-z
'ALPHA', [C] --> [C], {between(0x41, 0x5A, C)}.
'ALPHA', [C] --> [C], {between(0x61, 0x7A, C)}.


% DIGIT = %x30-39   ; 0-9
'DIGIT'(N, C) --> [C], {between(0x30, 0x39, C), N is C - 48}.


% HEXDIG =  DIGIT / "A" / "B" / "C" / "D" / "E" / "F"
'HEXDIG'(N,  C    ) --> 'DIGIT'(N, C).
'HEXDIG'(10, [0'A]) --> "A".
'HEXDIG'(11, [0'B]) --> "B".
'HEXDIG'(12, [0'C]) --> "C".
'HEXDIG'(13, [0'D]) --> "D".
'HEXDIG'(14, [0'E]) --> "E".
'HEXDIG'(15, [0'F]) --> "F".


% iunreserved = ALPHA / DIGIT / "-" / "." / "_" / "~" / ucschar
iunreserved,       --> 'ALPHA'.
iunreserved, [C]   --> 'DIGIT'(_, C).
iunreserved, [0'-] --> "-".
iunreserved, [0'.] --> ".".
iunreserved, [0'_] --> "_".
iunreserved, [0'~] --> "~".
iunreserved        --> ucschar.


% ucschar = %xA0-D7FF / %xF900-FDCF / %xFDF0-FFEF
%         / %x10000-1FFFD / %x20000-2FFFD / %x30000-3FFFD
%         / %x40000-4FFFD / %x50000-5FFFD / %x60000-6FFFD
%         / %x70000-7FFFD / %x80000-8FFFD / %x90000-9FFFD
%         / %xA0000-AFFFD / %xB0000-BFFFD / %xC0000-CFFFD
%         / %xD0000-DFFFD / %xE1000-EFFFD
ucschar, [C] --> [C], {between(0xA0,    0xD7FF,  C)}.
ucschar, [C] --> [C], {between(0xF900,  0xFDCF,  C)}.
ucschar, [C] --> [C], {between(0xFDF0,  0xFFEF,  C)}.
ucschar, [C] --> [C], {between(0x10000, 0x1FFFD, C)}.
ucschar, [C] --> [C], {between(0x20000, 0x2FFFD, C)}.
ucschar, [C] --> [C], {between(0x30000, 0x3FFFD, C)}.
ucschar, [C] --> [C], {between(0x40000, 0x4FFFD, C)}.
ucschar, [C] --> [C], {between(0x50000, 0x5FFFD, C)}.
ucschar, [C] --> [C], {between(0x60000, 0x6FFFD, C)}.
ucschar, [C] --> [C], {between(0x70000, 0x7FFFD, C)}.
ucschar, [C] --> [C], {between(0x80000, 0x8FFFD, C)}.
ucschar, [C] --> [C], {between(0x90000, 0x9FFFD, C)}.
ucschar, [C] --> [C], {between(0xA0000, 0xAFFFD, C)}.
ucschar, [C] --> [C], {between(0xB0000, 0xBFFFD, C)}.
ucschar, [C] --> [C], {between(0xC0000, 0xCFFFD, C)}.
ucschar, [C] --> [C], {between(0xD0000, 0xDFFFD, C)}.
ucschar, [C] --> [C], {between(0xE1000, 0xEFFFD, C)}.


% pct-encoded = "%" HEXDIG HEXDIG
pct_encoded, [0'%,C,D] -->
  {must_be(between(0, 255), C), X is C // 16, Y is C mod 16},
  "%", 'HEXDIG'(X, C), 'HEXDIG'(Y, D).


% sub-delims = "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="
sub_delims, [0'!] --> "!".
sub_delims, [0'$] --> "$".
sub_delims, [0'&] --> "&".
sub_delims, [0''] --> "'".
sub_delims, [0'(] --> "(".
sub_delims, [0')] --> ")".
sub_delims, [0'*] --> "*".
sub_delims, [0'+] --> "+".
sub_delims, [0',] --> ",".
sub_delims, [0';] --> ";".
sub_delims, [0'=] --> "=".


isegments, [0'/] --> "/", !, isegment.
isegments --> [].


% isegment = *ipchar
isegment([H|T]) --> ipchar(H), !, isegment(T).
isegment([])    --> [].


% isegment-nz = 1*ipchar
isegment_nz, [C] --> ipchar, isegment_nz0.
isegment_nz0     --> ipchar, isegment_nz0.
isegment_nz0     --> [].


% ipchar = iunreserved / pct-encoded / sub-delims / ":" / "@"
ipchar, [C]   --> iunreserved, !, ipchar.
ipchar, [C]   --> sub_delims,  !, ipchar.
ipchar, [0':] --> ":",         !, ipchar.
ipchar, [0'@] --> "@",         !, ipchar.
ipchar, [C])  --> pct_encoded, !, ipchar.
ipchar        --> "".
