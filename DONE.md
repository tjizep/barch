# Done

Finished entries from `TODO.md`, newest last. Each records what was actually found,
which is not always what the original entry predicted.

## 1. Out of bounds read in both glob matchers [26-07-2026]

*Was `TODO.md` entry 5.*

`glob.cpp` read one byte past the end of the pattern in four places, not the two the
original entry described. The star run collapse and the trailing star loop each
appear in both `asterisk_impl` and `stringmatchlen_impl`:

```c
while (patternLen && pattern[1] == '*')   // guarded patternLen, not index 1
while (*pattern == '*')                   // no guard at all
```

Now `patternLen > 1 && pattern[1] == '*'` and `patternLen && *pattern == '*'`.

The read was not the whole problem. When the byte past the end happened to be `*`,
the loop consumed it and drove `patternLen` to 0 or -1. A negative length reads as
truthy in `while (patternLen && stringLen)`, and `if (patternLen == 0 && stringLen ==
0) return 1` then never fires, so matches silently became misses and a negative
length propagated into the recursive call. Unfixed, 12 of 16 probes gave the wrong
answer.

Covered by stage 5 of `test/globdifftest.cpp`. It carries expected answers rather
than comparing the two implementations, because both matchers shared the bug - stages
1 to 4 report zero disagreements on the unfixed code, so a differential could never
have found it. Two passes are needed:

- string literals with a hostile trailing `*`, which reproduce the wrong answer
  deterministically in any build but stay invisible to a sanitizer, since a literal
  `"**"` is three bytes and index 1 is still owned memory
- exactly sized `new char[len]` buffers, where index `len` lands in a redzone. This
  pass asserts nothing about the result, because the neighbouring byte is arbitrary;
  it exists only so a sanitizer build has something to trap

Verified both ways against a copy of the unfixed file: unfixed gives 12 wrong answers
and an ASAN `heap-buffer-overflow` in `asterisk_impl`, fixed gives neither.

One expectation in the original probe set was wrong and was corrected rather than
"fixed" in the code: `*` does not match an empty subject. The main loop is `while
(patternLen && stringLen)`, so with an empty subject it never runs and only an empty
pattern returns 1. That is stock redis behaviour.

## 2. VALUES globs over values and answers with keys [26-07-2026]

*Was `TODO.md` entry 6.*

Confirmed intended, no code change. `VALUES 3` matches the value `"3"` and replies
with `solo:only`, the key holding it - it is the inverse lookup `KEYS` cannot do.
`COUNT` replaces the reply with the number of matches.

The question was raised because `replyshapetest.py` had been written against observed
behaviour, so the assertions could equally have been recording a bug. They are now a
specification, and the test comment says so.

The behaviour was also missing from the command's own documentation: the block above
`VALUES` in `barch.cpp` said "match against all values using a glob pattern" without
mentioning that the reply is keys, which is the surprising half. That is now written
down where someone reading the implementation will see it.

## 3. HELLO implemented for RESP2, protocol 3 refused with NOPROTO [26-07-2026]

*Was `TODO.md` entry 7.*

`HELLO` did not exist, so barch answered `unknown command` to the handshake every
modern redis client opens with. redis-py sends it whenever the protocol is not 2, and
a newer release flipped `DEFAULT_RESP_VERSION` from 2 to 3, which is what broke CI:
the failure was at connect, before any test command ran.

`HELLO` now lives in `barch.cpp` and is registered in `barch_apis.cpp` under the
`connection` category. It answers `HELLO` and `HELLO 2` with the usual handshake
fields as a flat array, which is how RESP2 carries a map - `server`, `version`,
`proto`, `id`, `mode`, `role` and an empty `modules` array. Anything outside protocol
2 is refused with `NOPROTO unsupported protocol version`, and a non numeric version
with `Protocol version is not an integer or out of range`, both matching redis's own
wording. `SETNAME` is accepted and ignored, as `CLIENT SETINFO` already was.

Two deliberate limits:

- `AUTH` inside `HELLO` is refused rather than supported. barch's `AUTH` replies `OK`
  on success and there is no way to run it from inside `HELLO` without that `OK`
  landing in front of the handshake and corrupting the reply. Separating the
  authentication from the reply would mean refactoring `auth_api.cpp`, which is out of
  proportion to the benefit: the `HELLO AUTH` form is only reachable for a RESP2
  client that also has credentials, and such a client can send `AUTH` as its own
  command. Clients are told so in the error.
- A RESP3 client still cannot connect. It now gets a correct, diagnostic `NOPROTO`
  instead of `unknown command`, but the connection still fails, because redis-py
  rejects a handshake whose `proto` does not match what it asked for. Closing that gap
  needs real RESP3 in the writer and is carried forward as `TODO.md` entry 9.

Covered by the HELLO section of `replyshapetest.py`: field presence and pairing at
both `HELLO` and `HELLO 2`, `proto` being 2, `modules` arriving as a nested empty
array, `NOPROTO` for versions 3, 1 and 0, the non numeric error, `SETNAME` accepted
and an unknown option rejected.

O**Superseded in part by Nr 4.** RESP3 is now served, so `HELLO 3` negotiates instead
of being refused and only versions outside 2..3 answer `NOPROTO`.

## 4. RESP3 support, so a default configured client connects [26-07-2026]

*Was `TODO.md` entry 9.*

A client left on its own defaults asks for RESP3, and barch could only answer
`NOPROTO`, so it failed to connect. Every test had to pass `protocol=2` to work
around it. RESP3 is now served and the workaround is no longer load bearing.

RESP3 is a superset: `+`, `-`, `:`, `$` and `*` mean the same in both versions, so
only the shapes that gained a type of their own are switched on the negotiated
version. A connection that never sends `HELLO 3` sees byte for byte what it always
did.

- `variable.h` gains `map_t`, `set_t` and `verbatim_t`, appended to `variable_t` so
  the existing indices stay put, plus `var_map`, `var_set` and `var_verbatim`. A map
  and a set carry the same elements an array does and exist only so the writer can
  tell the three apart. `Variable::elements()` reaches into whichever of the three it
  is, and the numeric and string accessors treat them as they already treated arrays.
- `caller.h` gains `start_map`/`end_map`, `start_set`/`end_set`, `push_verbatim` and
  the protocol accessors. Every one defaults to the RESP2 behaviour, so `vk_caller`
  and any other builder that cannot tell the shapes apart stays correct without
  changes.
- `rpc_caller` closes an aggregate through one `close_aggregate(kind)` path shared by
  array, map and set, and carries the negotiated version. The caller lives for the
  life of the session, so the negotiation sticks for every command that follows.
- `redis_parser.h` threads the version through `rwrite` and writes `%` for a map, `~`
  for a set, `_` for null, `#t`/`#f` for a boolean, `,` for a double and `=` for a
  verbatim string when the connection is on 3, and the RESP2 spelling of each when it
  is not. A map counts pairs rather than elements, which is the one place the two
  differ by more than a sigil.
- `HELLO` accepts 2 and 3, sets the version before replying - a client reads the
  handshake with the parser it is about to switch to - and answers with a map, which
  the writer renders as `%` or as a flat array to suit.

A real bug fell out of this. `rwrite(TS&, double)` did `rwrite(io, v.c_str())`, and
`const char*` binds to the `bool` overload ahead of `std::string`, because a pointer
to bool conversion is a standard conversion and beats a user defined one. Every
double reply went out as `:1` whatever its value. Confirmed with a standalone
overload resolution check against the same overload set rather than assumed.
`ZINCRBY z 1.5 member` now answers 2.5 where it used to answer 1.

Not implemented: `>` push, which needs pubsub barch does not have, and `(` big
number, which nothing produces. The writer has no case for either.

`HELLO AUTH` is still refused, carried forward as `TODO.md` entry 10.

Covered by the RESP3 section of `replyshapetest.py` - handshake arriving as a map,
arrays keeping shape and arity at every count, null reaching the client as `None`,
booleans arriving as `True`/`False` rather than 1 and 0, and a double surviving the
round trip on both protocols. `redispytest.py` now takes the version from
`BARCH_TEST_RESP` and CTest runs it twice, so the whole command surface is exercised
over RESP2 and RESP3.
