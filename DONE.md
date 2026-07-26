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

**Superseded in part by Nr 4.** RESP3 is now served, so `HELLO 3` negotiates instead
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

`HELLO AUTH` is still refused, carried forward as `TODO.md` entry 10, and settled
there as Nr 5.

## 5. HELLO AUTH runs the real AUTH and takes its OK back [26-07-2026]

*Was `TODO.md` entry 10.*

`HELLO 2 AUTH user pass` used to answer an error telling the client to send `AUTH`
separately. The obstacle was that barch's `AUTH` replies `OK` by pushing it, so
running it from inside `HELLO` put that `OK` in front of the handshake - the same
class of corruption the reply shape work had just fixed elsewhere.

The earlier entry assumed this needed `auth_api.cpp` split into an authenticating
function and a thin command wrapper. It did not. `caller` gained one method:

```cpp
virtual bool pop_value(Variable& into);
```

which takes the value most recently pushed back off the reply, from the aggregate
under construction if there is one and from the reply itself otherwise. `HELLO` now
builds an argument list with `AUTH` back in front, calls the ordinary `::AUTH`, and
pops its answer rather than letting it travel. No change to the authentication logic
at all, so nothing security sensitive moved.

Failure needs no special handling: `AUTH` queues an error, and `rpc_caller::call`
already turns a queued error into a failed call, so the handshake is never written
and the client gets exactly what `AUTH` said. The default `pop_value` answers false,
so a reply builder that cannot rewind - `vk_caller` - is unaffected.

Verified that the success reply is the handshake alone: 14 elements, seven pairs, an
`OK` travelling in front would make it 15. A wrong password and an unknown user both
fail with `authentication failed` and no handshake, and `AUTH` with one argument is a
syntax error. The point of the whole thing is covered too - a redis-py client
constructed with `username`/`password` and `protocol=3` connects and works on its
own, which is the `HELLO 3 AUTH default empty` handshake it sends unprompted.

## 6. The length parameter is gone from end_array [26-07-2026]

*Was `TODO.md` entry 3.*

`end_array(size_t length)` took a length that both implementations ignored, while
callers passed three different things into it - `KEYS` the reply count, `SCAN` a
literal 1, `STATS` a literal 0 - so it read as though it did something. It is now
`end_array()`, and `end_map()` and `end_set()` lost theirs with it for consistency.

36 sites across nine files, all mechanical: the arguments were only ever literals or
plain identifiers, so one pattern covered the declarations, the overrides and every
call. `rpc_caller` never looked at the value, and `vk_caller` counts elements itself
in `call_counter` and passes that to `ValkeyModule_ReplySetArrayLength`, which is
untouched - so there was no behaviour to change.

The compiler is the real check on a signature change like this: it either resolves
every call site or it does not build. It builds clean across all targets, and the
full suite passes unchanged - 17 python tests, `redispytest` over both RESP2 and
RESP3, and `globdifftest`.

## 7. Audit of the other commands that could open an empty array [26-07-2026]

*Was `TODO.md` entry 2.*

`bpop` used an empty top level array to mean "no answer yet" and relied on the splat
to discard it. The question was whether any other command did the same. 24 sites call
`start_array`, `start_map` or `start_set`.

Every one of them that can be reached over RESP was driven into a zero result state
and answers correctly: `KEYS`, `VALUES`, `RANGE`, `HKEYS`, `ZRANGE`, `ZRANGEBYSCORE`,
`ZPOPMIN`, `ZPOPMAX` and `ZINTER` all give an empty array, `HGETALL` an empty map,
`SCAN` an empty array behind its cursor, and the counting forms `ZCARD`,
`ZINTERCARD` and `VALUES ... COUNT` a number. None of them treats an empty array as a
sentinel, so `bpop` was the only one. Those cases are now pinned in
`replyshapetest.py` rather than left as an inspection result.

One real inconsistency turned up, in `ZOPER`, the shared body behind `ZDIFF`,
`ZINTER`, `ZINTERCARD` and the `*STORE` forms. It opened the array under
`aggr == agg_none && store.empty()` and closed it under `store.empty()` alone, so an
aggregating call with no destination closed an array it never opened. That was a
no-op, because `end_array` does nothing with an empty stack, but it read as a matched
pair when it was not. The close now carries the same condition as the open, which is
behaviour preserving - verified byte for byte against the same probe before and after.
Whether the mismatched path is reachable at all is unproven: it needs
`replies != 0` with an aggregate and no store, and every aggregating call tried
returns through the `replies == 0` branch above it.

Four of the 24 could not be audited this way because they are not registered for
RESP: `HGETEX` is commented out in `barch_apis.cpp`, `HQUERY` and `ZCOUNT` were never
added, and `HUPDATEEX` is a helper with a different signature rather than a command.
`COMMAND` is unregistered too. That overlaps with entry 1 and is carried forward as
entry 11.

A false alarm worth recording. The first probe made it look as though `ZINTER`
returned nothing for a non-empty intersection and `ZDIFF` returned the intersection.
Both were the probe's fault: the two sets shared members but with different scores,
and barch intersects on member and score together, while `ZRANGE key 0 -1` is a score
range here rather than an index range, so it reported both sets as empty. With
asymmetric sets and matching scores `ZINTER`, `ZDIFF` and `ZINTERCARD` are all
correct. Nothing was changed on the strength of the first reading.

Covered by the RESP3 section of `replyshapetest.py` - handshake arriving as a map,
arrays keeping shape and arity at every count, null reaching the client as `None`,
booleans arriving as `True`/`False` rather than 1 and 0, and a double surviving the
round trip on both protocols. `redispytest.py` now takes the version from
`BARCH_TEST_RESP` and CTest runs it twice, so the whole command surface is exercised
over RESP2 and RESP3.

## 8. The swig_api.cpp flat view verified at every site [26-07-2026]

*Was `TODO.md` entry 1.*

The 49 sites in `swig_api.cpp` were moved onto `flat_size`, `flat_empty`, `flat_at`
and `append_flat` by mechanical substitution, and the compiler cannot check that kind
of change: a nested array is a perfectly good `Value`, so a missed site would return
one opaque value where it used to return several, silently. The binding tests that
existed reached only some of them.

`test/bindingtest.py` now drives the binding surface directly rather than over RESP -
33 methods across `KeyValue`, `HashSet`, `OrderedSet`, `List` and `Caller`. The
assertion that matters is the count: a method that should answer n values has to
answer n, not 1. Asserting only "not empty" would pass on a missed site, which is why
the counts are spelled out. Sites answering a single `Value` are checked not to have
become a sequence. All 49 are correct; nothing was missed.

The test found a segfault, unrelated to the substitution and predating it.
`Caller::call` began:

```cpp
std::string cn = std::string{params[0]};
auto ic = barch_functions->find(cn);
```

`params` is a member holding the previous call's arguments, and it is only assigned
the current method further down. So the lookup ran whatever the object was asked for
last time, and on a freshly constructed object `params` is empty, so `params[0]` read
past the end and crashed. Confirmed against `HEAD` that this is not something the
flat view work introduced. The lookup now uses `method`, and `params` is built before
it is read. Verified by putting the old body back: unfixed segfaults with exit 139,
fixed passes.

Three expectations of mine were wrong rather than the code, and were corrected in the
test rather than "fixed" in the source:

- `HashSet::expireat` always inserts its flags token, so an empty string becomes an
  empty argument the spec refuses. It needs a real flag such as `NX`.
- `OrderedSet::range` over an empty span answers a single null, not an empty vector -
  `if (sc.flat_empty()) return {nullptr};`. That is what it has always done, and
  `flat_empty` agreeing with the old `results.empty()` is exactly what wanted pinning.
- `OrderedSet::revrange` takes its bounds ascending like `range`; only the order of
  the answer is reversed.

## 9. rpc_caller and vk_caller reconciled on the discarded array [26-07-2026]

*Was `TODO.md` entry 4.*

`bpop` opened its array before knowing whether it could pop anything, and when it had
to block it took the array back with `discard_array()`. That worked on the RESP path,
where the reply is assembled in memory and can be rewound, but the base default could
only close the array instead - a builder that streams as it goes, as the valkey module
one does with `VALKEYMODULE_POSTPONED_LEN`, cannot unsend a header it has already
written. So the same blocking pop contributed nothing on RESP and an empty array under
valkey.

Settled the way the entry suggested: do not open the array until there is something to
put in it. `bpop` now opens it immediately before its first push, which is the only
place anything is written, and closes it only if something was popped. Nothing is
opened when the call goes on to register a block, so there is nothing to take back and
both builders behave the same.

`discard_array()` is gone from `caller` and `rpc_caller`. It had no other user, and
leaving it would have been a trap: its base default is exactly the divergence this
entry set out to remove, so the next caller to reach for it would have reintroduced
the same split.

The RESP side is covered by the blocking pop section of `replyshapetest.py` and by
`bstartest.py`. The valkey module side is not exercised here - it needs a valkey
server, which only the `TestStarter` sub project builds - so the claim for that path
rests on `start_array` no longer being reached rather than on a test.

One thing that looks like a change and is not: `bstartest.py` prints its `b` and `c`
results in a different order from run to run. Both the main thread and the `ctest`
thread block on `testkey1`, and only one value is ever pushed, so exactly one of them
wins and the other times out. The test allows `None` for either. Five consecutive runs
all pass with the winner varying.

