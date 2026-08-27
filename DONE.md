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

## 10. SCAN threading and service queueing reviewed [26-07-2026]

*Was `TODO.md` entry 8.*

The entry raised three things. Two were real and are fixed; the third turned out to be
deliberate.

**The cursors a SCAN leaves behind.** `SCAN` keeps a per connection iteration holding
shard pointers and a page buffer. One followed to the end drops itself, but an
abandoned one stayed until the connection closed, with nothing reporting it and nothing
capping it - which is what the `TODO` in `SCAN` was pointing at. Now:

- `CLIENT INFO` reports `iters` and `iters-mem`, so a connection leaking cursors is
  visible. `iteration_count()` and `iteration_memory()` on `caller` provide them, the
  memory counting the page buffer, the shard pointers and the struct.
- `max_scan_iterators` caps how many one connection may hold, default 128, registered
  like every other variable so it can be changed at runtime. A `SCAN` that would exceed
  it is refused with an error naming the way out.
- `CLIENT CLEAR_ITERS` drops them and answers how many went, for a client that knows it
  has abandoned scans and would rather not reconnect.

While in `CLIENT INFO` the hardcoded `resp=2` was changed to report the negotiated
version, which had been wrong for every RESP3 connection since Nr 4.

**The single `glob_queue` mutex is intentional and stays.** It admits one glob at a
time, and `logical_allocator::iterate_pages` spawns exactly
`barch::get_iteration_worker_count()` threads to walk the pages - four by default, and
tunable at runtime like anything else. Together they bound scan work at one glob times
N threads however many clients ask for `KEYS` at once, which is what stops a scan
swamping a multi threaded server. The comment above the mutex already said so. Nothing
changed here; it is recorded because the entry read it as an unexamined bottleneck and
it is not one.

Covered by the cursor section of `scanglobtest.py`: the fields being present, abandoned
scans accumulating, memory non zero while held, a completed scan adding nothing, the
clear returning the right count and zeroing both, and the limit refusing the next cursor
with a message that says how to recover.

## 11. ZCOUNT registered, the other three documented as deliberate [26-07-2026]

*Was `TODO.md` entry 11.*

Four commands were implemented but answered `unknown command` over RESP, and nothing
said whether that was a decision or an oversight.

`ZCOUNT` is now a command: declared in `barch_apis.h` and registered in
`barch_apis.cpp` as read/ordered/data. It needed one more thing than the entry
suggested - its definition in `ordered_api.cpp` was the only one in that file without
its own `extern "C"` line, so the declaration expected C linkage and the definition had
C++ linkage. The module built and then failed to load with `undefined symbol: ZCOUNT`.
That is consistent with it never having been exported before, and is why the link error
only appeared once something referred to it.

The other three stay unregistered, and now say so where someone will look:

- `COMMAND` serves the valkey module, where the server asks the module to describe
  itself. A RESP client that sends it gets `unknown command` and falls back, which is
  what we want until there is a command table worth publishing.
- `HGETEX` and `HQUERY` are implemented and reachable from the module side but have not
  been settled over RESP. `HGETEX` in particular shares `HUPDATEEX`'s option parsing,
  and `HUPDATEEX` is not a command at all - it is a helper taking extra arguments, so it
  cannot go in the table as it stands.

Notes to that effect sit next to the commented out registration in `barch_apis.cpp` and
next to the declarations in `barch_apis.h`.

`replyshapetest.py` covers `ZCOUNT` over a full range, a narrow one, an empty one and a
missing key, and pins the other three as answering `unknown command` - so registering
one of them becomes a deliberate change to this test rather than something that drifts
in unnoticed.

## 12. The asynchronous call path started a second read chain [26-07-2026]

*Was `TODO.md` entries 12 and 13.*

A pipeline run failed `TestGlobPerformance` with a SEGFAULT after a `VALUES` call went
seventy five seconds without answering. It looked like a hang, then like a memory
pressure problem, then like a three thousand fold slowdown in writes. It was none of
those.

gdb during the stall showed every server thread idle - the asio pools parked in
`scheduler::do_run_one`, the maintenance thread in its semaphore wait, nothing in
`art::`, no lock held. The server log said why: `redis_parser::read_new_request`
throwing `invalid array size` five times in the same millisecond on the session thread,
at the instant the stall began. The parser had lost its place in the byte stream, the
connection stopped answering, and the client waited at zero CPU on a reply that would
never come.

`KEYS` and `VALUES` are marked asynchronous so a long scan goes to the worker pool
rather than occupying one of the few service threads. When a batch of requests
contained one, `asio_resp_session.h` resumed reading from two places - the worker did
it on completion and the io thread did it as well:

```cpp
for (auto ctx: asynch_calls) {
    asio::post(workers,[this, ctx]() { ... do_write(ctx); do_read(); });
}
...
do_write(stream);
do_read();
```

`do_read()` has no guard: it posts `async_read_some` into the session's single `data_`
buffer and feeds its single `parser`. So from the first asynchronous call onwards there
were two read chains on one socket sharing one buffer and one parser. Each context was
also posted independently, so several `async_write` calls could be outstanding on the
same socket at once and their replies could leave in any order - `run_params` goes out
of its way to make every call after the first asynchronous one asynchronous too "to
preserve order", and that ordering was then lost on the way out.

It survived small traffic and came apart on the next sizeable pipelined write, which is
why the globs themselves looked fine and the load after them died. `KEYS` and `VALUES`
were the only asynchronous commands, so a glob had to run before anything went wrong.

Now a batch owns the connection while it runs. `run_asynch_batch` walks the contexts in
the order they were read, each call on the worker pool, and `write_then` starts the next
only once the previous reply is on the wire - so there is never more than one write
outstanding, replies leave in request order, and the read chain is resumed exactly once,
by the last of them. The latency isolation the asynchronous flag was added for is kept:
the calls still run off the service threads, only the socket work is serialised.

Verified both ways. With the old body restored the new `asyncpipelinetest.py` fails on
the first round with `GET s:00000 answered b'v1'` and 18 parser errors in the log; with
the fix it passes, and so does the reproducer that started this - a load after sixteen
globs goes from 301.5s and ten parser errors to 0.0s and none.


## 13. The hash set looked keys up through a thread_local side channel [01-08-2026]

`barch::hashed_key` stores nothing but a 4 byte logical address; the key bytes live in
the leaf at that address. That is what keeps the index small relative to the data it
indexes, but it left no way to express "find the key with *these* bytes", because a
`hashed_key` for a key that is not stored yet has no address to point at.

The way out taken at the time was a sentinel. `hashed_key(value_type)` built an entry
with `addr == 0`, the bytes were parked in a file scope `thread_local value_type
query_key` by `set_hash_query_context`, and `hashed_key::get_key` returned `query_key`
whenever `addr` was zero. Every lookup was therefore two statements that had to stay
adjacent and in order:

    set_hash_query_context(key);
    auto i = h.find(key);

The reason it was done that way is real and still holds: the obvious alternative is to
materialise a temporary leaf so the query has an address, and that means allocating in
the logical allocator. Read only queries run concurrently under a shared lock, so they
cannot touch the allocator at all. The thread_local avoided the allocation.

What it cost was safety. `query_key` is one variable per thread shared by every shard,
so its lifetime is "until something else on this thread sets it". Anything that ran
between the two statements above - a callback, an iteration, a `dependencies->search()`
into another shard - would silently repoint the query, and the `find` would then be
asking about the wrong bytes. Nothing enforced the pairing, and nothing would have
diagnosed a violation: the lookup would just return the wrong answer, or no answer.
`shard::remove` already had such an interleaving (`dependencies->search(key)` sits
between the two), and only escaped because the dependency happened to be searched with
the same bytes.

The fix is the one art already uses. `art_search` carries the caller's key down the
tree and stores nothing, so it has no such problem; the hash set can be given the same
property by making a lookup a *different type* from a stored entry rather than a
crippled instance of one:

  - `barch::key_query` holds the caller's `value_type` plus the hash of those bytes,
    taken once up front. It is never stored in the set, needs no address, and so never
    needs the allocator.
  - `hk_hash` and `hk_eq` gained overloads for it and declare `is_transparent`, which
    is also what turns on ankerl's heterogeneous lookup for the overflow table.
  - `oh::unordered_set`'s `find`, `erase` and `contains` are templated on the query
    type, so both halves of the double hash accept it. The pointer overloads of
    `erase`/`remove` are SFINAE'd out of the template, or they would lose overload
    resolution to it.
  - `set_hash_query_context`, `query_key` and `hashed_key(value_type)` are gone.

Removing the implicit `hashed_key(value_type)` constructor is what makes this stick.
It was the conversion that let `h.find(key)` compile while quietly depending on state
set elsewhere; without it every one of the eight call sites became a compile error
until it was rewritten as `h.find(key_query{key})`. A future call site that forgets
cannot compile either.

`hashed_key::get_key` still tolerates `addr == 0`, but now returns an empty
`value_type` rather than borrowed state. Address zero is only ever what a slot vacated
by `remove` holds, those slots are guarded by `has[]`, and an empty key matches nothing
because a filtered key always carries its null terminator and so has size >= 1.

Measured: no behaviour change intended and none seen - the 31 tests that run without an
external valkey server all pass. The 16 that need one on 127.0.0.1:7777 fail the same
way before and after, which was confirmed against a stashed tree.

The same hazard one layer down, in `tree_filter_key`, is entry 14.


## 14. The filtered key borrowed a shared per thread buffer [01-08-2026]

Found while fixing entry 13, and it is the same shape: state that a value depends on,
parked somewhere with a lifetime nobody declared.

An art key has to carry its null terminator. `s_filter_key(std::string& temp_key,
value_type key)` supplies one, and it is careful about it - if the key is already
terminated it is returned untouched and no copy is made; only otherwise is a terminated
copy written to `temp_key`, and the returned `value_type` then points into that buffer.
The function itself is fine, and its buffer is a parameter precisely so the caller can
decide where the storage lives.

The convenience wrapper is what leaked. `art::tree::tree_filter_key` called it with a
file scope `thread_local std::string temp_key`, so every filtered key on a thread
pointed into one shared buffer. A filtered key was therefore valid only until the next
`filter_key` anywhere on that thread. Nothing said so, and nothing checked.

Six callers, of which `shard::remove` was actually exposed:

    auto key = filter_key(unfiltered_key);
    ...
    auto dep = dependencies->search(key);   // filters again, same thread, same buffer
    ...
    h.erase(key_query{key});                // and key is still used here

Why it had not bitten: `s_filter_key` only writes the buffer when it has to copy, and
the first call had already produced a terminated key, so the second call took the early
return and left the buffer alone. Correct, but by coincidence - it rests on an invariant
about the *first* call that is nowhere stated, and any caller that filtered a different
key in between would have broken it silently.

Fixed by giving each caller its own buffer rather than by documenting the invariant.
`opt_rpc_insert` already did exactly this with a local `std::string tk`, so this is the
existing idiom, not a new one, and it is close to free: the common case is an already
terminated key, where the local is never written and never allocates. The five other
shard.cpp callers (`update`, `evict`, `remove`, `is_present`, `search`) and the
`art::iterator` constructor now do the same.

With no callers left, `tree_filter_key`, `shard::filter_key`, the pure virtual
`abstract_shard::filter_key` and the `thread_local temp_key` are all gone. As with
entry 13, deleting the convenient wrapper is what stops the hazard coming back: there
is no longer an overload that hides where the storage came from, and `s_filter_key`'s
one remaining form makes the caller name the buffer. The ownership rule is written out
at its declaration.

Also folded in here: `is_avalanching` was declared on `hk_eq`, where ankerl never reads
it, instead of on `hk_hash`, where it suppresses a redundant re-mix of already
avalanched wyhash output. Moved. This changes bucket layout, which is harmless because
`h` is rebuilt from the leaves on load and no hash value is ever persisted.

Measured: the full suite passes, 47 of 47. The 16 tests that need a valkey server were
failing for an unrelated reason - a `dump.rdb` left in the build directory from April,
written in RDB format version 80, which the valkey 8.1 the harness builds refuses with
"Can't handle RDB format version 80", so the server exited before binding 7777 and
every one of those tests reported the symptom as "connection refused". Moving the stale
file aside was all that was needed; it is worth knowing that this failure presents as a
port problem rather than as a load problem.


## 15. The lower bound trace was read back out of a thread_local [01-08-2026]

Third of the same family as entries 13 and 14, and the one with the worst failure mode.

`art.cpp` kept a `thread_local art::trace_list tlb`, filled by `art_search` and by the
one argument `art::lower_bound`. For those two it is legitimate: the trace describes the
path walked to reach a leaf, they discard it as soon as they return, and per thread
storage means concurrent readers under a shared lock do not collide while still reusing
capacity on a very hot path.

What broke the arrangement was `art::get_tlb()`, which handed the buffer back out. Only
one caller used it, and it used it like an out parameter from the previous call:

    art::node_ptr n = lower_bound(t, key);   // fills tlb
    auto &trace = get_tlb();                 // aliases tlb
    ...
    node_ptr new_leaf = updater(n);          // caller supplied, may walk the tree
    zip_update(trace.rbegin(), trace.rend(), new_leaf);

`trace` is a reference into the shared buffer, taken before `updater` runs and used
after it. `updater` comes from outside art - `shard::update` passes one through from its
own caller. Any lookup inside it refills `tlb`, and `zip_update` then walks a trace
belonging to a different key, rewriting parent pointers along a path that has nothing to
do with the leaf being replaced. Entries 13 and 14 could return a wrong answer; this one
corrupts the tree, and would surface later and far away.

The tempting fix - move the buffer onto `art::tree`, where a trace member already exists
- is the wrong direction. A `tree` is shared between threads, so readers that the
thread_local currently keeps apart would start racing on it. The buffer wants to stay per
thread; the read back is the part that has to go.

So `art::update` now declares its own `trace_list` and uses the two argument
`art::lower_bound(trace, t, key)` overload, which already existed for exactly this
purpose. Its lifetime is a local, visible at the point of use, and nothing can refill it.
`get_tlb` is deleted, the buffer is now `static thread_local scratch_trace` and private
to art.cpp, and both overloads of `lower_bound` say in the header which one to reach for
and why.

Two details worth recording. The one argument `lower_bound` counts `statistics::lb_ops`
and the two argument one does not, so `update` now counts it explicitly and the stat is
unchanged. And `update` already had its own try/catch, so dropping the inner one costs
nothing - an exception from the walk is still logged once and still returns false.

Taken together, 13, 14 and 15 were the same mistake three times: a value whose backing
store lived in a thread_local that something else was free to overwrite. The fix each
time was not to document the rule but to delete the accessor that hid it, so the storage
has to be named at the call site. Each removal turned every affected caller into a
compile error until it was rewritten, which is what makes them stay fixed.

Measured: full suite green, 47 of 47.


## 16. Sharding layer defined and keys_api converted onto it [01-08-2026]

Before this, every command that touched storage knew the key space was sharded. Three
separate concerns were copied into each one:

  - routing, `kspace()->get(key)`, at about 50 sites
  - fan out and cross shard ordering, `for (shard : ks->get_shards())`, at about 38
    sites, each re-deriving how to reduce a per shard answer to a global one
  - locking, `storage_release` / `read_lock` / `ks_shared`, chosen by hand at each site

`barch::sharded_store` in sharded_store.h/.cpp now owns all three, and is the only
thing that knows a key space has shards. key_space keeps ownership and lifecycle; the
store is a cheap value made per call over it.

Shape, and why:

  - `shard_for(key)` and `shards()` are virtual, and every other operation is composed
    from those two. That is the whole answer to "may function in a stateful manner
    later": a subclass that keeps a routing table for remote shards, or carries a
    transaction, overrides two methods and inherits the rest.
  - the ordered operations take a callback rather than returning a `value_type`.
    This is forced, not stylistic. `MIN` used to hold `ks_shared` across its
    `push_encoded_key`, because the key it found points into a leaf and the lock is
    what keeps it alive. An operation that took the lock, found the key, released the
    lock and returned the key would hand back a dangling pointer. The callback runs
    inside the lock. The rule is written at the top of the header.
  - locking is per operation and deliberately not uniform, because it already was not:
    a point read locks one shard, an ordered reduction locks the whole space because it
    reads every shard, `count` locks each shard in turn because it only measures a
    distance and never hands a key back, and `glob` locks nothing at all because the
    shards copy each page to a working buffer before matching. Unifying these would
    have been wrong in both directions.
  - `with_key_write` / `with_key_read` are the escape hatch for sequences that must
    hold one lock across several steps: INCR's update-or-create, APPEND's
    read-modify-write, EXPIRE's read-decide-write. They still route and lock inside the
    layer; they just hand the shard out.

keys_api.cpp lost 402 lines and gained 160. The clearest wins: KEYS and VALUES were
near identical 40 line copies differing in one bool, and are now one shared body;
MIN/MAX/LB/UB were four hand rolled reductions with four slightly different spellings
of the same "smaller than what we have so far" comparison, and are now four one line
calls.

Deliberately not changed, and marked as such in comments at each site, because a
refactor should not quietly alter behaviour:

  - TTL treats a tombstone as present with no expiry (-2), where `search` would treat
    it as absent (-1). Kept via `with_key_read`.
  - MGET neither decompresses nor skips tombstones, both of which GET does. Kept.
    These two look like real bugs and are worth settling separately.

One bug was introduced and caught by the suite. Converting GET to
`int r = call.push_null(); store.search(..., [&]{ r = call.push_vt(vt); });` reads as a
default, but `push_null` is not a value - it writes a reply immediately, so a hit
emitted two replies. TestBarchRPC failed on `k.get(str(i)) == str(i)` and a later test
hung on the desynchronised stream. The same mistake was in LENGTH, TTL and EXPIRE. All
four now compute into a `found`/`answered` flag and push exactly once on every path.
Worth remembering: with this caller API a "default value" has to be a default *branch*.

Measured: full suite green, 47 of 47.

Still on the old idiom, see TODO 17 and 18: the other API files, and SCAN, whose cursor
lives on the connection rather than in the store.


## 17. Remaining API files converted onto the sharding layer [01-08-2026]

hash_api, list_api, ordered_api, barch.cpp and configuration.cpp now go through
`barch::sharded_store` as keys_api already did. What the layer needed on top of entry
16 fell into three shapes, and the shapes are the interesting part.

**Containers.** A hash, ordered set or list keeps every member on the shard that owns
the container key, because a member key is a composite built from it. So these commands
route once, by the container, and then do many member operations under one lock -
exactly `with_key_write`, and now also spelled `with_container_write` so the invariant
is named where it is relied on. One thing that had to be preserved carefully: routing
uses the raw container bytes, not the composite, and if a converted site had started
routing on the composite instead the members would have scattered to a different shard
and the data would have been silently lost.

**Held locks.** Several commands cannot take a callback scope, because the lock has to
span a whole function with several exits - `bpop` registers a blocking callback before
it returns. So the layer also hands out guards: `lock_key_write`, `lock_space_write`,
and `write_locked(key)`, which returns the routed shard and a lock on it together and
dereferences to the shard. That last one is what made the twenty ordered_api sites a
two line change each with the bodies untouched, rather than twenty lambda wrappings
with early returns to unpick. Given how entry 16 went, the safer mechanical route was
worth choosing deliberately.

**Whole space.** `each_shard`, `each_shard_write`, `each_shard_read` and
`each_shard_parallel` cover the per shard lifecycle operations - begin/commit/rollback,
clear, pull, load, save, and applying configuration. The lock is named at the call
site, so the choice is visible rather than implied by which of three RAII types
somebody happened to write.

A real bug found on the way: **HDEL and HGETDEL took no lock at all.** Both re-routed
per member - `call.kspace()->get(argv[1])->remove(key, del_report)` inside the loop -
with no `storage_release` anywhere in the function, so the removes ran unsynchronised
against concurrent readers, on a shard other commands were locking properly. Both now
route once and hold a write lock, as HSET always did. This is the kind of thing the
layer exists to stop: the lock was not forgotten so much as invisible, because nothing
about `kspace()->get(k)->remove(...)` suggests a lock is missing.

Two deliberate behaviour changes, both bounded:

  - `each_shard_parallel` uses `shard_thread_processor`, the project's own bounded
    helper, so LOAD and RELOAD no longer spawn one thread per shard. On a default space
    that was 347 threads. SAVE already used it.
  - `is_avalanching` moved in entry 14 is unrelated; nothing else changed shape.

Not converted, and why: `swig_api.cpp` deliberately drives shard 0 for a benchmark, and
key space administration locks two spaces at once, which a single space layer does not
model - see TODO 20.

Measured: full suite green, 47 of 47.

## 18. SCAN cursor split between the connection and the store [01-08-2026]

The question left open in entry 16 was where a scan cursor belongs, given it was a
`struct iteration` living in caller.h while holding a key space pointer and a vector of
shards. The answer turned out to be that the two halves belong in different places, and
trying to put the whole thing in one was what made it awkward:

  - **the lifetime is the connection's.** A scan spans commands, so something must hold
    the cursor between them, and how many a connection may keep open, what they cost,
    and when they are dropped is connection level accounting - `max_scan_iterators`,
    CLIENT INFO's iters and iters-mem, CLIENT CLEAR_ITERS. `caller` still owns the
    collection.
  - **the content is sharding state.** Which shards remain, which page of the current
    one, the copy of that page being walked, and the rule that a key from a pull source
    is only emitted when the shard shadowing it has none of its own. Only the store
    should know any of that.

So `barch::scan_cursor` is defined with the layer, `caller` stores
`shared_ptr<scan_cursor>` and never looks inside one, and `sharded_store::open_scan`
and `sharded_store::scan` do the walking. `push_page`, which was a 60 line static in
keys_api.cpp reaching into both the caller and the shards, is now `scan_page` inside
the layer with no knowledge of replies at all - it takes a callback that returns false
to stop.

SCAN itself is now about thirty lines: get or create a cursor, enforce the per
connection limit, walk, and drop the cursor when the walk reports it is finished. The
cursor's own memory accounting moved onto `scan_cursor::memory()`, so caller's
`iteration_memory` no longer has to know that a cursor contains a page buffer and a
shard list.

This was the first real exercise of the "may function in a stateful manner later"
requirement from entry 16, and it is the shape that requirement was pointing at: state
that outlives one call, held by whoever owns the session, interpreted only by the layer.

Measured: full suite green, 47 of 47.


## 19. SIZE and HEAPBYTES relaxed to read locks [01-08-2026]

Carried over from entry 17, where the write locks were preserved on purpose so that the
sharding conversion changed no locking. Changed here on its own.

Both commands only read counters:

  - `SIZE` sums `shard::get_size()`, which is const and adds `h.size()`, the tree size
    and the source's size, then subtracts the tombstone count.
  - `HEAPBYTES` sums `get_bytes_allocated()` off each allocator, also const.

Neither writes anything, so `storage_release`'s unique lock was giving them exclusive
access to shards they only ever looked at, and blocking every concurrent reader while
they walked all 347 of them.

The detail that makes this safe rather than merely plausible: `get_size()` recurses into
`dependencies->get_size()`, so it reads the source chain as well as the shard, and a
lock covering only the shard would not be enough. `read_lock` already walks
`t->sources()` and takes each one shared before locking the shard itself - the same
chain `storage_release` was locking shared, since it only upgraded the shard at the end.
So the source coverage is unchanged and only the shard's own lock is weakened, which is
exactly the intended change.

`each_shard_write` now has a single caller left, `ApplyEvictionType`, which really does
write shard options.

Noted but not changed: `SIZEALL` reads the same counters through `barch::all_shards`
with no lock at all. That is a cross space walk rather than a single space one, so it
belongs with the cross space question in TODO 20 rather than here.

Measured: full suite green, 47 of 47.


## 20. Command declarations and registrations moved to their own api files [01-08-2026]

barch_apis.h declared every command in the system and barch_apis.cpp registered every
one of them, so both files had to be edited to add a command anywhere, and neither said
anything about which subsystem a command belonged to beyond a comment.

Keys, lists, hashes, ordered sets and info now each declare their own commands in their
own header and register them from their own translation unit through
`register_*_api(function_map&)`, called from `functions_by_name()`. The five blocks of
`(*r)["NAME"] = {...}` are gone from barch_apis.cpp, replaced by five calls. What is
left there is the commands that have no category file yet - see TODO 22.

barch_apis.h keeps the shared vocabulary that all of this is built from: barch_function,
barch_info, the category map, function_map and functions_by_name(). The category
headers include it, so the dependency runs one way.

The naming distinguishes the two registrations that already existed and were easy to
confuse: `add_*_api(ValkeyModuleCtx*)` registers commands with the valkey module, and
`register_*_api(function_map&)` registers them for RESP. Lists have only the second,
which is now stated in list_api.h rather than being apparent from an absence.

Two things worth recording:

**Moving a registration next to its implementation can create an ambiguity.**
`r["HEXPIRE"] = {::HEXPIRE, ...}` compiled fine in barch_apis.cpp, where only the two
argument command was declared. In hash_api.cpp both that and the three argument helper
that HEXPIRE and HEXPIREAT are written in terms of are visible, so the name no longer
resolves and needs a static_cast to the command's own signature. The old code was not
wrong, it just could not see enough to be ambiguous.

**The check that mattered was the command table, not the build.** A registration
silently dropped in the move would compile and link perfectly and only show up as a
command answering "unknown command" at runtime, which the suite might well not cover.
Extracting the registered names from HEAD's barch_apis.cpp and from all six files after
the move gave 112 both times, with nothing lost and nothing added.

Also removed: an empty `class info_api {}` stub in info_api.h, which nothing referenced.

Measured: full suite green, 47 of 47.


## 21. CLIENT INFO emitted a stray $ in front of the id field [01-08-2026]

Spotted while writing TODO 23, because CLIENT LIST would have to reuse the same line
builder and would have inherited it.

`get_info_l` in rpc/asio_resp_session.h built its reply starting `"$id="` where redis
and valkey both start `id=`. The `$` was inside the payload, not RESP framing - the
bulk string header is added by push_string afterwards - so a client saw a first field
literally named `$id`.

Confirmed rather than assumed, by running both servers side by side and comparing:

    barch  : $id=1 addr=127.0.0.1:34340 laddr=127.0.0.1:14000 fd=10 ...
    valkey : id=4 addr=127.0.0.1:57504 laddr=127.0.0.1:7777 fd=18 ...

and again after the change, where barch's line begins `id=1` like valkey's.

Why nothing caught it: the one test that reads CLIENT INFO parses with
`re.findall(r"(\S+?)=(\S*)", s)`, which happily yields a key named `$id`, and then only
asserts on the `iters` and `iters-mem` fields barch adds. A generic parser cannot tell a
misnamed field from a valid one. A client looking up `id` - which is how you find a
connection to CLIENT KILL - would have got nothing.

`git log -S` dates it to 629ce22, release v0.4.3.0b, so it had been shipping for about
seven months.

One process note for next time. The first two attempts to verify the fix reported it
still broken, both times because of my own command rather than the code: valkey rewrites
its process title to `*:7777`, so `pkill -f "valkey-server --port 7777"` never matched
the old process, which kept port 14000 and answered the query from the stale binary.
Then `pkill -9 -f valkey-server` matched the shell running it and killed the test job.
Match server processes with `pgrep -x`/`kill <pid>`, not a `-f` pattern that the killing
command's own command line also contains.

Measured: full suite green, 47 of 47.


## 22. CLIENT LIST implemented [01-08-2026]

CLIENT accepted only INFO, SETINFO and CLEAR_ITERS; LIST now joins them.

The line format needed no work - `get_info_l` in rpc/asio_resp_session.h already emits
the full redis field set for the calling connection, and DONE 21 fixed the stray `$` in
front of the id that would otherwise have been copied into every line of the reply.
What was missing was reach: the sessions live in `tcp_sessions` and `uds_sessions`
inside `server_context` in rpc/server.cpp, and a `caller` only knows about itself.

The walk stays behind the server boundary. `server::list_clients(caller&)` is declared
in server.h with `caller` forward declared, and `server_context::append_client_lines`
does the work, so no session object is ever handed out and the header does not have to
expose `resp_session`.

Locking: the session vectors are appended to by the accept path and nulled out by the
collector thread, both under `session_latch`, so each vector is copied under the latch
and walked outside it. Holding the latch across the walk would block new connections for
as long as the reply takes to build. Copying a vector of `shared_ptr` has the second
effect of keeping every session alive for the duration of the walk, which matters
because building a line reads the socket's endpoints.

Two ways a line can be wrong, both handled: a slot the collector has already swept holds
a null, and a socket can be closed but not yet swept, so both are skipped. Beyond that,
a peer can go away between the `is_open()` test and the endpoint read, and asio reports
that by throwing - that connection is leaving anyway, so its line is dropped rather than
failing the whole command.

Verified against a real valkey rather than by inspection. With four idle connections
held open plus the one asking, both servers answer identically: a bulk string (not an
array), five lines, and three lines after closing two of them - so closed sessions are
excluded rather than accumulating. Under churn, eight threads connecting and
disconnecting in a loop against sixty concurrent CLIENT LIST calls, there were no errors
and no reply that was not a bulk string, and the line count stayed at nine throughout
rather than growing, which is what a leak of swept sessions would have looked like.

Not implemented: redis also takes `CLIENT LIST TYPE normal|master|replica|pubsub` and
`CLIENT LIST ID <id>...`. Both need the session to carry more about itself than it
does, so the command rejects any argument for now. `cmd=client|info` and `fd=10` are
still literals in the line builder - harmless for CLIENT INFO, wrong on every line of a
LIST, and noted in TODO 23's replacement.

Found while testing, unrelated and pre-existing: a bare `PING` answers "Wrong Arity",
because barch's PING is the replication one. See TODO 25.

Measured: full suite green, 47 of 47.


## 23. Redis configuration names, and the other CONFIG subcommands [01-08-2026]

CONFIG GET was already redis shaped - glob patterns, a map reply, values written the way
CONFIG SET takes them back - but every name it knew was barch's own, so a client asking
for `maxmemory` got an empty map. It now answers to redis's names as well, and
RESETSTAT, REWRITE and HELP are implemented.

The mapping is in configuration.cpp, not in the CONFIG command, so GET and SET both get
it. Three kinds:

  - **aliases**, where a redis name means exactly one barch variable: maxmemory,
    maxmemory-policy, maxclients, bind, port and the three tls files.
  - **aliases needing a value translated**. maxmemory-policy is the one - barch's
    eviction_policy already uses redis's vocabulary for every policy except "do not
    evict", which barch spells none and redis spells noeviction.
  - **fixed answers**, where barch has no such setting but can say something true:
    appendonly and appendfsync are no because there is no append only file at all,
    cluster-enabled no, daemonize no, timeout 0. `save` is derived from save_interval
    and max_modifications_before_save, which is exactly redis's "<seconds> <changes>".

These refuse CONFIG SET rather than accepting a write that would do nothing, and say
why: `cannot set 'appendonly': there is no append only file`. That needed
is_read_only_configuration(name, why), because the existing int return could not tell a
refusal apart from a value that failed to parse.

`databases` is deliberately absent, and it is the interesting omission. Barch's key
spaces are named rather than numbered and SELECT takes a name, so any number here would
mislead a client about what SELECT accepts. An absent name reads as an empty map, which
is what redis answers for a parameter it does not know - a better answer than a
plausible looking lie.

**The byte units were the real trap.** maxmemory looked like a pure alias and is not.
Redis reads 1k as 1000 and 1kb as 1024; barch's parser takes a single letter and reads k
as 1024. Passing the string through failed outright on the two letter forms - CONFIG SET
maxmemory 512mb was rejected - and would have been silently wrong by about 5% on every
value written the decimal way, which is the kind of thing that surfaces months later as
a memory limit that was never what anybody set. A redis size is now resolved to a plain
byte count before it reaches barch. Checked against a real valkey across 512mb, 512m,
1gb, 1k and 1kb: identical on all five.

RESETSTAT clears the counters and the per command call counts INFO reports as
commandstats. It deliberately leaves the gauges alone, and that distinction is not
cosmetic: node and leaf counts, logical_allocated and shards describe what the server
holds right now, so zeroing them would make it misreport its own state; and
read_locks_active, redis_sessions and the rest are incremented then decremented, so
zeroing one mid flight wraps it to near UINT64_MAX. Verified: leaf_nodes held at 201
across a reset while vacuum_count went 8673 to 0, and the data was untouched.

REWRITE answers with the error redis gives when it was started without a config file,
which is barch's permanent condition - it has none of its own, being configured through
its host server. A client that already handles that error needs no new case.

TestConfig caught this, which is the point of it: it asserts CONFIG GET * matches the
registered set exactly, so a variable added on one side and not the other fails rather
than being skipped. The redis names are a new category rather than drift, so the test
now knows about both sets, still round trips only barch's own variables, and gained
coverage for the alias behaviour, the byte units, the policy translation, the read only
refusals, and RESETSTAT leaving gauges alone. Its old assertion that REWRITE is
unsupported moved onto an unknown keyword instead.

Found while testing, unrelated: bare `INFO` answers "not implemented" - see TODO 26.

Measured: full suite green, 47 of 47.


## 24. PING renamed to RPING and redis's PING added [01-08-2026]

barch's PING took `PING <host> <port>` and reached out to another barch to check it
answered. Redis's PING takes nothing and answers PONG, and is what every client sends
as a health check and what a connection pool sends before handing a connection out - so
a bare PING got "Wrong Arity", which is a poor first impression for a server claiming
to speak RESP.

The two are unrelated, not two spellings of one idea: one opens a connection to
somewhere else and says nothing about the server being asked. So the replication one is
now RPING, and PING is redis's - no argument gives the simple string PONG, one argument
echoes it back as a bulk string, more is an arity error. Checked against a real valkey
on all three.

Three things made this cheap, and each was worth confirming rather than assuming:

  - **replication does not send the string.** `temp_client::ping()` writes the binary
    opcode `cmd_ping`, so nothing on the wire between barch nodes names the command,
    and renaming it cannot break a running pair.
  - **the valkey module side is namespaced.** Module commands are registered through
    NAME(), which prefixes `B.`, so `B.PING` never collided with valkey's own PING and
    `B.RPING` sits beside it.
  - **the binding did not have to change its name.** swig's `ping(host, port)` is
    unambiguous as it stands - it takes a host and a port - so it keeps its name and
    only sends RPING instead. That is what left the eight test files that call
    `barch.ping("127.0.0.1", PORT)` untouched.

Categories differ slightly: RPING keeps `{"read","connection","data"}` and PING is
`{"read","connection"}`, since answering PONG touches no data.

Measured: full suite green, 47 of 47.


## 25. Every command moved into a {category}_api file [01-08-2026]

The second half of entry 20. Keys, lists, hashes, ordered sets and info had already been
given their own headers and register_*_api(); everything else was still declared in
barch_apis.h and implemented in barch.cpp. Five new pairs finish it:

  - **connection_api** - HELLO, CLIENT, MULTI, EXEC, PING, COMMAND
  - **keyspace_api** - USE/SELECT, UNLOAD, SPACES, KSPACE, KSOPTIONS, SIZE/DBSIZE,
    SIZEALL, SAVE, SAVEALL, CLEAR/FLUSHDB/FLUSHALL, CLEARALL, and the BEGIN/COMMIT/
    ROLLBACK transaction markers
  - **repl_api** - PUBLISH, PULL, LOAD, RELOAD, START, STOP, RETRIEVE, RPING, and
    ADDROUTE/ROUTE/REMROUTE, which stay implemented in rpc/server.cpp where the routing
    table is and are only declared and registered with the rest of their family
  - **config_api** - CONFIG and TRAIN
  - **auth_api** and **info_api**, which already existed, gained declarations and a
    registration; STATS, OPS, HEAPBYTES and the VACUUM and MILLIS wrappers joined INFO

barch.cpp is now 179 lines and holds the two module entry points and nothing else. Its
OnLoad is a loop over the eight add_*_api functions rather than eighty lines of
CreateCommand. barch_apis.h is 56 lines and declares no commands at all - what is left
is the vocabulary they are built from: barch_function, barch_info, the category map that
drives ACLs, and the table. functions_by_name() is ten calls.

The name collision the entry predicted did turn up, and this time it was fixed the way
it should be rather than with a cast. HEXPIRE is a command and also a three argument
helper that both HEXPIRE and HEXPIREAT are written in terms of; once the registration
moved into hash_api.cpp both were visible and `::HEXPIRE` stopped resolving. Entry 20
reached for a static_cast. The helper is now INNER_HEXPIRE, so the exported name is
unambiguous again and the cast is gone. That is the convention for this: the internal
one gets the prefix, because it is the one nothing outside the file should be naming.

Two things went wrong mechanically and are worth recording, because both were caught by
checks rather than by the compiler:

  - **the extern "C" block.** The generated files closed extern "C" around the
    valkeymodule.h include and then emitted the command bodies outside it, which
    compiles as a stray brace error rather than as a linkage problem - but had it
    compiled, every command would have had C++ linkage and nothing would have resolved.
  - **a doc comment was left behind.** The extractor took a preceding comment block by
    walking back over lines starting with // or *, and KSPACE's block documents its
    subcommands with lines starting with `-`. So it took only the closing `*/` and
    stranded the rest in barch.cpp. Found by looking for a `*/` with no opener rather
    than by trusting the build, since a dangling comment can silently swallow code.

The check that actually matters for a move like this is not the build. A registration
dropped in transit compiles and links perfectly and only shows up as a command answering
"unknown command" at runtime. So both tables were compared against HEAD across every
file: 112 RESP commands before and after, 86 valkey module commands before and after,
nothing lost and nothing added.

Measured: full suite green, 47 of 47.


## 26. Configuration from the environment [01-08-2026]

Every setting can now be given on the environment: BARCH_ followed by its name in upper
case, so `export BARCH_MAX_MEMORY_BYTES=100m` before starting the process configures it.
The redis names work too, with hyphens written as underscores because an environment
name cannot carry one - BARCH_MAXMEMORY and BARCH_MAXMEMORY_POLICY. Values are in the
form CONFIG SET takes, which is what makes 100m and 256mb mean what they look like, and
what makes the redis unit handling from entry 23 apply here for free.

`barch::apply_environment_configuration()` walks the two name lists and calls
set_configuration_value for each name that is exported. It reports what it took, and
says so when it cannot: a setting barch reports but cannot change - BARCH_APPENDONLY -
is refused with the reason rather than ignored, because somebody who exported it is
expecting an effect. A value the setter will not take is refused the same way, and
neither stops the rest from being applied.

**Where it is called matters more than what it does.** For the valkey module it is the
last thing in OnLoad, after ValkeyModule_LoadConfigs - never before. LoadConfigs applies
a registered default to every setting the config file does not mention, so anything read
from the environment earlier would be silently undone. That also settles the precedence
question: the environment wins over valkey.conf, which is what somebody exporting a
variable to configure a process expects.

For the bindings it is a SWIG %init, so it runs when the module is imported and before
the caller can do anything, leaving an explicit setConfiguration() as the last word. The
%init is guarded to python and lua: Java has no module init for SWIG to attach it to,
and an unguarded block emits code at file scope that does not compile. A Java caller
uses setConfiguration().

If a setting is exported under both its own name and a redis alias, barch's own name
wins - the aliases are applied first and the native names second, the more specific of
the two landing last.

Two things cost time and are worth knowing about this repo:

  - **the python module under test is the installed one.** `venv/bin/pip install .` is
    itself a ctest step, so site-packages only refreshes when the suite runs. A manual
    check straight after a build tests the previous build, which is exactly what it
    looked like when the first attempt reported the environment having no effect.
  - **`python -c` cannot import barch from the build directory.** With -c python puts
    the working directory first on sys.path, and the build directory holds a barch.so
    belonging to another target which shadows the real module - the import fails with
    "does not define module export function (PyInit_barch)". Running a script file puts
    that script's directory first instead. The new test writes its child to a temporary
    directory for that reason; -P would also fix it but only on python 3.11 and later,
    and ubuntu 22 in CI is older.

test/envconfigtest.py covers it end to end, driving child processes with the environment
set because the environment is read at import and this file's own import has already
happened by the time it runs: barch's own names, a redis alias, a redis byte unit, the
policy value translation, which name wins when both are exported, a read only setting
being refused without stopping the rest, and a bad value leaving the default in place.

Measured: full suite green, 48 of 48.


## 27. A logging style that takes an initializer list [02-08-2026]

lzr_log.h/.cpp add:

    log({"loaded", count, "shards in", seconds, "s"});
    warn({"timeout after", secs, "seconds"});
    err({"could not save shard", shard_num});

The functions are not templates and all the formatting is in lzr_log.cpp, which is the
only translation unit that needs fmt. Output is identical to the old logger's for the
same content, checked side by side; warn is a third level the old one did not have,
tagged W where the others are M and E. Both loggers take the same mutex, so lines cannot
interleave while call sites are still on the old one.

**It went through two designs, and the difference between them is the whole story.**

The first took `std::initializer_list<Variable>`, reusing the type the rest of barch
carries values in. It worked, and it was *slower to compile than what it replaced*.
Measured over a thousand log statements, call sites only, with the include cost
subtracted:

    old, std_log(a, b, c)                   +2.00s
    new, log({a, b, c}) over Variable       +2.10s

Trading fmt's make_format_args for std::variant's converting assignment is close to a
wash: the variant does overload resolution across eleven alternatives on every
instantiation. Naming the ordinary types on Variable so the variant machinery is skipped
brought it to +1.75s - a 12% win, not the order of magnitude the idea deserved. And
lzr_log.h had to include variable.h, which costs about 1.0s per translation unit on its
own, so including the new logger was *more* expensive than including the old one.

The second design drops Variable for a `log_value` defined in lzr_log.h: the six kinds a
message can contain, a borrowed string_view rather than an owned string, and nothing
else. It is built entirely from non-template constructors, so a call site instantiates
nothing at all, and the header includes only <cstdint>, <initializer_list>, <string> and
<string_view>. That changes the picture completely:

    include logger.h                        +0.65s
    include lzr_log.h                       +0.12s
    logger.h  + 1000 std_log                 2.61s   (call sites +1.94s)
    lzr_log.h + 1000 log                     0.71s   (call sites +0.58s)

73% off the translation unit, with the call sites themselves about three times cheaper.
So the original premise was right, and the reason the first attempt did not show it was
that Variable brought its own template machinery and its own include cost.

Two details worth recording:

  - **a std::string needs a constructor of its own.** It reaches string_view through a
    user defined conversion, and a second one to log_value is more than an implicit
    conversion sequence will do, so log({some_std_string}) does not compile without it.
    That is the only reason <string> is in the header, and it costs 0.06s of the 0.12s.
    An art::value_type is not named at all: .to_view() at the call site keeps its
    include out.
  - **a stray pointer is now a compile error.** log_value(bool) would otherwise swallow
    any pointer through the pointer to bool conversion and quietly log "true", so
    pointers are deleted, with const char* binding to its own non-template constructor
    ahead of the deleted template. The old logger would have printed an address.

variable.h is untouched: the constructors added while the first design was being
measured went back once the logger stopped using it. What they were fixing is real and
is recorded as TODO 28.

Measured: full suite green, 48 of 48.


## 28. All logging converted to lzr_log [02-08-2026]

196 std_log and std_err call sites across 35 files became log({...}) and err({...}), and
another 30 barch::log(e, __FILE__, __LINE__) became err({e.what(), __FILE__, __LINE__}) -
which is exactly what that wrapper did, so the output is unchanged. Checked against a
running server: the startup lines read the same as before.

logger.h is now included by one file, logger.cpp, plus keys.cpp for the streaming
std_start/std_continue/std_end form that only its key dumper uses and that lzr_log does
not have. Everything else, including statistics.h, moved to lzr_log.h.

statistics.h was the one that mattered. It included logger.h for a single std_err inside
throw_exception, and it is included nearly everywhere, so every translation unit in the
project was paying for fmt's core, chrono, format and color headers to get one line.

**The compile time result is much smaller than the isolated measurement suggested, and
that is worth recording plainly.** A file that does nothing but log is 73% faster to
compile. Real files are 2 to 6%:

    src/shard.cpp          2.78s -> 2.61s
    src/keys_api.cpp       2.79s -> 2.73s
    src/configuration.cpp  2.81s -> 2.74s
    src/rpc/server.cpp     4.05s -> 3.97s

The logger was simply never the dominant cost in a file that also includes asio, the art
headers and variable.h. Removing 0.65s of includes from a 2.8s translation unit is worth
having and is not what the exercise felt like it was worth. TODO 29, variable.h at about
1.0s on its own, is where the remaining time is.

So the honest reason to keep this is the API and the smaller include graph, with the
compile time as a real but modest bonus.

Three things the conversion turned up:

  - **a non const char\* is an exact match for the deleted pointer template**, where
    const char\* only reaches its constructor through a qualification conversion, so it
    needed naming too. Without it any char\* was a compile error rather than a log line.
  - **logical_allocator.h included <logger.h> in angle brackets**, which a search for
    the quoted form missed, so logger.h stayed in the include graph of every file that
    touches the allocator until it was found by asking the compiler what it had actually
    included rather than by grepping.
  - **the second conversion pass double wrapped the first pass's work.** Rewriting
    barch::log(args) to barch::log({args}) hit the barch::log({...}) calls the first
    pass had already produced, giving log({{...}}) in 74 places. The compiler caught it,
    but only because log_value cannot be built from an initializer list - had the type
    been more permissive this would have compiled and logged nonsense.

Left alone deliberately: err({std::runtime_error("...").what(), ...}) in hash_arena.cpp
constructs an exception purely to read its message back, which the conversion made more
visible but did not introduce. It is faithful to what was there.

Measured: full suite green, 48 of 48.


## 29. Variable would not take an unsigned type narrower than 64 bits [02-08-2026]

`Variable v = some_uint32_t` did not compile. uint32_t converts without narrowing to
both int64_t and uint64_t, so std::variant's converting constructor could not choose
between them and rejected it outright - in a type whose whole job is to hold any value,
and with uint32_t common in this codebase.

Latent rather than live: nothing that fails to compile can be in the code, so this was a
hole nobody had fallen into rather than a bug producing wrong answers. It surfaced while
the logger was briefly built on Variable, and was left alone when the logger stopped
using it, because at that point it was no longer part of that change.

Three constructors, naming the unsigned types narrower than 64 bits and picking uint64_t,
which is what they are. Only those three: every other arithmetic type already resolves,
and leaving them to the template means nothing that compiled before changes which
alternative it lands on. Verified across every arithmetic type, all seventeen now
resolving to the alternative they should.

bool is deliberately still not named. A Variable(bool) would swallow any pointer through
the pointer to bool conversion and silently store true, where today that is a compile
error - checked, and it still is.

Not taken: the same technique applied to every arithmetic type measured about 12% off
translation units that build a lot of Variables, when it was tried during the logger
work. That is a compile time change rather than a correctness one, so it belongs with
TODO 29 and the rest of the compile time question, not here.

Measured: full suite green, 48 of 48.


## 30. The range sharding option, ahead of the algorithm [02-08-2026]

`opt_range_sharded` on key_space, alongside opt_shard_count and opt_ordered_keys and set
the same way: from the configuration space, as `<name>.range_sharded`, read once when
the space is first built. Nothing routes by it yet - that is TODO 30. What exists is the
option, its plumbing and its reporting, deliberately settled before the algorithm so
that the two can be got right separately.

Three things it had to respect:

  - **not the default space.** node takes its settings from the server rather than from
    the configuration space, and the constructor already skips node and configuration_
    when reading per space options, so this falls out of where it was put rather than
    needing a special case.
  - **only where it means something.** A range is meaningless in a hash table, so a
    space that asks for range sharding without ordered keys has the option dropped and
    says so in the log. Dropped rather than half honoured, so that reading it back
    describes what the space is actually doing - which is the whole point of a flag
    nothing implements yet.
  - **visible from both sides.** KeyValue::getRangeSharded() for the bindings, next to
    getShards and getOrdered, and a `sharding: range|hash` line in INFO SHARD, which
    reports a key space setting per shard because that is where somebody looks. The
    test asserts the two agree with each other for every case, since two ways of asking
    that can disagree is worse than one way.

test/rangeshardtest.py covers the truth of it: an ordered space that asks gets it, an
unordered one is refused, not asking leaves it off, explicitly off stays off, the node
and configuration spaces are not configurable this way, INFO SHARD reports each case,
and the space still stores and reads back keys whatever it says.

Two things that cost time and are worth knowing:

  - **a key space reads its options once, when it is first built.** Setting the
    configuration after touching a space does nothing, because the space is cached for
    the life of the process. Every case in the test uses a name of its own for that
    reason.
  - **ninja does not rebuild the SWIG wrapper when swig_api.h changes**, only when
    barch.i does, so a new binding method silently does not exist until barch.i is
    touched. The test failed with AttributeError on a method that was in the header and
    in the built library.

Measured: full suite green, 49 of 49.

## 31. Ordered range sharding implemented [02-08-2026]

`opt_range_sharded` now selects something. A range sharded key space routes by
`range_index` - a sorted flat vector of the minimum key of every shard above 0 - instead
of by hash, and a sweep on the key space's maintenance thread moves keys between
neighbouring shards to keep the partition even.

The algorithm came from the prototype (DONE 30 / TODO 30) unchanged in its essentials:
shed in both directions, cascade with a budget per level, shed only far enough to meet
the neighbour half way, and give the threshold slack. What follows is what the real
implementation had to settle on top of it, all of which the prototype could not answer
because it is single threaded and its shards are `std::map`.

**Routing and locking cannot be one step, and that is the whole problem.** There is
always a gap between deciding which shard owns a key and holding the lock that stops it
changing. Hash routing does not care - a key's shard is a pure function of the key.
Range routing rebalances, so in that gap a boundary can move and leave the caller
holding a lock on a shard the key no longer belongs to, which would insert a key where
nothing will look for it or report a live key as absent.

What closes it is one rule kept by the writers and one check made by the readers:

  - a rebalance changes the table only while holding a write lock on *every shard whose
    span is changing* - both of them, taken in shard order, with the new table published
    before either lock is dropped;
  - a router routes, takes its lock, and then routes again. Same answer means no
    rebalance can move that key while the lock is held, because moving it needs the lock
    the router is holding. A different answer means the boundary moved in between: drop
    the lock and go again.

That is `sharded_store::route_locked` and `key_space::route_moved`, and it costs one
atomic load and a binary search per operation, which is why every point operation could
be put on it rather than only the range sharded ones. The retry loop cannot spin: losing
twice needs two rebalances of the same boundary, and those are bounded by the
maintenance interval.

**Rebalancing belongs on the maintenance thread, and the prototype was extended to prove
it before it was written that way.** Strategy 5 in the prototype does no work on the
insert path and sweeps every N inserts instead. Two things came out of it:

  - it holds up - at 8 shards and sweeps 256 inserts apart, imbalance 1.26x and a peak
    of 1.29x against 1.25x for the inline version, at the same moves per insert;
  - but only if **the sweep's work is driven by the skew, not by a fixed pass count**. A
    fixed four cascades per sweep quietly stops keeping up as sweeps get further apart:
    at 1024 inserts between sweeps an ascending workload goes to 11.30x while the moves
    per insert *falls* to 0.53, which is the signature of a rebalancer that has given up
    rather than one that is working hard. Converging instead, with a cap on lock pairs
    rather than on work, holds 1.26x out to 4096 inserts between sweeps.

The other reason to put it there is not performance: it makes the maintenance thread the
only writer of the table, which is what makes the read side above as cheap as it is.

**The tolerance is a band, not a ceiling.** The first working version balanced 20000
ascending keys over 8 shards to `[3134, 3133, 3123, 3122, 3113, 3095, 1280, 0]`. That is
1.25x - balanced, by the definition it was given - with an eighth of the shards holding
nothing. Six shards at exactly 1.25x the average leaves almost nothing for the rest, and
the arithmetic works out. This is the "12 of 64 shards were empty while still measuring
balanced" note in TODO 30, and it is not as harmless as it looked: those shards are
capacity and concurrency that the space paid for and is not using.

Two changes fixed it, both checked in the prototype first as strategy 6:

  - a shard sheds when a neighbour is meaningfully smaller, not only when it is over the
    threshold. A cascade gated on the threshold dies at the first shard under it and
    never reaches the ones past it, which is exactly how the tail is left empty. The
    half way rule stops the ungated cascade on its own: once neighbours are within a key
    of each other there is nothing to take.
  - the sweep is finished when no shard is above `tolerance * average` **and** none is
    below `average / tolerance`.

Measured in the prototype at 8 shards: empty shards go from 1 to 0 on every workload,
random imbalance improves from 1.24x to 1.16x, and ascending costs 3.37 moves per insert
against 2.72 - about 24% more work for the tail actually being used.

**Turning the option on for a space that already has data has to repartition it.** The
table is derived from the shards, never persisted, so a load rebuilds it by asking each
shard for its first key. That only works if the shards are already an ordered partition.
If the space was hash sharded when it was written, they are not, and routing by the
boundaries of that finds almost nothing. So the load checks, and repartitions when it
has to:

  - boundaries from a sample - at most 4096 keys per shard, sorted, cut into quantiles.
    An exact answer needs the whole key space in order, which is the thing we cannot
    afford to build; every key still lands on the right side of whatever boundaries are
    chosen, so the result is a correct partition either way, and the sweep polishes the
    balance afterwards.
  - then each shard is drained in batches bounded by bytes rather than keys, because
    iterating a tree while removing from it is not safe and holding a copy of every
    misplaced key at once would be the whole shard.

5000 keys over 4 shards convert in 7ms, moving 3696 of them, and land on 1250 each.

**What the ordered partition buys, which is the point of doing any of this.**
`sharded_store::range` no longer walks every shard in striations, collects an unsorted
list up to `limit * shard_count` long and sorts it. It starts at the shard that owns
`lo`, walks shards in order, hands each key to the callback as it finds it and stops at
the first key not below `hi`. Nothing is collected and nothing is sorted. `minimum`,
`maximum`, `lower_bound`, `upper_bound` and `count` stop at the first shard that can
answer instead of reducing over all of them. All of it is behind `ordered_shards()`,
which is false for a space with a pull source: its keys are not all its own and the ones
upstream are not part of the partition.

Three smaller things that were decided rather than discovered:

  - **a shard with pull sources is never rebalanced.** A delete against it leaves a
    tombstone rather than removing anything, and a key it answers for may live upstream,
    so neither its size nor its minimum means what the algorithm needs them to mean.
  - **a move is not user traffic.** It reuses what the defragmenter does when it
    reinserts a leaf into its own shard - carry expiry, volatile and compressed across
    and `tree_insert` - and decrements the op counters afterwards, so a rebalance does
    not show up as inserts and deletes nobody asked for. `range_shard_keys_moved` counts
    them instead.
  - **keys are canonicalised before they are compared.** Storage appends a null
    terminator to a key that does not have one, so the same key reaches the router as
    `abc` from a command and `abc\0` out of a leaf, and `abc` sorts below `abc\0`. Both
    the boundaries and the key being routed drop a trailing null first. It is order
    preserving because a key may not contain an interior null.

Two things that cost time:

  - **`art::iterator`'s one argument form walks nothing.** It finds the tree minimum but
    never fills the trace list, then reads `last_node` off the empty one. Every existing
    caller uses the two argument form, so nothing had noticed. Both walks here seed the
    iterator from the shard's own minimum instead; the constructor itself is left alone
    and is TODO 31.
  - **shard sizes read one at a time do not add up while a sweep is running.** The test
    asserted a total over 8 `INFO SHARD` calls with nothing holding the space still, and
    a key moved between two of the reads is counted twice or not at all. It is the test
    that was wrong, not the code, but it is worth knowing before reading those numbers:
    they only settle once the sweep has nothing left to do.

The one thing that has not changed since TODO 30, and that decides whether to use this
at all: an append only workload costs O(shard_count) moves per insert and there is no
way around it. Range sharding being opt in per key space, and off by default, is right.

Measured: full suite green, 51 of 51, including two new tests - `rangeroutetest.py` for
routing, balance, ordered reads, deletes and reload, and `rangeconverttest.py` for the
hash to range conversion, which needs two processes because a key space reads its
options once.

## 32. Every Z* command declared an ACL category that does not exist [09-08-2026]

All twenty-one ordered set commands registered `ordered` as one of their ACL categories.
There is no such category. The list in `barch_apis.cpp` defines the name as `orderedset`,
`cats2vec` discards any name it does not recognise, and so the bit was never set on a
single `Z*` entry.

What made that worse than a cosmetic slip is the shape of the authorisation test.
`is_authorized` in `rpc/asio_resp_session.h` walks the categories the *command* declares
and refuses the call if the user is missing any of them. A category a command never
declares therefore cannot be required, and cannot be revoked either. The effect was that
any user holding `read`, `write` and `data` reached every ordered set command whether or
not they had been granted `orderedset`, and taking `orderedset` away from a user changed
nothing at all. `docs/ACL.md` has always described category 6 as "orderedset : for Z*
calls", which is what it was meant to mean and now does.

Found while generating the RESP command index for `barch-docs.html` from the registration
tables, which is a fair argument for generating documentation out of source rather than
writing it alongside: nobody reading either file had noticed, and the mismatch was obvious
the moment the two were put in the same table.

The fix is the one word, in the twenty-one registrations in `ordered_api.cpp`. Nothing
else refers to the category by name, so nothing else moved.

Worth knowing about the failure mode rather than just the instance: category names are
strings checked at runtime against a list, and an unknown one is dropped rather than
rejected. That fails open. A misspelling in any future registration will widen access to
that command instead of breaking it, and will do so silently. The obvious guards are to
reject unknown names where the entry is built instead of ignoring them, and to have at
least one test that asserts a category is genuinely enforced, since none currently does -
which is the other reason this survived.

## 33. The RESP commands that answered with the wrong thing [09-08-2026]

Documenting all 110 commands (TODO 35) turned up a long list of places where a command
did not do what its name promises a redis client. TODO 37 gathered them and named the
decision that had to come first: whether the RESP surface is meant to be redis compatible
or is a barch surface borrowing redis names. That has now been answered - **redis
compatibility is the aim** - and this entry is the first group fixed under it, being the
ones that returned a wrong answer or the wrong RESP type. The rest are TODO 38.

Fixed, each verified against a running server rather than only by reading:

  - `SET k v GET` answered with the key instead of the previous value. The insert
    callback ignored the node it was handed, which is the only place the old value could
    come from; it now reads and copies it, decompressing where the leaf was compressed.
    The copy matters because the insert that follows can replace the leaf.
  - `HINCRBY` and `HINCRBYFLOAT` replied 0 and did nothing whatever. shard::update does
    not reach its updater when the key is absent, so a missing field was never created,
    and an accumulator that started at zero was what got replied. Both now go through a
    new HNUMERIC helper that does update-or-create under one container write lock, which
    is the same shape BarchModifyInteger already used for plain keys.
  - `HSET` always replied 0. It counted the insert callback, which only fires for a field
    that was already there; it now counts what `insert` reports as newly added.
  - `HGET` returned a one element array, because it shared HMGET's reply path, and a bare
    nil when the hash itself was missing - three shapes for one command. HQUERY grew an
    `as_array` flag: the multi field readers (HMGET, HTTL, HEXPIRETIME) keep the array,
    the single field ones (HGET, HEXISTS) do not.
  - `HEXISTS` returned a two element array holding an empty array and then the flag. Same
    fix; it is now a plain integer.
  - `TTL` had -1 and -2 the opposite way round from redis, which silently inverts every
    client's "does this key exist" check. Now -1 is present with no expiry and -2 is no
    such key.
  - `EXISTS` answered a single boolean, true only when every named key was present. Now a
    count, duplicates included, as redis does.
  - `DEL` was another name for REM, so it took one key and answered with the removed value
    where a redis client's parser wanted an integer. DEL is now its own handler taking any
    number of keys and answering with how many were removed. REM keeps its old behaviour
    under its own name.
  - `MSET` replied with an integer, and an odd argument list walked off the end of argv
    and surfaced as small_vector's out_of_range rather than a wrong arity. Now OK, and the
    pairing is checked up front.
  - `ZPOPMIN` and `ZPOPMAX` returned the pair score first. Now member first, as redis does.
  - `FLUSHALL` was the same handler as FLUSHDB and cleared only the selected key space.
    Now mapped to CLEARALL, so it reaches every space the way redis's does. FLUSHDB is
    unchanged.

Also tightened while in there: HGET, HEXISTS and HSET now check their arity rather than
relying on the argv walk to fail, and HSET refuses an odd field/value list.

Not done here, deliberately: the `H` option on SET is still parsed and then overwritten a
line later. Removing it is a behaviour change of a different kind - it turns an accepted
no-op into a syntax error - and it belongs with the naming decisions in TODO 38.

Measured: 24 wire level assertions over the changed commands all pass, covering both the
hit and the miss path of each. The full ctest suite was run to check nothing that depended
on the old shapes broke.

## 34. SET NX and XX were parsed and then ignored [09-08-2026]

Found by writing the reply shape test in TODO 38, which is the first test in the tree that
reads the raw wire reply. It asserted redis's answer for `SET k v NX` on a key that
already exists - nil, nothing written - and got `+OK` back, so the write had gone through.

Both conditions were entirely inert. `key_spec` parses `nx` and `xx` correctly, but
`key_options`, which is what actually reaches storage, has no flag for either, and the
`key_options(const key_spec&)` conversion copies expiry, keep_ttl and hashed and drops
them. Nothing downstream had ever seen them. So `SET k v NX` overwrote a key that was
already there, and `SET k v XX` created one that was not - both silently, both replying OK.

That is worse than the reply shape defects in DONE 33, because SET NX is the primitive
every distributed lock is built on. A lock taken with it was never exclusive.

Fixed in SET rather than in key_options. Adding flags to key_options would have been the
tidier place, but it is serialised by writep/readp and travels between nodes, so a new flag
is a wire format change for something that does not need to be stored - the condition is
answered once, at the moment of the write, and is not a property of the key afterwards. So
SET now takes the owning shard's write lock itself, looks for the key, and decides inside
that lock whether to insert. The check and the insert have to be under the same lock or two
callers both find the key absent and both write, which is exactly the race NX exists to
prevent.

With GET the reply is the previous value whether or not the condition allowed the write,
which is what redis does. Without GET it is OK when the write happened and nil when the
condition refused it.

Not fixed, and now written down in TODO 38: option order is still positional, so
`SET k v NX GET` is a syntax error while `SET k v GET NX` is accepted.

Also added here, and the reason the defect was found at all: `test/respshapetest.py`,
registered as TestRespShapes. It speaks RESP2 over a socket and asserts the exact bytes of
45 replies. The suite could not have caught any of this before - every other test drives
barch through the embedded interface or through redis-py, and redis-py parses a bulk string
and a one element array holding a bulk string into python values that compare equal. The
test asserts literal bytes for that reason.

One thing it asserts as it currently behaves rather than as it should: `HINCRBY` on a field
holding a non numeric value, and `INCR` on a string key, both treat the value as zero and
overwrite it with the result instead of refusing. `SET s abc` then `INCR s` leaves s as 1.
That is silent data loss on a type error and it is in TODO 38; the assertion is written so
it fails when that is fixed.

## 35. A barch abort inside valkey hung the process instead of ending it [09-08-2026]

Reported twice as "there are valkey-servers not using any cpu". Caught the third one while
it was still running and traced all seven threads, which answered it outright.

Several `barch::shard::load` threads went through art::resolve_read_node and
logical_allocator::basic_resolve into arena::base_hash_arena::get_page_data, and hit
`abort_with("invalid page address")`. More than one thread called abort() at the same
moment. abort raises SIGABRT, valkey handles it in sigsegvHandler, and that takes a global
`signal_handler_lock` - three threads were parked there. The thread that won the lock went
on into printCrashReport, doFastMemoryTest, killThreads and killMainThread, which calls
pthread_join on the main thread. The main thread was one of the ones waiting for
signal_handler_lock. So valkey's crash reporter joins a thread that is waiting for the
crash reporter's own lock, nothing progresses, nothing uses cpu, and the process never
exits.

Worth recording that the hypothesis this replaced was wrong. The symptom - threads parked
on a futex, zero cpu, at shutdown - fits static destruction order well enough that it was
worth checking, and there was a genuine hazard of that shape in shared_mutex.cpp which is
now fixed anyway (TODO 41). But it was not this. One gdb invocation settled in a screen
what the theory could not:

    gdb -p <pid> -batch -ex "set pagination off" -ex "thread apply all bt"

The fix is that an address which fails validation while reading a shard file is bad input,
not a broken invariant, and it now throws instead of aborting. The four sites are the two
in hash_arena.h's get_page_data and the two in logical_allocator.h's address validator.
`barch::shard::load` already wrapped `_load` in a try/catch that logs "could not load" and
returns false - the intent was there all along, and abort() simply walked past it.

Measured against the exact data that produced the hang: valkey-server loads with the
module, reports "could not load invalid page address" 490 times, one line per unreadable
shard, then serves normally - PING answers PONG, B.SET and B.GET round trip - and
SHUTDOWN NOSAVE exits cleanly with no process left behind. Before the change the same
binary against the same directory hung with all threads in futex_do_wait.

What is deliberately not addressed here is why those addresses were invalid. That is still
open in TODO 42; the difference is that a bad shard file now announces itself instead of
taking the process down.

## 36. The lua test harness never cleaned up, and three tests asserted the old behaviour [09-08-2026]

Two unrelated things that both surfaced while confirming DONE 33 to 35.

**The harness never shut anything down.** `test_starter.cpp` composed its command strings at
static initialisation:

    static std::string valkey_path = "/_deps/valkey-src/src/";
    static std::string valkey_cli  = valkey_path + "valkey-cli -p 7777";
    static std::string ping_cmd    = valkey_cli + " -e PING ";

and `main` then called `wait_to_stop()` *before* assigning `test_build_dir = argv[3]`. So the
shutdown it sent went to a bare `/_deps/valkey-src/src/valkey-cli`, which does not exist. The
shell reported "not found", `wait_to_stop` treated the non-zero result as "no server running"
and returned success, and a valkey-server left over from an earlier run was never cleared. The
next test then could not bind port 7777. That is why TestAbc, TestKeys and TestComposites
failed in ways that had nothing to do with what they were testing, and it is the other half of
why leaked servers were so disruptive - nothing ever collected them.

Fixed both ends: `test_build_dir` is read before anything shells out, and the composed commands
are now functions rather than statics, so none of them can be built from a value that has not
been set yet. This is the pattern the rest of the tree already uses for `ksp()` and `latch()`.

Worth noting the shape, because it is easy to miss: it is not an initialisation *order* bug
between translation units, which is the usual suspect. All of these are in one file and
initialise in declaration order. It is a stale *copy* - a string built from another string
before the second one was finished with. A getter fixes it for the same reason it fixes the
cross-TU case.

**Three tests asserted behaviour that DONE 33 and 34 corrected**, which is the expected cost of
changing a documented reply and is recorded here so it is clear they were updated deliberately
rather than made to pass:

  - `testbarch.py` read `z.popmin("z1").i()` expecting the score, because ZPOPMIN answered
    score first. It answers member first now, so the assertion reads `.s() == "one"`. Worth
    knowing that the old form did not fail an assertion - `.i()` on a non numeric string throws
    out of `to_e`, uncaught, and terminated the interpreter.
  - `testkeys.lua` asserted `B.SET j 1 get` returns `'j'`, the key. It returns the previous
    value now, which is `'1'`.
  - `testcomposites.lua` read `B.HGET(...)[1]`, indexing the one element array HGET used to be
    wrapped in. It is a bulk string now. The same file already asserted that HINCRBY returns 1
    then 3, which is correct redis behaviour that could not pass until HINCRBY was fixed - so
    that file had been recording the right answer and failing against the wrong one.

## 37. Redis compatibility, the behaviour that differed under a shared name [09-08-2026]

DONE 33 corrected the commands that returned the wrong value or the wrong RESP type. This
is the other half: the ones where the behaviour itself differed, so fixing them breaks
whatever was built against the old shape. The direction had been settled - redis
compatibility is the aim - so these were a question of sequencing rather than of whether.
Done in the order that put the item which destroyed data first and the item with a
migration attached last.

**The one that lost data.** All eight increment commands treated a value that is not a
number as zero and overwrote it with the result: `SET s abc` then `INCR s` left s holding
1. The cause was shared, which is why all eight behaved identically. `leaf_numeric_update`
returned a null node for three different reasons - not numeric, overflowed, compressed -
and `shard::update` reports a declined update exactly as it reports a key that was not
there. INCR read the decline as a miss and took the insert branch. It now returns a
`numeric_status` saying which, and both callers keep a `present` flag so a decline and a
miss are told apart. A non numeric value is refused and left alone.

**Reply and argument shapes.**

  - `ZRANGE` and `ZREVRANGE` read start and stop as positions unless BYSCORE or BYLEX says
    otherwise, so `ZRANGE key 0 -1` is the whole set rather than an inverted score range
    answering empty. Negative positions count from the end and out of range ones are
    clamped. Implemented as its own walk - one pass in score order, then the slice - which
    is simpler than rank lookups at both ends and is what REV already needed.
  - `ZRANK key member [WITHSCORE]` reports that member's position from the low end, nil
    when it is absent. The range count it used to do is left to `ZFASTRANK`, which answers
    it in constant time. Note the member is stored encoded, the same way ZADD writes it -
    comparing the raw argument never matches, which cost a build to find out.
  - `LPOP` and `RPOP` take an optional count and answer with what they removed - one bulk
    string without a count, an array with one. The bytes are copied before the entry is
    removed, since the leaf goes with it.
  - `LPUSH` prepends and `RPUSH` appends, with `LPOP`, `RPOP`, `BLPOP` and `BRPOP` on the
    matching ends. All six were reversed. The flag they pass was called `left` while it
    selected the high index end, which is most of why it went unnoticed; it is `at_tail`
    now. The stored layout does not change, so a saved list reads back in the same order -
    what changes is which command built it.
  - `SET`'s options are order free. The parser loops over what follows the key and value
    instead of walking fixed positions, and refuses an unknown word, a repeat, or a
    contradictory pair such as NX with XX or KEEPTTL with EX. The dead `H` option is gone.
  - `SELECT` is its own handler. A number is a database - 0 the default space, n above zero
    `db<n>` - and a name still selects that space, which barch has always allowed here. A
    superset of redis rather than a departure.

**Cheap and uncontroversial.** Seven ACL categories corrected: `LBACK`, `ZCARD`, `ZRANGE`,
`ZDIFF` and `ZINTERCARD` are read rather than write, `HEXPIRETIME` is read, and `CLIENT`
moved from stats to connection. These matter for the reason DONE 32 established - a
category a command declares wrongly demands a permission the caller should not need.
`ZDIFFSTORE` and `ZINTERSTORE` check their arity before reading argv[1]. `EXPIRE` refuses
a word in the condition position that is not NX, XX, GT or LT rather than consuming it.
`LFRONT`, `LBACK` and `LLEN` take read locks, as DONE 19 did for SIZE.

**What this cost in callers, which is the part worth remembering.** Changing `ZRANGE`'s
default meaning silently changes every caller that passed score bounds - it does not error,
it answers a different question. Four of ours had to be updated: `OrderedSet::range` and
`revrange` in the binding, which take doubles and now say BYSCORE, and three sites in
testcomposites.lua. An external caller doing the same will change behaviour without
noticing, and no test will say so. `List::pop` had to change with its command, from `long`
to the values removed - the old `.i()` on an array reply terminated the interpreter rather
than failing an assertion. `zwbenchy.lua` was passing the dead `H` flag, so it really did
reach callers. `testzrank.lua` was a differential test using ZRANK as the slow reference
for ZFASTRANK; repurposing ZRANK removed the reference, so it compares against ZCOUNT now
and still passes, which is an independent check that ZFASTRANK is right.

**Two mistakes of mine in here, recorded because they are the kind that repeat.** The
index path check added to `ZREVRANGE` matched `ZREVRANGEBYSCORE` as well - the replace
string was not unique - and sent a fractional score down the position parser. And the
first `SELECT` refused names outright, which broke spacethreadtest.py, where
`gr.select("g")` carries the comment "Yes! we can select strings too". Reading what a test
asserts before deciding a behaviour was accidental would have saved that.

Also worth knowing: the SWIG wrappers do not regenerate on a header change. Changing
`List::pop`'s return type failed to compile until `barch*_wrap.cxx` was deleted.

Measured: full suite 52 of 52, no stray servers, against a library verified newer than the
newest source edit. test/respshapetest.py grew from 45 to 77 wire level assertions,
covering each behaviour changed here.

One defect was surfaced rather than caused and is left open as TODO 44: array replies
decode wrongly through every remote binding - remote HMGET answers
`['v1', 'false', '0.0']` where local answers `['v1', 'v2', 'v3']`. It only became visible
because LPOP started returning an array.

## 38. Command names were case sensitive [09-08-2026]

barch's RESP dispatcher looked commands up with an exact string match against a table
keyed in upper case, so `SET` worked and `set` and `Set` were answered with
`unknown command`. Redis is case insensitive here, every example in its documentation is
lower case, and redis-cli sends whatever the user typed.

Found by the first run of the translation harness in TODO 40, which is the point of that
harness: valkey's tcl suite writes every command in lower case, so all twenty one
translated cases failed against barch with `unknown command` while passing against
valkey-server. No test in this repository had ever sent a lower case command.

The fold happens after the `space:COMMAND` prefix is split rather than before, because a
key space name is not case insensitive and folding the whole word would have quietly
renamed the space.

Worth noting how narrowly the existing suite missed this. Every python test drives the
embedded interface or builds commands in upper case, the lua tests call `B.SET` and
friends in upper case, and redispytest.py uses redis-py's own method names, which send
upper case. A defect that makes a plain `redis-cli` session unusable survived 52 tests.

## 40. Every element after the first of a remote reply was decoded one byte late [09-08-2026]

A binding constructed with a host and port answered correctly for a single value and
wrongly for anything longer. Same build, same data, the only difference being how the
handle was made:

    local  HMGET f1 f2 f3   -> ['v1', 'v2', 'v3']
    remote HMGET f1 f2 f3   -> ['v1', 'false', '0.0']
    remote OrderedSet.range -> ['one', 'false']
    remote KeyValue.range   -> []            (three keys in range)

The values were not truncated, they were the wrong *types*, which is what pointed at the
decoder rather than at the commands. In `rpc/server.cpp`:

    for (size_t i = 0; i < buffers_size; i++) {
        auto v = get_variable(i, replies);
        result.emplace_back(v.first);
        i = v.second;
    }

`get_variable` answers with the offset of the next variable, so assigning it and then
letting the loop's `i++` run as well steps one byte past every value. The first is read
from offset zero and is fine; the second onwards takes its type byte out of the middle of
the preceding payload and becomes whatever that byte happens to mean - usually a bool,
sometimes a double, sometimes nothing at all. Now a while loop that does not advance i
itself, with a guard that throws if a decode fails to make progress rather than spinning
on a malformed reply.

This is on the binary replication protocol rather than RESP, so it is the embedded client
talking to a remote server - the path the README describes as the reason the embedded L1
exists. Anything reading a multi value reply across it has been getting rubbish.

Only a reply with more than one value can show it, and that is why it lasted: remotetest.py
is the only test that drives a binding over RPC, and until LPOP was changed to answer with
the values it removed (DONE 37) it only ever asked for one thing at a time. The defect was
already there behind every array returning call.

Guarded now by test/remotearraytest.py, registered as TestRemoteArrays. It is a shape test
rather than a value test - it runs the same command through a local handle and a remote one
and asserts the two agree, over HMGET, HGETALL, ZRANGE with and without scores, RANGE, KEYS
and LPOP. Local is a fair reference because it shares no code with the reply decoder. It
also asserts each reply has more than one value, since a single value decodes correctly
either way and a check that quietly degraded to one would prove nothing.

## 41. string.tcl and keyspace.tcl translated, and what they found [09-08-2026]

TODO 45, the second of the four translation entries. The harness from DONE 40 was pointed
at valkey's unit/type/string.tcl and unit/keyspace.tcl - 150 tests between them, against
the surface DONE 33 and DONE 37 changed most.

Translated 71 of those 150 mechanically, on top of the 26 of 29 already done for incr.tcl.
The rest are stubs carrying their original tcl: loops, `foreach` over encodings, and the
`assert_encoding` and `debug object` helpers, which are valkey's internals and will never
translate. valkey itself trusts 77 of the 97 translated cases; the 20 it rejects are
translation artefacts and are dropped rather than reported, which is what the valkey-first
step is for.

Of the 77 faithful cases barch passes 35 outright. The 42 differences are all accounted
for and none of them is a wrong answer:

  - 22 are commands barch does not have - RENAME, RENAMENX, COPY, MOVE, RANDOMKEY, SETNX,
    GETSET, MSETNX, STRLEN, SETBIT, GETBIT, SET IFEQ, and three cases that set themselves
    up with SADD. Written up as TODO 52, grouped by how much thought each needs.
  - 6 are error wording: refused correctly, but `Syntax Error` where redis says
    `ERR syntax error`, and `Wrong Arity` where redis names the command. TODO 50.
  - 2 are WRONGTYPE, which barch does not implement at all. TODO 49.
  - the rest are the INCRBYFLOAT and bit-command gaps already recorded.

Eight commands were implemented along the way rather than accepted as gaps, because they
are ordinary string operations with no design question in them: `SETRANGE`, `GETRANGE`,
`SUBSTR`, `GETDEL`, `GETEX`, `SETEX`, `PSETEX` and `LCS`. LCS is the only one with a cost
worth knowing - it builds an (n+1)(m+1) table, so its memory is the product of the two
lengths, which is what redis does and what redis warns about. All eight are covered by
wire level assertions in respshapetest.py, which is now 123 of them.

`SELECT` was settled here too. It takes a number or a name: a number maps to the space
named by the new `db_number_prefix` option followed by the number, 0 being the default
space, and a name selects that space directly. The prefix is configurable because which
name a number maps to is a choice barch has to make and redis does not; setting it empty
restores the older behaviour where `SELECT 1` selects a space called `1`. Three
consequences are written up in the Named Key Spaces article rather than left to be
discovered: the mapping itself, that a space named with a bare number is only reachable
through `USE`, and that there is no fixed database count - redis refuses `SELECT 16` while
`SELECT 999` here simply creates that space.

The thing worth carrying to entries 46 and 47: the yield is not the point, the accounting
is. 97 translated of 199 sounds poor until the stubs are read - most are valkey testing
its own encodings - and the 42 differences sound alarming until they are grouped, at which
point they are one list of absent commands and two decisions already recorded. A harness
that cannot tell those apart would have produced 42 things to investigate.

## 42. One lock order, written down once [09-08-2026]

TODO 20. The concern it recorded was that the rule which avoids deadlock between two key
spaces was written out at each call site rather than in one place. It was right, and by
the time it was looked at the sites disagreed: `KSPACE DEPENDS` took source then dependent
while `KSPACE RELEASE` took dependent then source. Two callers doing those concurrently on
the same pair is the textbook deadlock, waiting only for the load to find it.

The rule is now in keyspace_locks.h and it is a total order rather than advice:

  1. key spaces by canonical name, byte order
  2. shards within a space by shard number

`ks_two` takes two spaces with a mode each and locks them in that order whatever order the
caller names them in - a caller says it wants the source shared and the dependent exclusive
and does not get to decide which is taken first, which is the whole point. The same space
named twice is locked once with the stronger of the two modes, because taking a space's
locks twice waits on itself. The three KSPACE sites and the pair in sharded_store now go
through it.

`sharded_store::with_two_keys_write` is the same rule one level down, for the commands that
touch two keys. It locks by shard number, collapses to one lock when both keys are on the
same shard, and re-checks the route once the locks are held - a key on a range sharded
space can move under a router, so a route that changed between deciding and locking means
dropping both and retrying. That retry cannot spin: moving a key needs the locks being held.

The trap the new helper introduces, which cost a debugging round and is commented at both
sites that hit it: while a whole-space lock is held, sharded_store's own search and insert
must not be used. They take a shard lock of their own, and asking for one already held by
this thread waits forever - it showed up as `read lock wait time exceeded` from cross space
COPY and MOVE. Under ks_two the shards are addressed directly through shard_for, which
routes without locking.

## 43. The commands string.tcl and keyspace.tcl expected [09-08-2026]

TODO 52, all of it. Nine commands, in the order the entry recommended - the cheap ones
first, the ones that touch two keys after the lock order was settled in DONE 42.

  - `SETNX`, `GETSET` - the older spellings of `SET NX` and `SET GET`, differing only in
    the reply. Both do the test and the write under one lock, or two callers both find the
    key absent.
  - `STRLEN` - not the alias for LENGTH the entry guessed. LENGTH answers nil for a key
    that is not there and redis's STRLEN answers 0, and callers compare that to a number.
  - `MSETNX` - all or nothing, which is the whole point of it, so it holds a write lock on
    every shard while it works. MSET has never been atomic across keys and says so; this
    one cannot afford not to be. The cost is that it stops writes to the space for its
    duration, which is only defensible because MSETNX is a small occasional call.
  - `RANDOMKEY` - a random shard among those holding anything, then a bounded walk into it.
    Arbitrary rather than uniform, and that is written in the function: keys near the start
    of a shard come up more often and a shard with ten keys is as likely as one with ten
    thousand.
  - `RENAME`, `RENAMENX`, `COPY` with DB and REPLACE, `MOVE` - all through
    with_two_keys_write. The value bytes are copied out before anything is written, because
    when both keys land on one shard the write can move or free the leaf being read.

Two things were found by writing them rather than by reading anything.

`RANDOMKEY` answered with the same key every time. It had been seeded with an empty
value_type, which is TODO 31's broken iterator form - it finds the minimum and never fills
its trace, so it walks nothing - and the fallback then returned the global minimum. Seeded
from the shard's own tree_minimum it returns 34 distinct keys over 60 calls on 50 keys.
That entry has now caused two bugs, which is an argument for deleting the form rather than
leaving it callable.

And the harness had been misattributing failures. The valkey pass ran every case so its
side effects landed, while the barch pass ran only the trusted ones - and since cases in a
tcl file share state, the two servers drifted apart and later comparisons blamed barch for
it. Both sides run everything now and only the comparison is restricted to the trusted set.
That had been inflating the difference count since the harness was written.

Measured: the differential trusts 85 of 97 translated cases and barch agrees on all 85,
with the remaining divergences accounted for - the bit commands and SET IFEQ are not
implemented, WRONGTYPE is TODO 49 and the error wording is TODO 50. barch passes 58 of the
97 cases run, up from 35 of 77 when this entry was opened. Full suite 55 of 55.

## 44. INCRBYFLOAT, and the numbers being stored differently from how they were answered [09-08-2026]

TODO 48. INCRBYFLOAT for plain keys, which only existed for hash fields. It creates the
key at the increment when absent, refuses a value that is not a number and leaves it
alone, and refuses NaN and infinity with the message redis uses - the tests match on
"would produce", which is a different message from "not a valid float", and the argument
has to be examined as text to tell them apart because readers disagree about parsing
"+inf".

Found while writing it: `std::to_string` on a double gives six decimal places whatever the
value, so the reply said 3 and the stored value said 3.000000, and a GET after an
INCRBYFLOAT disagreed with the INCRBYFLOAT. Both now go through one `numeric_to_text` that
renders a float the way redis does - seventeen significant digits, no fraction when there
is not one - and HINCRBYFLOAT was storing the same wrong thing and is fixed with it.

## 45. WRONGTYPE, as far as it can go without a stored type tag [09-08-2026]

TODO 49, which was left as a decision rather than a task because the obvious
implementation costs something on the hot path. It turned out the entry understated the
problem: barch did not merely fail to report a wrong type, the types actively interfered.
One name could hold a string, a list, a hash and an ordered set at once - they are
genuinely different keys - and on such a name HLEN answered 3 while ZCARD answered 4,
because both walk the same container prefix and were counting each other's entries.

The type is observed rather than stored. `barch::kind_of` probes the plain key, which
means a string, and then the container prefix, which means a list, hash or ordered set.
The second probe only runs when the first misses, so the ordinary path costs one extra
lookup on a key that is not there - which is what makes it affordable, and was the concern
the entry recorded.

Three things had to be got right, and each was wrong first:

  - The two probes route to *different shards*. A plain key and its container prefix are
    different byte sequences. The first version probed both on the shard the caller had
    already locked, found nothing ever, and passed every check silently.
  - It has to run before the command takes its own lock. Asking the store for a second
    shard while holding the first is the self deadlock cross space COPY hit in DONE 42.
    The gap between checking and writing is a race, but it is redis's race too and the
    loser writes a value nobody wanted rather than corrupting anything.
  - `SET` must not be guarded. redis's SET replaces whatever the name held, including a
    list, and string.tcl relies on it. Every other string command refuses.

Two real defects came out of it:

  - `DEL` never deleted a collection. It removed the plain key only, so a deleted list
    lived on under its own keys. `remove_container` collects the run under the prefix and
    removes it, and DEL uses it.
  - A removed key lingers as a tombstone that lower_bound finds and search does not, so a
    deleted collection went on reporting itself as a container - GETRANGE on the name
    answered WRONGTYPE forever after a DEL. The probe walks past tombstones now.

What this does not do is tell one collection from another, and that limit is the reason
TODO 53 exists. A hash and an ordered set under one name are still indistinguishable and
still miscount each other, and SET over a collection does not remove what it replaces.
Both need a type tag written when a container is created, which changes the stored format
and needs a migration for anything already saved - a decision that should not be taken
while chasing a red test.

Measured: the differential trusts 85 of 97 translated cases and barch agrees on all 85, up
from 58 when these two entries were opened. Full suite 55 of 55.

## 46. Error codes, and a wrong argument count reported for a wrong argument [09-08-2026]

TODO 50. barch answered `Wrong Arity` and `Syntax Error` where redis answers
`ERR wrong number of arguments for 'incr' command` and `ERR syntax error`, and every other
message was a bare phrase with no code in front of it.

The code is the half that matters to a client. The first word of a redis error is not part
of the sentence, it is a code, and clients read it - redis-py maps ERR, WRONGTYPE, NOAUTH
and the rest onto exception classes, so an error without a recognised one arrives as a
plain ResponseError and anything branching on the type gets the wrong answer. One rule now
covers all of them: a message already beginning with an all upper case word keeps it,
anything else is a sentence and gets `ERR` in front. That fixed every push_error site
without touching any of them, and left WRONGTYPE and NOPROTO alone.

The entry said naming the command in an arity error was the larger half, because
`wrong_arity()` does not know which command it is serving. That was not true.
`rpc_caller::call` already copies the parameters into `args` before dispatching, so
`args[0]` is the command name - it had simply never been looked at. Lower cased, the way
redis reports it whatever case arrived, and the whole change is four lines.

Found while doing it, and the reason this was worth more than tidier text: **DECRBY and
UDECRBY answered with a wrong arity error when the increment failed to parse**.
`DECRBY k v` told the caller the argument count was wrong when the count was fine and the
value was not a number, which points at entirely the wrong thing. INCRBY and UINCRBY got
this right, so the two halves of one family disagreed. Only visible because valkey's test
asserts which message comes back.

Measured: barch agrees with valkey on all 85 faithful cases and passes 77 of the 97 run, up
from 70 when this entry was opened, with no divergence accepted for wording any more. Full
suite 55 of 55.

Worth noticing about the entries themselves: this is the third in a row whose recorded
reasoning turned out to be wrong on inspection - STRLEN was said to be an alias for LENGTH
and is not, LPUSH and RPUSH were said to need a data migration and did not, and
wrong_arity() was said to be unable to know its own command. The entries are more reliable
about what is broken than about what fixing it would cost.

## 47. A lock nothing used, and the hypothesis it was never worth [10-08-2026]

`rh_shared::shared_mutex` was written as a shared lock for read heavy work loads, which
is what a cache is, so the intent was sound. It was never adopted. The shards latch on
`std::shared_timed_mutex` and always did, and a grep for `rh_shared` outside its own two
files returns nothing at all - `abstract_shard.h` includes the header twice and uses
none of it, `sastam.h` includes it and uses none of it. Three hundred lines, reachable
only from themselves.

That would be harmless if it were inert, and it was not. The class kept a registry of
live threads in a file scope `static rh_state s;` holding a std::mutex, an unordered_set
and a thread set, read on every lock and unlock. Static destruction order made that a
real hazard on paper: the threads that would have taken these locks are not all joined
before static destruction runs, and a thread reaching unlock() after `s` is gone locks a
destroyed pthread mutex, which does not fault - it blocks. A process in that state sits
at zero cpu forever.

Which is exactly what the stalled valkey-servers looked like, and that is the part worth
keeping. The shape of the hypothesis fitted the symptom precisely, and it was wrong. The
registry was rewritten into the canonical never-destroyed form, and the stalls continued
unchanged. Attaching to a live one settled it in a single command:

    gdb -p <pid> -batch -ex "set pagination off" -ex "thread apply all bt"

and the answer was somewhere else entirely - an abort inside the module running valkey's
sigsegvHandler, which blocks on signal_handler_lock and never returns (entry 35). A
hypothesis that fits the symptom is not evidence, and the code that fits it best here
could not have caused anything, because nothing ever called it.

So the resolution is not a fix. The whole of `rh_shared` is behind `#ifdef _EXPERIMENTAL_`
in both files - undefined, the header declares nothing and the implementation is a
translation unit with only its own include in it. Defined, it builds as before for anyone
who wants to finish the idea. Nothing else in the tree changes, since nothing else
referred to it: the suite is unchanged at 55 of 55.

This also disposes of a smaller thing noted in the entry. Lines 233 to 294 were a four
thread, ten million iteration stress test of the lock ending in `static int tested =
test();`, sitting inside `#if 0`. It is now inside two guards rather than one, so the one
character change that would have run a long threaded benchmark before main on every load
of the module no longer does.

Not covered here, and moved to TODO 57: two other file scope statics with the same
destruction order exposure - `art/art.cpp`'s `static std::mutex glob_queue{}`, and
`repl_api.cpp`'s `static restarter restart;`. Those are live code, unlike this, so they
are worth the reading that this one turned out not to deserve.

## 48. The kind of a collection, put in the key rather than beside it [10-08-2026]

A list, hash and ordered set under one name were one key range. Nothing separated them,
so HLEN walked the prefix and counted an ordered set's members as fields, ZCARD returned
the favour, and `SET` over a name holding a collection wrote a string beside it instead
of replacing it.

The entry proposed a stored type tag, written when a container is created and read where
a type is checked. What was implemented instead is a lead byte per kind - tcomposite_list,
tcomposite_hash and tcomposite_ordered_map, with tcomposite_extend reserved and defined as
"the next byte is the real code" so that the escape hatch is spent now rather than in a
later format break. The suggestion came from the author and is better than the entry on
three counts: the kind is part of the address, so it cannot disagree with the data or
outlive it; it costs no extra entry and no extra lookup, since HLEN simply stops seeing an
ordered set's keys rather than learning to skip them; and the entry's third open question -
check at creation or in every command - stops existing, because every command is confined
to its own range by construction.

What it does not do is stop both existing. Separate ranges mean HSET and ZADD on one name
would both succeed and be mutually invisible, which is two objects sharing a name rather
than a miscount. So the kind is claimed where a collection is created - HSET, LPUSH, ZADD,
ZINCRBY - and that is the only place three cross shard probes are paid for.

The order of the work mattered, and it is the part worth keeping. The fall through in
keys.cpp for an unrecognised lead byte is a bare abort(), and an abort inside valkey
deadlocks on the signal handler lock (entry 35), so a lead byte that a decoder had not
been taught about would not have produced a wrong answer - it would have produced a server
stuck at zero cpu. The four comparison sites were therefore replaced with a single
`art::is_composite_lead()` first, before anything wrote a new byte, and only then were the
bytes emitted. That predicate then absorbed the one mistake made here: a bulk edit matched
`.create({` but ordered_api.cpp reaches its composites through a handle, so 26 sites at
`->create({` kept the old lead and ZADD wrote where ZCOUNT was not looking. Six tests
failed and none of them hung.

Three things were found on the way that the entry did not predict:

  - container detection had never worked at all. `kind_of` built its probe prefix with
    `create()`, which zero terminates, and a prefix ending in the terminator cannot prefix
    a key that carries components after the name. It always answered none, so every
    container direction WRONGTYPE check in keys_api.cpp was dead code. Building the prefix
    unterminated fixed it, and STRLEN over a hash now answers WRONGTYPE where it used to
    answer 0.
  - DEL on an ordered set left its member index behind. The index is keyed member to
    score and begins with the index marker rather than the name, so a sweep of the name's
    prefix walked straight past it and a deleted set went on answering ZRANK. Now swept.
  - the python module under test is installed into the build's venv by a ctest step, so
    an ad hoc probe run between suites exercises whatever the last suite installed. Half
    an hour went on a guard that was not firing and instrumentation that would not print,
    both of which were simply not in the module being loaded. `venv/bin/pip install .`
    before probing, or do not believe the probe.

Storage version went to 12. Reusing 11 was tempting since 11 is recent, but if it has
shipped then a file written under it would be accepted and read with the wrong container
encoding, and being wrongly accepted is the one failure worse than being refused.

TestContainerKinds covers the claim and the counts, asserted against literals. The old
assertion in testcomposites.lua compares HLEN against the length of HKEYS, which holds
whenever both are wrong in the same way - and they were: see TODO 58, where a single field
hash reports zero from both.

## 49. A range over a shard holding one key answered nothing [10-08-2026]

Reported as HLEN and HKEYS returning empty for a hash with a single field while HGET and
HGETALL read it perfectly well. The entry guessed at the bounds those two commands build.
The bounds were fine, and the fault was neither in hashes nor in the container leads it
was found next to.

`inner_lower_bound` has an early return for the case where the walk reaches a leaf with
nothing in the trace:

    if (trace.empty())
        return (l->get_key() < key || l->expired()) ? nullptr : n;

It answers with the leaf and leaves the trace empty, which is honest - there is no parent
to record and nothing to increment to. `art::range` then opened with

    if (lb.null() || tl.empty()) return 0;

and lost the leaf entirely. So a range over a tree that is a single leaf returned nothing,
whatever it was looking for.

The trace was empty because the tree really was one leaf. It was never about the number of
fields: two fields share a container prefix and route to the same shard, so that shard's
root becomes an inner node and the trace fills in. One field sits alone in the shard it
routes to, and with 347 shards a collection with a single entry lands alone easily. HGET
and HGETALL were unaffected because neither goes through `range`.

Which makes the real scope wider than the entry: any range query over a shard holding
exactly one key answered nothing - hashes were only where it was noticed. `range` now
visits that leaf directly, with the same upper bound and expiry checks the loop applies,
and returns, since there is nowhere to advance to.

Confirmed by instrumenting the bail: it printed exactly twice for a one field hash, once
for HLEN and once for HKEYS, and not at all once a second field existed. After the fix
HLEN is 1, HKEYS is [f], and ZCARD, ZRANGE, ZCOUNT, KEYS and SCAN are all correct over a
single entry.

The reason this lasted is in the test that should have caught it. testcomposites.lua
asserts `HLEN == #HKEYS`, and both sides go through `range`, so both were zero and the
assertion held. A test that compares two implementations of the same walk agrees with
itself whenever they fail together. TestContainerKinds asserts the single entry case
against literals instead.

The second `art::range` overload, taking a LeafCallBack, has the same shape and is inside
`#if 0` with its call site in shard.cpp commented out. Left alone deliberately - it is not
built, and correcting dead code invites the belief that it works.

## 50. as_composite rewrote the key it was given [10-08-2026]

`strtok_r` writes a terminator over every separator it finds, and the buffer it was being
handed was the caller's key. So the first conversion of `1.1 a` turned it into `1.1\0a`
in place. A second conversion of the same key then found no separator, took the fast path
and encoded it as a single string containing an interior null, which `s_filter_key`
refuses:

    FILTER-THROW size=7 bytes=03312e31006100

This sat harmlessly for as long as every command converted its key exactly once. The type
checks broke that: a command that asks what a name holds before reading it converts twice,
once in the check and once to do the work.

Worth being exact about who broke what, because the first reading was wrong. GET was the
command that failed the suite, and GET's check was added the same afternoon - so it looked
like the new code was at fault. It was not. STRLEN and GETRANGE had been carrying the same
check since earlier in the day and were already broken by it; no test passes them a key
holding a separator, so nothing said so. GET is covered by redispytest.py line 147, and
that is the only reason any of it surfaced. The 08-02 build settles it: GET works there
and the other two do not exist yet.

Fixed as the entry proposed, by tokenising a copy. One thread_local string, assigned only
on the branch that has a separator to find, which is the only branch that reaches strtok_r
at all.

The general shape is worth remembering: a function that quietly mutates its argument is
correct until something calls it twice, and the second caller is usually a new feature
that has no idea it is the second. TestContainerKinds now converts one such key several
times over.

## 51. What a keyspace command sees when the name holds a collection [10-08-2026]

Once container detection started working (entry 48), the commands that work on names
rather than on collections turned out to disagree with redis in four places. Three are
fixed here and the fourth is documented as a deviation.

  - `GET` on a hash answered nil, as though the name were free. It now answers WRONGTYPE.
    STRLEN and GETRANGE already did; GET simply never called the check, which is the sort
    of gap that comes from adding guards one command at a time.
  - `EXISTS` answered 0 for a name holding a collection, because it looked only for the
    plain key and a collection has none. It now sees containers, which matters more than
    it sounds - EXISTS is the command redis expects a caller to ask this question with.
  - `KEYS` and `SCAN` reported the collection's internals rather than the name: a hash
    with two fields came back as `k:h f` and `k:h g`, and `KEYS k:h` came back empty. Both
    now report the name, once.

The name reporting needed the matcher changed and not only the reply. The pattern was
being matched against the rendered entry, so naming a hash directly matched nothing at
all - the reply shape was the visible half of the problem and the matching was the other.
One helper, `encoded_container_name_len`, gives the length of a container key's lead plus
its name component; slicing there leaves something that still decodes as a composite but
decodes to the name alone, so both matchers and both reply paths use the renderer that was
already there. Deduplication is by name, so the set stays the size of the reply rather
than the size of the key space.

SCAN's deduplication spans one call rather than the whole cursor, deliberately. SCAN
promises that everything present throughout an iteration is reported at least once and
explicitly allows repeats, so a container whose entries straddle two calls may be named in
both. Carrying the set in the cursor would hold memory for the life of the scan to buy a
guarantee the contract does not ask for.

DBSIZE is left counting stored keys, on the author's call, and is documented as a
deviation in both keyspace_api.cpp and the docs rather than quietly diverging. It reads
counters the shards already keep; counting names means walking the key space on every call
or maintaining a second set of counters on every container write, and barch's storage is
different enough from redis's that the number would still not line up.

Finding all this cost one unrelated bug, which is entry 50 - the type check converts a key
twice and as_composite was rewriting it.

## 52. The translation harness, and what it took to make it faithful [10-08-2026]

The machinery was already written and running when this entry was picked up again -
translate.py walks the tcl and emits the cases it can read, differential.py runs each one
against valkey first and only trusts the ones valkey passes. incr.tcl and scan.tcl, the
two proofs the entry asked for, were done: 26 of 29 incr cases translate, and scan.tcl
translates to nothing at all, for the reason recorded in the entry - half of it is SSCAN,
HSCAN and ZSCAN, which barch does not implement, and the rest is built out of while loops
and populate. The four scan tests that are about a promise barch actually makes are hand
written in test/scantest.py.

So what was left was the part the harness is for: the cases valkey itself rejected. Twelve
of ninety seven, and the entry's own rule says what that means - if valkey fails a case,
the translation is wrong, not barch. Reading them one at a time turned up three defects in
the translator and one in the comparison, and the count of faithful cases went from 85 of
97 to 89 of 95:

  - **tcl escapes were never resolved.** `"\x00foo"` was compared as the five characters
    backslash x 0 0 rather than as a null byte and foo. Quoted words now go through a tcl
    unescape - hex, octal, and the usual single letters - while braced words are left
    alone, which is what tcl itself does.
  - **expectations were stripped of their whitespace.** GETRANGE of "Hello World" from 5
    answers " World", and the leading space is the answer. Only a bare word may be
    stripped now; a quoted or braced one is kept verbatim.
  - **and stripped again on the way out.** `matches()` did its own strip, which quietly
    undid the fix above. That one is worth remembering: two layers each doing something
    defensible, and the second cancelling the first.
  - **a nil inside a list prints as {} in tcl**, not as nothing, so MGET over four keys
    with one missing reads `a b c {}`. The renderer now knows whether it is rendering an
    element or a whole reply.

Two cases used tcl's `binary format` to build their expectation, which is the tcl runtime
rather than anything barch could answer. They are stubs now rather than failures - the
same treatment as populate and the while loops - which is why the translated count went
down by two while the faithful count went up by four.

Six remain unfaithful and are meant to. Four depend on state that neighbouring tests set
up, and those neighbours are stubs: `KEYS with empty DB` contains nothing but an assertion
that the db is empty, and is only true after tests we cannot translate have run. `DBSIZE`
expects 6 for the same reason. Flushing before each case would make those two honest and
break the others, so they stay reported rather than papered over. The remaining two are
COPY and MOVE across databases, which is where barch's named key spaces and redis's
numbered ones stop lining up.

The headline is the one that matters: **barch agrees with valkey on all 89 faithful
cases**. Nothing in the accepted list has been quietly enlarged to get there - it still
holds only the eight cases for commands barch does not implement, each naming TODO 52.

## 53. hash.tcl and expire.tcl, and the crash they found in the first minute [10-08-2026]

150 tests translated to 51 cases, taking the harness from 95 translated to 146 and the
faithful set from 89 to 136. barch agrees with valkey on all 136.

The first run did not get that far. It cored:

    abort_with("invalid leaf size")  <- art::make_leaf  <- SET

`SET k v EX 10000000000000000` aborts the server. `now() + given * 1000` overflows to a
negative deadline, and the two places that decide how big a leaf is then disagree about
it: `leaf::make_size` is declared to take a bool and was handed the raw ttl, so any
non-zero value reserved eight bytes for an expiry, while the leaf constructor only records
one when it is positive. The leaf is built one size and measured another, and the check
between them aborts - which inside valkey is the signal handler deadlock of entry 35, a
server at zero cpu that ignores SHUTDOWN. A caller could do this from the command line.

Fixed at both ends, because either alone would have been half a fix. The commands refuse
an expiry that cannot become a deadline, with redis's own wording - "invalid expire time
in 'set' command" - through one checked conversion now shared by SET, EXPIRE, GETEX and
SETEX. And `make_leaf` passes `ttl > 0` so the two sizings use the same predicate, which
makes the abort unreachable rather than merely unlikely.

The rest of what the two files found, in the order the differential reported it - 29
differences, then 10, then 1, then none:

  - `is_integer` was `[0-9]+`, so a negative argument was a syntax error before anything
    could judge it as a number. redis reads it and then refuses the value, which is a
    different message and the one its clients match on. Now `-?[0-9]+`.
  - EXPIRE read one condition word and refused everything after it, so `EXPIRE k 10 NX GT`
    could not say what was wrong. It reads them all now and names the combination the way
    redis does - GT with LT, NX with anything.
  - an unknown word after the TTL, and an empty TTL, both said "syntax error". They say
    "Unsupported option AB" and "value is not an integer or out of range" now.
  - HGET and HMGET answered nil for a name holding a string, which reads as "no such
    field" rather than "wrong kind of thing". They answer WRONGTYPE.
  - HINCRBYFLOAT refused an infinite increment as "not a valid float". redis parses it
    perfectly well and refuses what it would do to the value - "value is NaN or Infinity".

The last difference standing was not a difference at all, and it is the one worth keeping.
`HSET hash f a` answered WRONGTYPE where valkey allowed it, reproducibly, on what looked
like a fresh store. It was correct: the key `hash` held a string - a Wikipedia search
result left in the .dat files in the build directory by some earlier session. barch loads
whatever is in its working directory and valkey starts empty, so the harness had been
comparing two different databases and calling the difference a defect. It flushes barch
before the run now. Two hours went into a bug that did not exist, and the fixture was
never in the frame.

What is left is written down rather than fixed: TODO 60 for the nine commands these files
expect and barch does not have - PERSIST, PTTL, PEXPIRE, PEXPIREAT, EXPIRETIME,
PEXPIRETIME, HSETNX, HMSET, HRANDFIELD, and LRANGE - each an accepted divergence naming
that entry. And TODO 61 for the deeper one: `art::now()` is monotonic since boot, so EXAT
and PXAT compare a unix time against a clock that started when the machine did, and a
persisted deadline means nothing after a restart. The differential found it as a trivial
overflow disagreement, which is the shallowest symptom it has.

## 54. The commands hash.tcl and expire.tcl expect, implemented [10-08-2026]

Ten commands, plus three the entry did not list and one it had no idea about. barch still
agrees with valkey on all 136 faithful cases, and now does it with the accepted list
holding one entry rather than ten.

The expiry surface came out as two shared implementations rather than seven commands.
TTL, PTTL, EXPIRETIME and PEXPIRETIME are one read with four ways of reporting it; EXPIRE,
PEXPIRE, EXPIREAT and PEXPIREAT are one write with two axes - the units the caller writes,
and whether the number is a duration or a moment. EXPIREAT was not on the list and fell
out of the same code. PERSIST is its own small thing, and answers 1 or 0 depending on
whether there was a deadline to remove, which is the distinction redis makes.

The hash surface: HMSET, HSETNX and HRANDFIELD as asked, and HSTRLEN and HVALS because
hash.tcl's wrong type case reaches for every hash command there is and finds the gaps one
at a time. HSET, HMSET and HSETNX are one write with three ways of answering. And LRANGE,
with redis's clamping - 0 to -1 is the whole list whatever its length.

Four things worth keeping, in the order they hurt:

  - **a regression of mine from entry 53.** Making the parse produce an absolute deadline
    left EXPIRE still adding now() to it, so `EXPIRE e 100` answered a TTL of 392759. The
    differential had not caught it because the cases that would are stubs, and it took
    writing PTTL - and checking its answer against TTL's - to see it.
  - **HRANDFIELD took the server down twice.** A negative count asks for exactly that many
    with repeats, so `-9223372036854770000` grew the reply until the process died. redis
    survives the same command because it streams to the socket while this builds the whole
    reply first, so beyond redis's two bounds there is a third at a million entries. That
    is a real difference and it is written down as one rather than left as a surprise.
  - **EXAT and PXAT were silently wrong and are now right.** They hand over a unix time and
    barch measures deadlines against a clock that starts at boot (TODO 61), so the caller's
    moment is now read as a distance from now and re-expressed. `SET e v EXAT now+90` gives
    a TTL of 89 where it used to give decades. A deadline already past deletes the key, as
    redis does - which took two goes, because taking the difference first made a large
    negative wrap to an enormous positive.
  - **the differential could not have caught the worst of it.** HMSET was already
    registered with the valkey module and a second registration made module init fail, so
    every lua test aborted with "Can't load module" while the differential stayed green.
    The RESP server and the module register through different tables; a green differential
    says nothing about whether the module loads. Only the full suite covers that.

Two barch tests changed rather than the code. `EXPIRE k 100 NONSENSE` answered "syntax
error" and now names the word, and SETEX's "invalid expire time" now names the command.
valkey answers both the new way and expire.tcl asserts them, so those expectations were
ours alone and were the thing that was wrong.

Left behind: HSCAN, which is a cursor scoped to a prefix rather than a wrapper over
something that exists. It is TODO 62, and it is the last accepted divergence in the hash
file - every other command in that case now answers WRONGTYPE correctly.

## 55. Expiry measured against unix time rather than machine uptime [10-08-2026]

`art::now()` was `steady_clock`, whose epoch is when the machine started, and every
deadline in the store was measured against it. Within a single run that is coherent -
EXPIRE stored now+10000 and the key went when the clock passed it - which is exactly why
it survived this long without anyone noticing.

Two things were wrong with it anyway, and only one of them had a symptom.

The visible one was absolute time. `EXAT`, `PXAT`, `EXPIREAT` and `PEXPIREAT` take a unix
time from the caller and it was being compared against a clock counting from boot, so on a
machine up for an hour `EXAT <now+60>` described a deadline about fifty five years away.
That was patched first, in DONE 54, by reading the caller's moment as a distance from now
and re-expressing it - correct, and a workaround.

The one with no symptom at all is the reason to fix the clock rather than the commands: a
deadline written to a shard file meant nothing after a restart, because the clock it was
measured against restarted too. Nothing reports that, no test could catch it in one
process, and the data is silently wrong from the moment it is loaded.

So `now()` is milliseconds since the unix epoch, and the workarounds came out rather than
accumulating. expiry_ms no longer translates between two frames, since there is only one;
EXPIRETIME and PEXPIRETIME answer the stored number instead of rebuilding it from the time
remaining; the local unix_now_ms helpers are gone from two files.

Every caller of now() was read before changing it under them. `time_conversion.h` has its
own, on high_resolution_clock, and is untouched. The only duration measured with this one
is a client's age in INFO, which is cosmetic if the clock moves.

The cost is real and is written down where the function is: a wall clock can step. An NTP
correction moves every deadline relative to now, and keys expire early or late by however
far it moved. redis has the same exposure for the same reason, and a stepped clock is a
smaller surprise than a saved deadline that means something different tomorrow.

Storage version 13. A deadline saved at 12 reads as a moment in 1970, so the key it belongs
to would be expired on load - refusing the file is the only safe answer.

The differential went from 125 to 126 cases: valkey refuses `EXPIRE foo 9223370399119966`
because adding its basetime overflows, and barch used to accept it because its basetime was
the few hours since boot rather than fifty five years of unix time. That case is no longer
an accepted divergence, and the accepted list is down to one entry.

Worth noting how it was found, since it was not found by looking. It surfaced as a trivial
disagreement about an overflow in a translated test - the shallowest symptom it has - and
the entry it turned into was written while explaining why that one case could be accepted.
The restart problem was never observed at all; it followed from reading what the clock was.

## 56. zset.tcl and list.tcl, and a blocking pop that took the store with it [10-08-2026]

The last of the four translation entries. 316 tests became 102 cases, taking the harness
from 146 translated to 248 and the faithful set from 136 to 226. barch agrees with valkey
on all 226.

The entry expected the work to be filtering rather than translating, and it was - most of
zset.tcl is set algebra and most of list.tcl is commands beyond push and pop, none of which
exist here. What it did not expect is that three of the four hardest problems were in the
harness rather than in either file, and one was a server that stops answering.

**A blocking pop with no bound on its timeout.** `BLPOP blist1 0x7FFFFFFFFFFFFF` was
accepted - the timeout was read after the locks were taken and never judged - so the wait
never ended, and because the locks were already held the whole store went with it. Every
command after it answered nil until the server was restarted. That is what forty seven
"differences" turned out to be: string.tcl running after list.tcl against a wedged server.
The timeout is now read before anything is locked and judged as redis judges it, with
strtod rather than the strict reader so that a hex or exponent form is a number that is too
big rather than a word that is not a number. `BLPOP notalist 0` was the same shape from the
other side - a wrong type that blocked forever instead of answering - and now answers
WRONGTYPE.

**Two more cases poisoning every case after them.** `HELLO 3` succeeds and leaves the
connection speaking a protocol the client is not parsing, so replies are misread rather
than refused; `MULTI` with a command the server does not know leaves a transaction the next
case inherits. Both were reported as barch failing cases it had never been asked about. The
runner now takes a fresh connection after any case that changes the connection's state.
Recognising this took longer than it should have because the symptom - a string command
answering empty - looks nothing like its cause.

**A translator that stopped at the first surprise.** zset.tcl has a body whose braces the
scanner cannot read, and the exception took the whole run down rather than the case. A case
it cannot parse is a stub like any other now, which is the difference between 0 and 77
translated cases from that file.

What was actually wrong in barch, beyond the blocking pop:

  - `LLEN` and `LPOP` answered as though a string were an empty list rather than saying
    wrong type.
  - `ZADD` took its score on trust. `ZADD z nan m` stored the word and answered as though
    it had worked, and so did an empty score - the score is judged before the key check
    now, because an unreadable score is a bad score rather than a bad key.
  - `ZINCRBY` refused `+inf`, which redis stores happily, and said "invalid argument" for
    everything - it reads scores the way redis does now and refuses only a NaN, in redis's
    wording.

Left written down rather than fixed: TODO 63 for the commands these two files expect and
barch does not have - the set algebra, ZSCORE and ZMSCORE, ZRANDMEMBER, the list moves -
and TODO 64 for the range bound validation, the NaN result that the member index path
misses, and two cases that pass alone and fail in sequence.

One thing worth carrying into 63. Twice today a case has failed reproducibly and turned out
to be correct - `hash` holding a leftover string, and these two zset cases. Each time the
first instinct was that barch was wrong. The differential is only as good as the fixture,
and when a case fails alone it is a bug, while a case that fails only in company is a
question about the company.

## 57. HSCAN, and a cursor scoped to a prefix [10-08-2026]

The entry said HSCAN was the one of the three worth doing - there is no set type for SSCAN
and ZSCAN duplicates what ZRANGE covers - and that the work was a cursor scoped to a prefix
rather than to a shard. That turned out to be right, and smaller than it sounds.

A hash does not span shards. Its fields are a contiguous run of keys under one prefix in
the shard its name routes to, so a position in that run is a whole cursor, where a key
space scan needs a shard and a page as well. `scan_cursor` already carries a buffer for the
page it is walking; HSCAN uses it to hold the key to resume from, and everything else about
the cursor - how many a connection may hold, what they cost, when they are dropped - is the
machinery SCAN already had.

The contract is redis's: a full iteration reports every field present throughout it,
repeats are allowed, COUNT is a hint about work per call rather than a promise about the
size of the reply, and a cursor the server does not recognise starts again from the
beginning rather than erroring, since a client is allowed to lose one. MATCH and NOVALUES
both work.

One thing cost most of the time, and it is worth writing down because it will recur. MATCH
found nothing at all - not even `*`. The field name taken out of a key is still encoded: a
component carries its own type byte and terminator, so the pattern was being compared
against bytes no caller ever typed. The fix is to hand it back to the ordinary key
renderer, and that needs the right framing - the renderer starts reading components at byte
two, so a lead byte alone is not enough and the component needs the companion byte a real
composite carries between its parts. A pattern matching nothing whatsoever, rather than
matching badly, is the signature of comparing against raw encoded bytes.

`Hash commands against wrong type` is no longer an accepted divergence - it was the case
that walks every hash command, and HSCAN was the last one it could not reach. The accepted
list is down to the commands that genuinely do not exist.

SSCAN and ZSCAN are not done and are not planned. There is no set type at all, and ZSCAN
would answer what ZRANGE already answers with a cursor bolted on; if a caller needs to page
an ordered set, positions do it without server side state.

## 58. The set algebra, which was answering a different question [10-08-2026]

`ZINTER` was not a slow or approximate intersection, it was a different operation. For
each member of the first set it sought the other set at `{set, score}` and checked whether
the score sitting there matched, so what it actually asked was "does that set hold anything
at all with this score". Two sets sharing a score looked like a match whatever their
members were, which is why an intersection came back looking like a union - and why
`ZINTERSTORE` counted three where valkey counted two.

The store has an index for exactly this question - member to score, the one ZADD maintains
alongside the ordered keys - and membership now goes through it.

Two more things in the same function were wrong and had never been visible underneath the
first. `WEIGHTS` was indexed by how far through the first set the walk had got rather than
by which input the score came from, so the weights applied to arbitrary members. And only
the first set's score was ever used, which left `AGGREGATE` with nothing to aggregate; it
had grown a barch specific meaning instead, reducing the whole reply to a single number.
Both are now what redis means: each member's score is what AGGREGATE does with the scores
it had in every input it appeared in, after each was multiplied by that input's weight.

`ZUNION` and `ZUNIONSTORE` did not exist, and the comment where the union case should have
been said "does not work yet". It could not have: the loop walked the first set and asked
about the others, and a union has to walk them all. With the loop reshaped around a map
from member to accumulated score, union is the same code as the other two.

Added with it: `ZSCORE` and `ZMSCORE`, which fall straight out of the member index helper,
`ZRANDMEMBER`, which is HRANDFIELD over an ordered set including the bound on a negative
count that keeps a reply from taking the process down (DONE 54), `ZREMRANGEBYSCORE`, and
`LPUSHX` with `RPUSHX`.

Three option rules were wrong as well, all found by the differential rather than by
reading: `WITHSCORES` with `AGGREGATE` was refused for every command, when that is the rule
for the STORE forms alone - they have nowhere to put scores in a reply; the arity demanded
four arguments, so `ZUNION 1 s` with a single input was a wrong argument count; and
`ZINTERCARD` never parsed `LIMIT` at all. redis distinguishes a missing LIMIT value, which
is a syntax error, from one that is a word or negative, which gets the LIMIT wording, and
so does this now.

barch went from 159 of 247 translated cases to 195, and ten accepted divergences were
removed rather than added.

The one that needed judgement is in testcomposites.lua. Four of its assertions encoded the
old behaviour, and two of those depended on the barch specific AGGREGATE - `ZINTER ...
AGGREGATE SUM` answering the string "16.5". That is not a reply shape anyone should depend
on, and it only existed because the intersection was wrong: the number was the sum over
whichever members happened to share a score. The assertions were rewritten to what redis
answers, with the values checked against a live server first rather than worked out on
paper. Anything relying on a scalar from AGGREGATE is broken on purpose.

Left in 63, deliberately rather than for lack of time: `LINSERT`, `LMPOP`, `ZMPOP` and
`BZMPOP`, and the moves - `RPOPLPUSH`, `LMOVE` and `BRPOPLPUSH`. The moves are why the line
is drawn here. They take two keys, so they need the lock order of DONE 42, and the last
two key path written without that care self deadlocked and took a long time to find. That
is not work to start at the end of a long session.

## 59. Ordered set validation, and two cases that were not what they looked like [10-08-2026]

The entry asked for range bound validation and named two cases that "pass on their own and
fail only in the translated sequence". It ended with a caution: confirm that is all it is,
because a duplicate member in a range answer is exactly what a real bug would look like
too.

Both were real bugs. Neither had anything to do with the sequence.

**ZREM abandoned the rest of its arguments, and could remove the wrong member.** The member
index is walked with an iterator, which is a lower bound, so a member that is not there
lands on whichever member comes after it. The code then did two things with that: it
`break`ed out of the whole loop, so `ZREM k missing present` removed nothing and answered
0; and it never checked the key it landed on was the one asked for, so the member that
happened to be next could be removed in place of the one that was not there. The second is
the serious one - a caller removing a member that does not exist could lose one that does.
It now compares the found key against the sought key and skips rather than breaking.

**ZINCRBY never wrote the member index for a new member.** A member has two keys, the score
ordered one and its entry in the index; that branch wrote only the first. So nothing could
find the member afterwards: ZSCORE answered nil, and the next ZINCRBY did not find it
either - instead of adding to the score it wrote a second entry for the same member. That
is where the duplicate in a ZRANGE reply came from. `ZINCRBY z 5 k` twice gave 3 rather
than 8.

**ZADD could not store a positive infinity.** It re-encoded the score from the text with a
reader that does not accept "inf", so `+inf` was stored as the string "inf" - a component
of a different shape - and every read of that member missed it. `-inf` survived, which is
why the two behaved differently and why the asymmetry looked like nothing in particular. It
now encodes from the number it has already parsed and validated, which is the same number
the NaN check uses.

With those three fixed, the NaN result the entry describes appears on its own: `+inf` then
`-inf` now reaches the check and is refused, where before the second call could not find
the member at all.

The validation the entry asked for: ZRANGEBYSCORE refuses a bound that is not a number,
ZRANGEBYLEX refuses one without the leading `(` or `[`. That second one needed more than a
check - barch had never understood redis's lex syntax, only the bare form, so validating it
would have refused the only bounds it could handle. The bracket is stripped before the
bound is encoded now. Exclusivity is parsed but not yet honoured in the walk: `(a` behaves
as `[a` does, which is TODO 65.

Three test expectations changed, and all three were encoding a bug rather than a choice:

  - testcomposites.lua used bare lex bounds, which is what barch understood and redis
    refuses.
  - testbarch.py asserted `z.remove("z1","1") == 1`. "1" is a score in that set, not a
    member, so the answer is 0 - it only ever returned 1 because of the ZREM bug above,
    which means that line was quietly deleting "two".
  - the same test asserted the store shrank by one key when a member was removed. It
    shrinks by two, the score key and the index entry. Expecting one is what the old ZREM
    actually did: it removed the score key and orphaned the index entry.

And a trap worth knowing about in the python binding, found while fixing that test:
`remove()` takes a list of members, and a bare string is taken as a sequence of characters,
so `z.remove("k","two")` asks to remove "t", "w" and "o" and removes nothing. The line only
looked right because "1" is one character long.

All six of the entry's accepted divergences are gone rather than re-explained. barch went
from 195 of 247 cases to 201.

## 60. An exclusive lex bound, and the two ends of the range [10-08-2026]

The entry described this as a flag that was parsed and thrown away, and said honouring it
meant skipping the first entry when the bound is exclusive. That is exactly right for the
start and not enough for the stop.

The start is what the entry describes: the walk begins at the member the bound names, so
skipping that first entry is the whole of it.

The stop is not, and the first attempt at it looked correct and did nothing. The loop
compares a truncated composite - the key cut to the length of the bound - against the upper
key, and the member equal to the bound is a *prefix* of that key, so it reads as less than.
Changing `<=` to `<` therefore excluded nothing, and `[a (c` went on answering a, b and c.
It recognises the whole key for the named member now and ends the walk before reporting it,
which is what the ordering already guarantees is the right place to stop.

`-` and `+` were not in the entry and had to be done with it. The validation added in
DONE 59 accepts them, because redis does, but the walk had never supported them: barch only
understood a bare member as a bound, so `ZRANGEBYLEX k - +` - the ordinary way to ask for
everything - would have looked for members named `-` and `+` and found none. A validator
that accepts syntax the implementation mishandles is worse than one that refuses it, so
`-` starts the walk at the first member there is and `+` drops the upper bound entirely.

All eight cases were checked against a live server: inclusive at both ends, exclusive at
the start, exclusive at the stop, exclusive at both, `- +` over the whole set, `- [b`,
`(b +`, and a bare bound still refused.

The regression is hand written in TestContainerKinds, and the entry said why before the
work started: nothing in the translated valkey tests covers lex exclusivity. That is how it
went unnoticed in the first place, and a test that only exists because someone predicted
its absence is worth more than the fix it guards.

## 61. A logical export, so a version bump has somewhere for the data to go [10-08-2026]

`storage_version` refuses a shard file written by a different build, which is right - a
format that has changed should not be read as though it had not. What it leaves behind is a
user whose data is intact and unreadable. That version moved three times today alone: the
tplain key encoding, the per kind container leads, and the clock. Each time the answer to
"what do I do with the data I already have" was nothing.

EXPORT writes the current key space as the commands that would rebuild it - SET, HSET,
RPUSH, ZADD - and IMPORT replays them. It is deliberately not a copy of anything: a page
dump is faster and smaller and is exactly the thing that stops working when the format
moves. Commands go through the ordinary command path, so they land correctly on any build
that has those commands at all, which is the only property that matters here.

The stream is RESP rather than lines, and that is not a stylistic choice. A value may hold
a newline, a null, or nothing at all, and a line based format loses all three; the test
exports `a\\0b\\nc\\r\\nd` and reads it back. It also means the file can be replayed by
anything that speaks the protocol - `redis-cli --pipe` - and not only by IMPORT.

Two details worth stating because they are decisions rather than mechanics. An expiry
travels as an absolute PXAT, so a slow export does not quietly shorten every deadline the
way a remaining-seconds form would. And IMPORT merges rather than replaces: it overwrites
the names the stream mentions and leaves everything else alone, so clearing the space first
is the caller's choice to make.

IMPORT replays each command through a caller of its own. Handing it the connection's caller
would push every reply into the reply being built, and IMPORT answers with a count.

The round trip is a test rather than a claim: seven keys of every kind through a FLUSHALL
and back, including an empty value, a binary one, and a set whose scores are negative. It
also asserts that a two member ordered set exports as one ZADD, because exporting a set
twice is what it looks like when the member index is mistaken for data.

Which is the one thing this turned up and did not fix. `KEYS` reports an ordered set twice,
as `z` and as a phantom `\\x03z` - its member index, read as though it were a key someone
wrote. It predates this work, comes from the name reporting of DONE 51, and the export is
not affected by it. Three ways of telling the index apart were tried and all three failed;
what each one established is written into TODO 66 so the next attempt starts from facts
rather than from my guesses. The hook where the answer goes is already in place and is a
no-op until someone settles how an empty component encodes.

## 62. The member index had no empty component, so it collided with a real name [10-08-2026]

`KEYS` listed every ordered set twice: once as itself and once as a phantom made of its
index bytes. Five attempts were made at telling the two apart in the reader and all five
failed, which was the clue and was not read as one.

The author asked for examples of keys with empty components. There were none. That is the
whole answer:

    a real empty component      {""}                 0a 01 03 01
    two, empty first            {"", "z"}            0a 01 03 01 03 7a 01

    what the writer produced    {IX_MEMBER, z, a}    0a 01 03 03 7a 01 03 61 00
    a set actually named \\x03z  {"\\x03z", a}         0a 01 03 03 7a 01 03 61 00

`IX_MEMBER` was the bare literal `""`. Neither `comparable_key("")` nor
`comparable_key("", 0)` builds a component - both leave out the separator that ends one -
so the component after the marker merged into it, and the index key for a set named `z`
came out byte for byte identical to the score key of a set named `\\x03z`. Only `convert()`
lays a component out properly, and nothing had ever passed an empty value through it.

So the two keys were the same bytes. No reader could have distinguished them, and every
attempt to write one was an attempt to answer a question the data could not answer. This is
the same shape as the tplain collision of entry 48 - two different things sharing one
encoding - and it has the same kind of fix, which is at the writer.

`conversion::empty_component()` builds the marker through convert(). The index is ten bytes
now instead of nine and cannot be confused with anything; the readers skip a container key
whose name decodes empty, which is what the index genuinely is once it is written properly.

Storage version 14, because the stored shape of every ordered set's index changed. That is
the third bump today, which is exactly the situation the export of entry 61 exists for.

Two things said earlier in the session were wrong and are corrected here, because both were
reported with confidence:

  - that a probe showed the index and score keys decoding to the same name. They do not.
    The probe printed candidates with `%s`, and the index decodes to a name with a leading
    0x03 - invisible in a terminal, and identical to the right answer at a glance.
  - that an empty component encodes as a bare `03`. It encodes as `03 01`. What the writer
    produced was not an empty component at all.

The lesson is the one the failures were pointing at the whole time: five reader side fixes
that each looked reasonable and each did nothing is not five mistakes, it is evidence that
the distinction being asked for is not present in what was read. The question to ask after
the second failure is not "what else could tell these apart" but "are these actually
different".

## 63. Per shard statistics, so clearing one tree stops erasing the rest [12-08-2026]

The reproduction from the entry, before and after:

    USE alpha; 5000 keys        leaf 5000   n4 1684   logical 215219
    USE beta;  5000 keys        leaf 10000  n4 3403   logical 428434
    FLUSHDB   (beta only)       leaf 5000   n4 1684   logical 215219     was 0 0 0
    USE alpha; DBSIZE 5000
    FLUSHDB   (alpha too)       leaf 0      n4 0      logical 0

The interesting line is the third rather than the fourth. Clearing beta returns every
counter to exactly the value it had when only alpha existed - not approximately, the same
integers - which says the increments and decrements are matched per shard and not just
that the subtraction happens.

`owned_content_stats` sits on `abstract_alloc_pair` (logical_address.h) rather than on
`alloc_pair`, which was the one design decision worth making carefully. The allocator holds
its pair as `abstract_leaf_pair*`, and the node templates reach theirs through
`address.get_ap<>()`, so putting the counters on the base is what lets the three
`logical_allocated` sites in logical_allocator.h and the node and leaf sites all reach them
without a cast or a new accessor. `node256` was the only site that could not: it touches
`node256_occupants` from three places and `address` is private to it, but
`encoded_node_content::get_logical()` already existed and returns exactly the pair.

The globals stay, maintained beside the per shard ones. That was the open question in the
entry and the answer is that `logical_allocated` is read on the insert and eviction paths -
shard.cpp 698, 739, 768, 1243, 1268, 1274 and hash_arena.cpp:154 - so a sum over 347 shards
per insert was never a serious option. Nothing reads the sum; INFO reads the same global it
always did.

Load and rollback turned out to be one function. `stream_to_stats` reads the file into a
temporary and moves each global by `loaded - owned` before assigning `owned = loaded`. On a
load `owned` is zero, so the globals gain the whole of what the file holds, which is the
fix. On a transaction rollback - `shard::begin` snapshots through the same pair of
functions - `owned` holds whatever the transaction did, so the globals give back exactly
that and no more. The delta form is not a cleverness, it is what both callers need.

Two counters are deliberately not per shard. `value_bytes_compressed` is incremented in
keys_api where no shard is in scope, and `oom_avoided_inserts` counts refusals rather than
contents; neither is attributable to a tree. Both are now written as zero into the shard
files and ignored on read, and `_clear` leaves them alone along with `keys_found`,
`new_keys_added` and `keys_replaced` - clearing a shard does not unmake something that
happened. `value_bytes_compressed` therefore no longer survives a restart, which is a real
if small loss and is the honest position: it was persisted before, but per shard, and
whichever shard loaded last decided the value.

Storage version 15, because the shard file no longer means the same thing by those fields.

`test/shardstatstest.py` measures the gauge against `DBSIZE`, which is independent of it,
across an operation that touches part of the store. That is the shape that catches this
class of bug: the old code passed every existing statistics test, because they all read the
counters right after filling a single space.

## 64. A failed EXPORT leaves an empty file where the old one was [12-08-2026]

*Was `TODO.md` entry 68.*

EXPORT opened the destination with `std::ios::trunc` before it walked anything. The
memory-ceiling refusal in `names_in` - and the write-error path after the walk - then
returned with that file already empty. For a command whose purpose is a way back from a
bad state, that is the wrong way round.

It writes `path.tmp` now and only `rename`s it onto the target after `flush` succeeds.
A rename in the same directory is atomic, so the path is the previous export or the new
one and never a partial file. The temporary is removed on every failure path, including
a rename that does not take.

The ceiling is still the only failure the suite can provoke on purpose. The test writes a
good export, drops `max_memory_bytes` to 1, exports again to the same path, and checks
that the bytes did not change and that the `.tmp` is gone. The existing round trip still
covers a successful replace.

The command index said the path was "created or truncated". That described the old open
and is no longer true, so the arg and the reply now say the file is replaced only when
the export finishes and a failure leaves the previous one alone.

## 65. The list commands zset.tcl and list.tcl expect, and the two key moves [12-08-2026]

*Was `TODO.md` entry 63.*

What was left after DONE 58. The ordered set algebra was already in; this is the list
side and the multi-key pops, including the two-key moves that stopped the earlier
session.

`LINSERT` keeps the list dense. A middle insert shifts everything after the hole up by
one. A gap would break `LLEN` and the pops, which assume consecutive indices. Inserting
at an end is `LPUSH` or `RPUSH` and does not move. Missing key answers 0, a missing
pivot answers -1, a string is WRONGTYPE.

`LMPOP` / `BLMPOP` and `ZMPOP` / `BZMPOP` pop from the first of several keys that has
anything. One key takes its shard; several take the space, the way `BLPOP` already did.
The blocking forms share `parse_block_timeout` in keys.cpp - the check that used to live
only in `bpop`, so a second copy could not hide another unbounded wait (DONE 56).
`ZMPOP` removes both the score key and the member index, which is what `ZREM` does.
`ZPOPMIN` / `ZPOPMAX` still only remove the score key; they were not rewritten, but they
now say WRONGTYPE on a string, which is why `ZPOP/ZMPOP against wrong type` could leave
the accepted list. `ZADD` wakes a blocked `BZMPOP`.

The moves are `LMOVE`, `RPOPLPUSH` (`RIGHT` then `LEFT`), `BLMOVE` and `BRPOPLPUSH`.
They go through `with_two_keys_write`, lock by shard number, collapse to one lock when
both names land on the same shard, and copy the value bytes out before the destination
is written. Type probes run before those locks. Same-list `LMOVE` is one lock and a
pop then a push on the same header.

list.tcl: barch agrees with valkey on all 24 faithful cases. The `$type` bodies are
still stubs. zset.tcl's new `ZMPOP` cases pass; the one remaining zset difference is
`ZUNIONSTORE` writing `-nan` where valkey writes `0` for opposite infinities, which
predates this work.

## 66. The command index did not know the commands from DONE 65 [13-08-2026]

*Was `TODO.md` entry 71.*

The nine names were registered and the list.tcl differential already agreed with
valkey. The family tables and the `CMDS` blob in `docs/index.html` still stopped at
`LRANGE` and `ZPOPMAX`. Clicking a new name would have shown nothing.

There is no generator in the tree. The blob was produced once (entry 35) and is a
single JSON object in the page script. The nine entries were added to that object
and a row was added for each name in its family. Replies and arity errors were taken
from a live server. None of the nine has a SWIG method, so the Python and Lua lines
go through `execute`, as EXPORT does.

The index chip now reads 163 names and 160 handlers. Each of the nine names is in
its family table and in `CMDS`. Nothing on the page still lists them as missing.

## 67. Claude-isms in docs/index.html [14-08-2026]

*Was `TODO.md` entry 72.*

The command tables and the `CMDS` blob were left alone. The tics sat where the
entry said they would: ledes, design-context blocks, and notices. The memory
article was the densest. The limitations lede restated its first design-context
paragraph almost word for word, so the duplicate paragraph was dropped.

The real contrasts stayed. `DBSIZE` still counts stored keys, not names. An
over-ceiling insert still fails silently instead of raising. Range sharding
still routes by range, not by hash. A layered delete still leaves a tombstone.
Expiry is still against the system clock, not a monotonic one. A too-large
reply still fails the call instead of growing the buffer. A bad prefix still
errors instead of falling back. An unset space option still takes the server
default, not whatever the data was written with. The 409 notice still says
those outcomes are data, not faults. The range-sharding branch note still
starts "This page documents", because it names which branch the article
describes.

The cadence went. Headings and openers that told the reader what they were
about to learn, or sold the sentence before stating it:

- "What this site covers" / "Per project decision"
- "For teams fronting BARCH with an HTTP gateway"
- "What is extended is the interpretation of the command name"
- "Why it is worth using"
- "This site documents each command once"
- "This page covers how to design keys"
- "the headline consequence, as measured by the project"
- "Why this is a design lever, not just a property"
- "the single highest-leverage decision"
- "this is the X use case" / "the standard overlay/copy-on-write shape"
- "Training is optional but leverage is not"
- "a superset rather than a departure"
- "disagree on purpose rather than by oversight"
- "process boundaries are the L1 security perimeter"

Decorative "rather than" was rewritten as the fact plus the thing it is not,
in its own sentence, when that second thing was a refusal the caller can hit.
The remaining "rather than" in the file are table notes and `CMDS` summaries,
which this pass did not touch.

## 68. User and developer sections in docs/index.html [14-08-2026]

*Was `TODO.md` entry 73.*

The mix was an audience problem, not a missing article type. Callers of barch
are users even when they are technical. Developers are people who work on the
tree. Platform and Interfaces did not say that, and Interfaces sat between
the getting-started pages and the command reference, so the binding write-ups
read as a step on the way to SET.

Renaming the group was not enough on its own. The three pages moved below
Reference. Platform became User. Interfaces became Developer. Article ids
stayed, so existing `#resp` links still work. The three pages kept the
three-block spine: they still document a surface, they just are not where a
caller starts.

RESP belongs with the two SWIG pages. Against the command index, that
article's job is the listener, dispatch, and the prefix on argument zero.
A caller connects from Quickstart and looks up commands in Reference. The
prefix form stays on the RESP page; the keyspaces table still points at it.

`DOCUMENTATION-STANDARD.md` now states the split and forbids the old group
names. The overview's access-surfaces paragraph points callers at Quickstart
and the command index, and at Developer for how the surfaces are bound.

## 69. art_* tree functions moved into namespace art [14-08-2026]

*Was `TODO.md` entry 74.*

The guess was right. The live leftovers were the functions declared in `art.h`
and defined in `art.cpp`. `art_statistics`, `art_ops_statistics`,
`art_repl_statistics`, `art_evict_lru`, `art_sessions` and `art_fun` are
structs, a shard helper, a counter and an RPC type. They stayed. `tree_destroy`
is the same old C entry point but is not an `art_*` name, so it stayed too.

The names dropped the prefix to match `art::maximum`. `delete` is a keyword, so
`art_delete` became `art::erase`. The rest are `art::insert`,
`art::insert_no_replace`, `art::search` and `art::minimum`. `art_iter` and
`art_iter_prefix` already had `#if 0` definitions; their declarations went
behind `#if 0` as well so they are not a live `art::iter` with no body.

`art::minimum` collided with a file-static `minimum` that walks a node. That
helper is now `inner_minimum`, next to the existing `inner_maximum`.

Call sites were only in `shard.cpp`. They now say `art::insert`, `art::erase`,
`art::search` and `art::minimum`. No `using art;` was added. `lbarch` built.

## 70. SAVE and RELOAD raced the range rebalancer [14-08-2026]

*Was `TODO.md` entry 75.*

CI failed `rangeroutetest.py` on one worker at

    assert shard_sizes(r, "rs_route", SHARDS) == before_reload

The log had no sizes, so the later GET and RANGE checks never ran. The question
the assertion was meant to answer - did reload restore the same partition - was
not answered either.

DONE 31 already said shard sizes read one at a time do not add up while a sweep
is running, and blamed the test. That is still true. It is not the whole of this.
SAVE and RELOAD took no space lock. The sweep moves a key under write locks on
two neighbouring shards. A parallel save that has already written shard A and
has not yet written shard B will persist that key twice, or not at all, if the
sweep crosses the pair in between. RELOAD does `_save`, `_clear`, `_load` per
shard the same way, then rebuilds the table from whatever is left. The assertion
compared the vector captured before that window with the vector read after it.

`settle()` returned at 1.30x. The sweep stops at a band around the average,
1.25x above and 1/1.25x below. Anything between those two is a space the test
called done and the sweep still worked on. The next maintenance tick is 440ms
later. A loaded worker that settled at 1.26-1.30x and whose tick landed on save
or reload is exactly one CI failure.

SAVE of a range-sharded space now takes a space read lock for the snapshot. The
sweep waits. Hash-sharded spaces do not pay for that lock; their keys do not
move. RELOAD takes a space write lock for the reload and the table rebuild.
`shard::reload` no longer takes the latch itself: a worker waiting on a lock
the command thread already holds never returns. Failed shard reloads increment
the error count. They did not, before, so RELOAD always answered OK.

The test still allows 1.30x. It now waits for two identical size readings so a
return is between ticks. After reload it compares, and if a tick landed in the
gap it settles again and checks that every key is still there and no shard is
empty. Tightening settle to the sweep's 1.25 band timed out on the post-delete
`rs_rand` space: the live sweep often sits a little above the target once
inserts have stopped, which is why 1.30 was there.

Left alone: LOAD still replaces shards in parallel without a space lock and
does not rebuild the table. SAVEALL still walks every shard of every space the
same way. Neither is on the path the test takes. That is TODO 76.

Measured: `rangeroutetest.py` four times green, `rangeshardtest.py` once.

## 71. LOAD and SAVEALL raced the range rebalancer [14-08-2026]

*Was `TODO.md` entry 76.*

DONE 70 froze SAVE and RELOAD and left these two. They were the same race on
paths the test did not take.

LOAD replaced shards in parallel with no space lock, then returned. The
routing table is never persisted. A range-sharded space that is LOADed
therefore kept the table it had before the files were read, so a key whose
span moved on disk was looked up in the shard that used to own it. GET and
RANGE would miss. The sweep could also move a live key into a shard that had
already been replaced, or out of one that had not, and lose it.

SAVEALL walked every shard of every space the same way SAVE used to: no
space lock, so a sweep that crossed a pair mid-write persisted that key
twice or not at all.

LOAD now takes a space write lock, loads without taking the latch again
(`load_holding_lock`), and rebuilds the table before those locks drop. It
also `_clear`s each shard before `_load`. Overwriting a live tree without
that left the arena free list populated, and `read_emancipated` logged
"erased should be empty" once per shard. RELOAD already cleared; LOAD is
the overwrite path and did not.

SAVEALL collects every range-sharded space, takes a space read lock on each
in canonical name order, then writes every shard. Hash-sharded spaces are
walked without that lock. Their keys do not move.

`rangeroutetest.py` now LOADs `rs_route` after a save, and SAVEALLs a new
ascending space without settling first, then LOADs it. Both must return
every key, in order. The second is the case a sweep is still working.

Measured: `rangeroutetest.py` green, including the two new sections. No
"erased should be empty" once LOAD cleared first.

## 72. Stateful sharding is a key space check, not a range-sharding special case [14-08-2026]

*Was `TODO.md` entry 77.*

DONE 70 and 71 froze SAVE, LOAD, RELOAD and SAVEALL because range sharding
can move a key. The freezes keyed off `is_range_sharded()`, so they read as
a special case of that algorithm rather than as the duty of any method whose
partition is state.

`key_space::is_stateful_sharding()` is that duty. Today it returns
`opt_range_sharded`. The four commands lock when it is true, and do not
when it is false. Hash-sharded LOAD and RELOAD go back to each shard's own
latch. The range table is still rebuilt only when the space is range
sharded, because the table belongs to that method.

A later method that can move a key returns true here and inherits the four
freezes. `routes_move()` stays the route-then-lock-then-route-again check.
The two happen to agree today. They answer different questions.

## 73. bloom_t is heap::vector<bool>; the substitution does not break tests [14-08-2026]

*Was `TODO.md` entry 70.*

The entry said set `bloom_t` to `heap::vector<bool>` and some of the tests fail,
and asked which and why: a different `operator[]`, a size that no longer matches
`static_bloom_size`, a filter that answers missing for a live key, or memory
the allocator counts that `std::vector<bool>` never did.

None of those happened. `heap::vector<bool>` is `std::vector<bool, heap::allocator<bool>>`.
That is still the bit-packed specialization. `operator[]` is still a proxy over a
word. `size()` is still a bit count. `create_bloom` still resizes to
`static_bloom_size` (4 194 304 bits, 512 KiB per shard). `hash_arena` already
stores the same thing as `heap::std_vector<bool>`.

The allocator rebinds to `unsigned long` for the packed words. `heap::allocate`
and `heap::free` see matching sizes, so the canaries in `heap_checks` survive
turning the filter on and off. The bytes are now in `heap::allocated`. They
were not, with `std::allocator`. That is the point of the switch: a filter
that is on is 512 KiB per shard, and the tracker should see it.

`configtest.py` turns the filter on and off. A probe of a thousand keys with
the filter on read every one of them back and rejected a miss. `rangeroutetest.py`,
`rangeshardtest.py`, `envconfigtest.py`, `shardstatstest.py` and `redisinfotest.py`
were green. The failures the entry remembered were not reproduced. The typedef
stays on `heap::vector<bool>`.

## 74. Git hash on done lines, and a TODO for every code-changing instruction [14-08-2026]

*Was `TODO.md` entry 78.*

Two rules, written into the TODO/DONE section of `CLAUDE.md`.

A `[Done]` line now carries `git rev-parse --short HEAD` after the DONE number.
Nothing is committed here, so that hash is the tree the working copy sits on
when the entry is closed, not a commit of the change. Older done lines have
no hash and are left as they are.

A new `TODO.md` entry is opened for every instruction that will change the
tree, unless one already covers that work. It is written before the edits.
Questions, reviews, and planning that do not change the tree do not get one.

## 75. Auto-flush of the current RESP array level [14-08-2026]

*Was `TODO.md` entry 67.*

The entry wanted `flush()` on `caller` so a reply did not have to sit in
Variables until the command finished. The first cut encoded into another
buffer on the caller and only copied it at `write_result`. That is not a
flush. A flush puts the bytes on the path `write_result` already sends:
the session stream.

RESP2 still needs `*N` first. The open array reserves a fixed-width header
on that stream, appends encoded elements as it hits the limit, and patches
the count when the level closes. Nested arrays write the parent's pending
items first so the child cannot overtake them. SCAN's cursor stays in
`results` and is prepended after the array has already gone out.

`flush_interface` is the place those bytes go. Auto-flush is off when the
pointer is null, which is why SWIG and the valkey module never encode early.
`asio_resp_session` implements it and points it at the buffer that
`write_result` already uses. An asynchronous call gets its own flusher on
its own stream. The limit is read at the start of each `call()` and is not
changed by a CONFIG SET mid-call. MULTI/EXEC turns the limit off for the
inner calls, so their replies stay as Variables for the EXEC array.

The default is 64. 0 disables it. A flush also runs when
`heap::allocated` is above `pre_evict_thresh` of max memory. Only the open
array is flushed, not the top-level `results` vector: SCAN keeps the cursor
there and patches it with `set_string(0)` after the array closes.

`test/autoflushtest.py` checks KEYS at 64, 0 and 1, HGETALL at 1, and SCAN
at 1. `configtest.py` and `rangeroutetest.py` stay green with the default
on.

## 76. KEYS writes each key to the socket [15-08-2026]

*Was `TODO.md` entry 79.*

Auto-flush (DONE 75) encoded into the session `vector_stream` and still
only called `asio::async_write` after the command. That is not a flush.
The mechanism was also general: every `push` on every command, a reserved
`*N` header, SCAN cursor patching, MULTI suppression. It was rolled back
in full. `flush_interface.h`, `resp_max_auto_flush`, the rpc_caller flush
stack, `caller::flush`, and `test/autoflushtest.py` are gone.

KEYS now asks the caller whether it can write the socket. `rpc_caller`
does that only when the RESP session has given it a function that calls
`asio::write` on the connection. SWIG and the valkey module still build
the reply as Variables.

RESP2 needs `*N` first, so the walk runs twice: count, write the header,
then write each key as it is found. One Variable lives at a time. If the
second walk finds fewer keys than the count, the missing slots are null,
so the client is not left waiting on a short array. VALUES is unchanged.

A pipeline that has a GET before KEYS already has that GET encoded in the
session stream. KEYS has to write that stream to the socket before it
writes its own header, or the client sees KEYS first. `run_asynch_batch`
drains `ctx->stream` for the same reason.

`test/keysstreamtest.py` checks an empty match, COUNT, 200 keys, a
pattern, VALUES, and a pipelined GET after KEYS. `asyncpipelinetest.py`
and `configtest.py` stay green.

## 77. KEYS second walk loads only pages that hit [15-08-2026]

*Was `TODO.md` entry 80.*

`art::glob` takes an optional page-id list and can fill one. The first
KEYS pass records every page that produced a callback. The second pass
loads only those ids, through a new `iterate_pages` that copies listed
pages and does not walk the arena. Pull sources keep their own page
ids, so `shard::glob` does not forward the list to them.

The list is `vector<size_t>` per shard. A `vector<bool>` or a compressed
bitmap can replace that later without changing the walk: page ids are
arena keys, not `0..occupied`, so a bitset sized by max page id would
count holes.

VALUES still builds the reply as Variables and does not use the list.
`test/keysstreamtest.py` and `asyncpipelinetest.py` stay green.

## 78. Chaos test for KEYS under restart and memory pressure [15-08-2026]

*Was `TODO.md` entry 82.*

`test/chaostest.py` runs many threads (32 on this box) mixing SET, GET,
DEL, INCR, KEYS, VALUES COUNT, SCAN, hashes, lists, ordered sets and a
pipelined GET+KEYS. A second thread flips `max_memory_bytes` and
`pre_evict_thresh`. A third stops the server while those calls are in
flight and starts it again.

Disconnects and WRONGTYPE during the storm are allowed. After a quiet
period SET, GET and KEYS have to answer. Seed 1, two restarts, six
seconds: the process did not hang or abort. KEYS hit the memory ceiling
on the tight limits, which is the short-reply path. `CMakeLists.txt`
registers it as TestChaos.

## 79. N-gram text index is composite keys, documented [15-08-2026]

*Was `TODO.md` entry 83.*

The index is not a new ART leaf type. A caller puts frames in a
keyspace (`txt`) as `<gram> <offset>`: `txt:SET "This 0"`,
`txt:SET "his_i 1"`. The space splits the key; the offset token is a
number, so `as_composite` encodes it as an integer. Spaces inside the
gram are written `_` so they do not split. `RANGE "is_is" "is_is~"`
is the lookup.

Documented in `docs/NGRAM.md` and `docs/index.html` (`#ref-ngram`).

## 80. H3 geospatial index is composite keys, documented [15-08-2026]

*Was `TODO.md` entry 84.*

Same recipe as the n-gram index. `geo.py` and `overture.py` write
`<h3> <id>` into `spatial_data`, with the cell as
`int(latlng_to_cell(...), 16)` so `SET` encodes it as an integer. A
search RANGEs the first child of a resolution-8 parent to its last
child. Documented in `docs/H3.md` and `docs/index.html` (`#ref-h3`).

## 81. Chaos test covers a larger RESP subset, including n-grams [15-08-2026]

*Was `TODO.md` entry 85.*

Workers now pick from strings and TTL, hashes, lists, ordered sets,
RANGE/COUNT/MIN/MAX/LB/UB, INFO/STATS, n-gram `txt:SET`/`txt:RANGE`
as in `docs/NGRAM.md`, and H3-style `spatial_data` composites.
`RANGE "is_is" "is_is~"` does not find `is_is 2`: a lone gram is a
different encoding from gram-plus-integer. The bounds are
`"is_is 0"` … `"is_is 999999"`. The n-gram docs were corrected.
Seed 2, two restarts, six seconds: SET/GET/KEYS and a gram RANGE
still answer after the storm.

## 82. SET at the memory ceiling raises not enough memory [15-08-2026]

*Was `TODO.md` entry 86.*

`hash_insert` used to return false when `logical_allocated` was over
`max_memory_bytes`, and `art::insert` used to catch the failure and
return false. SET ignored that false and answered OK. Both paths now
throw `not enough memory`. `opt_rpc_insert` and `update` use the same
words. `rpc_caller` and `vk_caller` already catch it and turn it into
an error reply. `oom_avoided_inserts` still increments. The error
model and `SET.md` no longer call this silent. `lrutest.py` ignores
the exception on the post-eviction writes.

## 83. Luau instruction budget is a slice, not a kill [17-08-2026]

*Was `TODO.md` entry 87.*

`foreign_script_insns` is still the same number. Hitting it no longer
raises `-ERR FOREIGN script budget`. The interrupt yields instead,
and the job goes back on the four-thread foreign pool so another
fetch can run. The query timeout is what stops a runaway.

`lua_yield` from the interrupt only works if `resolve` is running
under `lua_resume`. `lua_pcall` is a C boundary and is not yieldable.
The script is loaded as before, then `resolve` is started on a
`lua_newthread`. `sql.query` is still a blocking C call: a slice
that expires in the middle of one waits for the query, then yields
at the next Lua safepoint.

Yielding inside `fetch()` would have kept the worker parked on that
call. `run_fetch` now uses `fetch_async`. Fake, MySQL and Postgres
still run `fetch` on the same job and then complete. Luau returns
after a slice and the write-back runs when the script actually
finishes.

A 20 000-iteration loop with a 200-instruction slice still returns
the value. An infinite loop with a 400 ms query timeout is
`-ERR FOREIGN timeout`. Four infinite scripts on the four workers
do not hold a cheap GET on another space: that GET came back in
under a second while the hogs ran 2.5 s each.

## 84. CI MULTI, DROP deadlock, and foreign write-back [18-08-2026]

*Was `TODO.md` entry 88.*

EXISTS and MGET were marked `is_asynch` so a miss could leave the
ASIO thread. A redis-py pipeline is MULTI plus EXISTS plus EXEC.
The first async call copies the caller, so EXEC ran on a copy that
had no queued commands. The client saw the wrong number of replies.
They park the same way GET does, on `has_blocks`. They are not
asynch.

`SPACES DROP` held a unique lock on every shard, then
`unload_keyspace` called `fail_foreign_flights`, which locked them
again. On the Ubuntu 22 runner that is `Resource deadlock avoided`.
DROP now drops the locks before unload. The fail path uses
`try_lock_for` so a leftover holder does not hang or abort.

`kick` and the SWIG start path used to `enqueue` while still holding
the shard write lock. The worker can finish the fetch and try the
same lock. That is now after the unlock. A throw on the foreign
worker is logged instead of `terminate`.

`redispytest.py` (RESP2 and RESP3), `mergetest.py`, `foreigntest.py`,
and `foreign_luau.py` pass.

## 85. Incoming keys can split on a per-space regex [18-08-2026]

*Was `TODO.md` entry 89.*

`as_composite` still splits on a space when the space has no
`key_split`. Set `<name>.key_split` to an ECMAScript regex and
incoming keys break on that instead. `:` and `,` and `[:,]` all
work. An invalid regex is logged and ignored; the space split
stays. The compiled regex lives on the key space and is applied
through `encode_key` on GET/SET and the other user-key commands,
so `$0`/`$1` see the same parts.

`KSPACE OPTION GET KEY_SPLIT` returns the pattern, or empty when
unset. `test/keysplittest.py` covers colon, comma, a character
class, a bad pattern, and a fake foreign fill.

## 86. key_split feeds $n [18-08-2026]

*Was `TODO.md` entry 90.*

`$0` and `$1` now come from the same cut as the incoming key.
`bind_key` takes the space and calls `key_parts(internal, ks)`. A
one-character pattern (`:` or `,`) is a separator, not a regex —
compiling `:` as ECMAScript was giving one part `dept:` and
`encoded_key_as_string` aborted on that composite. A real regex
still goes through `std::regex`.

`FOREIGN FAKE PARTS` returns the parts a query would bind, so the
test does not need MySQL. `dept:42` on a `:` space is `dept` and
`42`. `Smith 42` on a space with no split is still two parts.
Live MySQL/Postgres tests also GET `Smith:42` with `$0`/`$1`.

## 87. TestForeign no longer aborts on the write lock [18-08-2026]

*Was `TODO.md` entry 91.*

CI has two RESP threads. `call_unblock` used `executor.execute`, which
can run the waiter on the same thread that still holds the shard
write lock. `reply_after_wait` then takes that lock again.
`try_lock_for` fails at once and the process `terminate`s.

`do_block_continue` now `asio::post`s, so the waiter runs after the
lock is gone. `finish_fetch` also wakes sessions after it drops the
write lock. TestForeign completes.

## 88. N-gram frames split on | so the gram keeps its spaces [18-08-2026]

*Was `TODO.md` entry 92.*

`txt.key_split` set to `|` before the first open. A frame is
`<gram>|<offset>`. The gram is the window as it appeared, including
spaces. No more `_`. RANGE `"is is|0"` … `"is is|999999"` finds
those offsets.

## 89. FOREIGN waiter uses a millisecond clock [18-08-2026]

*Was `TODO.md` entry 93.*

The parked-GET timer was `time_t_timer`. Its clock is `std::time`,
which only moves once a second. `expires_after(200ms)` stayed in
the future until the next whole second, so a 500ms fake fill always
answered the GET. The timer is `asio::steady_timer`. If the fetch
still lands after the waiter deadline, that GET is a timeout and
the next GET is the stored value. TestForeign's `fx_to` case
passes; the isolated GET timed out in 200ms.

A one-character `key_split` is a literal separator. Compiling `|`
as a regex is alternation and matches empty strings, which smashed
the gram. RANGE joins components with that same character, so the
reply is `is is|2` and not `is is 2`. The n-gram page, the chaos
n-gram keys, and `keysplittest.py` follow this.

## 90. Idle MySQL and Postgres pool connections have a maximum age [19-08-2026]

*Was `TODO.md` entry 94.*

Each SQL pool kept idle connections forever. Checkout now drops any
idle connection whose age is past `foreign_pool_max_age_ms` (global
default 30000) and opens a new one. Checkin stamps the idle time and
does the same walk, so a quiet pool does not keep a stack of stale
sockets until the next miss. Global 0 keeps idle connections. A
space that sets `<name>.foreign_pool_max_age_ms` uses that instead
of the global; unset still inherits, including a later `CONFIG SET`.

`KSPACE OPTION GET FOREIGN_POOL_MAX_AGE` reports the resolved value.
TestConfig round-trips the global. TestForeign checks inherit and
override. The live MySQL and Postgres tests (docker) assert that two
misses within the age reuse a backend id and a miss after it does not.

## 91. cmake --build . failed on barchlua's Lua headers [19-08-2026]

*Was `TODO.md` entry 95.*

A full RelWithDebInfo build died compiling `barchLUA_wrap.cxx`.
Luau's `lua.h` was on the include path ahead of LuaJIT's, so
`lua_pushcclosure` was the four-argument Luau macro and the SWIG
wrap still called the three-argument LuaJIT form. The wrap file
needs LuaJIT. `luau_driver.cpp` needs Luau. They cannot share an
include path.

`luau_driver.cpp` is no longer in the barchlua source list. It is
compiled as `barch_luau_driver` with Luau first, then linked in.
The other targets are unchanged. `cmake --build .` in
`cmake-build-relwithdebinfo` now finishes.

## 92. Idle SQL pool drop moved to the key space maintenance thread [19-08-2026]

*Was `TODO.md` entry 96.*

Checkout and checkin no longer walk the idle list. Each pool
implements `sql_backend::drop_idle`, which takes the pool lock and
calls `drop_idle_older_than`. `key_space::drop_idle_sql` is the
wrapper the maintenance thread runs once per cycle, after the shard
sweep. Checkin still stamps `idle_since`. The live MySQL and
Postgres age tests still pass: reuse inside 200 ms, a new backend
id after it.

## 93. `latch_t` is now `debuggable_server_lock` [20-08-2026]

*Was `TODO.md` entry 100.*

The new lock was not a drop-in `shared_timed_mutex`. It named the
write path `try_lock_write_for` / `unlock_write`, used `std::mutex`
with `try_lock_for` (that does not compile), drained reader slots
one core at a time so a reader could sneak back onto an earlier
slot, and keyed reader counts by `sched_getcpu()` so a migrating
thread decremented the wrong slot. Timeout rollback also unlocked
the upgrade mutex twice.

Those are fixed: SharedMutex names, `std::timed_mutex`, one
predicate over all slots, thread-pinned slots, and the unique_lock
owns the upgrade mutex until success. Isolated `locktest` covers
reader/writer exclusion, timeouts, `std::shared_lock` /
`std::unique_lock`, nested shared, a mixed load, and read-heavy
throughput. Four readers scale ~3.8x vs one; `shared_timed_mutex`
did not (it got slower). A 64-byte `alignas` on a member of the
lock itself crashed shard construction, because shards are
`malloc`'d. That alignment stays on the reader-slot vector only.

`latch_t` is the new type. `testbarch.py` starts and SET/GET
under it.

## 94. Deadlock dumps name the latch, holders, and held list [20-08-2026]

*Was `TODO.md` entry 101.*

A timeout of a second or more now prints the lock label (`node#17`),
Linux tids, how long the waiter waited and the writer has held, the
waiter's held-lock list (the ABBA clue), last reader tid per slot,
the writer's stack from acquire, and the waiter's stack. The same
text is written to `barch-lock-timeout-<pid>-<tid>-<n>.txt` in the
cwd so CI keeps it if stderr is truncated. Blocking `lock()` /
`lock_shared()` dump every 15s instead of waiting forever.

Shard construction labels the latch `name#shard`. The extra stores
on the read path are a tid and a two-word TLS push; four-reader
throughput is still ~3.8x one reader. `locktest` checks the snapshot
contents. `testbarch.py` still runs.

## 95. CI locktest missing and SpaceThread lock livelock [20-08-2026]

*Was `TODO.md` entry 102.*

Two CI failures, two different causes.

TestDebuggableServerLock was Not Run because the Ubuntu workflows
only build `barch`, `lbarch`, and `globdifftest`. CMake registered
`locktest`, ctest looked for `build/locktest`, and the binary was
never compiled. `locktest` is now a dependency of `barch` so the
existing `--target barch` line produces it, and the three workflow
files also name `--target locktest` next to globdifftest. It links
pthread. The isolated tests still pass, including a new one that
`try_lock()` actually succeeds when the lock is free (a zero-timeout
deadline check used to make that always fail) and that `lock()`
refuses new readers while a writer is waiting.

TestSpaceThread segfaulted at 61s after a pile of lock-timeout
dumps. `lock()` was `while (!try_lock_for(15s)) {}`. Every failed
try dropped `write_intent` and the upgrade mutex, so readers
flooded back in and defrag never drained. SIZE then waited 60s,
threw `read lock wait time exceeded`, and `read_lock_t`'s
destructor `unlock_shared`d a latch it had never acquired, which
drives the reader count negative so the writer wait never ends.

`lock()` now keeps write intent and the upgrade mutex across the
15s dumps. `try_lock_for` dumps on the same interval without
releasing until the real deadline. `read_lock_t` only unlocks
itself if `lock_shared` succeeded, the same flag `storage_release`
already had for unique. Defrag uses a 100ms try-lock and skips
the cycle when the shard is busy, so maintenance does not sit
behind DEPENDS holding 347+347 latches.

SpaceThread now finishes in about five seconds here with no dumps
and no crash. Four-reader throughput is still ~3.8x one reader.

## 96. Nested shared self-deadlock under write_intent [20-08-2026]

*Was `TODO.md` entry 103.*

The full CI log made the SpaceThread dump complete enough to see
the hold list. `SPACES DEPENDS tN ON g` already held `g_#0` through
`g_#15` (and the rest, but `max_held` was 16 so they were dropped)
and was waiting 60s for `g_#281` shared. `write_intent` was 1,
writer was none. The python threads timed out on that DEPENDS.

After the first DEPENDS, tN's source is g. The next one takes every
g shard shared, then unique on each tN shard. `storage_release`
nested-shared-locks the matching g shard. If defrag has set
`write_intent` on that shard, the nested `lock_shared` used to wait
instead of counting as a recursive reader. The outer hold never
dropped, the writer never drained, SIZE threw, the process died.

A thread that already holds a latch as a reader now `lock_shared`s
it again without touching the slot count or waiting on write
intent. The matching unlock only drops the slot on the last nest.
`max_held` is 2048 so a whole space fits in the dump. locktest
covers nested shared while a writer waits; a different thread is
still refused. SpaceThread still passes, about three seconds.

The coverage job's exit 143 was the runner getting killed during
the compile, not a compiler error.

## 97. CI fails compiling Luau on unused parameters [20-08-2026]

*Was `TODO.md` entry 104.*

Luau's CMake puts `-Wno-unused` on Ast/VM/Analysis, but not on
Compiler or Bytecode. Those two were compiling under our
`-Wextra`, so unused parameters in Types.cpp, Compiler.cpp,
CostModel.cpp, BytecodeBuilder.cpp, and Ast.h inlined into them
showed up as errors on the instance that treats warnings as
errors.

`LUAU_WERROR` is forced off. Our `CMAKE_CXX_FLAGS` are cleared
around the Luau fetch so `-Wextra` does not leak in. After
fetch, every Luau target gets `-Wno-unused` /
`-Wno-unused-parameter` and matching `-Wno-error=` so a later
flag cannot promote them. `Luau.Compiler` rebuilds here with
none of those diagnostics.

## 98. Latch dumps and writer backtraces behind BARCH_LOCK_DEBUG [20-08-2026]

*Was `TODO.md` entry 105.*

Every unique acquire was capturing a `backtrace()`, which made
uncontended writes ~100× slower than `shared_timed_mutex`. Dumps,
labels, last-reader tids, and writer stacks are now under
`#ifdef BARCH_LOCK_DEBUG`. Nested shared and the lock itself stay
in every build.

CMake turns the define on for Debug and RelWithDebInfo, off for
Release. `-DBARCH_LOCK_DEBUG=ON` enables it in Release (slow CI).
locktest always compiles with it so the snapshot checks still run.

Measured here, 400 ms lock/unlock loops:

- with dumps: unique 0.55M vs std 69M
- without: unique 16M vs std 70M (~4×, the timed mutex and drain)
- shared 4 threads still ~3.3× one reader, std still gets slower

The remaining unique gap is the writer-preference drain, not the
debug path.

## 99. Shared-to-unique upgrade for compress-under-read [20-08-2026]

*Was `TODO.md` entry 106.*

Upgradable used to set `write_intent` immediately, so new readers
stopped during compress, and upgrading from a live shared hold
deadlocked on this thread's own reader count.

It is now a reader plus the exclusive right to become unique later.
Other readers continue. Writers wait on the mutex, so the value
cannot be replaced. `write_intent` is set only at upgrade, after
this thread has dropped its own reader slot, then it drains the
others.

`lock_upgradable` / `try_lock_upgradable_for` take that hold.
`try_upgrade_to_write_for` / `upgrade_to_write` escalate. From a
plain shared hold, upgrade tries the mutex without blocking: if a
unique waiter is already draining us, it returns false instead of
deadlocking. locktest covers readers during upgradable, a blocked
writer, upgrade excluding readers, upgrade from shared, and that
deadlock case.

## 100. Coverage CI killed mid-compile (exit 143) [20-08-2026]

*Was `TODO.md` entry 107.*

The ubuntu24-sanitize log is not a compiler or linker diagnostic.
gcc never printed `error:`. The job hit 100% of compiling barch
with RelWithDebInfo+COVERAGE and gmake was SIGTERM'd (`Terminated`
on abstract_shard.cpp.o, exit 143). Unlimited `--parallel` plus
coverage instrumentation is the usual way that runner runs out of
memory. The workflow now builds with `-j2` and skips lbarch, which
is a second compile of the same sources; ctest only needs `_barch.so`.

## 101. locktest four-reader throughput fails on 2-core CI [21-08-2026]

*Was `TODO.md` entry 108.*

TestDebuggableServerLock failed `four readers beat one reader`.
Ours dropped to 0.34x one thread; `shared_timed_mutex` dropped to
0.16x. The runner is oversubscribed: four threads on two cores,
and slots were one per CPU so two pairs bounced the same atomics.

Slots are now `max(16, 4 * hardware_concurrency())`, thread-pinned.
The perf check only requires multi-thread ops to beat one thread
when `shared_timed_mutex` itself scales on that box. Overlapping
readers are still `test_many_readers`. Locally four readers remain
~3.8x one.

## 102. GET one lookup and vector_stream memcpy [21-08-2026]

*Was `TODO.md` entry 109.*

RelWithDebInfo perf on unordered 6-thread pipeline-50 90/10 GET/SET
(1M preloaded keys, 100% GET hits) had GET at 40% inclusive, split as
`kind_of`/`exists` 22% then `search` 15% on the same encoded key.
`vector_stream::write` was the hottest `_barch.so` self sample at 3.2%,
a byte loop that reloaded `pos` from memory every iteration.

GET now searches first and returns the string. A collection lives under
a different prefix and may sit on another shard, so the WRONGTYPE probe
only runs on a miss, the same shape EXISTS already used. A hit no longer
takes two shared locks.

`vector_stream::write` copies with `memcpy` after the resize. The null
and empty checks are unchanged.

Verified with TestContainerKinds (GET WRONGTYPE, string, miss),
TestRespShapes, TestReplyShape, TestRespClientLocal, TestCompression,
TestBarchList, TestBindings, and TestBarchPy.

## 103. Parser views, shared-lock fast path, GET bulk write [21-08-2026]

*Was `TODO.md` entry 110.*

The second RelWithDebInfo profile had the parser at 9% (`fast_float`
on tiny RESP integers, every bulk string copied into `std::string`),
vdso `clock_gettime` at 2.3% on every shared acquire, and GET building
a `$`-prefixed string then copying it again in `rwrite`.

The parser now keeps offsets into the recv buffer and only builds
`string_view`s when a request is complete. MULTI and async still copy
to owned strings. Clearing the buffer between TCP reads had to wait
until the parser is idle: a TRAIN with ~550 args spans two reads, and
clearing mid-request made `params[0]` a slice of the next packet
(`unknown command [NTIAL]`). Integers use a digit loop, not
`fast_float`.

`try_lock_shared()` does the uncontended increment without a clock.
`abstract_shard::lock_shared` tries that first and only then the
60s timed path. `try_lock_shared_for` also does one untimed attempt
before `steady_clock::now()`.

GET `push_bulk` writes `$<len>\r\n` + value + `\r\n` into the
session stream from the leaf, under the lock. MULTI/EXEC still go
through `push_vt`. Lengths use `std::to_chars` on the stack.

Verified with locktest, TestContainerKinds, TestRespShapes,
TestReplyShape, TestRespClientLocal, TestCompression (the TRAIN
split), TestAsyncPipeline, TestBarchList, and TestBarchPy.

## 104. Command cache, short headers, bulk header, skip empty repl [21-08-2026]

*Was `TODO.md` entry 111.*

Third RelWithDebInfo profile after 110 still had run_params at 2.5%
self (a `std::string` and `toupper` on every GET),
`buffer_get_valid_item` memchr-ing a pipelined buffer for `*2\r\n` /
`$3\r\n`, `rwrite_bulk` as five `writep`s, and every SET copying the
pipeline into strings for `repl::call` with no replicas.

The same-command cache now compares the recv view to `prev_cn` and
skips the string when memtier already sent `GET`. Size headers look
for CRLF in the first 13 bytes before `memchr`. Bulk replies write
`$len\r\n` in one stack buffer. `repl::call` only runs when
`has_destinations()`.

Release unordered 6t p50 90/10 stayed in the same band as 110
(7.09M / 7.27M / 7.03M 20s vs 7.11M before). 80/20 6.88M vs 6.99M.
The remaining CPU is the lookup: `basic_resolve`, `get_shard_index`,
hash `find`.

Verified with TestContainerKinds, TestRespShapes, TestReplyShape,
TestRespClientLocal, TestCompression, TestAsyncPipeline, TestBarchList,
and TestBarchPy.

## 105. Empty bulk RESP parse timed out zadd empty score [21-08-2026]

*Was `TODO.md` entry 112.*

CI TestValkeyDifferential had one difference after the short-header
scan in 111: zset.tcl "ZSET commands don't accept the empty strings
as valid score" timed out on `zadd myzset "" abc`. Locally the same
request logged "Bulk string size does not match" and then the socket
went quiet for 15s.

`$0\r\n\r\n` is an empty bulk. After the size header is consumed, the
payload is the remaining `\r\n`, and `read_next_item(0)` passed hint
0. The new short scan only started at byte 2, so it skipped that
terminator and took the next item's CRLF. Hint 0 was also the default
for "no length", so size headers and empty payloads used the same
path.

`buffer_get_valid_item` now takes -1 as no hint. A payload hint,
including 0, waits for CRLF at that offset and does not scan earlier
bytes. Size headers still use the short scan.

Verified with TestValkeyDifferential (barch agrees with valkey on
all 231 faithful cases), TestRespShapes, and TestCompression.

## 106. More valkey cases, and the zset/expire bugs they found [21-08-2026]

*Was `TODO.md` entry 113.*

The translator only understood a slice of the tcl, so 416 of 669 cases
were stubs. About fifty of those were ordinary command sequences it
refused for small reasons.

`assert_equal` now takes either argument order. `set v [r cmd]` stores
the reply, `list $v1 $v2 [r get foo]` builds a list from those names,
and `set _ $result` is the value after cleanup. `assert {[r cmd] eq …}`
is a single equality. `create_default_zset` and friends expand to DEL
plus ZADD. `{OK {} 1}` is a real expectation - a regex that stopped at
the first nested brace used to drop it. Bare `\[b` unescapes to `[b`,
which is why ZRANGE BYLEX was unfaithful.

The new cases found real bugs, not just missing translations:

- EXPIRE answered -1 when a condition refused, which is TTL's "no
  expire", not EXPIRE's 0/1. GT on a key with no TTL is an infinite
  current deadline, so it never applies.
- ZADD options only worked in one order, XX updated the new score key
  (which did not exist), GT/LT/INCR were ignored, and CH counted tree
  size including the member index.
- ZREVRANGEBYSCORE took max then min and walked high to low, which is
  empty. LIMIT ran on the forward walk, so REV LIMIT took the wrong
  end. COUNT 0 was treated as no limit.
- Exclusive `(0` was left on the bound and compared as text. ZLEXCOUNT
  did not exist.

Those are fixed. LPOS stays a stub - it is not implemented and a
partial run left `mylist` dirty for Variadic RPUSH. ZRANGESTORE is
accepted the same way TYPE is.

665 cases, 303 translated. valkey trusts 278 of 302, 24 dropped as
before. barch agrees on all 279 faithful cases, up from 231.

Verified with TestValkeyDifferential and TestRespShapes.

## 107. ZREVRANK, ZREMRANGEBYRANK, and ZRANGESTORE [22-08-2026]

*Was `TODO.md` entry 114.*

Phase 1 of the Z* plan. Three commands that were missing from the
valkey sorted-set list, all built on walks we already had.

ZREVRANK is ZRANK counted from the high end: walk the set once, then
`n - 1 - position`. WITHSCORE is the same extra score ZRANK already
answers. Nil when the member is not there.

ZREMRANGEBYRANK copies the index slice ZRANGE uses, then deletes
those members through both keys, the way ZREM does. Negative indexes
and inverted ranges are the same rules as ZRANGE.

ZRANGESTORE is a ZRANGE collected into owned member/score pairs, then
written to dest. WITHSCORES is a syntax error. LIMIT without BYSCORE
or BYLEX is too. An empty range deletes dest rather than leaving an
empty set. Wrong-type src is WRONGTYPE and leaves dest alone.

The already-translated ZRANGESTORE cases came off ACCEPTED. barch
went from 255 of 302 live cases to 263. The differential still agrees
on all 279 faithful cases.

Verified with TestValkeyDifferential.

## 108. Translator expansions for remaining zset stubs [22-08-2026]

*Was `TODO.md` entry 115.*

Phase 2 of the Z* plan. The translator now reads `assert {$retval == 2}`
against a stored reply, turns `catch {r cmd} e` plus `assert_match`
into expect_error, and expands `create_long_zset KEY N` (N up to 64)
into one ZADD.

That unlocked the variadic ZADD parse-error tests, ZINCRBY's arity,
ZRANGEBYSCORE LIMIT, ZMSCORE missing members, and the ZRANGE /
ZRANGESTORE syntax cases. Three of those were real bugs, not
translation artefacts: ZADD wrote the good pairs before a bad score
and left the key behind; ZINCRBY ignored extra arguments; ZRANGE
accepted LIMIT without BYSCORE/BYLEX, and ZREVRANGE/ZRANGEBYSCORE
silently swallowed options they do not have.

zset.tcl is 114 translated of 168 (was 105). Overall 312 of 665.
valkey trusts 286 of 311. barch agrees on all 287 faithful cases,
up from 279.

Verified with TestValkeyDifferential.

## 109. TestKeys and TestComposites asserted pre-compatibility answers [22-08-2026]

Both lua tests failed against the current server, and in both cases the server was right
and the test was recording what barch used to answer.

`testkeys.lua` line 33. `a` is set with `px 11000`, so it has a TTL, and then
`B.EXPIRE a 9 nx` and `B.EXPIRE a 9 gt` are both refused - NX will not replace an existing
expiry, and 9 seconds is not greater than the ~11 left. A refused condition answers 0, not
-1; -1 is what TTL says for "no expiry" and EXPIRE never uses it. `expire_command` in
keys_api.cpp has said 0 since the redis compatibility pass, with a comment saying so. The
two assertions now read `== 0`, and the `lt` line below them was already correct.

`testcomposites.lua` had four stale lines, not one. The script dies on the first, so the
other three only appeared once it was fixed - worth knowing, because "one assertion is
wrong" was the obvious reading of the first failure and it was wrong:

  - `B.ZREVRANGE cbgame 1 3 BYSCORE`. ZREVRANGE takes positions and `[WITHSCORES]`, that
    is all, and the new guard in ZREVRANGE refuses BYSCORE, BYLEX and LIMIT. Replaced with
    `0, -1`, which is the whole set. The score bound version of the question is the
    ZREVRANGEBYSCORE two lines down, so nothing is lost.
  - `B.ZREVRANGEBYSCORE cbgame 1 3.01` answered empty. The REV forms read max first, then
    min, so this asked for everything from 1 down to 3.01. Now `3.01, 1`.
  - `B.ZREVRANGEBYLEX cbgame [a [z` answered empty for the same reason. Now `[z, [a`.
  - `B.ZRANGE cbgame a z WITHSCORES REV BYLEX` is a syntax error - a lex range has nowhere
    to put a score. Dropped WITHSCORES and the expected count went from 6 to 3.

Verified by running each call against a server by hand before touching the tests, which is
what turned up the other three, and then `ctest -R "TestKeys|TestComposites"` green. The
nine other lua tests were run afterwards and all pass.

Two things found on the way that are not fixed here, because neither breaks a test today:

  - `B.ZRANGEBYLEX key [a [z WITHSCORES` still answers 6. Real valkey has no WITHSCORES on
    ZRANGEBYLEX and rejects it; the guard that went into ZRANGE was not mirrored here. When
    that is fixed, testcomposites line 111 breaks with it.
  - `B.ZRANGE key a z REV BYLEX` answers 3 but `B.ZRANGE key z a REV BYLEX` answers empty,
    which is backwards - valkey wants max first there - and inconsistent with
    ZREVRANGEBYLEX, which already reads max first. So the replacement line above passes for
    the wrong reason and will need its bounds swapped when this is corrected.

## 110. OrderedSet.revrange sent a command the server no longer accepts [22-08-2026]

TestBindings failed on `OrderedSet.revrange should answer 5 members, answered 1`. The one
was not a member - it was the null that every binding in `swig_api.cpp` answers when
`sc.flat_empty()` is true, which is what an errored call leaves behind.

`revrange` built `ZREVRANGE k start stop BYSCORE`. ZREVRANGE takes positions and
`[WITHSCORES]` and nothing else, the way valkey has it, and the guard added to it refuses
BYSCORE, BYLEX and LIMIT - so the call was a syntax error and the binding quietly answered
one null instead of five members. The same stale form that broke testcomposites line 101,
except here it is in the C++ rather than in a test, so the test was right and the code was
wrong.

The signature takes doubles, so the bounds really are scores and the command has to say so
some other way. It is now `ZRANGE k start stop BYSCORE REV`. ZRANGE BYSCORE REV sorts the
two bounds out itself - `0 100` and `100 0` both answer m4 down to m0 - so revrange still
takes them the same way round as range, which is what the comment above the assertion in
bindingtest.py promises. `range` on the line above already used ZRANGE BYSCORE, so the two
now differ only by the REV.

`ZREVRANGE` is the only place in swig_api.cpp that was affected; the other thirteen zset
forms it builds are all still accepted.

Two things about running this that cost time and are worth writing down:

  - **The python module has to be reinstalled before the test sees a rebuild.**
    `cmake --build` refreshes `_barch.so` in the build directory, but bindingtest.py is run
    as a script from `test/`, so `sys.path[0]` is `test/` and the import resolves to the
    copy pip put in `venv/lib/python3.12/site-packages`. That copy is only refreshed by
    the `TestBarchInstallPy` step (`venv/bin/pip install .`). Building and re-running the
    test on its own reproduces the old failure exactly, which reads like the fix not
    working. Run `ctest -R "TestBarchInstallPy|TestBindings"`.
  - **A probe script has to live outside the build directory.** `python3 -c` and a heredoc
    on stdin both put the current directory first on the path, and there is a `barch.so` in
    the build directory which is not the python extension, so the import dies with
    "does not define module export function (PyInit_barch)". Running a file from the
    scratchpad avoids it.

TestBarchPy failed while checking this and it is not related: `barch.sizeAll()` counts the
keyspaces on disk, so the `*.dat` left behind by running tests out of order pushes it past
the 2 the test expects. `TestClean` (`rm -f *.dat`) runs before the python block in a full
ordered run. With TestClean first, TestBarchPy passes.

## 111. exists_many probed every key twice [22-08-2026]

The question was whether the two lines

    if (store.exists(key)
        || barch::kind_of_container(store, argv[i]) != barch::container_kind::none)

probe the store twice for the same thing. They do not. `exists` asks about the plain key -
one shard, one lookup, and the bloom filter can rule it out without touching the tree.
`kind_of_container` asks about the container key ranges, which is three `has_container_of`
probes, one per lead byte, each on its own shard with its own lock and its own lower_bound
and tombstone walk. `SET k v` leaves nothing under the container leads and `RPUSH k v`
leaves nothing under the plain key, so neither can answer for the other, and `||` means the
three container probes only run when the plain key is missing.

The redundancy was one level up: the same pair ran again in the loop that counts, for every
argument including the ones the first loop had already found. A key that is nowhere cost
four probes to start the fetch and four more to count it. The encode_key was repeated too.

Only a key that went through `start_or_join` can have changed state across `wait_joins`.
One that was already present stays present, and the reply is a snapshot either way, so the
first pass now records what it learned in a small vector and the second pass re-probes only
the keys it did not find. Common case - EXISTS over keys that are already local - is one
probe per key instead of two.

`conversion::comparable_key` is safe to hold in a vector: it has a copy constructor and a
copy assignment that re-point `data` at the new object's own storage, which matters because
`data` can point into the object's own `integer` member. There is no move constructor, so
the vector copies on growth, and `reserve` is there so it does not.

Verified with the four foreign tests, and with a script that drives multi-key EXISTS
directly, since nothing in the tree did - `foreigntest.py` only ever calls EXISTS with one
key, and `exists_many` is the multi-key path. Two keys fetched from the source answer 2 and
cost exactly two queries, a second EXISTS answers 2 and costs none, plain keys and all three
container kinds answer 4, duplicates count separately the way valkey counts them, absent
keys answer 0, and a mixed call answers what it should.

`get_many` just above has the same repeated encode and an extra `search` on the first pass,
but its second pass is a real read rather than a repeated probe, so it is left alone.

## 112. The Z* compatibility plan in Z-COMPAT-PLAN.md [22-08-2026]

*Was `TODO.md` entry 122.*

The four-phase plan from the earlier session, written into the repo root so it is not
only in the chat. The file is the original plan with a short status at the top: phases 1
and 2 are already done (DONE 107 and 108), zset.tcl is 114 of 168 translated, the
differential agrees on 287 faithful cases, and phases 3 and 4 are what is left.

Nothing else changed. Phase 3 (BZPOPMIN/BZPOPMAX and the ZPOP member-index remove) and
phase 4 (remrange helpers and a second connection for blocking tests) are still not
started.

## 113. TTL truncated the seconds where redis rounds to nearest [22-08-2026]

`ttlGenericCommand` in valkey's expire.c ends with

    addReplyLongLong(c, output_ms ? ttl : ((ttl + 500) / 1000));

so every second form rounds to the nearest second. barch divided by 1000 and truncated, so
`SET x v PX 1600` then `TTL x` answered 1 where valkey answers 2, and TTL went one low as
soon as any time at all had passed after an EXPIRE. That is what made
TestValkeyDifferential flap on "EXPIRE - set timeouts multiple times": the case wants
`1 [45] 1 10` and got `1 5 1 9` whenever the second TTL landed after the tick. The `[45]`
already in that expectation is the same rounding at the earlier step, tolerated rather
than chased down.

**TODO 119 said the deadline forms were already right. They were not.** `output_abs` sets
`ttl = expire` and then falls into the same `(ttl + 500) / 1000`, so EXPIRETIME rounds too.
Checked against a real valkey rather than read off the source: a key whose PEXPIRETIME ends
in .886 reports an EXPIRETIME one higher. Three call sites, not two.

Also added the clamp valkey does before it reports, `if (ttl < 0) ttl = 0`. An overdue key
that is still present answers 0. Without it a key a second past its deadline would answer
-1, which already means "present, no expiry" - a wrong answer that looks like a valid one.

**TTL had its own copy of the whole function.** It did not go through `ttl_query`; it
repeated the with_key_read, the search, the answered flag and the two negatives, and then
computed the seconds itself. That is why the drift was possible at all, and it is why the
fix had two obvious call sites rather than one. TTL is now
`ttl_query(call, argv, ttl_report::seconds)` like the other three, and the tombstone note
that was sitting on it has moved to the shared doc comment where it applies to all four.

Verified by probing all four commands against barch and against valkey side by side, by
walking PEXPIRETIME through six deadlines .1 to .9 of a second apart and checking each
EXPIRETIME is `(p + 500) / 1000`, and by running the differential six times in a row: six
clean runs, and the trusted count is now a steady 287 where it used to alternate between
286 and 287. Full suite, 66 of 66.

The reinstall trap from DONE 110 caught me again and is worth repeating: a probe script
imports the pip copy in the venv, so `cmake --build` alone leaves it reading the old
module and the fix looks like it did nothing. Run TestBarchInstallPy first.

## 114. ZUNIONSTORE and ZINTERSTORE stored NaN where redis stores 0 [22-08-2026]

An infinite score times a zero weight is NaN in IEEE, and so is +inf plus -inf. Redis
guards both and keeps the convention that the answer is 0. barch stored the NaN, so

    zadd z -inf neginf
    zunionstore out 1 z weights 0
    zrange out 0 -1 withscores      ->  neginf -nan     (valkey: neginf 0)

There are three guards in valkey's t_zset.c, not one: `if (isnan(score)) score = 0` after
the weight multiply on the union walk (2702) and again on the intersection walk (2754), and
`if (isnan(*target)) *target = 0.0` inside the SUM arm of zunionInterAggregate (2365). MIN
and MAX are deliberately not guarded - once the multiply is clean neither operand can be
NaN, and flattening an infinity there would be wrong.

barch had the same three places, in `ZOPER`: the two `score * weight_of(which)` in `note`,
the `sc * weight_of(0)` and `other * weight_of(k)` on the intersection walk, and the sum
arm of `combine`. The multiplication is now a `weighted` lambda that zeroes a NaN, and the
sum arm zeroes one too. MIN and MAX are left alone and still answer -inf and inf.

A NaN in the store is worse than a wrong number, which is worth saying because "it is only
a weird score" undersells it: NaN is not equal to itself, so it sorts unpredictably against
the ordered keys and never compares equal to anything a later range asks for. The member
becomes unreachable by score.

Verified by probing every path - weight 0 against -inf and against +inf, in ZUNIONSTORE and
ZINTERSTORE, and the same member scoring +inf in one input and -inf in another under SUM -
all six answer 0 now, and MIN and MAX still answer -inf and inf. Then zset.json on its own,
which is where the case is trusted: 106 of 106, where it used to be the one difference.
Full suite, 66 of 66.

Found while checking that ordinary weights still worked: **WEIGHTS only parses integers**.
`keyspec.h:746` gathers them with `while (is_integer(spos)) weight_values.push_back(tol(spos++))`,
so `ZUNIONSTORE o 1 n WEIGHTS 2.5` is a syntax error where valkey answers 2 and scores the
members 5 and 7.5. A fraction part way along a list is worse - it ends the list early and
the rest of the weights are read as though they were options. Not fixed here; it is TODO 123.

## 115. WEIGHTS only parsed integers [22-08-2026]

`keyspec.h` gathered the weights with

    while (is_integer(spos)) weight_values.push_back(tol(spos++));

so a weight had to be a whole number. `ZUNIONSTORE o 1 n WEIGHTS 2.5` was a syntax error
where valkey answers 2 and scores the members 5 and 7.5, and the failure mode part way
along a list was worse than a refusal: the loop ended at the first fraction and everything
after it was read as though it were an option, so `WEIGHTS 1 2.5 3` weighted one input and
then failed on the rest.

`base_key_spec` now has a `to_double` next to `tol`: strtod's parse, the whole token has to
be consumed so `1.5x` is not 1.5, and NaN is refused - which is what `getDoubleFromObject`
does. The weights loop reads with that instead.

Checked against a real valkey rather than against what the arithmetic ought to be, because
weights and aggregates combine in an order that is easy to get plausibly wrong:

  - `WEIGHTS 2.5` over {two 2, three 3} - both answer `two 5 three 7.5`
  - `WEIGHTS 1 2.5` over two inputs - both answer `two 27 three 53`
  - `WEIGHTS 1.5 2 AGGREGATE MIN` - both answer `two 3 three 4.5`

and the forms that should still be refused are: `nan` and `1.5x` both error. Exponent forms
and `inf` work. Integer weights are unchanged - the existing tests that pass `WEIGHTS 3 3 3`
still pass.

**What this deliberately does not fix.** Valkey takes exactly `numkeys` weights and errors
if there are more, with "weight value is not a float"; barch reads as many numbers as
follow. So `ZINTER 2 a b WEIGHTS 3 3 3` is a syntax error there and is accepted here. That
is a second divergence in the same option and it is left alone on purpose: testcomposites
has four assertions that pass more weights than inputs and expect them to be tolerated, so
tightening it is a test change as well as a parser change, and it belongs in its own entry
rather than being smuggled in behind a fix for the fraction. Barch's error wording for a bad
weight is also the generic syntax error rather than redis's specific one. Both are TODO 124.

Full suite 66 of 66, and zset.json and string.json on their own both agree with valkey on
every trusted case.

## 116. The differential ran every file into one server, and lost a case to its own keying [22-08-2026]

Two ways the harness was quietly removing cases from the comparison. Neither showed up as
a failure, which is what made them worth chasing: a dropped case reads as "the translation
was not faithful", and that is indistinguishable from a case that really is untranslatable.

**All eight files ran into one server session.** Every tcl file is written against an empty
db, so state from one broke the setup of a case in the next. The ZUNIONSTORE NaN case was
the expensive one: by the time zset was reached `z{t}` already held a string, so `zadd z{t}
-inf neginf` failed on valkey with WRONGTYPE, the case was dropped as unfaithful, and the
real difference it exists to catch - DONE 114 - was invisible in the combined run. It only
showed up because zset.json was run on its own. `run_all` now issues a FLUSHALL when the
source file changes, which is what the tcl suite does between files anyway.

**Results were keyed by case name, and two cases share one.** expire.tcl has two tests
called "EXPIRE with unsupported options" - `EXPIRE foo 200 AB` and `EXPIRE foo 200 XX AB`.
Both ran; the second overwrote the first in the results dict, so one of them was never
compared. That is the whole explanation for a number that had been sitting in the output
unremarked: "312 translated" but "286 of 311 cases pass". Cases are keyed by source and
position now.

The effect, on the combined run:

    before   312 translated, valkey: 286 of 311, 25 dropped, barch : 271 of 311
    after    312 translated, valkey: 289 of 312, 23 dropped, barch : 274 of 312

Three more cases compared: "KEYS with empty DB" and the ZUNIONSTORE NaN case, both
recovered by the flush, and the second "EXPIRE with unsupported options", recovered by the
keying. barch agrees with valkey on all 289, four runs in a row, and the NaN case is now a
standing regression test for DONE 114 in the normal run rather than something only a
per-file invocation would catch.

Worth keeping in mind for the remaining 23 drops: they are still read as translation
artefacts, and after this the reading is more trustworthy, but it is not proof. A case that
valkey rejects because barch's translator skipped its setup and a case that valkey rejects
because the translation is wrong still look identical from here.

Full suite 66 of 66.

## 117. WEIGHTS took any number of weights, and named a bad one badly [22-08-2026]

The two leftovers from DONE 115. Redis reads exactly one weight per input:

    if (remaining >= (setnum + 1) && !strcasecmp(c->argv[j]->ptr, "weights")) {
        j++; remaining--;
        for (i = 0; i < setnum; i++, j++, remaining--)
            if (getDoubleFromObjectOrReply(c, c->argv[j], &src[i].weight,
                                           "weight value is not a float") != C_OK) ...

Three different outcomes come out of that, and it is worth being precise about which is
which, because they are easy to collapse into one:

  - too few, or none at all - the `remaining >= setnum + 1` guard means WEIGHTS is not
    recognised as a keyword, and it falls through to a plain syntax error
  - too many - the extras are left to be read as options, and fail as a syntax error
  - the right count with a value that is not a float - and only this one gets the
    specific "weight value is not a float"

barch now does the same: the loop reads exactly `numkeys` weights, refuses early if that
many do not follow, and sets a `bad_weight` flag that ZOPER turns into the wording, next to
the `bad_limit` that already worked that way.

Checked row for row against a real valkey rather than reasoned about, because the guard
that makes a short list a syntax error rather than a weight error is not obvious from the
loop:

    ZINTER 2 a b WEIGHTS 3 3 3            syntax error                  both
    ZINTER 2 a b WEIGHTS 1                syntax error                  both
    ZINTER 2 a b WEIGHTS                  syntax error                  both
    ZINTER 2 a b WEIGHTS 1 2 3 AGGREGATE MIN  syntax error              both
    ZINTER 2 a b WEIGHTS 1 zz             weight value is not a float   both
    ZINTER 2 a b WEIGHTS 1 2              x                             both
    ZINTER 2 a b WEIGHTS 1 2 AGGREGATE MIN    x                         both

and fractional weights still work, so DONE 115 is intact.

**Six assertions in testcomposites.lua changed**, which is the reason this was not folded
into DONE 115. Four passed three weights for two inputs and expected the extra to be
ignored; they now pass two. The other two are more interesting: they name three inputs -
one of the "inputs" being the word WEIGHTS or AGGREGATE - and used to answer an empty array
because the loose gathering left the odd words to be read as input names. Under the strict
count they read three weights, the third being the word AGGREGATE, and answer the float
error. Valkey answers the same, so these two assertions test more than they did before;
they use `vk.pcall` and check the message rather than counting an empty reply.

Full suite 66 of 66, and the differential agrees with valkey on all 289 trusted cases,
twice in a row.

## 118. Z-COMPAT-PLAN phase 3: BZPOPMIN/BZPOPMAX, and ZPOP's leftover index [22-08-2026]

The plan says do the index bug first, and it was right to: the pop it fixes is the pop the
new commands are built on.

**ZPOPMIN and ZPOPMAX left the member index behind.** They removed the score-ordered key
with `i.remove()` and nothing else, where ZREM removes both that and the `IX_MEMBER` entry.
The index is what ZSCORE reads and what the BYLEX walks iterate, so a popped member kept
answering ZSCORE and kept appearing in ZRANGEBYLEX. `zmpop_one` already did it properly for
ZMPOP, so both now go through that instead of keeping their own loop, and the two commands
are a line each on a shared `zpop_flat`.

**A second bug fell out of reading them side by side.** ZPOPMIN checked its count with

    if (call.ok() != conversion::to_ll(argv[2], count))

and `to_ll` answers a bool while `ok()` is zero, so a count that parsed cleanly compared
unequal and *every* ZPOPMIN with a count was an error. ZPOPMAX had written the same check
correctly, which is why nobody noticed - and why sharing one helper is worth more than the
duplicate lines it saves. Now `ZPOPMIN q 2` pops two, `ZPOPMIN q 0` is an empty array, and
`-1` and `ab` both answer "value is out of range, must be positive", which is the one
message redis gives for both.

**BZPOPMIN and BZPOPMAX** are a parse and a reply in front of the machinery ZMPOP already
has. They differ from BZMPOP in three ways and no more: the timeout is last rather than
first, there is no count, and the reply is a flat `{key member score}` rather than a nested
array. `parse_block_timeout` and `add_block` are unchanged.

Checked against a running valkey and then against barch: `BZPOPMIN z 0.1` answers `z a 1`,
BZPOPMAX answers `z b 2`, a name holding a string is WRONGTYPE rather than a wait that can
never end, several names skip the empty ones, a missing key waits out its 0.3s and answers
nil, and a ZADD from another connection wakes a parked waiter in 0.3s. Bad timeouts answer
"timeout is not a float or out of range" and "timeout is negative", both valkey's wording.

**They are registered in `register_ordered_api` only, not in `add_ordered_api`.** That is
not an oversight to be tidied up later: the waiter is `add_block`, which belongs to the RESP
side, and the module path has no way to park a call. BZMPOP has always been registered that
way. Registering the two new names as module commands as well is what made the whole module
fail to load - `Module initialization failed` and the server aborting, which surfaced as
seventeen tests dying with "Subprocess aborted" and "server not started in time" rather than
as anything mentioning BZPOPMIN. There is now a comment saying so where the registrations
are, because the absence is the kind of thing that reads like a gap.

Full suite 66 of 66. The differential agrees with valkey on all 289 trusted cases, twice.

What phase 3 does not include: the translator's `foreach {ZPOPMIN ZPOPMAX ZMPOP_MIN ...}`
expansion, which the plan calls optional and puts after the index bug. The zset stub count
is unchanged at 114 of 168, so the count in Z-COMPAT-PLAN.md still stands. Phase 4 is what
is left.

## 119. Z-COMPAT-PLAN phase 4: the remrange helpers, and why the blocking half is not harness work [22-08-2026]

**The remrange half is done.** The three "basics" stubs - ZREMRANGEBYSCORE, ZREMRANGEBYRANK
and ZREMRANGEBYLEX - each define a helper inside the test and call it a dozen times:

    proc remrangebyscore {min max} {
        create_zset zset {1 a 2 b 3 c 4 d 5 e}
        assert_equal 1 [r exists zset]
        r zremrangebyscore zset $min $max
    }
    assert_equal 3 [remrangebyscore 2 4]

That is a fixture, a check, and one command whose reply is the proc's value, which is the
shape the plan said to inline rather than interpret. `inline_inner_proc` lifts the proc out
of the body, substitutes the parameters into each call site, and writes the fixture lines
out in front of the call. It runs before the UNTRANSLATABLE check, which lists `proc`.

It only accepts that one shape: the last line has to be a single `r ...`, nothing before it
may be something the translator would refuse anyway, and the helper may not call itself.
The moment an expansion starts guessing it stops being safe, and a wrong translation is
worse than no translation - that is the whole premise of the differential.

zset.tcl goes from 114 to 117 translated, valkey trusts all three new cases, and the
totals move 312 -> 315 translated and 289 -> 292 faithful.

**The first thing a new case did was catch a bug.** ZREMRANGEBYSCORE read its bounds with
`read_score`, which refuses a leading paren outright, so `ZREMRANGEBYSCORE zset (1 5`
answered "min or max is not a float" - and the inclusive comparison meant it had no notion
of an open end anyway. ZCOUNT and ZRANGEBYSCORE both handle `(` and have done all along.
Fixed by reading the bounds the way ZCOUNT does and skipping a member whose score equals an
open bound. All four combinations now match valkey: `(1 5` and `1 (5` remove 4, `(1 (5`
removes 3, `-inf +inf` removes 5.

**The blocking half is not what the plan thought it was.** It says a second redis-py
connection in `differential.py`, "harness work, not a Z* command". A second connection is
necessary and nowhere near sufficient. All 19 deferring-client tests in zset.tcl need three
separate things:

  - a deferring connection in the harness - issue, go and do something else, read later.
    This is the part the plan described, and it is the easy one
  - `foreach {popmin popmax} {BZPOPMIN BZPOPMAX BZMPOP_MIN BZMPOP_MAX}` around the test,
    and an outer helper `verify_bzpop_response` that branches on which of the four names it
    was handed. Pair-wise foreach expansion plus a proc with an `if` in it is a different
    order of translator work from the one-command helper above
  - `wait_for_blocked_client`, which every one of them calls, and which reads
    `blocked_clients` from `INFO clients`. barch answers `INFO clients` with
    "not implemented", and `blocked_clients` does not exist anywhere in src

The third is a server feature, not harness work, and it gates the other two: without it the
tests cannot tell a client that is parked from one that has not sent yet, so they would
race rather than wait. Left open as TODO 127 with those three named, rather than started
and left half done.

Full suite 66 of 66. The differential agrees with valkey on all 292 trusted cases.

## 120. The blocking half of phase 4: blocked_clients, and the pop-name foreach [22-08-2026]

TODO 127 named three prerequisites. Two are done, the third turned out not to be needed
for most of what it was blocking, and a fourth thing was found on the way that was worth
more than any of them.

**`INFO clients` now reports `blocked_clients`.** It is a `statistics::` atomic, raised in
`rpc_caller::transfer_rpc_blocks` when a session parks and lowered in `erase_blocks` when
it stops - both of which are per session, so a caller waiting on three names counts once,
which is what redis reports. A `counted_blocked` flag on the caller keeps it from drifting,
because the two are not perfectly paired: `erase_blocks` is also called on the disconnect
path and on a timeout.

Only that one field is reported. Redis's Clients section has sixteen and barch does not
know fifteen of them; inventing `connected_clients:1` would make INFO a worse source than
no INFO at all. Measured: 0 idle, 1 with a BZPOPMIN parked, 2 with two parked, back to 0
after both are woken by a ZADD, 1 while one is waiting out its timeout and 0 after it
expires, and 1 - not 3 - for a single waiter on three names.

**The pop-name foreach is expanded, and the bzpop verify helpers are inlined.**
`foreach {popmin popmax} {BZPOPMIN BZPOPMAX BZMPOP_MIN BZMPOP_MAX}` is written out once per
tuple. Only a loop whose values are all pop names is touched - the encoding loop wraps the
whole file and expanding that would double all 168 tests for nothing, which the plan says
not to do. `verify_bzpop_response` and its two-key twin become an ordinary assert, with the
BZMPOP branch's `lassign [split $pop "_"]` done at translate time: BZMPOP_MIN is BZMPOP
plus MIN, and the arguments are numkeys, then the names, then the direction.

**The deferring client turned out not to be needed for these.** Every use of those two
helpers is against a set that already has members, so the pop answers at once and there is
nothing deferred about it - it can run on the one connection the harness already has. So
`set rd [valkey_deferring_client]` and `$rd close` are dropped as the dead handles they
are. A body that really does park still refuses, because its `$rd read` and
`bzpop_command $rd` lines are not understood, and that is the right outcome rather than a
lucky one.

**The thing that was worth more: `render` flattened nested lists.** A reply that is a list
of lists came out looking exactly like a flat one, so BZMPOP's `{key {{member score}}}`
rendered as `key member score` - identical to BZPOPMIN's reply, and unequal to the
expectation. Valkey failed its own answer and the case was dropped as an unfaithful
translation. Rendering a nested list braced, the way tcl prints one, recovered seven cases
at once: the three BZMPOP ones this phase added, and four that had been quietly dropped all
along - "Vararg DEL", both variadic ZADD cases, and "BRPOPLPUSH inside a transaction",
which is a real divergence that now lands in ACCEPTED where it belongs rather than
vanishing. Nothing was newly dropped.

The numbers, against DONE 119's 315 translated and 292 faithful:

    674 cases, 323 translated, 351 stubs
    valkey: 302 of 323 cases pass, 21 dropped
    barch : 286 of 323, agrees with valkey on all 302 faithful cases

zset.tcl is 125 of 177 translated - 177 rather than 168 because the foreach expansion makes
more tests out of the same file.

**Still not done, and now the only thing left in phase 4:** the ten tests that genuinely
park. Five of them also use MULTI. The other five need a deferring connection in
`differential.py` plus `wait_for_blocked_client`, which can now be written because
`blocked_clients` exists - the mechanism is proven, a redis-py
`connection_pool.get_connection` sends without reading and `read_response` collects later,
and it was measured against a parked BZPOPMIN while writing this. It is TODO 128. What
stopped it here is that the tests around it also want `$rd read` interleaved with ordinary
commands, which is a step model the case format does not have yet - `defer`, `wait_blocked`
and `read` ops rather than one list of commands against one connection.

Full suite 66 of 66, and the differential is stable across repeated runs.

Corrected the same day: this entry first said the five MULTI tests were out because
MULTI is not implemented, citing TODO 63. Both halves were wrong. MULTI and EXEC work,
and TODO 63 was closed on 12-08-2026 and was about list commands, not transactions. Both
mistakes came from repeating differential.py's ACCEPTED reasons without checking them -
see DONE 121.

## 121. Every reason in ACCEPTED pointed at closed work, and two of them were wrong [22-08-2026]

Prompted by a fair question about DONE 120: it said five zset tests were out because MULTI
is not implemented, "see TODO 63". TODO 63 is closed - 12-08-2026, DONE 65 - and it was
about the list commands zset.tcl and list.tcl expect, never about transactions.

Both halves of that sentence came from `differential.py`'s ACCEPTED list, repeated without
checking. Looking properly, the whole list had the same problem: every reason in it cited
TODO 52 or TODO 63, and both were closed months ago - 52 in DONE 43, 63 in DONE 65. A
pointer to a closed entry reads as "tracked and pending" about work that is finished, which
is worse than no pointer, and this is the file whose entire job is justifying differences.

Checked each reason against a running barch rather than rewriting them from memory:

  - **MULTI and EXEC exist.** Registered in connection_api.cpp; a queued ZADD and DEL run
    and EXEC answers both replies, and the effect is right. What actually differs is the
    reply to a *queued* command - nil here, +QUEUED there - and DISCARD is unknown. So the
    reason was wrong, not just its reference
  - **LMOVE exists too**, so "which needs LMOVE" was wrong about the only case it excused
  - TYPE, SADD, SETBIT and GETBIT really are unknown commands, and SET's IFEQ really is a
    syntax error. Those reasons were right; only their references were stale

**Checking the LMOVE one turned up a real bug.** "Extended SET GET with incorrect type
should result in wrong type error" does not fail over a missing command. `SET foo bar GET`
where foo holds a list answers nil and *replaces the list with the string*. Redis refuses
with WRONGTYPE and leaves the list alone - the RPOP the case ends with answers `waffle`
there and raises WRONGTYPE here, because the list is gone. That is data loss sitting under
an accepted difference, hidden by a reason that pointed somewhere else entirely. TODO 129.

The reasons now say what was measured, and a pointer only appears where something is
genuinely open. The header says why, so the next person does not have to rediscover that
the numbers had rotted.

Worth stating plainly, because it is the lesson rather than the changelog: an ACCEPTED
entry is a claim that a difference has been understood. Two of these had stopped being
that and become a way of not looking, and one of them was covering a bug that destroys
data. The list needs the same scepticism as the code it excuses.

## 122. The deferring client, and the two bugs the parking tests found [22-08-2026]

TODO 128, the last of Z-COMPAT-PLAN phase 4. The ten tests that park a client now
translate and run, and they immediately found two real bugs.

**The harness can defer.** redis-py has no deferring client, but its Connection is one:
`send_command` writes and returns, `read_response` collects later. `Deferred` wraps that,
and a case gets three new step kinds - `defer` sends on a named connection, `wait_blocked`
polls `INFO clients` until that many clients are parked, and `read` collects a deferred
reply and compares it. `sleep` covers the tcl's `after`. Connections are closed when the
case ends, and then the harness waits for `blocked_clients` to fall back to zero, because
closing a socket is not the same as the block being released and a case that inherits a
parked client has its wake stolen - DONE 116's lesson in a different costume.

**The translator reads the shapes around it**: `set rd [valkey_deferring_client]` names a
handle, `$rd <command>` sends on it, `bzpop_command` and `verify_pop_response` expand the
same way the other pop helpers did, `wait_for_blocked_client` and
`wait_for_blocked_clients_count` become waits, `assert_equal [$rd read] {x}` is a read
either way round, and `if {$::valgrind} {after 100}` is dropped - it is a pause for a slow
build, and it would trip the `if {` in UNTRANSLATABLE.

zset.tcl goes from 125 to 138 of 177 translated. Overall 323 -> 336 translated, and valkey
trusts 302 -> 315. All thirteen new cases are faithful translations.

**Bug one: MULTI leaves the connection out of step.** Not a blocking bug at all - it
reproduces with `MULTI; PING; EXEC` and no waiter anywhere. Queued commands answer nil
where redis answers +QUEUED, EXEC's own reply lands misaligned, and commands sent after
EXEC are swallowed rather than run. In the tests that means the ZADD after the transaction
never happens, so the parked client is never woken and the read times out. Seven of the ten
differences are this. TODO 131.

**Bug two: a blocking pop whose key list repeats a name answers nil on every second park.**
`BZPOPMIN z1 z2 z2 z1 0` woken by a ZADD answers correctly, then the next identical park
answers nil, then the one after that is correct again - and the missed reply turns up on
the following read, one behind. BZMPOP does it too, and BLPOP with distinct names does not,
so it is in the shared block registration rather than in either command: the same name is
registered twice and released once. TODO 132.

Worth being explicit about how these were separated, because the first look was wrong. Nine
differences appeared and two of them - the variadic ZADD pair - looked like a wake picking
the wrong member, `zset {{bar 1}}` where `zset {{foo -1}}` was wanted. By hand, outside the
harness, that case is correct every time. Running the parking cases without the MULTI ones
made them pass. They are downstream of bug one, not a bug of their own, and treating them
as one would have sent someone hunting in the wake path for something that is not there.

**Ten cases are now in ACCEPTED, which is more than I would like.** Every one names what
was measured and points at an open TODO, which is the contract DONE 121 just finished
restating. The alternative was leaving TestValkeyDifferential red, which hides the same
information behind a failing build rather than in a list that says why. When 131 and 132
are fixed these six patterns come straight back out, and that is the test of whether this
was the right call.

Full suite 66 of 66. The differential agrees with valkey on all 315 faithful cases.

## 123. MULTI's reply protocol, and the bug that was hiding behind it [22-08-2026]

TODO 131 said three things: queued commands answer nil instead of +QUEUED, EXEC's reply
lands misaligned, and a command after EXEC is swallowed. The first was right, the second
was nearly right, and the third was wrong - and correcting it uncovered a different bug
that the protocol noise had been masking.

The way to see it was to stop using a client library and dump the bytes:

    barch   MULTI | PING | EXEC -> $-1 $-1 +PONG
    valkey  MULTI | PING | EXEC -> +OK +QUEUED *1 +PONG

Three defects, all the same shape - a reply that was never pushed, written out as whatever
an empty results vector renders as:

  - **MULTI answered `$-1`.** It returned `call.ok()` with nothing pushed, and the writer
    turns an empty results vector into a RESP null. Now it pushes `+OK`
  - **A queued command answered `$-1`.** The buffering branch of `rpc_caller::call`
    stored the command and returned 0 without a reply. Now it pushes `+QUEUED`
  - **EXEC dropped its array header when the transaction held one command.** The writer
    renders a results vector of one as that value alone and an empty one as a null, so
    `MULTI; PING; EXEC` answered a bare `+PONG` and an empty transaction answered `$-1`.
    finish_call_buffer now closes the collected replies as an aggregate, so EXEC answers
    `*1 +PONG` and `*0` - which is what the writer's own comment says aggregates are for

All three now match valkey byte for byte, including the empty transaction. The dead clause
in `is_buffering() && (params[0] != "EXEC" || params[0] == "MULTI")` went too - the second
half can never be true when the first is false.

**The third claim was wrong and worth correcting.** Commands after EXEC are not swallowed:
on the wire, `PING` after `EXEC` has always answered `+PONG`. That claim came from watching
redis-py go out of step, which it did because the reply *contents* were wrong, not because
replies were missing - the count was right all along. Reading it as a lost command sent me
looking for a queueing bug that does not exist.

**What was underneath.** With the protocol fixed the seven MULTI cases still failed, so the
same raw socket, with a client parked on the key:

    MULTI | ZADD zset 0 foo | DEL zset | EXEC  ->  +OK +QUEUED +QUEUED *2 :1 :0
    the waiter got                            ->  zset foo 0

The waiter is woken by the ZADD *inside* the transaction and pops the member the DEL is
about to remove. That is precisely what the case called "ZADD + DEL should not awake
blocked client" exists to catch, and it also explains the `:0` from the DEL: the member was
already gone, popped by the waiter, so there was nothing left to delete. TODO 133.

So the accepted entries stay, with reasons that now name the right bug. Seven cases moved
from "MULTI leaves the connection out of step" to "woken by a write inside the transaction",
which is the difference between a protocol defect and a broken isolation guarantee - and
only the first of those was fixed here.

Full suite 66 of 66, and the differential agrees with valkey on all 315 faithful cases.

## 124. A blocking pop with a repeated key name, and the double signal behind it [22-08-2026]

TODO 132: `BZPOPMIN z1 z2 z2 z1 0` answered correctly, then nil on the next identical park,
then correctly again, with the missed reply turning up one read late. BZMPOP did it too.

The entry guessed at the cause - "the same name is registered twice and released once,
leaving a stale entry" - and that was half right about the registration and wrong about
what went wrong with it.

**The first fix was for a real bug that was not this one.** `shard.h`'s `unblock_key_`
erased inside a range-for over the vector it was erasing from, which invalidates the loop's
iterator and the one being erased with, and then stepped past whatever moved down into the
gap. It is undefined behaviour and it does drop the wrong entries. It is now the
remove-erase idiom, and an emptied key comes out of the map rather than sitting there for
the life of the shard. Fixing it changed nothing about the symptom, which is worth saying:
a bug found on the way to another bug is still worth fixing, but it is not evidence.

**What it actually was.** The measurement that settled it: a fresh connection for every
park always worked, the same connection alternated. So it was per session, not the shard's
registry.

`call_unblock` walks the list of sessions waiting on a key and calls `do_block_continue` on
each. A pop names its keys as the caller wrote them, so `z2` twice put the session in z2's
list twice, and one wake signalled it twice. The first signal *posts* the reply - it does
not send it inline - so the second signal runs while `has_blocks` is still true and posts
another. The second reply then finds the set already emptied by the first and answers nil.
That is the extra reply: the connection is one ahead from then on, every second park reads
a nil that belonged to nobody, and the real answer arrives one read later.

The fix is that `add_rpc_block` registers a session against a key at most once, however
many times the caller named it. That is what the wake semantics want anyway - the list is
"who is waiting on this key", not "how many times did they ask".

Worth keeping: the symptom looked like a lost reply and was an extra one. Alternating
correct/nil/correct is the signature of a stream one ahead, not of a wake going missing,
and reading it the other way is what put "released once" in the TODO.

"BZPOPMIN with same key multiple times should work" is out of ACCEPTED and passing. The
differential agrees with valkey on all 315 faithful cases and the full suite is 66 of 66.
Nine cases are still accepted, all of them TODO 133.

## 125. A parked client saw inside a transaction, and the lower case EXEC [22-08-2026]

TODO 133. Three things had to change, and the third was not in the entry at all.

**Wakes raised inside a transaction are held until it ends.** `defer_wakes` is an RAII
scope around the EXEC loop; `call_unblock` checks it and puts the shard and key aside
instead of signalling. When the scope ends the held wakes are sent, deduplicated, each
under its shard's latch - which every other caller of call_unblock already holds, because
they are inside a write when they wake somebody. Nesting is counted, so a transaction
inside anything that already defers is harmless. The state is thread local: a transaction
runs on the thread executing EXEC and nowhere else.

**A wake that finds nothing keeps waiting.** Holding the wake is not enough on its own. The
key really was written during the transaction, so the waiter is signalled once it ends,
looks, and finds the member already deleted - and the pop callbacks answered nil, which
unblocks a client redis leaves parked. `caller::retry_block` says "not for me", the session
re-registers its blocks (call_unblock took it out of the key's list when it woke it) and
stays parked. Only the timeout answers nil now. The foreign GET waiter is deliberately not
changed: for it, a fetch that produced nothing is a miss and nil is the right answer.

**And EXEC only worked in upper case.** With both of the above in place the tests still
failed, and the hand-written reproduction still passed - because the reproduction pipelined
`EXEC` and the tests send `exec`. `rpc_caller::call` compared `params[0] != "EXEC"` against
the parameter as the client wrote it, while the dispatcher folds only the name it looks the
function up by. So a lower case `exec` was queued into the transaction it was meant to end,
and everything after it was queued too, for the life of the connection. There is now a
`name_is` that folds the case.

That one is worth dwelling on. The wire tests said the server was correct and the harness
said it was not, and both were right: they were sending different bytes. Two rounds were
spent looking for a difference between the harness and the socket before the difference
turned out to be the case of four letters. When a reproduction disagrees with a test, the
reproduction is a suspect too.

Measured against valkey, same sequence, byte for byte identical on both:

    MULTI | ZADD zset 0 foo | DEL zset | EXEC  ->  +OK +QUEUED +QUEUED *2 :1 :1
    the waiter                                ->  stays parked, then gets zset bar 1

The DEL answering :1 rather than :0 is the tell that nobody stole `foo` on the way past.

Nine cases came out of ACCEPTED and eight of them pass. The differential goes from 290 to
298 of 336, and agrees with valkey on all 315 faithful cases.

**The ninth is a different property and got its own entry.** "BZMPOP with multiple blocked
clients" parks four clients on two keys and one transaction wakes them all. Redis serves
them first come first served; barch serves them in whatever order the posted wakes happen
to run, so the second client was answered from the wrong key. TODO 134.

Full suite 66 of 66.

## 126. The string writers and a name that holds a collection [22-08-2026]

TODO 129 said `SET ... GET` against a key holding a list answers nil and destroys the list.
That was right, and it was one command out of ten with the same hole - and the entry's
suggested fix, refusing the write, would have been wrong for four of them.

The first change I made was to refuse a string write against any collection, in SET, APPEND,
GETSET and SETNX. Then I checked what redis actually does, which is not one rule but two:

    SET k v         OK, and the collection is replaced
    SET k v GET     WRONGTYPE, and the collection is left alone
    SETNX k v       0 - the name is taken, so NX declines. Not an error
    SETEX k 100 v   OK, replaced
    MSET k v        OK, replaced
    APPEND          WRONGTYPE      GETSET     WRONGTYPE
    SETRANGE        WRONGTYPE      GETDEL     WRONGTYPE      GETEX  WRONGTYPE

A command that only writes replaces the name. A command that has to read a string first -
GET's half of SET, APPEND, GETSET, SETRANGE, GETDEL, GETEX - refuses. SETNX answers "the
key exists" because that is what it means. Refusing everything would have broken three
commands that were behaving correctly, which is what checking first is for.

Measured against a running valkey, one row per command, and then the same table against
barch until every row matched.

**Replacing had its own bug underneath.** In barch a collection and a plain key live in
different key ranges, so writing the plain key never removed the collection - the name held
both. That is why the symptom looked different per type: after `SET k v` on a list, LRANGE
answered WRONGTYPE, and after the same on an ordered set, ZRANGE still answered its
members. The three replacing commands now call `remove_container` first, so the name holds
exactly one thing, and `GET k` after `SET k v` on a list answers the value.

What changed, by command:

  - **SET** refuses only when GET is present, otherwise removes the collection and writes
  - **SETNX** answers 0
  - **SETEX and MSET** remove the collection and write
  - **APPEND and GETSET** refuse - these two were destroying data as badly as SET was
  - **GETDEL and GETEX** refuse, where they used to answer nil
  - **SETRANGE, GET and the increments** already had the check and are untouched

"Extended SET GET with incorrect type should result in wrong type error" is out of ACCEPTED
and passing, and it is the only case in the suite that covered any of this - the other nine
commands were found by asking what else writes a string, not by a test.

The differential goes to 299 of 336 and agrees with valkey on all 315 faithful cases. Full
suite 66 of 66. One case is still accepted, TODO 134.

## 127. Waiters on one key are served in the order they arrived [22-08-2026]

TODO 134, and the last of what the parking tests turned up.

`call_unblock` woke *every* session waiting on a key, and each of them posts its own
continuation, so which one got the data was whichever the executor ran first. The valkey
case parks four clients on two keys and wakes them with one transaction:

    rd1  bzmpop 0 2 myzset{t} myzset2{t} min count 1
    rd2  bzmpop 0 2 myzset{t} myzset2{t} max count 10
    rd3  bzmpop 0 2 myzset{t} myzset2{t} min count 10
    rd4  bzmpop 0 2 myzset{t} myzset2{t} max count 1

Redis serves them first come first served: rd1 takes one member of myzset, rd2 takes the
four that are left, rd3 takes all of myzset2, rd4 waits. barch answered rd2 from myzset2
with all five of its members, because rd3 had run first and drained myzset.

**One at a time, and the served waiter passes the turn on.** `call_unblock` now takes the
first session off the key's queue and leaves the rest where they are. When that session has
been answered it calls `call_unblock` again on the key it was woken by, so the next waiter
gets its look - which is what a client asking for one member out of five has to do, since
four are still there.

The turn is passed only after a waiter is *served*. One that found nothing means the key is
empty, and waking the next would send it round the same loop for nothing; a later write
wakes them again. That is what keeps this from spinning, and it only works because DONE 125
gave a woken waiter a way to say "not for me" instead of answering nil.

The wake now carries the key that caused it - `do_block_continue(key)` - which the session
needs to know which queue to hand on to. The one other caller, a key space being unloaded,
passes an empty name: it is failing the blocks rather than satisfying them, so there is no
turn to pass.

One divergence left, written down rather than chased: a waiter that is woken and finds
nothing re-registers, which puts it at the back of the queue, where redis would keep its
place. It needs three waiters and a write that satisfies none of them to notice, and no
case in the suite does that.

All thirteen parking cases pass, "BZMPOP with multiple blocked clients" is out of ACCEPTED,
and the differential is 300 of 336 with agreement on all 315 faithful cases - no zset case
accepted at all now. Full suite 66 of 66, and the four tests that exercise blocking under
threads were run three times over to check nothing hangs.

## 128. `ACL SETUSER` accepted `~pattern` and threw it away [22-08-2026]

TODO 136. Found while designing per key space ACLs (TODO 135), not by anything failing.

`acl_spec::parse_set` in keyspec.h matches `~*` as `flag_filter`, inserts the pattern
into `filters` and sets `is_filter`. A grep for both names finds the two lines in
keyspec.h that write them and nothing anywhere that reads them: `ACL` in auth_api.cpp
calls `add_cats` with the categories and the secret only.

So `ACL SETUSER alice on >s ~key:*` answered OK and granted nothing of the kind. The
worse case is the mixed one - `+read ~key:*` - where the categories were applied and the
pattern was not, so the user came out with rights the caller never meant to give on their
own. That is the syntax every redis client sends when it wants key level rights, so the
failure mode is a client that believes it has confined a user to a key prefix.

Refused rather than implemented. Key level rights are a real feature and belong with
TODO 135, and an error cannot break anyone who was relying on the old behaviour because
there was no behaviour to rely on. The check sits in `ACL` right after `parse_options`,
so it catches the pattern before anything is written - which is what makes "nothing was
stored on the way to refusing it" true and testable.

test/aclfiltertest.py is new, registered as TestAclFilter: a pattern on its own is
refused, a pattern mixed with categories is refused, `ACL GETUSER` shows nothing was
written in either case, and a plain `+read` SETUSER still works and keeps its category.
The test was run once against the previous build before the fix went in and failed on the
first assertion, which is the negative control.

## 129. `art::iterator::last()` found nothing in a single key tree [24-08-2026]

TODO 138. Found while writing `sharded_store::maximum_below` for the function
visibility work in TODO 98: MAX answered null for every user it was meant to filter,
on a space holding 21 keys. 347 shards and 21 keys is one key per shard for the ones
that hold anything, which turned out to be the whole story.

`last_child_off` answers an empty trace element for a leaf. A tree holding one key
keeps that leaf *as* its root, so `extend_trace_max` pushed the empty element, looked
at its null child, asked `last_child_off` about that, got another empty element and
returned false. `last()` treated that as "nothing here" and answered false, on a tree
that plainly had something in it.

The other two places that touch a trace already knew about this shape. The lower bound
constructor ends with `c = t->get_tree_size() == 1 ? lb : last_node(tl)`, and `end()`
only calls an empty trace the end when `get_tree_size() > 1`. `last()` was the third
and only one that did not, which is why it read as a deliberate design everywhere else
and a bug here.

Fixed by giving it the same case: if the root is a leaf, that leaf is the answer and
the trace stays empty. An empty trace is correct rather than a compromise - there is
nothing before the only key, so a later `previous()` answers false, which is true.

`next()` and `previous()` were checked on the same shape and are fine, for a better
reason than luck: both move *from* a position by walking the trace and treat a false
return as "no more". With one key the trace is empty, so both answer false, which is
the true answer. The bug was specific to `last()` because it is the one that has to
construct a position out of nothing rather than move from one.

No test of its own. `sharded_store::maximum_below` is the only caller a thinly spread
space exercises, and the MAX assertion in test/functiontest.py - a user without the
function category getting the largest ordinary key rather than null - fails if this
regresses. The workaround that was in `maximum_below` (taking `tree_maximum()` for the
below-the-bound case) has been removed, so that path now runs on `last()` alone.

## 130. `foreign_script` kept Luau source in the configuration space [25-08-2026]

TODO 139, and the same mistake TODO 98 J had already settled for globals - left in the
foreign path only because it predates functions being keys at all.

`<name>.foreign_script` is a configuration key, and `load_source` took the value itself
as the script when it began with `--`. So a fill script lived in `configuration`, a
store whose job is settings, and test/foreign_luau.py set it that way in all eight of
its cases.

It names a function now: SETF the script, point `foreign_script` at the name, and it is
looked up in the space and then in the global one - the same order a call resolves in.
That gets replication, persistence, protection from eviction and the ACL category for
free.

Three things had to move together, and two of them were forced rather than chosen.

The entry point is unified on `call`. A fill defined `resolve(key, space)` while SETF
refuses anything without `call()`, so storing a fill as a function was impossible
without picking one of them. Every stored Luau function now answers `call`, and a fill
is invoked as `call(key, space)`, which reads naturally since arguments became varargs.

Compilation defers to the first fill. `prepare_luau` runs from the key_space
constructor at key_space.cpp:316, before `shards_out.resize` at 340, so the space has
no shards and a tfunction key cannot be read there at all. What looked like an obstacle
is an improvement: a script stored *after* its space was built now starts working,
where before the space had to be rebuilt. The rewritten test relies on it - `install()`
does USE, which builds the space with foreign=luau and no script yet, and only then
stores the script.

The file path form stays. A path is a reference rather than code in the settings store,
so it is not the mistake, and it is tried after the name. The inline form is refused
with a message saying to SETF it and name it.

The rewrite of foreign_luau.py needed a second pass: extracting the script text out of
the Python source kept the escapes as written, so the first attempt stored scripts whose
newlines were literal backslash-n. The whole script was then one comment line and every
case failed with "luau script has no call()" - which is at least the error it should
have been.

Not covered: a fill surviving a restart. The script is an ordinary key so it persists
the way any other does, but nothing asserts it.

## 131. `barch.call` built a whole caller for every command [25-08-2026]

`runner_for` in function_api.cpp constructed an `rpc_caller` per call. That
constructor runs `update_routes()` and then a real AUTH command through the auth
shard, and the object itself allocates a function state holder and copies the
global ACL vector - all of it thrown away one command later.

Two smaller costs sat beside it. The command table was fetched through
`functions_by_name()` (a shared_ptr copy) and looked up through a freshly
upper-cased `std::string` every call, and every argument was copied into a
`std::vector` before the call even though `call()` only indexes and iterates
what it is given.

Fixed by hoisting the sub caller to the connection, the same move that worked
for the script interface. `call()` already clears args, errors, results and temp
at its head, so one caller serves any number of commands. It is built on the
first call that needs it and lives as long as the interface.

The part that needed care is re-entrancy, and it is not hypothetical: CALLF is
an ordinary built-in, so a script can reach a script, and the inner call would
otherwise have cleared the results the outer was still producing. A `busy` flag
covers it - a nested call builds a fresh caller for its duration rather than
keeping a stack of them, since nesting is rare and the cost belongs on the rare
path.

The name lookup is cached on the name as the script wrote it, so no case folding
on a repeat, and the table shared_ptr is held rather than re-fetched. What is
cached is only that a name resolves and to what. The ACL check stays outside the
cache and runs every call, because a session's rights can change under it.

Measured with memtier, one connection, pipeline 50, Release:

                                     before        after
    GET, C++                        0.80 us      0.83 us
    Luau through the store          2.61 us      2.82 us
    Luau through barch.call         6.77 us      1.69 us

So barch.call went 4.0x faster, and its cost above a raw GET fell from 5.96 us
to 0.89 us. The two unchanged rows moved by about 8% between runs, which is the
run to run noise on this box and worth knowing when reading the other number.

The surprise is that a script now reaches a built-in *faster* than it reaches
the store interface. That is not a measurement error: the store path builds a
`sharded_store` and encodes the key itself on every access, while barch.call
hands the work to the same optimised C++ path a client would hit. It suggests
the store interface has the same kind of per access setup left in it that the
caller had, which is a separate job.

Re-measured afterwards against the fuller set the earlier report used, one
thread, pipeline 50, Release. PING and GET reproduce the old figures within 3%,
so the two runs are comparable:

                              1 connection        4 connections
                            before    after     before    after
    PING                    0.596 us  0.597 us  0.286 us  0.288 us
    GET                     0.785 us  0.759 us  0.326 us  0.328 us
    noop function           1.023 us  1.094 us  0.345 us  0.363 us
    LUAU_GET                1.611 us  1.425 us  0.491 us  0.466 us
    barch.call              6.778 us  1.594 us  8.369 us  0.525 us

The concurrency column is the one that says what the bug actually was. Before,
barch.call was the only thing that got *slower* with four connections than with
one - 6.78 us to 8.37 us, while everything else improved. That is contention,
and it was the AUTH every constructor ran through the auth shard, with four
connections queueing on one store. Hoisting the caller removed the contention
along with the work, which is why the aggregate number improved 16x where the
serial one improved 4x.

Two notes on reading the table. `LUAU_GET` is `barch.store.get(k)`, the form the
driver documents at luau_driver.cpp:1487 - not `barch.space[""][k]`, which is a
different and heavier path measuring 2.63 us and 1.11 us in the same run. That
gap of about 1.2 us per read is the store interface overhead noted above,
arriving from a second direction: the space path builds and encodes per access
where the caller no longer does. And the `vs GET` ratios drift from the earlier
report because GET itself came out 3% faster this time while the Luau rows did
not, so the microsecond figures are the ones to compare, not the ratios.

functiontest, foreign_luau, aclfiltertest, spaceacltest and functionevicttest
all pass unchanged.

## 132. `barch.space.NAME` rebuilt a store interface on every call [25-08-2026]

Reading through `barch.space[""][k]` cost about 1.2us more than the same read
through `barch.store.get(k)`. The read was not the difference - `space_read` and
`store_get` have the same body and both end in `s->get(...)`. Getting `s` was.

`store_get` uses `st->store`, which the interface built once for the connection.
`barch.space.NAME` went through `space_open`, which looked in `st->opened` - and
`finish_job` cleared that map at the end of every call. So every call naming a
space rebuilt a whole `store_access` through `store_for`, fifteen or so
std::functions with a heap allocation each, to serve one read.

Fixed by moving `opened` off the per call state and onto the `call_interface`,
where `store` already lives. The state keeps a pointer to it, set when a job is
pumped and cleared when it finishes, so a handle still cannot reach a space once
its call is over - which is what the old clear was really protecting. The
interface is the right owner because it is already rebuilt when the running
space changes, when the defined space changes and on `set_acl`, which are
exactly the three things that make a cached store_access wrong.

Measured, one thread, pipeline 50, Release, comparing within a run:

                              1 connection        4 connections
                            before    after     before    after
    barch.store.get(k)      1.425 us  1.434 us  0.466 us  0.457 us
    barch.space[""][k]      2.633 us  1.657 us  1.108 us  0.507 us
    the gap                 1.21  us  0.22 us   0.64  us  0.05 us

So 1.6x faster serial, 2.2x aggregate, and at four connections the two forms are
within 11% of each other. What is left at one connection is the userdata
allocation, the extra metamethod hop and the std::string built from the name,
which is roughly what was predicted and is a much smaller thing to chase.

Two notes. The first run after the change showed barch.call 18% slower, which
was noise - a repeat put it back at 1.61 us against 1.59 us before, and nothing
in this change touches that path. It is worth repeating a sweep before believing
a number that has no mechanism behind it. The second: the script side workaround
still helps a loop, `local sp = barch.space[""]` hoists the handle out of it,
but it never helped the shape being measured here, where a call does one read.

functiontest, foreign_luau, aclfiltertest, spaceacltest, functionevicttest and
containerkindtest all pass.

## 133. The SQL foreign tests still wrote Luau source into `foreign_script` [25-08-2026]

CI failed in test/foreign_mysql.py:203 with

    FOREIGN a luau script belongs in a function: SETF it, then name it in
    foreign_script

Fallout from DONE 130, which made `foreign_script` name a stored function rather
than hold source. foreign_luau.py was rewritten for that; foreign_mysql.py:189
and foreign_postgres.py:189 were missed. Both set the option to a script body,
and both wrote `function resolve(...)` where the entry point is `call`.

Fixed by following the order foreign_luau.py already documents: configure, USE
to build the space, SETF the script under the name the configuration points at,
then fill. The option is now just "filler" in both.

Why local runs stayed green while CI did not: the block these lines sit in only
executes with a live database, so a machine without docker skips the whole thing
and reports success. That is worth knowing about this pair of tests generally -
passing them locally says nothing unless the container actually came up. Both
were run here against real mysql and postgres containers, and both print their
"complete" line.

The two remaining sites named in DONE 130 turned out to be the only ones: a grep
for foreign_script across test/ finds nothing else but configtest.py, which only
lists `foreign_script_insns` among the option names and does not set a script.

## 134. A stored function that returned nothing took the server down [26-08-2026]

`function call(k) end` - no return statement, nothing else wrong with it - crashed
with SIGILL. Found while writing tests for the `barch.store` write half, but
nothing to do with it: it was simply the first script in the suite that returned
nothing.

`pump_call` converted the reply with `to_variable(job->T, lua_gettop(job->T), ...)`.
An empty stack makes that index 0, which is not a valid Lua index, so
`lua_type(L, 0)` read off the frame rather than answering LUA_TNONE.

An empty stack now answers null, which is what redis gives a script with no return
statement, and `to_variable` treats LUA_TNONE as nil so no other caller can repeat
it. test/functiontest.py has a function that returns nothing followed by a PING -
the PING is the point, since a wrong answer here is a dead server rather than a
wrong reply.

Worth noting how close this came to shipping unnoticed. Every script in the suite
happened to return something, and a function with no return is the most ordinary
thing a user would write - a setter. The bug was reachable by the second script
anyone would try.

## 135. A function's slice and deadline are its own, and `barch.store` writes [26-08-2026]

Two things off 98's build order, I.2 and I.6.

I.2. The budget was `foreign_script_insns` and the bound was the space's
`foreign_query_timeout_ms`. A fill and a command a client invoked are not the same
risk, and since a script yields rather than dying the instruction count is a
*slice* size while the deadline is the real bound - so they are named for what they
now are: `function_slice_insns` and `function_deadline_ms`, both server settings
with per space overrides, 0 meaning use the server's, the same shape
`script_insns()` and `waiter_timeout_ms()` already had. `KSPACE OPTION GET
FUNCTION_SLICE|FUNCTION_DEADLINE` reports them.

Two places had to move that are easy to miss, and both fail quietly rather than at
compile time: `configuration_names()` and the value reflection are separate lists,
so a variable registered in one and not the other reads back empty - configtest
catches it, which is what it is for. And the option name whitelist in
spaces_spec.h carries a hand written count of its own length, so a name added
without moving the count is a syntax error at the parser.

I.6. `barch.store` was read only while the space value could write, so the two
halves of one interface disagreed about what a script could do. `store_of` has
taken a `writing` flag since the rights went in and nothing used it - this is what
it was for. `barch.store.set(k, v)` and `barch.store.remove(k)` now exist, rights
checked the same way, with a nil value removing so `set(k, nil)` and `sp[k] = nil`
agree.

The entry's own note that writes were missing was overstating it: writes landed in
6b through `__newindex`, and containers got set and del with the container work.
What was actually missing was the symmetry.

One test bug worth recording because it looks like a real failure: a script
returning `{ value, nil }` answers with a one element array, since a trailing nil
in a Lua table is a shorter table rather than a hole. The assertion was wrong, not
the code - but chasing it is what produced 134.

## 136. The locked region: explicit locking, and an API that knows one is held [26-08-2026]

The last thing in 98 item 6. Everything else in the store interface copies under the
lock and lets it go, so a get followed by a set has a gap another connection can land
in. `barch.store.locked(key, fn)` closes it: fn runs with a write lock held on the
shard the key routes to, or on every shard in shard order when no key is named.

The hard half was not the lock, it was everything else having to know about it. The
shard mutex is not recursive - keyspace_api.cpp:219 and key_space.cpp:473 both say so,
both having been bitten - so an implicit acquire inside an explicit hold is EDEADLK on
the calling thread rather than a slower path. `shard_hold` is a thread local record of
what is held, and `route_locked`, `each_shard_read` and `each_shard_write` all ask it
before taking anything.

Three things fell out of doing it that were not obvious from the outside:

  - the guard has to sit on every shard acquisition, not just on explicit lock calls.
    A script that locks one key and then calls `store.range`, or reads a container
    living elsewhere, reaches a second shard through the implicit path without ever
    asking for one. Checking only explicit locks leaves exactly the case the check
    exists to prevent.
  - nothing else in the tree holds two shard locks at once. `each_shard_read` and
    `each_shard_write` scope the guard inside the loop body - take one, release, take
    the next - so single shard holders cannot form a cycle with anything, and the
    ordering only has to defend the all-shards form against another all-shards form.
    That is a much smaller thing to get right than it first looked.
  - the held record can be thread local, which avoids threading a lock token through
    every store_access entry point, and it is sound only because the region forbids
    yielding. A call that cannot yield cannot park, and a call that cannot park never
    resumes on another thread. The no-yield rule pays for the cheap implementation as
    well as for the deadlock it was there to stop.

What the region refuses, each of which would otherwise deadlock or stall every
connection on that shard: yielding (the interrupt raises instead, with a hard cap of
100000 instructions, since the ordinary budget ends in a yield), `barch.call` (a
command takes shard locks of its own), and `sql.query` (network I/O with a shard
stopped). A region inside a region is refused too - the inner one is already covered,
and letting it through would drop the outer hold when it left.

A second key on a different shard aborts with a message naming what happened and what
to do instead - put the keys in one container, which routes them to a single shard by
name. Two scripts each holding one shard and wanting the other is a deadlock nothing
can get out of, so it is an error rather than a wait.

The surface is scoped rather than a lock and an unlock the script pairs up itself. A
pair leaks the shard lock the first time a script errors between the halves, and
errors are exactly what a region with a hard cap produces. The guard is on the C++
side and the body runs under `lua_pcall`, so the lock goes back whether the script
returns, raises, or is cut off - then the error is re-raised once the lock is gone.

Measured rather than asserted, because a single threaded increment passes with no
lock at all. Eight connections, 250 increments each, one key:

    unlocked   1994 of 2000     six lost updates
    locked     2000 of 2000

Two companions went in beside it, because the region is deliberately restrictive and
a script should be able to find that out rather than discover it from an abort.
`barch.store.shardNumber(k)` answers which shard a key routes to, so two keys can be
checked for being together before anything is locked, and `barch.store.hasLock(k)`
answers whether this call already holds that key's shard, so a helper can be written
once and called from inside or outside a region without being passed a flag. Both are
tested: the same key routes the same way twice, a key on another shard is found by
comparing numbers rather than by catching the error, and hasLock reads false, true,
false across a region boundary.

The unlocked figure is the important one: it says the test can see the thing it is
testing. The locked case is in test/functiontest.py along with the whole space form,
the three refusals, the nested refusal, an error inside the region followed by a call
that proves the lock went back, and the cross shard abort actually firing.

## 137. The wall clock deadline, written down where someone tuning it will find it [26-08-2026]

98 item 7b recorded that a function's deadline is wall clock from when the call
arrives and counts time spent queued behind other scripts, and that "nothing says
so anywhere yet". Now something does.

The behaviour, confirmed against the code rather than taken from the note:
luau_driver.cpp:1652 sets `deadline = art::now() + deadline_ms` once when the call
starts, and :1561 refills the instruction slice on every resumption without ever
touching the deadline. So it keeps running while a call waits for its next slice.

What went into docs/index.html:

  - `function_slice_insns` and `function_deadline_ms` in the settings table, both
    global and per-space, with the KSPACE OPTION names to read them back.
  - a note in the caps and timeouts list saying plainly that under enough load a
    function fails rather than being slow - sixteen at once, each wanting 0.29s,
    three past a one second deadline - and that this is the deadline working, since
    a bound that stopped counting whenever the machine was busy would not bound
    anything. With what to do about it: raise the deadline for the load expected,
    not for how long the function takes on an idle server, and know that lowering
    the slice helps short calls get through and does nothing for a long one.
  - that a cut-off call answers an error and does not roll back what it already
    wrote, because a function is not a transaction.

Two stale claims went with it, both user facing and both wrong since DONE 130:
`foreign_script` was documented as a Luau file or inline source defining
`resolve(key, space)`. It names a stored function now and the entry point is
`call`, so both the settings table row and the "Luau instead of a fixed query"
prose said something a reader would have followed straight into an error. The
ordering that DONE 133 had to discover the hard way - configure, USE, SETF, then
fill - is written into the prose as well.

Worth noting for anyone adding a setting later: docs/index.html is hand written,
so a new variable does not appear in it on its own, and the settings table is the
first place a user looks. There is still no section for stored functions
themselves - SETF, GETF, REMF, KEYSF, CALLF and the barch.* interface are
undocumented outside TODO 98.

## 138. Keys are strings, in and out [26-08-2026]

The open question in 98 F2, decided: a key is a string to a script, because that is
what a key is everywhere else. It enters barch as text on the wire and leaves as
text, so a script seeing a third form would be the only thing in the system that had
to know about stored types - and there would be two ways to name one key.

Typed keys were built first and then taken out again, which is worth recording
because what made them attractive is real: `row.key == 42` is false for a key written
as `SET 42 x`, since everything arrives as text. What the typed version cost was
worse. `row.key` would return one of four Luau types depending on how the key
happened to be encoded, so any script touching a key it did not write would need a
type test first.

The probe that settled it, and it is worth knowing about this Luau: **its integer is
a separate type that does not mix with numbers.** With a key pushed by
`lua_pushinteger64`, `type(k)` is "integer", `k == 4242` is false against a literal,
and `k + 1` raises

    attempt to perform arithmetic (add) on integer and number

So the "natural" representation of an int64 key was not natural at all - it was a
value equal to nothing a script would write down. The workaround was going to be
numbers below 2^53 and integers above it, which trades one surprise for a worse one:
the type of `row.key` depending on the magnitude of the key. That is the point where
strings everywhere is obviously the simpler answer.

What that leaves, all of it as it already was: `row.key` is the decoded key text, the
same text KEYS prints, and `sp[k]`, `store.get` and the rest take that text back. A
number index still works, because Lua coerces it and `encode_key` parses it back to a
tinteger, so a script and a client naming 42 reach one key. A composite is its
components rejoined with the space's split character.

test/functiontest.py pins the round trip for an integer, a double, a string and a
composite key, all written by an ordinary client: each is seen as a string by a walk,
and each reaches its own value when handed back.

One process note, because it cost more than the work did. The revert was done with
scripted string replacements, and one of them silently corrupted an unrelated line of
the test - `barch.store.range` became `barch.tore.range` - which failed as "attempt
to index nil with 'range'" and looked exactly like a broken interface. Two probes
went into it before a diff showed all four source files byte for byte identical to
the committed state and the only changed file was the test. A scripted edit nobody
looks at is worth a diff before it is worth a test run.

## 139. Stored functions are documented [26-08-2026]

SETF, GETF, REMF, KEYSF, CALLF and the whole `barch.*` script interface existed
only in TODO 98. DONE 137 had put the slice and deadline settings into
docs/index.html, so the published docs named two settings for a feature they never
described.

A reference section now sits beside the others, `#ref-functions`, with its nav
entry after Coalesced Foreign Sources: what a function key is and why it is a key
rather than configuration, the five commands and the arity global, everything a
script can reach, the locked region, and how time and memory work.

The two things 98 specifically said should be written down and were not:

  - a name `KEYS` prints cannot be handed back to `GET`. Function keys are first
    class, so KEYS, SCAN, RANGE and the bounds see them and DBSIZE counts them,
    but GET, SET and DEL cannot address one - which falls out of the encoding
    rather than being a rule, since a client's key never encodes to the function
    lead. Written as a notice, because it is the kind of thing found by being
    surprised.
  - the locked region's rules: one shard or the whole space, a second shard
    aborts, no barch.call, no sql.query, no yielding, no nesting - and the
    consequence that `store.range` and a container elsewhere are unusable inside
    a single-key region while the whole-space form covers them.

Four claims taken from the entry rather than from a test were checked against a
running server before publishing, and all four held: `RANDOMKEY` does hand back a
function name, `FLUSHDB` does drop a space's functions, `KEYSF` answers folded
names, and a range asking for 100000000 is capped rather than refused. Worth
doing - the entry had only *measured* DBSIZE, KEYS, SCAN and MAX, and listed
RANDOMKEY as a prediction.

The HTML is hand written, so the section was checked with a parser after each
edit. One thing to know for next time: the nav list and the article are separate
edits, and a script that fails between them leaves the section unreachable rather
than broken - it renders, it just cannot be navigated to.

## 140. Nested script calls are bounded [26-08-2026]

98 section E asked for "a nesting depth limit, and the instruction budget and
deadline shared across the whole tree of nested calls rather than reset per call".
Neither existed. `CALLF` is an ordinary non-asynchronous built-in, so
`barch.call("CALLF", ...)` reaches another function, and every level started with a
deadline of its own - the deadline bounded one call and not a tree of them.

A chain is now capped at `function_max_depth`, default 100, a server setting like
the slice and the deadline. The depth is carried on the caller rather than in a
thread local: a nested call can park and come back on another thread, which would
lose the count exactly where it matters. It is put back when the sub caller is
released, because that caller is reused for the life of the connection - without
that, a connection would climb towards the limit one command at a time and start
refusing ordinary calls.

The first version worked and was still wrong to use. Recursion stopped, but the
message came back as a screen of

    RECURSE:3: ERR RECURSE:3: ERR RECURSE:3: ERR ...

with the reason nowhere in it. Each level raises through `luaL_error`, which
prefixes the chunk and line, so a hundred levels prefix each other and the actual
cause falls off the end when the reply is truncated. The prefix is worth having
once and worthless a hundred times, so a depth refusal now carries a marker,
`too_deep_marker` in driver.h: `runner_for` passes such an error back up whole
instead of wrapping it again, and `barch_call` raises it with `lua_error` rather
than `luaL_error` so no position is prepended. Every other error keeps its
position.

Left per level rather than shared across the tree, which E had originally asked
for. Dropped deliberately [26-08-2026]: the depth cap is what bounds the tree, and
it does it with one number an operator can reason about. A shared budget would
have to be threaded through every nested caller and would make a call's failure
depend on what its callers had already spent - harder to explain than "a hundred
deep is too deep", for no bound the cap does not already give.

One test lesson. The first cut asserted `CONFIG GET function_max_depth` was the
default, which fails as soon as configtest has run - it saves its own value to the
configuration space and that persists on disk between runs. A test should not
assert the value of a global that another test writes; the round trip belongs in
configtest, which already has it.

## 141. `store.get` tells a cached miss from an absent key [26-08-2026]

Half of G2's point-read asymmetry. A space with a foreign source has three states for
a key - the value, a tomb (the source was asked and had nothing, cached so it is not
asked again), and nothing at all - and `barch.store.get` answered nil for the last
two. A script could not tell "does not exist" from "not fetched yet".

`barch.tomb` is the third answer. A table with an identity rather than a boolean,
because `false` collapses under the `if not v` everyone writes and the whole point is
that a script has to say which case it means. Its metatable is locked, so a script
cannot rewrite it.

Two things in the store had to change, and the second was the actual work:

  - `sharded_store::search` returns false for a tomb and for a null alike, which is
    right for a command that wants the value and wrong here. `search_state` is the
    same read keeping the difference.
  - and the first cut of it did not work, because `shard::search` erases a tomb to
    null before a caller ever sees it - shard.cpp:1186. So `is_tomb()` on what came
    back could never fire. `local_leaf` is the raw accessor, and it is what the
    foreign path itself uses to decide whether to fetch, for exactly this reason.
    Two layers were hiding the same fact and only one of them was obvious.

An expired tomb reads as absent rather than tombed, which is the honest answer: once
`missing_ttl` has lapsed the source can be asked again, so the state really is
unknown.

Found on the way, and worth knowing when reading this code: a cached source miss is
not stored as an empty value with a TTL, as the shape of `insert_cached_miss`
suggests at first - it writes the empty value and then calls `set_tomb()` on the leaf
at shard.cpp:1134. The tomb flag is what matters, the empty value is incidental.

test/functiontest.py builds a fake-source space and asks a script for a key the
source has, one it does not, and one nothing has ever asked for - through both
`store.get` and the space value. Documented as a notice in the functions section.

Still open in G2: `barch.call("GET")` fills where `store.get` does not, and the
`ctx_swig` inheritance behind it that holds a worker for up to five minutes on an
external round trip. This entry only closes the part about telling the states apart.

## 142. FLUSHALL leaves the configuration space alone [26-08-2026]

`CLEARALL` called `barch::all_shards` and cleared every one, which reached
`configuration`. That space is not data: it holds each space's `<name>.foreign*`
settings, key_split and shard count, and now the global stored functions. So a
FLUSHALL of the caches silently unconfigured the server. 98 A had noticed it and
left it as "either right or wants refusing".

Refused. It iterates spaces by name now and skips `configuration` and
`configuration_`. FLUSHDB is untouched, so someone who really means to clear
configuration can `USE configuration` and say so - which is the difference between
an accident and a decision.

test/functiontest.py writes a setting and a global function, runs FLUSHALL, and
checks an ordinary key is gone while both of those are still there.

The test had to move to the end of the file, twice. FLUSHALL reaches every space,
so it cleared the functions that later assertions in the same suite depend on -
and the first move landed before a *container* cleanup loop rather than the
function one, which looked identical at a glance and failed the same way. A test
for a global operation belongs last, and "before the cleanup loop" is not a
location when there are two of them.

## 143. One Luau state per session, not one per session and space [26-08-2026]

Measured first, because the shape of the fix depended on the numbers. RSS deltas on
a live server:

    a compiled function, in a state that exists      ~0.5 kB
    a lua_State, one session and one space           ~50   kB

So the expensive thing was duplicated per space a session touched, while the cheap
thing was capped at 64 - and the cap was a cliff: the sixty fifth function threw
away the state, all 64 compiled functions and the ten libraries, then paid for a
fresh state and a re-sandbox. Backwards on both counts.

Almost nothing in a state was ever per space. The functions map was, and
`barch.foreign.space` in the registry was - read by `sql_query` to choose a
backend. `require` already resolved through `st->load`, which is re-pointed per
call, so it never was. The ten libraries and the API tables, which are the 50 kB,
are identical for every space.

So: one state per session, functions keyed by `space \0 name`, and the space a call
runs in carried on the coroutine with `lua_setthreaddata`.

Why not the function's environment, which is where it first looks like it belongs:
`luaL_sandboxthread` gives each function its own globals table and a script can
write to it - that is exactly what test/functiontest.py's `setsglobal` pins - so a
script could overwrite the space name and point `sql.query` at another space's
database. That is a privilege escalation, not a bug. Thread data is C side only,
with no Lua-visible key to shadow, and it is per coroutine rather than per VM,
which matters now that a nested CALLF is a second job on the same state.

Two places had to learn the qualified key and both failed the same way when they
did not - a cycle going undetected, because one side pushed one key shape and the
other looked up another:

  - `compile_into` runs the chunk's top level, where `require` fires, on a thread
    it creates itself. That thread needed the space *before* the chunk ran, not
    after, so the scope is set there and cleared on the way out - the thread is
    kept as the function's environment and must not be readable through a stale
    local.
  - the SETF compile check still compiled under the bare name. That is the path
    the cycle test actually exercises, since a cycle can only be built through a
    redefinition.

The separator is a nul rather than an underscore: space `A` with function `B_C`
and space `A_B` with function `C` would otherwise be one key.

`max_cached_spaces` is gone. `max_cached_functions` stays as a backstop at 4096 -
a couple of megabytes - and clearing it now calls `lua_unref` on all three refs of
every entry. That matters more than it did: the state used to be destroyed on
overflow, which freed everything; a state that lives for the session would just
keep them.

Measured after: a second session over the same 20 spaces went from 992 kB to
264 kB, and what is left is per-space bookkeeping and compiled functions rather
than twenty copies of the standard library.

Sixteen test files pass. test/functiontest.py pins the hazard the merge
introduces: the same function name in two spaces, reached directly and through
`require`, staying separate.

## 144. Luau memory is in the statistics [26-08-2026]

98 C asked for a line in the memory statistics for what the function states hold.
INFO MEMORY now carries four:

    used_memory_luau            bytes held by every Luau state in the process
    used_memory_luau_human
    luau_states                 how many states exist
    luau_functions_compiled     how many compiled functions they hold

Counted by an allocator installed with `lua_newstate`, not asked for with
`lua_gc(LUA_GCCOUNT)`. A state belongs to the session using it, so reading its
collector from whichever thread happens to be serving INFO would be a race. An
allocator sees all three kinds of state - the per session function states, the
foreign fill states, and the scratch one a SETF compile check builds - and sees
them live.

The round trip holds: zero before anything runs, zero again after a SETF (the
scratch state is created and closed, so it nets out), and back to zero after the
session that used a state disconnects. That last one is the check worth having,
since a counter that only goes up is worse than no counter.

**It also corrects DONE 143.** That entry measured a state at about 50 kB from RSS
deltas. The allocator says 362 kB. RSS undercounts because freed pages elsewhere
get reused and not every page is touched, so the honest figure for what a state
holds is the allocator's. A compiled function measures about 1.06 kB against the
0.5 kB estimated the same way.

Which strengthens rather than changes what 143 concluded: duplicating a state per
space was costing seven times what the RSS estimate suggested, so merging them was
worth more than it looked, and the ratio between a state and a function - the thing
the decision actually rested on - is 340:1 rather than 100:1.

One near miss worth recording. Routing every `lua_close` through the counting
wrapper was done with a scripted sweep over the file, and it rewrote the
`lua_close` *inside the wrapper itself* into a call to the wrapper - infinite
recursion on every state close. Caught by reading the result rather than by a test,
because a test would have crashed without saying why. That is twice now that a
blind textual sweep has produced something that compiles and is wrong.

## 145. User-defined Luau RESP functions [26-08-2026]

TODO 98, closed. The whole entry follows, because it is the design record and the
reasoning in it is what the code means - but what was actually built differs from
what was first planned in enough places to be worth saying up front.

What changed between the plan and the thing:

  - a function is an ordinary key under a lead of its own, which was the plan, and
    the audit that looked like the hard part turned out to be one predicate. What
    was not predicted is that GET, SET and DEL cannot reach the range at all, which
    falls out of the encoding rather than being a rule anyone wrote.
  - the store interface reads the cache and does not fill, and `barch.call` does
    fill. That was an open question for most of the entry and is now the deliberate
    split, with `barch.tomb` making the store side answerable rather than silent.
  - keys are strings, in and out. Typed keys were built and then taken out again:
    Luau's integer is a separate type that compares equal to nothing a script would
    write down, and a per-magnitude type would have been worse than text.
  - one Luau state per session, not per session and space. The entry assumed the
    latter throughout, and C's LRU cap was written for it. Measuring the parts made
    the cap unnecessary instead of smarter.
  - `_G["NAME"]` was dropped. `barch.call` is the one way in.
  - the locked region exists, which C described and nothing needed until containers
    and writes made read-modify-write reachable.
  - parking replaced `is_asynch`, and then a short first slice made parking itself
    the exception: the fixed cost of a call went 20.7us to 1.4us.

The detail of each piece is in its own entry - 127 through 144 - and the numbers
quoted below are the ones measured at the time, some of which later entries
correct. The most important correction: a Luau state is about 362kB, not the 50kB
an RSS delta suggested in 143. See 144.

What is not built, and was decided rather than forgotten: no per-tree instruction
budget for nested calls (the depth cap bounds the tree), no LRU on the function
cache (nothing is expensive enough), and no live reload of a running session's
compiled functions - that is TODO 137 and stays open on purpose.

---

98. User-defined Luau RESP functions.

    Register a RESP command whose body is a Luau script, so a client
    can call `MYTHING a b` and the script sees those arguments and
    answers through the same reply path as a built-in. This is not
    `foreign=luau`, which only fills a miss. It is a command.

    A function is a stored value, not a configuration string. It
    lives under `art::tfunction`, so it persists, replicates and
    exports the way every other key does, and a space's functions
    have the same lifetime as the space. Functions in the
    `configuration` space are global: defined once, callable from any
    client in any space, exactly like a built-in, which is the same
    scope the barch module itself has. Functions in one space may
    include each other. A cycle is an error, never a silent drop.

    A. Where a function lives: an ordinary key with a lead of its
       own.

    A function key is a composite key like any other multi-part key,
    with `art::tfunction` as its lead where a caller's key has
    `art::tplain`: `composite{ts_function, "KS1", "PRINT_NAME"}`.
    A composite component is `{lead, 0x00}` followed by the parts -
    which is why every decoder starts reading at [2] - so this needs
    a `ts_function` beside `ts_plain` in nodes.h and nothing else
    new in the encoding.

    `is_composite_lead` then has to say yes to 12, and explicitly
    rather than by widening the range it already tests, because
    `is_container_lead` shares that range and a function is not a
    container: no kind is claimed for it and
    `claim_container_kind` must never see one.

    That turned out to be the whole audit. Every site that reasons
    about a lead byte asks the predicate: keys.cpp:133, :202 and
    :382, `script_key` in luau_driver.cpp, the three in
    foreign/sql.cpp, and - the one that looked like separate work -
    conversion.h:244, where `is_composite_lead` is already one of
    the disjuncts in `comparable_key`'s accept list. So the throw
    and the abort both go away with the predicate, and the comment
    at `is_composite_lead` that says "one predicate, so the next
    type is one edit" turned out to be telling the truth.

    `is_container_lead` is the one that must not change. It tests
    the same span, and a function is not a container - nothing
    claims a kind for one - so tfunction is named explicitly in
    `is_composite_lead` rather than folded into the range.

    The AOF and RDB writers, the exporter and replication all treat
    it as an ordinary key, which is the whole point of doing it this
    way: a SETF persists and reaches a replica with no machinery of
    its own.

    First class also means the ordinary commands answer for these
    keys rather than hiding them. All of this is now observed
    rather than predicted - see test/functiontest.py:

      - KEYS, SCAN, RANGE and the bounds see them. DBSIZE counts
        them, so a SETF changes it.
      - TYPE cannot say `function`, because barch has no TYPE
        command - a grep finds no handler and no table entry, and
        containers have no answer there either. Adding one is its
        own job, redis-shaped, and it would have to answer for
        lists, hashes and ordered sets before it answers for
        functions. Nothing here depends on it.
      - GET, SET and DEL cannot address them at all - see below,
        it falls out of the encoding rather than being a rule.
        SETF, GETF, REMF and KEYSF are the commands that can; see
        K for why they are their own commands and not a flag.
      - FLUSHDB drops a space's functions with the space. FLUSHALL
        does not reach `configuration` at all - it is settings
        rather than data, and clearing the caches should not
        unconfigure the server. See DONE 142.
      - eviction never takes one. A function is a command, not
        data: losing one to memory pressure deletes a command, and
        because a session keeps whatever it compiled, the
        connections that already ran it would carry on while new
        ones met "unknown command".

        The guard sits at the policy level - the updater inside
        `abstract_eviction`, which every policy funnels through,
        and the else branch of `run_sweep_lru_keys` - and
        deliberately *not* in `shard::evict`, which looks like the
        one right place and is the wrong one. `erase_page` calls
        evict to lift a key out of a fragmented page before adding
        it back, and aborts with "key not marked as deleted but it
        was not found" if the key does not go. So a guard there
        would turn defragmenting a space holding a function into a
        hard abort. Defrag has to keep working; eviction must not
        happen.

        test/functionevicttest.py has a process of its own, because
        it drops maxmemory under what is already held to make the
        sweeper run at all - eviction is gated on
        `logical_allocated >= max_memory * pre_evict_thresh`, so
        nothing happens until the ceiling is below the floor. It
        waits for `keys_evicted` to actually move and fails if it
        does not, so a run where eviction never fired cannot pass
        by accident. It takes about 290 of 500 plain keys and
        leaves the function stored, listed and runnable.
      - EXPIRE, and the whole TTL family, is refused on the range.
        The original reason was the generation counter, which is
        gone - but the refusal stands on its own now: a session
        holds what it compiled for as long as it is connected, so
        a TTL would expire the key while every connected client
        went on calling the function. A setting that visibly does
        not do what it says is worse than not having it.

    Writing one. The precedent is a container key: hash_api.cpp:60
    builds `query.create(art::ts_hash, {container})` and inserts
    through the store. A function is the same move with a different
    lead:

        composite q;
        auto key = q.create(art::ts_function, {conversion::convert(name)});
        store.insert(opts, key, source, ...);

    with `fits_in_leaf(key.size, source.size)` checked first the way
    HSET does. The ceiling is `maximum_allocation_size`, a little
    under 256KB, which no sane script reaches - but that check is
    where `too_large_message` comes from and it belongs there.

    What makes the range safe is that nothing else can reach it. A
    client's key goes through `key_space::encode_key` and
    `conversion::as_composite`, which produce tstring, tinteger,
    tplain and the rest, never tfunction. So SET cannot address a
    function key and there is no guard to write for it: the refusal
    is the encoding itself.

    The same fact costs something, and the bullets above had it
    wrong first time round: GET and DEL cannot reach the range
    either, for exactly that reason. They do not read or delete a
    function. `GETF` and `REMF` are the surface, and they build the
    composite themselves - see K. KEYS and SCAN still
    show function keys, since the reply path decodes any composite,
    so the asymmetry to live with is that a name KEYS printed cannot
    be handed back to GET. Said in the docs now, as a notice under
    the commands - see DONE 139.

    A guard was planned for the paths that write an already encoded
    key, on the grounds that a forged tfunction key from a peer is a
    function body nobody loaded. Tracing them says there is nothing
    to guard, and the tracing found a real bug instead:

      - replication ships the command. `run_params` calls
        `repl::call(owned)` with the parameters, so a replica
        replays `SETF name source` through the ordinary handler and
        compiles it like anyone else.
      - IMPORT replays RESP commands too, looked up in
        `functions_by_name()`, and refuses a command the server does
        not have.
      - PULL and RETRIEVE move raw arena pages wholesale through
        `shard::retrieve`, not individual keys. Checking a lead
        there means nothing: a peer that can hand you pages can
        hand you any tree it likes, and that path is gated on the
        storage version anyway.

    What was actually broken was the other direction. EXPORT dropped
    functions silently. A tfunction key is not a container lead, so
    it fell through to the plain branch, was recorded by its decoded
    name, and `export_one` then re-encoded that name as a *string*
    key, found nothing under it and wrote nothing at all. So an
    export of a space holding one function and one ordinary key
    reported 1 and contained only the key - a backup with a hole in
    it, which is exactly what the comment beside the member index
    branch warns about.

    EXPORT now emits `SETF name source`, which IMPORT already knew
    how to replay. Function names are collected in a set of their
    own rather than in the `named` map, because a function and an
    ordinary key can hold the same name at once and one map keyed
    by name loses whichever is found second - asserted in
    test/functiontest.py, which exports a `greet` function and a
    `greet` string together and expects both back.

    Two more consequences of writing a new lead into a shard file:

      - `storage_version` in constants.h is bumped, 15 to 16, and
        it went in with SETF rather than with the key type. The
        bump protects an old binary from a new *file*, and no file
        could hold a function key until something could write one.
        Note that 15 was bumped without a line in that comment
        block; 16 has one.

        Checked against the shard files already sitting in the
        build directory: `arena_retrieve` in hash_arena.cpp:130
        compares the stored version, logs "data format is invalid"
        and answers false, so the space comes up empty rather than
        reading old bytes as something they are not. It says so
        once per shard file, which on 347 shards is 694 lines of
        it - correct, and worth a thought if a version bump ever
        needs to look like anything other than a wall of errors.
      - the value is the source text, not bytecode. Bytecode is
        tied to the Luau build, so storing it turns a Luau upgrade
        into a migration, and the compiled instance already lives in
        the session per C.

    The key is `{ts_function, name}`. The space is implied by the
    space the command runs in - `space:SETF` or `USE space` then
    `SETF` - so it is not in the key, and there is no way for a key
    in KS2 to claim it belongs to KS1.

    A stored function answers through a global `call`, where a
    foreign fill answers through `resolve`. That is the first place
    the two Luau contracts part company, and it cost a bug already:
    `compile_source` in luau_driver.cpp had "resolve" written into
    it, so pointing SETF at it would have refused every function
    ever written. The entry point is a parameter now -
    `compile_entry(source, bytecode, entry, err)` - and the two
    callers pass their own.

    SETF is SET with a different type byte: same name in, same
    value, `ts_function` where `encode_key` would have picked
    tstring or tinteger. At the byte level that is a one component
    composite, `{0x0c, 0x00}` then the name, rather than a literal
    swap of the lead on a tstring key. The two sort the same and
    both render as the bare name through `reply_encoded_key`, so no
    client can tell them apart - but the composite is already
    handled by every decoder through `is_composite_lead`, where a
    literal swap would need the tstring branch taught about
    tfunction in keys.cpp three times over, in conversion.h, in
    `script_key` and in sql.cpp. Same behaviour, none of the
    work.

    The ordering is the interesting part, and it cuts both ways.
    tfunction is 12, after every other lead, so function keys sort
    together at the end of an ordered space. In favour: FUNCTION
    LIST is a range scan over one contiguous span rather than an
    index that has to be kept in step, dropping every function in a
    space is one range delete, and a session rebuilding its cache
    reads them in a single walk. Against: MAX and `maximum()` now
    answer with a function key, `KEYS *` includes them, RANDOMKEY
    can hand one back, and on a range-sharded space every function
    lands on the last shard. None of that is wrong, but each is a
    change to a command that has nothing to do with functions, and
    each wants a test saying which way it went.

    Measured once SETF existed, on a space holding one string and
    one function: DBSIZE goes 1 to 2, `KEYS *` and `SCAN 0` both
    name the function, and `MAX` answers with it rather than with
    the string. `GET` on the same name answers null, which is the
    encoding keeping the two ranges apart. So the prediction held
    in both directions, including the awkward half.

    B. Naming, scope and resolution.

    A function is addressed `SPACE.NAME`, so `KS1.PRINT_NAME` is
    PRINT_NAME as defined in KS1. An unqualified `PRINT_NAME`
    resolves in the selected space first, then in `configuration`.

    That sits next to the `space:CMD` prefix `run_params` already
    parses, and the two are orthogonal rather than redundant: the
    colon says which space the call *runs against* - what
    `call.kspace()` returns - and the dot says where the definition
    is *loaded from*. So `KS1:KS1.PRINT_NAME` is valid and says
    both, and `KS2:KS1.PRINT_NAME` runs KS1's function against KS2,
    which is worth having if the function is written against an
    interface rather than against particular keys.

    Parsing goes in the same place and in this order: split the
    colon prefix, split at the first dot, then upper-case what is
    left. It has to be that way round because a space name is case
    sensitive and a command name is not, which is already why
    `run_params` splits the colon before folding. It also assumes a
    space name contains no dot - configuration keys are
    `<name>.foreign`, so that is assumed elsewhere too, but it is
    worth saying out loud rather than discovering.

    Built-ins are never overloaded. A SETF that takes the name of a
    built-in command is refused there and then, so dispatch
    looks the built-in table up first and never has to arbitrate
    between the two. A dotted name cannot collide by construction.

    C. Where the compiled function lives: one state per session and
       space.

    Threading is solved by not sharing. A RESP session keeps a
    `lua_State` per key space it has called into, and a string map
    of the functions compiled in it. The first call to
    `KS1.PRINT_NAME` on a session reads the tfunction key out of
    KS1, compiles it into that session's state for KS1 and caches
    it under that name; every later call on that session goes
    straight to it. Nothing is shared between sessions, so there is
    no mutex, no pool of states, and no state whose ownership moves
    between threads.

    The invariant this rests on is that one session never runs two
    calls at once, and today it does not: `run_asynch_batch` posts
    one call at a time and does not read again until the batch is
    done, and a parked call resumes through the same chain. That is
    worth writing down beside the cache, because the day something
    runs two calls of one session in parallel this stops being safe
    without looking any different.

    The space keeps the definitions; the session keeps one compiled
    instance of them, and once compiled it keeps that one. A
    session does not notice a redefinition: new code reaches new
    sessions. Reconnecting is how a client picks up a change, and
    that is the documented behaviour rather than a gap. Live
    reload and versioning are 137.

    That is worth what it saves. There is no generation counter, no
    atomic on the call path, nothing that has to reach into a live
    session from a write, and no rule about what happens to a call
    already running when its definition changes underneath it.

    Note what it does *not* cost: a function is compiled on first
    use, so SETF followed by a call on the same connection works -
    that session had nothing cached for the name and reads the key
    fresh. Only a redefinition of something the session has already
    run is stale.

    Teardown is the leftover. A session must key its states on the
    space name and never hold a `key_space_ptr`, or an UNLOADed
    space stays alive in every session that ever called into it.
    Size is the other one. A client that calls a hundred functions
    has a hundred of them compiled, a thousand connections have a
    thousand copies of the popular ones, and a session that roams
    over spaces holds a state for each. That is no longer true: a
    session keeps one state whatever it touches, so there is nothing
    left that is expensive enough to want LRU - see DONE 143. The
    line in the memory statistics is there too - DONE 144, which
    also corrects 143's per state figure from 50kB to 362kB. Eviction is not invalidation:
    an evicted function is compiled again from whatever the key
    says now, so a long-lived session can pick up a redefinition by
    accident. Either that is fine and it is written down, or the
    cache keeps the source it compiled from.

    Per session and space, rather than per function or one for the
    whole session, because of what `require` costs and how memory
    comes back. The three were:

      - one per function. Hard isolation, and closing the state
        frees everything exactly, which is what makes an LRU cap
        and a per-function memory ceiling actually mean something.
        But it defeats includes: two functions in different states
        cannot share a module, so every function carries its own
        copy of everything it requires, plus its own base
        libraries and string table.
      - one per session, holding every function that session has
        called. Cheapest, one module cache, and `require` is free
        after the first use. Isolation is `luaL_sandbox` plus a
        per-call thread rather than a separate heap, which is
        enough here - the functions all belong to the same client
        and the same ACL. Reclaiming memory for one evicted
        function is vaguer: it needs a collection, not a close.
      - one per session and space. `require` resolves within a
        space (and `configuration`), so this puts the module cache
        exactly where the resolution already is. It also makes the
        two awkward cases in the list above trivial: a generation
        bump or an UNLOAD closes that space's state and nothing
        has to be picked out of a map. The duplication it costs is
        the `configuration` globals, once per space a session
        touches.

    The third is what this entry now assumes, with the second as
    the fallback if per-space states turn out to cost more than the
    bookkeeping they remove. Either way the *cache* stays keyed by
    function name; the choice is only which heap the compiled thing
    sits in, which is why falling back later is cheap.

    Inside each of those states, globals are frozen once with
    `luaL_sandbox` and each call runs on a `luaL_sandboxthread` with
    its own writable global proxy, so one call cannot leave
    anything behind for the next. Includes are natural here: a
    required function compiles into the same session state and is
    cached next to its caller.

    The other contexts have no session to hang this on. SWIG, RPC
    and the valkey module either grow an equivalent cache or answer
    `-ERR FUNCTION not supported on this path`, which is the shape
    of foreign's context check and is where the first cut should
    start.

    D. Includes, and cycles.

    Include is `require("NAME")`, resolved against the same space
    then `configuration`, with the result cached per state. That
    gives back the global `open_safe` currently blocks outright,
    restricted to a resolver that can only see functions.

    Cycles are refused, at load, with the path in the message:
    `-ERR FUNCTION cycle A -> B -> A`. The check is a DFS over the
    declared edges, and it has to run on every LOAD and every
    DELETE, not only the first, because A can include B today and B
    can be redefined to include A tomorrow. Whether the edges can
    be read off the source statically is a question: `require(x)`
    with a computed name cannot be, so there is a runtime backstop
    too - a "loading" mark on each module, which turns a cycle the
    static pass could not see into an error instead of a stack
    overflow.

    Deleting a function that something else includes is refused,
    with the dependents named. `FORCE` can override it and leave
    them broken, which is at least visible.

    E. Calling other commands.

    `call("NAME", "p1", "p2")` with the same string-vector shape the
    caller classes use everywhere else. The implementation is a
    derivative of rpc_caller: `callv(params, f, def)` already runs a
    command and hands back a `Variable`, which is one conversion
    away from a Luau value.

    It must be a *sub*-caller, not the caller that is answering the
    client. `rpc_caller::call` clears `results`, `errors` and `args`
    on entry, so calling through the outer one destroys the reply
    being built. `finish_call_buffer` has the pattern already -
    `collecting_exec` plus a buffer - and a script's nested call
    wants the same treatment.

    Things that need an answer before this ships, and the cheap
    answer for the first cut is to refuse them:

      - a blocking command inside a script. BLPOP parks the caller
        through `add_block`; a script cannot park half way through
        a Lua stack and still be resumable.
      - an asynchronous command inside a script. KEYS is
        `is_asynch` and expects to own a worker.
      - a command that parks on a foreign fill - the same problem,
        arriving by a different route.
      - MULTI/EXEC from inside a script.

    The depth limit is built - `function_max_depth`, default 100,
    carried on the caller so a nested call that parks and resumes
    elsewhere does not lose the count. See DONE 140.

    Sharing the budget and the deadline across a tree of nested
    calls was asked for here originally and is dropped. The depth
    cap is what actually bounds the tree, and it bounds it with one
    number an operator can reason about; a shared budget would have
    to be threaded through every nested caller and would make a
    call's failure depend on what its callers had already spent,
    which is a much harder thing to explain than "a hundred deep is
    too deep". The per level deadline stays.

    ACL is not optional here: the script runs as the calling user
    and `call` checks that user's acl vector, or a function becomes
    a way to launder a command past the check that would have
    refused it. There is also a nesting depth limit.

    `_G["NAME"]` was going to be a convenience on top of `call`.
    Dropped, and the reasons it was hedged about from the start are
    the reasons: a command name can collide with a base global
    (barch has `KEYS`, which is also the name redis's own Lua API
    uses), `_G` cannot express the `space:CMD` form at all, and the
    set of names would have to be rebuilt whenever a function is
    loaded. So it would be a second way to call a command that
    works for most names, silently does something else for a few,
    and cannot express one of the two forms - against `barch.call`,
    which is one way that always works.

    `call` was always the primitive and it stays the only one.

    F. The store interface.

    `sharded_store` is the object to expose - it is already the
    per-call, cheap-to-construct front door to the shards, the hash
    and the art, and it already has the read and write scopes,
    the container scopes, the bounds, `range`, `glob` and `scan`.
    Plus a read-only view of the space's own configuration.

    "Safe" has one rule behind it: script code never runs while a
    shard lock is held. Two ways to keep that true, and they can
    coexist:

      - iteration hands back bounded batches. The cursor takes the
        lock, copies n entries, drops the lock, returns them. That
        is what `scan_cursor` already does.
      - where a callback under a lock is genuinely wanted, mark the
        region on the run context: no yield, no `call`, no
        `sql.query`, and a hard instruction cap inside it, with the
        interrupt raising an error rather than yielding. A yield
        under a shard lock is the deadlock the first draft of this
        entry was worried about, and this is what makes it
        impossible rather than merely discouraged.

    F6. What the locked region actually needs: an explicit lock, and
    an API that knows one is held.

    The shape: a script locks a key, or locks the whole space when it
    names no key, and inside that the ordinary interface works as it
    always did - except that nothing takes the lock a second time.

    That second half is not a refinement, it is the whole difficulty.
    The shard mutex is not recursive - keyspace_api.cpp:219 and
    key_space.cpp:473 both say so, both having been bitten - so an
    implicit acquire inside an explicit hold is EDEADLK on the
    calling thread, not a slower path. Every entry point that opens a
    scope today has to ask whether the shard it wants is already held
    and skip taking it if so.

    Which means the guard belongs on *every* shard acquisition inside
    the region, not only on the explicit lock call. A script that
    locks one key and then calls `store.range`, or reads a container
    that lives elsewhere, reaches a second shard through the implicit
    path without ever asking for it. If only explicit locks are
    checked, that case deadlocks silently, which is the failure the
    check exists to prevent.

    The rule for how many: one shard, or all of them in shard order,
    and never an arbitrary subset. A second explicitly locked key
    that lands on a different shard aborts the call with a loud
    error rather than taking the lock - deadlock is not worth being
    clever about, and a script that wants two keys atomically can be
    told to put them in one container, which routes them to one
    shard by name.

    Two things make this cheaper than it looks:

      - nothing today holds two shard locks at once.
        `each_shard_read` and `each_shard_write` scope the guard
        inside the loop body, so they take one, release it, and take
        the next. So single-shard holders cannot form a cycle with
        anything, and only the all-shards form is a new kind of
        holder - which is why it has to be ordered, and why it only
        has to be ordered against itself.
      - the held set can be thread local, which avoids threading a
        lock token through every store_access entry point. That is
        only sound because the region forbids yielding: a call that
        cannot yield cannot park, and a call that cannot park never
        resumes on another thread. The no-yield rule pays for the
        cheap implementation as well as for the deadlock.

    The surface should be scoped rather than a lock/unlock pair:

        barch.store.locked(key, function()
            local n = tonumber(barch.store.get(key)) or 0
            barch.store.set(key, tostring(n + 1))
        end)

    A pair a script has to close itself leaks the shard lock the
    first time a script errors between them, and errors are exactly
    what happens inside a region with a hard instruction cap. With
    the callback form the C++ side holds an RAII guard across the
    call and the lock goes back whether the script returns, raises,
    or is cut off at the cap or the deadline.

    The goal being most of the built-ins re-implementable in Luau is
    a good test of the interface: pick three of different shapes -
    say GETRANGE, HRANDFIELD and ZRANGEBYSCORE - write them in Luau
    against this interface, and see what is missing.

    F3. Rights, for reads that do not go through a command.

    `barch.call` checks the command's categories against the outer
    caller's ACL, which is what stops a function being a way round
    a check the connection would have failed. `barch.store` had no
    such check at all: it reads through `sharded_store` directly,
    so a user holding `+function` and nothing else could read any
    key in the space. That shipped, and is fixed now.

    The shape it took, because 6b needs the same thing for writes:

      - `store_access` carries `may_read` and `may_write`, decided
        once by whoever builds it - function_api, which knows the
        ACL - and enforced in the driver, which knows how to raise
        a Lua error. Both default to false, so one that nobody
        filled in refuses rather than allows.
      - the categories are the equivalent command's: read, keys and
        data for a read, write, keys and data for a write. So the
        two routes to the same key answer to the same rights, which
        is the property worth having and the one the test asserts -
        the restricted user is refused the function *and* a plain
        GET.

    `barch.space.sp1.key1 = v` in 6b is a write and wants
    `may_write`; the read wants `may_read`. Nothing new is needed
    for it beyond what is now there.

    Where it stops being enough is 135. This is one pair of flags
    for the whole call because ACL categories are global, and
    `barch.space.other.k = v` reaches a second space that the pair
    knows nothing about. Per space rights turn it from a pair
    decided once into a question asked per space touched, which is
    another reason 135 belongs before 6b.

    F4. Who may see that a function exists.

    Asked because a walk meets function keys the way KEYS does, and
    someone who may not read a function has no business learning
    which ones there are. Checking it found the exposure is not
    where it looked.

    Inside a script there is nothing to fix, for two reasons that
    are independent of each other:

      - invoking any stored function needs the `function` category
        before a line of Luau runs, so a walk inside a script
        always belongs to someone who already holds it. A user with
        `+read +keys` is refused the bare name, CALLF and GETF
        alike - asserted in test/functiontest.py.
      - a range cannot reach the function range anyway. Bounds go
        through `encode_key`, which makes tstring, tinteger and the
        rest and never tfunction, so every bound a script can name
        sorts below the range. `range("", "\255")` answers plain
        keys only.

    So `min` and `max` are the only store entry points that can
    surface a function name, and they filter their single answer on
    a `may_see_functions` flag beside may_read and may_write. The
    matching clamp on range and count cannot fire today, and says
    so in a comment rather than looking load bearing.

    Where it was real is the commands, and that is now filtered. A
    `+read +keys` user with no `+function` used to see:

        KEYS *   ->  ['SECRETFN', 'plainkey']
        MAX      ->  b'SECRETFN'

    and now sees `['plainkey']` and null. `visible_key` in
    keys_api.cpp is the predicate - a function key is visible only
    to a holder of the category - and it is applied at all eleven
    places a key reaches a client: both of KEYS's passes, its
    result-stack path, RANGE, SCAN, MIN, MAX, LB, UB and RANDOMKEY.

    KEYS counts before it writes, so the count pass and the emit
    pass have to skip on identical terms or the `*N` header stops
    describing the body that follows. Same predicate in both, and
    that is the reason it is a named function rather than an
    inline test.

    Two things about this that are not tidy:

      - MAX needed a reverse walk and now has one.
        `sharded_store::maximum_below(bound, cb)` is the largest
        key strictly under a bound: the iterator constructor is a
        lower bound, so it positions at where the functions start
        and steps back once. MAX uses it for a user who may not see
        the range, and answers the largest ordinary key rather than
        a null.

        MIN, LB and UB needed nothing. Functions are contiguous at
        the top, so if the first key at or past a bound is one,
        there really is nothing visible past it and null is the
        true answer. Only MAX was wrong, because its answer is at
        the end where the functions are.

        RANDOMKEY still answers null when it lands on a function.
        It is random, so a null now and then is not a wrong answer,
        only an unhelpful one; retrying would be the fix if anyone
        minds.
      - KEYS now answers differently per user, which is a real
        property to have taken on, and it is the price of A's
        decision that functions are ordinary scannable keys. The
        alternative was hiding them from the key space entirely,
        which costs more.

    DBSIZE is deliberately not filtered: a count that varies by who
    asks confuses more than it protects, and the number it gives is
    not a name.

    F2. The space as a value, and iterating it.

    Where F's first cut ended up - `barch.store.get(k)` - reads like
    a C API in a language that does not need one. The shape to aim
    for instead:

        barch.space.sp1.key1              -- read
        barch.space["sp1"]["key1"]        -- the same thing
        barch.space.sp1.key1 = "value1"   -- write
        barch.space.sp1.key1 = nil        -- remove

    through `__index` and `__newindex`, so a key space reads as a
    table and a script says what it means rather than which
    function to call.

    Points that decide whether it works:

      - it survives the sandbox, but only just, and the reason is
        worth knowing. `luaL_sandbox` walks the globals table and
        sets readonly on the tables it finds *one level down* -
        that is the whole loop in linit.cpp. So `barch` is frozen
        and nobody can replace `barch.call`, while `barch.space`
        and a space handle are nested and never touched, which is
        exactly what leaves the writes working.
      - userdata for the space table and for each handle, not
        tables. `__index` on a table only fires when the key is
        absent, so a table could be rawset past; userdata always
        goes through the metamethods and can carry the space name
        without a lookup.
      - a number index is an integer key and a string index a
        string key, which is what `encode_key` does with a client's
        key already. So `barch.space.sp1[42]` and `SET 42` reach
        the same place, and they have to, or a script cannot see
        what a client wrote.
      - touching a space must not build it. `get_keyspace` creates
        one; `is_keyspace` asks. Same care `functions::resolve`
        takes, and for the same reason: a name in a script is not
        permission to make a key space.
      - a missing key reads as nil, and a failed write raises. That
        is the Lua split, and it matches what `store.get` already
        answers.

    This makes 135 urgent rather than nice to have. `barch.space.other.k
    = v` is a cross-space write that no category can currently
    describe: ACL categories are global, so a user who may write
    anywhere may write everywhere, and a function makes that
    trivial and implicit rather than something a client had to
    spell out with a prefix. Per space rights want to exist before
    the space table does.

    Iteration, and why page copy is the right answer.

    Copy a page under the read lock, drop the lock, walk the copy.
    The lock is held for the copy and nothing else, and erasure
    while iterating stops being a question - what is being walked
    is nobody's live memory.

    `scan_cursor` in sharded_store.h is already precisely this: a
    shard index, a page index, a position, and `buffer` holding the
    copy of the page being read. SCAN has worked this way all
    along, and the glob path copies pages for the same reason. So
    this is reuse rather than new machinery.

    What it promises is what SCAN promises: everything present
    throughout the iteration is seen at least once, repeats are
    allowed, and a key written or erased after its page was copied
    may or may not appear. That is worth saying in the docs next to
    the loop, because a script that assumes a snapshot will be
    wrong in a way that is hard to see.

    Containers, as a triple.

        for c, k, v in barch.space.sp1 do

    where a plain key is `c == nil, k = key, v = value`, a
    container member is `c = container, k = member, v = value`, and
    a container header is `c = container, k == nil, v == nil`. One
    loop covers a space holding both, and a script that only wants
    plain keys skips on `c ~= nil` without needing a second call.

    The row says what it is, rather than leaving it to be worked
    out from which fields are nil:

        row.type       "key", "list", "hash", "orderedset",
                       "function"
        row.container  nil for a plain key, the name otherwise
        row.key        nil for a container header
        row.value      nil for a container header

    `type` costs nothing to provide - it is the lead byte, which
    the walk has already read to know what it is looking at - and
    naming the container kinds after the ACL categories keeps one
    vocabulary rather than two.

    It is not optional, for a reason that only appeared once
    functions became keys: a space holds `tfunction` keys now, and
    an iteration sees them exactly as KEYS does. A script walking a
    space for data has to be able to skip them, and `row.type ==
    "function"` is how. Without it every such loop would need to
    know how a function key is encoded.

    Adding it makes the flat form a four tuple - type, container,
    key, value - and that incidentally fixes the generic for that
    the three could not manage. `type` is never nil until the walk
    is over, so

        for t, c, k, v in barch.space.sp1 do

    terminates when it should, where `for c, k, v` ended on the
    first plain key. So both shapes are viable now and the choice
    is only the eager against the lazy one that was measured above:
    the tuple decodes all four every step, which is the 3.3x slower
    case on a filter over 1KB values, and the row object is 30%
    worse when every field is read. The row object still wins on
    the loop people actually write, so it stays the recommendation
    - but the tuple is no longer broken, only slower for filters,
    and that is a much smaller reason to refuse it.

    A header is still `key == nil`, which is one way of asking; the
    type is the other and the better one, since a script that wants
    "every hash member" says so directly instead of testing two
    fields for nil.

    Composite keys render as the caller wrote them - the components
    rejoined with the space's split character, which is what
    `push_encoded_key` does for KEYS and MIN, and the two have to
    agree or a script and a client disagree about the same key.
    That was a bug in F's first cut: `store_for` used the default
    separator, so a space split on "." answered `a.b` through KEYS
    and `a b` through the script. Fixed, with a test.

    Two caveats belong beside it:

      - a regex split has no single character to rejoin with and
        falls back to a space, so a space configured that way
        cannot round trip its keys exactly. True of KEYS today,
        not made worse here, and worth documenting rather than
        pretending.
      - a composite sorts elsewhere. A key holding the split is
        stored under tplain and a plain string under tstring, so a
        range over string bounds does not contain it - bounds that
        hold the split are composites too and bracket it properly.
        The built-in RANGE behaves exactly the same way, so this is
        the store's order showing through rather than the interface
        being odd.

    While pinning that, `barch.store.max()` came back with a
    *function* name, because tfunction is 12 and sorts after
    everything. That is the ordering trade in A arriving from a
    third direction, and it is the concrete argument for
    `row.type` - a script walking a space cannot avoid meeting
    function keys and needs a way to say so.

    Answered, and built - see DONE 138. The rest of this paragraph
    is what the question was: a key
    stored as an integer - `SET 42 x`, which encodes as tinteger,
    not as the string "42" - should probably come back from
    `row.key` as a Luau number, since `barch.space.sp1[42]` will
    have to encode a number index as an integer key to reach the
    same place. If the read hands back "42" and the write takes 42,
    a script cannot round trip its own keys.

    The details that follow from the storage:

      - a container's members all live on one shard, because a
        container routes by its name, so within a shard they are
        contiguous. Plain keys interleave across shards, so
        containers come out interrupted and out of order. That is
        fine and is what the triple is for - the header is emitted
        the first time a container name is seen, not in any
        position that means anything.
      - the internal bookkeeping keys must not surface as members.
        `is_container_internal` already names them - the ordered
        set's member index is the one that bit the exporter, which
        wrote it out as a string key and imported a key nobody had
        written.
      - the header carries no value, so `v == nil` for it. A script
        cannot tell a header from a member with a nil value that
        way, which is fine because a member with no value does not
        exist.

    Built, and the storage had one more surprise in it than the
    bullets above predicted. The surface is a handle rather than a
    whole space walk: `sp:container(name)` gives something iterable
    as `for member, value`, and `sp:kind(name)` answers "list",
    "hash", "orderedset" or "" - both reached through a `__namecall`
    on the space metatable. Member first means the control variable
    is never nil until the walk ends, which is the same fix the
    four tuple needed.

    Two things had to be got right and neither was guessable from
    the interface:

      - a container's keys live on the shard its *name* routes to,
        not wherever the whole key hashes. `with_container_read` is
        literally `with_key_read(container)`, which is why hash_api
        opens a scope and works on the shard inside it. A plain
        `store.search` on the full key compiles, runs, and finds
        nothing - the first cut did exactly that and every read
        came back empty while `kind` worked, because `kind` asks a
        different question.
      - an ordered set keeps two families of key under its name,
        and the obvious one is the wrong one. The score index is
        `{name, score, member}` with an empty value; the member
        index is `{IX_MEMBER, name, member}` and its value is the
        score index key. Walking the plain `{name, ...}` prefix
        therefore hands back the score where the member belongs and
        nothing where the value belongs - observed as
        `1.5 -> ` before it was `alpha -> 1.5`. So the walk uses
        the member index and cuts the score back out of the value,
        at `prefix.size` for `numeric_key_size` bytes through
        `enc_bytes_to_dbl`, formatted so `tonumber` reads it.

    Writing a member of an ordered set is refused with a message
    pointing at ZADD, and deleting one likewise. Both are two keys
    that have to agree, and the old score index entry has to be
    unlinked when a score changes - a generic setter that wrote
    only the member index would leave the set readable and wrong,
    which is worse than not offering it. Lists and hashes write
    normally.

    This closes what F recorded as impossible: `myHrandfield` and
    `myZrangebyscore` are now written in Luau in
    test/functiontest.py and answer correctly, alongside field
    reads cross checked against HGET, writes and removes, kind
    detection for all three, and a refusal for a name that is not a
    container.

    Measured, because the shape was worth testing before building
    it. A benchmark driving the Luau VM directly over 200,000 rows,
    counting through the same interrupt the budget uses, comparing
    the triple against a reused userdata that decodes a field only
    when it is read:

                              insns    40 byte     1KB values
      triple, key only       200002    29.0 ms       68.6 ms
      row, key only          200002    25.3 ms       20.8 ms
      triple, all three      200002    31.3 ms       76.4 ms
      row, all three         200002    54.0 ms       99.3 ms

    Three things come out of it.

    The instruction counts are identical - 200002 in every case,
    with singlestep on. The interrupt fires once per loop step
    whatever the step does, so the budget cannot tell these shapes
    apart at all, and the honest answer to "does the row object use
    fewer instructions" is no, it uses exactly the same number. It
    also means the wall clock deadline is the only thing bounding a
    long iteration, and that a loop over a million keys costs less
    budget than a thousand iterations of arithmetic. Worth deciding
    separately whether the iterator should charge the budget per
    key rather than per step.

    What the row object actually saves is decoding and allocation,
    which the budget never sees, and how much depends entirely on
    how much of each row the script reads. Reading one field of
    three it is 3.3x faster on 1KB values, because the value is
    never turned into a Luau string at all. Reading all three it is
    about 30% slower, because three metamethod dispatches cost more
    than three values already sitting in registers. The crossover
    moves with value size: at 40 bytes reading one field is barely
    a win, at 1KB it is decisive.

    An explicit cursor was the obvious next idea - a factory,
    `barch.iterator("sp1")`, and `while it:next() do ... it.key`,
    trading readability for speed. Measured against the generic for
    holding the same lazy row object:

                              insns    40 byte     1KB values
      row, key only          200002    20.9 ms       24.5 ms
      cursor, key only       400003    21.3 ms       20.7 ms
      row, all three         200002    53.2 ms       96.8 ms
      cursor, all three      400003    53.8 ms       97.1 ms

    It buys nothing. The times are the same within noise, because
    the laziness was already doing the work and the loop protocol
    was never the cost - and it burns twice the instruction budget,
    since a while loop has two interrupt points per turn (the
    `:next()` call and the back edge) where a generic for has one.
    So the readable shape is also the cheap one, and there is no
    speed to trade readability for.

    That leaves an explicit cursor worth adding only for control -
    seeking, stopping and resuming across calls, handing a position
    back to a client the way SCAN does - and if one is added it
    should be sold on that rather than on speed, with the 2x budget
    written next to it. `it:next()` with `__namecall` is the way to
    write it when that day comes: a plain `it.next` has to return a
    closure, and pushing one per access allocates.

    A further speedup exists and is not needed yet:
    `lua_registeruserdatadirectaccess` dispatches userdata fields on
    an interned atom, an int compare rather than the strcmp the
    benchmark used, so the row numbers above are a pessimistic
    bound. Marked experimental in lua.h, which is reason enough to
    leave it until there is a measurement asking for it.

    So the row object is the right shape, on the grounds that the
    iteration people actually write is a filter - walk the space,
    look at keys, touch the value of the few that matter - and that
    is exactly the case it is 3.3x faster in. A loop that reads
    every field of every row pays about 30% for the privilege,
    which is the right way round.

    Two things fall out of it that are not about speed:

      - `for c, k, v in barch.space.sp1 do` does not work. A
        generic for stops when the first value is nil, and the
        container is nil for every plain key, so the loop ends on
        the first one. The benchmark found this by iterating once
        and stopping. Either the key comes first - `for k, v, c` -
        or the iterator hands back a row object, which is never nil
        until the walk is over. The row object makes the problem
        disappear rather than working around it, which is another
        argument for it.
      - a reused userdata is what makes it cheap - one object for
        the whole loop rather than one per row - and that is a
        footgun. A script that stashes rows in a table gets a table
        of the same object, all reading the last row. It needs
        either a copy on demand, or documenting loudly, and
        probably both.

    `barch.store` from F's first cut becomes sugar for the space
    the call is running in, or goes. Two ways to read the same key
    is one too many.

    F5. What each call site costs.

    Measured with everything in one run and each figure beside the
    command that does the same job, because absolutes move by 2x
    with machine load and only the within-run comparison means
    anything.

    Which build matters, and the first pass got it wrong: all of
    this was measured against cmake-build-relwithdebinfo, which is
    `-O2 -g` *and* has BARCH_LOCK_DEBUG defined globally - the
    CMakeLists turns it off only for Release. That path adds
    mark_reader, a linear push_hold/pop_hold scan and an atomic to
    every lock, and everything that touches the store takes locks.

    Re-measured against Release (-O3 -flto, no lock debug),
    alternating the two so drift hits both:

                          RelWithDebInfo        Release
        store.get         0.38 / 0.30 us     0.37 / 0.34 us
        space[k]          0.50 / 0.56 us     0.41 / 0.45 us
        s[k] hoisted      0.48 / 0.48 us     0.39 / 0.46 us
        barch.call        5.97 / 6.18 us     5.09 / 4.95 us

    So Release is modestly ahead - nothing on store.get, roughly
    15% on a space read, and a consistent 18% on barch.call, which
    is the one doing the most locking. A single run each had
    suggested 40 to 60%, and that was a noisy baseline flattering
    it; alternating is what settled it.

    The conclusions hold either way: the boundary is cheap, the
    per-call setup dominates, and barch.call is the outlier. The
    absolute figures below are the RelWithDebInfo ones, so read
    them as an upper bound.

        PING              2.62 us    the floor: parse, dispatch, reply
        GET               3.64 us
        SET               5.57 us

        noop fn           4.18 us   +1.56 over PING    1.60x
        store.get         5.71 us   +2.07 over GET     1.57x
        space read        6.64 us   +3.00 over GET     1.82x
        space write       9.30 us   +3.74 over SET     1.67x
        barch.call GET   11.71 us   +8.07 over GET     3.22x
        require + call    4.35 us   +1.73 over PING    1.66x
        walk            2.26 us per row

    The ratios in that table are wrong, and the reason matters.
    Every figure was taken through redis-py, so each one is
    `python client + barch`, and the client is most of it: memtier
    reports about a million requests a second per thread at
    pipeline depth 50, which is under a microsecond a request,
    where this harness calls a PING 2.6us. Building and parsing a
    three hundred command pipeline in Python costs more than the
    server does.

    Differences survive that - the client cost is common to both
    sides and cancels - so the *additive* numbers stand: a function
    call adds about 1.5 to 2us, a store access about 0.35us,
    barch.call about 5us. The *ratios* do not: dividing by a
    denominator that is mostly Python flatters Luau badly. If a GET
    really costs 0.6us server side rather than the 3.6us measured,
    then LUAU_GET at +3us is closer to 5x than the 1.8x reported.

    Re-measured with memtier, release build, pipeline 50 - which is
    barch rather than a client. Connection count matters and is
    easy to misread: four connections spread over the eight service
    threads, so those figures are aggregate throughput, while one
    connection is the serial cost of a request.

        one thread, one connection:
        PING          1,654,778 ops/s    0.604 us
        GET           1,181,108 ops/s    0.847 us
        noop fn         404,161 ops/s    2.474 us
        LUAU_GET        323,742 ops/s    3.089 us     3.65x, +2.24us

    GET at 1.18M ops/s a thread is the figure barch is known to
    hit, so this is the right measurement rather than a lucky one.

        one thread, four connections (aggregate):

        PING          3,461,562 ops/s    0.289 us
        GET           3,098,255 ops/s    0.323 us
        noop fn         797,705 ops/s    1.254 us
        LUAU_GET        742,082 ops/s    1.348 us
        barch.call      122,566 ops/s    8.159 us

    The additive column survived: a function call adds 0.97us over
    a PING and LUAU_GET adds 1.03us over a GET, which is the 1 to
    2us this section had all along.

    The ratios did not, and were out by more than a factor of two.
    LUAU_GET is 3.65x a GET on one connection and 4.18x aggregate -
    not the 1.8x reported through redis-py - and barch.call is 25x
    rather than 3.2x. The ratio holding across both connection
    counts is what says it is real.

    The reason is that barch's own per command work is small: a GET
    costs 0.24us more than a PING on one connection, so the
    denominator is small and anything added to it dominates.
    Through redis-py the same denominator read 3.6us because it was
    mostly Python, which compressed every ratio toward 1.

    Where the microseconds actually are, taken by adding one thing
    at a time to a function that does nothing (memtier, release,
    one connection, pipeline 50):

        PING                      0.598 us
        call() -> nil             2.410 us    an empty function
        call() -> 1               2.413 us    +0.002 returning an integer
        call(k) -> 1              2.524 us    +0.111 taking an argument
        call(k) -> k              2.549 us    +0.026 handing it back
        call(k) -> store.get(k)   3.126 us    +0.577 the store lookup

    So an empty Luau function costs 1.813us before a line of Luau
    runs, and that is 72% of what a LUAU_GET costs. Everything the
    interface was suspected of is nearly free: arguments and return
    values together are 0.14us, and the boundary crossings measured
    at 20 to 58ns in isolation agree with that.

    The 1.813us is all C++ setup per call - resolve's store lookup,
    the job, the parked slot and wake key, the coroutine and its
    registry ref, and four interface objects built from a dozen
    std::functions. None of it depends on the script, and almost
    none of it needs to be per call: it wants building once per
    session and space, where the compiled functions already live.

    Done, and it paid:

                                  before      after
        empty fn, above PING      1.813 us    1.133 us
        LUAU_GET                  3.126 us    2.292 us
        LUAU_GET vs GET           3.79x       2.85x

    Two changes. The interface - loader, runner, store access and
    space opener, about a dozen std::functions - is built once and
    kept on the connection in `caller::script_interface`, keyed on
    the space it was built for and dropped when the rights change.
    And the parking apparatus is not built on the fast path any
    more: the wake key is a string and a shard lookup that a call
    finishing in its first slice never needed, so it is filled in
    only when the call actually parks.

    That wanted a release flag. The job can finish on the pool
    while the command thread is still writing the key, and a reader
    taking a half written std::string is a data race, so `can_wake`
    is published after the key and read before it. A wake that
    arrives too early does nothing and `after_blocks_registered`
    wakes again - which is what that hook was already for.

    A perf profile of the RelWithDebInfo build under memtier calling
    an empty function, before and after, says the same thing the
    timings do. Before:

        12.3%  _Sp_counted_base::_M_release
         7.8%  barch::functions::store_for
        ~5%    _Function_handler<...store_for...>::_M_manager
        ~15%   malloc / free / operator new
         1.7%  ~store_access

    After, `store_for` and its lambda managers are gone from the
    profile entirely, and what is left is:

        8.9%   _Sp_counted_base::_M_release
        ~15%   malloc / free / operator new
        4.4%   start_function
        1.0%   _Sp_counted_ptr_inplace<call_job>

    Almost none of it is Luau, the store or the ART - it is
    allocation and reference counting. The call graph says where:
    8.11% of the release traffic runs through
    `run_params -> rpc_caller::call -> functions::run`, and 5.27%
    of that is a `_Sp_counted_ptr_inplace` holding `_Function_handler`s.
    That is `call_job` being destroyed at the end of every call,
    taking the four interface objects it copied with it.

    Then the profile's own advice was taken. `call_job` held the
    interface by shared_ptr instead of copying it - one refcount
    where there had been about a dozen std::function constructions
    and as many destructions - and `key_space::canonical()` returns
    the undecorated name from a member rather than building it,
    which a call asked for twice.

                                  start     hoisted    +job
        empty fn, above PING      1.813us   1.133us    0.813us
        LUAU_GET                  3.126us   2.292us    2.085us
        LUAU_GET vs GET           3.79x     2.85x      2.60x

    So the floor is down by 55% and the ratio from 3.79x to 2.60x,
    all of it by not allocating rather than by doing anything
    cleverer. The profile agrees and has gone flat - nothing above
    4% where the first one had a 12% peak:

                            first   hoisted   now
        _Sp_counted_base    12.3%    8.9%     3.9%
        store_for            7.8%     -        -
        malloc family       ~15%    ~15%      4.0%

    What is left is spread rather than concentrated, and it is a
    different set of things:

        7.3%  resolve's existence check - sharded_store::exists,
              art::search, get_shard_index, key_space::get and
              composite::create between them. This is the store
              lookup that decides a bare name is a function, and it
              is now the largest single thing a call does.
        4.0%  Luau's collector - propagatemark, stack_init and
              friends. Visible only now that the allocation noise
              is gone; a coroutine and its garbage per call.
        4.0%  malloc and free, down from about fifteen.
        ~5%   dispatch and RESP parsing, which every command pays.

    That the biggest remaining item was *deciding the name is a
    function* rather than running it said the per-call machinery was
    about done. So the decision is cached, per connection, and only
    when it resolved:

                            start   hoist    job    resolve
        empty fn, above    1.813us 1.133us 0.813us 0.508us
        LUAU_GET           3.126us 2.292us 2.085us 1.911us
        vs GET               3.79x   2.85x   2.60x   2.41x

    Only hits are kept. A miss is looked up every time, which is
    what C insists on - a name tried before it existed has to
    resolve once it does, or SETF followed by a call on the same
    connection breaks, and functiontest asserts exactly that. The
    cache clears when the space changes, because a bare name
    resolves in its own space and then the global one, and on AUTH
    alongside the interface. Returning a pointer into it rather
    than a value took a std::function copy off the dispatch as
    well.

    So the floor is down 72% from where it started and the ratio
    from 3.79x to 2.41x, without touching Luau, the store or the
    ART - only by not building things per call that do not change
    between calls.

    A last profile says the character of the cost has changed
    completely. Summed by cause:

        5.86%  RESP parse and write - every command pays this
        5.81%  Luau coroutine and execution: stack_init,
               luau_execute, resume
        5.63%  Luau's collector: propagatemark, reallymarkobject,
               gcstep
        4.78%  malloc and free, from about fifteen
        3.35%  shared_ptr refcounts, from twelve
        0.60%  resolve's store lookup, from seven

    Nothing above 4% and no single villain. What used to be a C++
    allocation problem had become Luau's own machinery: the
    collector and coroutine creation together were 11.4%, both
    driven by making a fresh coroutine for every call.

    So they are pooled. A space keeps up to 32 spent coroutines and
    hands one back out, `lua_resetthread` putting it to empty on
    the way in and again on the way out - a thread that errored is
    holding the error object and should not keep it alive until it
    is next used. A call that yields keeps its own until it
    finishes, because `release` runs from `finish_job` and nowhere
    else.

                            resolve   pooled
        empty fn, above     0.508us   0.359us
        LUAU_GET            1.911us   1.592us
        vs GET                2.41x     2.02x

    Which lands where this started out predicting: about twice a
    GET, from 3.79x, with the fixed cost of a call down 80% from
    1.813us to 0.359us. Five changes, none of them clever - hoist
    the interface, hold it rather than copy it, cache the canonical
    name, cache the resolution hit, pool the coroutines. Luau was
    never the problem and the store was never the problem; it was
    building the same things per call that do not change between
    calls.

    The rest is honest floor. RESP parsing is what every command
    pays, and the store lookup a function does is now smaller than
    the parsing of the command that asked for it.

    What is left that is worth anything: `barch.call` at 8.16us,
    which none of this touched and which is still an order of
    magnitude worse than every other call site - a fresh
    `rpc_caller` per command whose constructor runs AUTH. One per
    function call rather than per command pays that once.

    What is left, in rough order: `resolve` builds a
    std::function closure per bare-name call, capturing a key space
    pointer and the name, and does a store lookup to decide the
    name is a function at all; `make_shared<parked>` and
    `make_shared<call_job>`; the args vector and a string per
    argument; the coroutine and its registry ref. The 1.4us first
    predicted has been passed, and 2x a GET now looks reachable.

    Two lessons rather than one:

      - a client in the measurement loop is not a constant to
        subtract. It cancels from differences and destroys ratios,
        and ratios are what get quoted.
      - barch's own per-command cost is far smaller than assumed.
        34ns of work for a GET is the number to hold in mind when
        judging what a script costs: there is very little to hide
        behind.

    What it says:

      - a function call is about 1us of fixed cost whatever it
        does. Against barch's own 34ns of work for a GET that is
        4x, not the 1.6x the client-bound measurement suggested.
      - `barch.space.x.k` costs about the same as
        `barch.store.get(k)`. The metamethod is not the expensive
        part, so the syntax that reads better is not paid for.
      - a write through the space handle is 1.67x a SET, not
        because writing is cheap but because SET is dear enough to
        hide the overhead. The heavier the command, the less a
        script costs relative to it.
      - `require` is free at call time - the same as a noop -
        because it resolved when the function was compiled. Only
        the first use in a session pays.
      - `barch.call` is the outlier at 3.22x and always has been.
        It builds an `rpc_caller` per command, whose constructor
        runs AUTH. One sub-caller per function call rather than per
        command would pay that once, and it is the only figure here
        with obvious headroom.

    The walk was the useful measurement. It came out at 3.6us a row
    with a body that only counts, which made no sense until the
    reason turned up: `page` was reading the value of every row as
    it built the page. The row object was lazy about making the
    *Luau string* while the C++ side had already done the store
    lookup, so the 3.3x the F2 benchmark predicted for a filtering
    walk was being thrown away before Luau ever saw it. Reading the
    value when a script asks for it took the row from 3.62us to
    2.26us, and a filter that skips most values now skips the
    lookups too.

    G2. Where a foreign retrieval happens, and what that means for
        a script.

    Traced because it decides what a function can reach. A foreign
    fill is a *command* level thing, not a store level one. There
    are four trigger points, all in keys_api.cpp - GET at :1864,
    EXISTS single at :2115 and many at :2121, MGET at :2246 - and
    `sharded_store` does not mention foreign anywhere. It has to be
    that way round today, because `park_or_wait` needs a caller:
    parking is `call.add_block`, and which parking it does depends
    on `call.get_context()`.

    So a script meets it twice, differently, and neither is right:

      - `barch.store.get(k)` reads through `sharded_store::search`
        and never fills. A key that lives in the source reads as
        nil, silently. That is the store layer being honest about
        what it is, and a script cannot tell that answer from an
        absent key.
      - `barch.call("GET", k)` does fill, and the way it does is an
        accident. `rpc_caller`'s default constructor sets
        `ctx_swig` (rpc_caller.h:118), so the sub-caller takes the
        SWIG branch of `park_or_wait`, which waits on a condition
        variable for `waiter_timeout_ms` - 300000 by default. That
        is five minutes of an asynchronous batch worker held on an
        external round trip, and when G's HTTP client lands it will
        be five minutes of a worker held on someone else's server.
        The instruction budget and the deadline have nothing to say
        about it.

    Both were found by writing a fake-source space and asking a
    script for a key that only exists in the source: the store read
    answered nil and the command read answered the value.

    Decided: the store interface reads the cache and does not fill.
    Ranges and iteration answer what is held locally, and that is
    deliberate rather than pending.

    It is not a compromise, for two reasons. A range over a foreign
    source would mean asking the source for a *range*, and there is
    no design for that - the whole foreign path is one key at a
    time, one query per miss, coalesced and tombed, and what a
    range even means against a SQL table or an HTTP endpoint is an
    open question rather than an implementation detail.

    And it costs nothing in consistency, which is the part worth
    checking rather than assuming. The four trigger points are all
    point lookups: GET, EXISTS single and many, MGET. No range
    shaped command has ever consulted a foreign source - RANGE,
    MIN, MAX, LB, UB and SCAN all answer from the cache today. So
    `barch.store.range` matching them is the interface agreeing
    with the commands, not falling short of them.

    Decided, and the asymmetry stays: `barch.call("GET")` fills and
    `barch.store.get` does not. That is the same split as everywhere
    else - filling is what a *command* does, and the store interface
    is the cache - so a script picks the one it means rather than
    getting one behaviour with two names. Written into the docs
    beside `barch.tomb`, which is what makes the store side usable:
    a script that reads the cache can now tell a cached miss from an
    unasked key, so choosing not to fill is an informed choice
    rather than a silent nil. See DONE 141.

    The ctx_swig hold: left as it is, and the five minutes above was
    overstated. Traced properly rather than repeated:
    `fetch_async` is given `foreign_query_timeout_ms`
    (foreign.cpp:183), and `finish_fetch` notifies `swig_cv`
    (foreign.cpp:111) whether the fetch succeeded, failed or timed
    out. So the waiter is woken when the *query* ends, and the hold
    on the worker is one query - a second by default, per space and
    settable, which is the timeout an outgoing call ought to have.
    `waiter_timeout_ms`, the five minutes, is the backstop for a
    wake that never arrives, not the normal case.

    So a script's sub-caller does take the SWIG branch of
    `park_or_wait` rather than parking (`rpc_caller` sets ctx_swig
    at rpc_caller.h:137), and that is a worker held rather than a
    session parked - but it is held for as long as the source is
    given, which is the bound that matters and is already in the
    operator's hands.

    What to watch rather than fix: the exposure is pool saturation,
    not duration. Enough concurrent foreign misses from scripts hold
    that many workers for the query timeout each. Lower
    `foreign_query_timeout_ms` on a space whose source is slow -
    that is the knob, and it is per space precisely so a slow source
    does not set the pace for everything else.

    One more thing that fell out of the same trace: `rpc_caller`'s
    constructor runs AUTH, so every `barch.call` authenticates
    `default` before running its command. A script in a loop pays
    that per call. It is not wrong, only wasteful, and a sub-caller
    that is built once per function call rather than once per
    command would not pay it at all.

    H2. Native codegen, and the interrupt that makes it pointless.

    Luau ships a native code generator - `Luau.CodeGen`, which the
    build already fetches - and the question was whether it is
    worth having and what it costs in sandboxing.

    The sandbox worry turned out to be the wrong one. Native code
    emits interrupt calls just as the interpreter does:
    `IrCmd::INTERRUPT` is lowered at loop back edges, LOP_CALL and
    LOP_RETURN, and a twenty million iteration loop fires the
    interrupt 20,000,001 times whether it is interpreted or
    native. So the instruction budget and the deadline keep
    working and a spinning script stays killable, which is the
    property that would have refused it outright.

    What it costs is that with our interrupt installed it is not
    faster. Measured on a compute heavy loop:

        interpreter, interrupt + singlestep      209 ms
        native codegen, interrupt + singlestep   217 ms   0.96x
        interpreter, interrupt, no singlestep    161 ms
        native codegen, interrupt, no singlestep  61 ms   2.64x
        interpreter, no interrupt at all         144 ms
        native codegen, no interrupt              60 ms

    Two things fall out of that table.

    The first is free and has been taken: `lua_singlestep(L, 1)`
    was costing 1.30x for nothing. It drives the *debugstep* hook,
    which neither driver sets, and turning it on forces the
    interpreter off its computed goto dispatch - the comment at
    the top of `luau_execute` says so. It does not change how
    often `interrupt` fires, which is what the budget rides on:
    identical counts either way. It was in the foreign driver and
    got copied into the function one. Gone from both.

    The second is that codegen is worth 2.64x on scripts that
    compute, and nothing at all on the ones we have. Our per call
    cost is machinery - the coroutine, the argv table, the
    Variable conversion - not bytecode, and a LUAU_GET body is a
    handful of instructions. Not adopted, for reasons that are
    about this architecture rather than Luau:

      - one state per session per space means native code would be
        compiled per session, unless `SharedCodeGenContext` and
        `SharedCodeAllocator` are used to share it. That is the
        real work in adopting it.
      - `CodeGen::create(L)` per state and a compile pass per
        function both land on the cold path, which is where a
        short lived connection spends its life.
      - it wants executable memory, which is a process level
        consideration this has not had to have before.

    Worth revisiting when F's ambition of writing real commands in
    Luau is real. A ZRANGEBYSCORE written as a script is exactly
    the case 2.64x is for.

    H3. What a script needs to compute with.

    The aim this is all pointed at: someone writing their own H3
    geo index in Luau and not paying for the choice. That is a
    compute bound script, which changes what matters - the per call
    overhead is 2 to 6us and irrelevant against real indexing work,
    while execution speed and the numeric types are everything.

    Three libraries are open now, all pure computation with no
    files, clock or network, so the sandbox is unchanged:

      - `integer`. The one that matters. Luau numbers are doubles,
        so a 64 bit identifier - an H3 cell, a hash, a snowflake -
        could otherwise only be carried as two halves through
        bit32, which is slow to run and worse to write. This is a
        native 64 bit type, `LUA_TINTEGER`, with the arithmetic,
        the bit operations, the shifts and the unsigned
        comparisons. Note both arguments to `lshift` are integers,
        not an integer and a number.
      - `buffer`. A mutable byte array with typed reads and
        writes, which is how a lookup table wants to be held
        rather than as a Luau table of boxed numbers.
      - `vector`, for the same reason on the float side.

    Opening `integer` needed the conversions to carry the width,
    and the first attempt did not: `to_variable` had no
    `LUA_TINTEGER` case at all, so a function returning one was
    refused as "not a value", and once that was added it used
    `lua_tointegerx` - the *32 bit* accessor, which silently keeps
    the low word and turned 2^62 into 0. `lua_tointeger64` is the
    one that means it, and `push_variable` pushes 64 bit the same
    way so a counter read back through barch.call keeps every bit.

    That truncation is worth remembering because of how it would
    have failed: an H3 cell keeps its mode and resolution in the
    *high* bits, so a script would have looked right at low
    resolutions and been quietly wrong where it counted.

    What is left between here and the aim:

      - codegen, per H2. 2.64x on compute, gated on
        SharedCodeGenContext so a hundred connections do not each
        compile their own copy.
      - the store interface, per F2 and 6b - an index is written by
        walking and writing keys, and the walk is not built yet.
      - nothing about the per call cost. It is already noise at
        this scale.

    G. SQL, and later HTTP.

    `sql.query` exists and is scoped to the space's driver; a
    function gets it on the same terms. The asynchronous HTTP client
    is entry 99, and when it lands the same object shows up here.
    Both are calls that can take a while, which is the next section.

    H. The slice, and what holds the worker.

    `run_ctx`, `interrupt` and `pump` in luau_driver.cpp are the
    machinery and should be lifted into something both callers share
    rather than written twice. The budget is its own setting
    (`function_script_insns`), not `foreign_script_insns`, because a
    fill and a client-invoked command are not the same risk, plus a
    wall-clock deadline as foreign has.

    The first cut registered the command with `is_asynch`, which
    ran it on the worker pool out of the asynchronous batch and
    never on a service thread - but held one worker for the whole
    script. That is gone; see I.7 for what replaced it. The proper answer is to park: pump on the
    foreign pool with requeue and wake the session when it finishes.
    A block with no key is never woken by the shard waiter registry -
    `add_block` keys on (session, key) and `call_unblock` fires from
    a write - so the completion holds the `abstract_session_ptr` and
    calls `do_block_continue` itself, resuming through
    `suspend_for_blocks` / `resume_after_blocks` so replies behind it
    in a pipeline do not overtake.

    I. Order to build it in.

      1. [done] tfunction as a key type: `ts_function` beside
         `ts_plain`, and `is_composite_lead` taught about the lead.
         That was the whole audit - see A.
      1b. [done] SETF / GETF / REMF / KEYSF in
         function_api.cpp, the `function` ACL category, and what
         KEYS / MAX / DBSIZE do now the range exists, pinned in
         test/functiontest.py. SETF compiles before it writes, so a
         script that will not run is refused rather than stored.
         The `storage_version` bump is done, and so is EXPORT -
         see below. Nothing outstanding here.
      2. [done] CALLF runs a stored function: arguments in as a
         1-based array of strings, the reply conversion in J below,
         a hard instruction cap and deadline, `is_asynch` so it
         runs on the worker pool. Covered in test/functiontest.py,
         including a spin being cut off with the connection still
         usable afterwards.

         The shortcut that was left here is gone: the budget was
         `get_foreign_script_insns()` and the deadline was the
         space's `foreign_query_timeout_ms`. They are now
         `function_slice_insns` and `function_deadline_ms`, named
         for what they are - a slice to run in, and the wall clock
         bound that is the real limit. Server settings with per
         space overrides, reported by `KSPACE OPTION GET
         FUNCTION_SLICE|FUNCTION_DEADLINE`. See DONE 135.

         (The session cache that was listed here as missing landed
         in 4.)

      2b. [done] Calling one by name. `barch::functions::resolve`
         answers for a name the built-in table missed, and
         `run_params` runs what it hands back through the
         asynchronous batch, so a script still never touches a
         service thread. `run_asynch_batch` falls back to the
         `barch_function` its context carries, since a stored
         function is not in the table to be looked up again.

         Three things worth remembering out of doing it:

           - the negative result was not the hazard it looked like.
             The cached path skips only the *lookup*; it still
             falls into the same "not a built-in" branch, so
             resolution runs every time and SETF followed by a call
             on the same connection works. The resolution is
             deliberately not cached beside `ic`.
           - function names fold. The dispatcher upper-cases a
             command name before looking it up, so a name stored as
             typed could never be found by it - `SETF greet` then
             `greet` looked for GREET and missed. Names are stored
             folded now, which also stops a space holding `greet`
             and `GREET` as two functions no client can tell apart.
             KEYSF answers the canonical form.
           - the space half of a dotted name is not folded, for the
             same reason the `space:` prefix is not.
      3. [done] Arguments in, value out, the reply conversion, the
         `function` category, and arity. The script declares its
         own arity as a global - `arity = 2` for exactly two,
         `arity = -2` for at least two, absent for anything -
         which keeps it travelling with the source instead of
         needing a second thing stored beside it. Checked before
         the script runs, so a wrong count costs nothing.
      4. [done, bar require] A `lua_State` per session and space,
         held on rpc_caller. Each function is compiled once, on a
         `luaL_sandboxthread` of its own, and pinned in the
         registry with `lua_ref`; the base globals are frozen with
         `luaL_sandbox`, so a global one function writes lands in
         its own table and the next function cannot see it. The
         source is read from the store only on a miss, so a warm
         call does not touch it.

         Two things worth knowing:

           - the cache is allocated in rpc_caller's constructor
             rather than on first use. An asynchronous call runs
             against a *copy* of the caller, and since every
             function call is asynchronous, a cache built lazily
             inside that copy would be thrown away every single
             time and never once be used.
           - it is bounded by a crude cap - 64 functions per space,
             8 spaces per session, and the state is thrown away and
             rebuilt when either is passed. C asks for LRU; this is
             what stops a roaming client holding everything open
             until it disconnects, and it is honest about being
             coarse.

         Measured against a build that compiles every call: about
         9us of the roughly 50us a round trip costs, so the cache
         is real but the wire dominates at this size. A script that
         is more than four lines long moves that number.

      4b. [done] `require("NAME")` between functions in a space,
         falling back to the globals in `configuration`, resolved
         through the same loader a call uses. What comes back is
         the required function's globals table, so a module can
         offer helpers as well as its own `call`.

         The part that changed the design while being built: SETF's
         compile check used a bare `luaL_newstate` with no
         libraries open, so `require` was nil there and *every*
         script using one was refused with "attempt to call a nil
         value". Fixed by compiling against a real state with the
         loader installed, which is what D asked for anyway, and it
         moves two failures from the first call to the write:

           - requiring something that is not there is refused by
             SETF. The price is that a function has to be stored
             after the ones it requires.
           - a cycle cannot be built at all. The first half would
             require something that is not there yet, so a cycle
             only appears through a redefinition - and that is
             refused, with the path in the message, leaving the
             previous definition standing.

         The self-require, the two-step cycle and the missing
         require are all in test/functiontest.py, along with a PING
         afterwards: a wrong answer here is a stack overflow rather
         than an error, so "the server is still there" is part of
         what is being asserted.
      5. [done] `barch.call("GET", "k")` into the ordinary commands.

         Named on a `barch` table, not as a bare `call`, because
         `call` is the name a script gives its own entry point and
         the two would collide. A refusal or a failed command is
         raised as a Lua error, so a script that wants to survive
         one wraps it in pcall - the split redis has between
         redis.call and redis.pcall, with the pcall half left to
         Luau's own.

         The sub-caller is the substance of it: `rpc_caller::call`
         clears results, errors and args on the way in, so handing
         a script the caller that is answering the client would
         destroy the reply being built. IMPORT and EXEC both solve
         that the same way and this follows them.

         What it refuses, and how each is caught:

           - a transaction, by name. MULTI on a caller nobody will
             EXEC just swallows everything after it.
           - an asynchronous command, from `is_asynch` in the
             table. It expects to own a worker and answer later,
             and there is nowhere for that answer to go.
           - a blocking command, from `has_blocks()` *after* the
             call rather than a list of names. That reads
             backwards but is right: a command that parks has not
             done anything yet, so refusing it afterwards is safe,
             and it needs no list to be kept in step with the
             blocking commands as they are added.
           - a foreign fill, which refuses itself. `park_or_wait`
             turns away ctx_valkey, and a sub-caller is one.

         ACL is checked against the *outer* caller's rights, which
         is what stops a function being a way round the check the
         connection would have failed.

         Stored functions are deliberately not reachable through
         barch.call - they are not in `functions_by_name()`. A
         function reaches another through `require`, which hands
         back its globals table and calls it in process. That split
         also means no recursion depth to bound here.

         Found while testing: the test redefined one function per
         refusal case and every case ran the first body, because
         this connection keeps what it compiled. The caching
         contract in C is real enough to trip its own test.
      6. [done] `barch.store` - get, exists, count, range,
         min, max, set, remove - and `barch.space()` for what the space is
         configured as. Reads run against the space the call is
         running in, not the one the function was defined in.

         The lock rule is what shaped it. `sharded_store::range`
         calls its callback under a shared lock, so calling into
         Luau from there would be script code running under a lock.
         The callback copies into a vector instead and the Luau
         table is built once the lock has gone. Every entry point
         is that shape.

         `range` takes a required limit, capped at 10000. An
         unbounded walk would copy a whole space into one table,
         which is the thing KEYS was made asynchronous to avoid.

         The three re-implementations F asks for are in
         test/functiontest.py - GETRANGE on store.get, COUNT on
         store.count, and a bounded KEYS-shaped walk on
         store.range, with the first checked against the built-in
         it copies. What they show is that the flat key half of the
         interface is enough.

         What was missing when they were written, and where it went:

           - containers. HRANDFIELD and ZRANGEBYSCORE could not be
             written against this at all. Both are now, in
             test/functiontest.py, through `sp:container(name)` and
             `sp:kind(name)`. The prediction that they wanted their
             own entry points rather than a flag was right, and for
             a sharper reason than the lead byte: a container's keys
             live on the shard its *name* routes to. See F2.
           - writes. This overstated it - writes landed in 6b
             through `__newindex`, and containers got set and del
             with the container work. What was actually missing was
             the symmetry: `barch.store` could read but not write
             while the space value could do both. `barch.store.set`
             and `.remove` close that. See DONE 135.
           - the locked region F describes, where a callback runs
             under a lock with yields and calls refused. Built -
             `barch.store.locked(key, fn)`, one shard or the whole
             space, with the API taught not to take a lock it
             already holds, plus `shardNumber` and `hasLock` so a
             script can see what the region will allow before it
             asks. See F6 and DONE 136.

         The space's own name is worth knowing about: the default
         space's canonical name is empty, since `undecorate("node")`
         is "" and there is no `space:` prefix for it, so a function
         there sees `barch.space().name == ""`. That is the
         vocabulary a client uses, not a gap.
      6b. [done] The space as a value - `barch.space.sp1.key1`, read,
         written and removed through `__index` and `__newindex` -
         and iteration by page copy, handing back a row object with
         container, key, value and type that decodes on demand. See
         F2, including why it is a row object rather than a triple
         and what that is worth. Per space ACLs (135) want to
         land first, or a function is a way to write any space the
         user can write at all.
      7. [done] Parking, replacing `is_asynch`.

         The script yields instead of dying. `function_interrupt`
         calls `lua_yield` when its slice is spent, so the
         instruction budget is a slice size and the *deadline* is
         what ends a runaway - the same lesson foreign learned in
         DONE 83. A call not on a coroutine has nowhere to yield
         to, so there the slice stays a cap.

         `start_function` runs the script on a coroutine, pumped on
         the foreign pool, and the command answers "parked" rather
         than a value. `call_job` owns the loader, the command
         runner and the store access, because those were built on
         the command's stack and would be dangling by the first
         yield, and it holds the states cache so a session's state
         cannot be closed under a suspended coroutine.

         The wake needed no new plumbing: `call_unblock(key)` fires
         whoever is registered under a key, which is how a foreign
         fill wakes a parked GET, so the call registers a block
         under a key nobody writes and wakes itself the same way.

         Two hazards, both handled rather than hoped about:

           - a stray write to that key would wake the call early.
             The block callback checks `finished` and re-parks
             through `retry_block()` if the script is still going.
           - the session puts blocks on their shards only after the
             command returns, so a script that finished first would
             wake nothing. `caller::after_blocks_registered` is the
             generic re-check - the same thing `foreign::kick`
             does for foreign spaces, which is where the shape came
             from.

         CALLF is no longer `is_asynch`, and the dispatcher runs a
         function inline - except when the batch is already
         asynchronous, where it has to queue or its reply overtakes
         the ones in front. test/functiontest.py pipelines a
         function with commands either side, both ways round.

         7b. [done] A short script does not park at all.

         Parking cost about 20us a call - a shard latch, a pool
         hop, a wake and a post back - which measured as the whole
         difference between a GET and a GET written in Luau:

                              PING   GET   noop fn   fn+store.get
             parking always    4.2   5.4     24.9        23.4
             short slice first 2.9   4.0      4.4         5.0

         The middle column is the finding: a function that does
         *nothing* cost 24.9us and one that read a key cost 23.4,
         so none of it was the work. It was all machinery a script
         that finishes in microseconds never needed.

         So the first slice runs on the calling thread and the call
         parks only if it actually yields. The slice is deliberately
         short - `inline_insns`, 20000 - because a full one on a
         service thread is what parking was for; this is long enough
         for a one liner and short enough that anything else parks
         almost at once.

         The fixed cost of a function call went 20.7us to 1.4us.

         What a Luau GET costs, measured by alternating it with the
         built-in inside one run so machine drift hits both - which
         matters, because comparing across runs flattered it:

             quiet machine   +2.5us   1.68x pipelined  1.08x not
             busy machine    +6.1us   2.15x pipelined  1.12x not

         The stable number is the *additive* one: a call adds
         roughly 2 to 6us whatever it does. The ratio is only a
         statement about what it is being compared with. Against a
         pipelined GET, which is 4 or 5us of real work, that is
         about 1.7 to 2.2x. Against a GET on its own round trip,
         where 30 or 40us of wire swamps it, it is 1.1x. Against
         any function that does actual work it disappears - at
         100us of logic it is 1.02x.

         So "Luau is about twice as slow" is true only for a
         function whose body is one store read *and* a client that
         pipelines. It is the wrong shape to plan with; the fixed
         couple of microseconds is the thing to keep in mind.
         Worst ping while sixteen long scripts ran went 32ms to
         0.5ms, because short work no longer queues behind them.

         Two things it left behind:

           - `barch.call` is still 14.5us against `barch.store`'s
             3.7us. That is the sub-caller: `rpc_caller`'s
             constructor runs AUTH, so every command a script calls
             authenticates `default` first. One sub-caller per
             function call rather than per command would pay it
             once, and G2 already noticed it - it is measurable now.
           - the deadline is wall clock from the start, so it
             counts time a script spends queued behind others. Under
             enough load a script fails rather than being slow:
             sixteen at once, each wanting 0.29s, and three of them
             hit the 1s deadline. That is right - it is what bounds
             a runaway, and a bound that stopped counting whenever
             the machine was busy would not bound anything - but it
             is a decision rather than an accident, so it is now
             written down where someone tuning it will find it. See
             DONE 137.

         Measured before the fast path: one script alone 0.35s,
         sixteen at once 1.00s
         rather than 5.6s, 89 pings served throughout at worst
         32ms, nothing failed. Worth being honest that this does
         not isolate parking from what `is_asynch` did - both use a
         pool of about the same size, so both predict a second.
         What parking gives that the old path did not is structural:
         the connection holds no worker while the script runs, and
         slices interleave instead of running to completion.

    J. Settled.

      - `KS2:KS1.PRINT_NAME` is allowed: one space's function run
        against another. The colon says what it runs against, the
        dot says where the definition comes from, and they are
        worth having separately - a function written against an
        interface rather than particular keys is exactly what this
        is for. Already how it behaves.
      - an evicted-then-recompiled function picking up newer source
        is acceptable. A session keeps what it compiled, but memory
        pressure is allowed to break that: it is the same source of
        truth either way, just read later.
      - arguments are varargs:

            arity = 1
            function call(key) return barch.store.get(key) end

        which reads the way a function of fixed arity should. A
        script that wants them as a table writes
        `local argv = {...}` and has both, so the host does not
        have to offer two shapes to give both.
      - a global may be redefined by a space-local function of the
        same name. The space wins; KEYSF shows what is shadowed.

      - and globals moved out of `configuration` into the default
        space. Putting code in `configuration` was a mistake: it
        holds `<name>.foreign` settings and has a job of its own,
        while the default space is where an unqualified client
        already works, so a function written there being callable
        everywhere reads as defining something at top level. The
        consequence is symmetrical rather than a gap - there is no
        way to write a function that is *only* in the default
        space, because the default space is the global namespace.

        The same mistake is still in the foreign path and predates
        this: `<name>.foreign_script` is a configuration key, and
        `load_source` takes inline Luau source when it begins with
        `--`, so foreign scripts do live in `configuration` today.
        Now that a function is an ordinary key, that wants to
        become a reference to a tfunction key instead. Not changed
        here - it is shipped behaviour with its own tests.

    K. The commands that address the range: SETF, GETF, REMF, KEYSF.

    Functions get their own commands rather than a flag on the redis
    ones. `SETEX fname "fbody" FUNCTION` and `REM keyname FUNCTION`
    would work, but they change the arity and syntax of commands a
    redis client thinks it knows, and the differential tests exist
    to catch exactly that. Keeping the two sets apart means the
    redis clones stay bit-compatible and nothing about functions has
    to be argued for in terms of what redis does.

    There is a second reason that matters more than compatibility:
    rights. SET is `{"write","keys","data"}`, so a flag on SET would
    make "may write a string" and "may define a function" the same
    permission, and a function is code. Separate commands carry a
    separate category, which is the only way that right can be
    granted on its own.

    So a `function` category is appended to `categories()` in
    barch_apis.cpp - appended, never inserted, because
    `get_category_map()` numbers them by position and
    `is_authorized` compares by index. Stored ACLs are keyed by
    name (`user:cat:<user>:<cat>`) and re-vectorised at AUTH, so a
    name added at the end costs nothing and one added in the middle
    silently reassigns everyone's rights.

    The set, all of them building the composite themselves:

      - `SETF <name> <source>` - compiles, refuses a built-in's
        name, refuses a script that will not compile, writes the
        key, bumps the space's generation.
      - `GETF <name>` - the source back.
      - `REMF <name>` - with the dependent check from D, and FORCE
        to override it.
      - `KEYSF [pattern]` - the range walk from A. Cheap because
        the functions are contiguous.

    Cats are `{"write","data","function"}` for SETF and REMF, and
    `{"read","data","function"}` for GETF and KEYSF. `data` has to
    be in there or the write never replicates - `run_params` only
    calls `repl::call` when the command `is_write() && is_data()` -
    and `data` is granted to every user anyway
    (auth_api.cpp always emplaces it), so the category that
    actually gates these is `function`.

    That also settles what to do about redis's own FUNCTION command:
    nothing. The name stays free, so a real redis-compatible
    FUNCTION - libraries, shebang, `register_function` - can be
    written later without having to unpick a barch-shaped one
    squatting on it.

    Reply conversion, settled and asserted in test/functiontest.py
    over both protocols: nil is null; a string is a bulk string; an integral
    number is an integer and anything else is a double on RESP3 and
    a bulk string on RESP2; a boolean is 1 or null, as in redis; an
    array table is an array converted the same way per element and
    nests up to eight deep; `{err = "..."}` is that error verbatim
    and `{ok = "..."}` that simple string; anything else is an
    error saying so.

    The RESP2 double is the one that surprises: `return 2.5` comes
    back as the bulk string "2.5" on a RESP2 connection and as a
    real double on RESP3, which is the rule working rather than a
    fault. Both halves are asserted, because a client that has to
    guess which it is getting cannot be written against.

    Tests follow test/foreign_luau.py, and the first of them are
    about the key range rather than about Luau: a function key
    through KEYS, SCAN and TYPE, MAX and RANDOMKEY on a space that
    has one, SET and EXPIRE into the range refused, SETF then GETF
    round-tripping, and the range surviving an export and a reload. Then: load and call, arity error,
    redefinition while a call is in flight, the instruction budget,
    the deadline, delete, the name going unknown again after delete,
    a call refused by ACL, a cycle refused at load, a global in
    `configuration` called from another space, and a function
    surviving a restart and reaching a replica.

## 146. Ordered ART GET: two failed speedups, two correctness fixes [27-08-2026]

*Was `TODO.md` entry 152.*

RelWithDebInfo perf of a 1-thread, 1-connection, pipeline-50 GET of 1M
32-byte keys (ordered) still has the same shape as the earlier 256-byte
profile. Baseline GET was 506k ops/s. Top self:

    17%  node16_v::lower_bound_child
    15%  inner_lower_bound
     8%  art::search
     7%  resolve_read_node

GET goes through `art::search`, which always does a lower-bound walk and
then checks the leaf key. `art::find` already does exact descent. Switching
search onto find looked like the obvious win.

It was not. The first version stopped at `key.length()`, which does not
include the terminating 0 that stored keys carry, so leaves that sit under
that last 0 were misses. Memtier then reported ~1200 misses/s and 574k
ops/s - faster because a miss is cheaper than a hit. After walking to
`key.size`, hits came back and GET dropped to ~430k, slower than the
lower-bound path. Search was put back.

A second try enabled unsigned SIMD (`_mm_max_epu8`) for node16
`lower_bound_child`. The old `#if 0` SIMD used signed `cmpgt` and would
have been wrong for bytes >= 128. The unsigned version was correct
(`GET k\x80` still hit) but slower: that function went from 17% to 21%
self, GET down to ~380-400k. The scalar loop is short, inlined, and often
exits early; a 16-wide compare is not cheaper here. Reverted.

Two correctness fixes stayed, neither of which is the GET hot path:

- `index_eq` treated a `memchr` miss as `NULL - keys`, a huge index.
  It now returns `occupants`.
- `art::find` walks to `key.size` so the terminating 0 can be a child hop.

The GET cost is still the lower-bound walk and the logical-address resolve
at each child, not the 16-byte key scan.

## 147. GET lower_bound on a stack path, not the heap trace list [27-08-2026]

*Was `TODO.md` entry 153.*

`art::search` used `inner_lower_bound` with a thread-local `scratch_trace`
it then threw away. The GET profile had that walk plus vector `push_back`
of fat `trace_element`s. GET only needs the leaf, and only if the key
matches.

A new `inner_lower_bound_notrace` does the same child walk, increment,
and extend, but the path is a thread-local array of 64 hops, not a
`std::vector`. Search is the only caller. Range, LB, and update still
use the heap trace.

A first cut that returned null on a non-equal byte was wrong: integer
keys share a long prefix, and finding "1" after "0" needs increment.
`expiretest.py` caught that on the first TTL. The walk has to be a real
lower_bound, just without the vector.

RelWithDebInfo, ordered, 1 thread, 1 connection, pipeline 50, 1M 32-byte
keys:

                    before     notrace
    GET             506k       418k
    populate SET    173k       131k
    GET misses/s    1.35       0.95

SET does not use search, so the 173k to 131k drop is the box, not this
function. Scaling GET by that factor lands around 550k, which is noise
rather than a win. Hits stayed ~100%. The function stays wired so the
experiment is in the tree; it is not a measured speedup.

A 60s A/B on a slower box (SET ~110k then ~102k) looked like a small
regression: notrace 395k vs heap-trace 387k. A 180s pair on the same
setup, after the box had recovered, was:

                    heap-trace   notrace
    GET 180s        556k         573k
    GET misses/s    0.56         0.57
    p50             0.079 ms     0.079 ms

Notrace is ~3% ahead, same band as the 60s pair, hits still ~100%. The
10s 506k→418k drop was load, not the trace list. Longer runs do not
show a real regression and do not show a measured win either.

A 1-thread client left the python process around half a core, so a
3-thread, 1-connection, 60s pair was run on the same RelWithDebInfo
ordered 1M 32-byte setup to push past that IO lag. Python CPU during
the heap-trace GET climbed from ~76% to ~143%. SET populate was 110k
then 112k, so the box was matched:

                    heap-trace   notrace
    GET 60s 3t/1c   1.566M       1.555M
    GET misses/s    1.57         1.57
    p50             0.087 ms     0.087 ms

That is ~0.7% the other way. Filling more of the server still does not
separate the two paths.

Same setup again with 4 clients per thread (12 connections). Python
CPU during GET was ~165% to ~369% on notrace and ~196% to ~403% on
heap-trace. SET populate was 114k then 110k:

                    heap-trace   notrace
    GET 60s 3t/4c   3.666M       3.881M
    GET misses/s    3.47         3.67
    p50             0.151 ms     0.143 ms

Notrace is ~6% ahead. SET was ~4% ahead on that side too, so most of
it is still the box. Hits stayed ~100%. At this point the extra
clients have filled several cores and the two GET paths still do not
split in a way that would justify keeping one over the other on
throughput.

## 148. Hybrid ART plus hash index [27-08-2026]

*Was `TODO.md` entry 154.*

Ordered GET walks the ART. Unordered GET is a hash of 32-bit pointers
into the data allocator, which is cheaper for a point lookup, but it
does not keep key order and it cannot fill a trace. The two indexes
used to be exclusive.

Hybrid keeps ART as the owner of every leaf and adds the same 32-bit
pointer to the overflow hash after the leaf is allocated. GET and a
same-size SET go through the hash. A SET that has to replace the leaf,
INCR, and anything else that needs a trace still walk the tree. The
hash is never the owner: leaves are not marked `is_hashed`, delete
drops the hash entry first while the leaf is still alive, and ART
frees it. Indexing after a replace was a use-after-free, because the
hash compare reads the old leaf; the index is now dropped before the
tree mutation and the new pointer is inserted afterwards.

`get_size()` counts the tree when ordered (hybrid included) and the
hash when unordered, so the index does not double-count. Turning
hybrid on rebuilds the index from the leaves; turning it off clears
the hash and leaves ART answering. Config is `hybrid_keys` (default
off), also `<space>.hybrid` and `KEYSPACE SET HYBRID`. Covered by
`test/hybridtest.py`.

RelWithDebInfo, ordered hybrid, 3 threads × 4 clients, pipeline 50,
1M 32-byte keys, 60s. Ordered ART-only on the same setup was 3.88M GET:

                    ART-only     hybrid
    GET 60s 3t/4c   3.881M       4.338M
    GET p50         0.143 ms     0.127 ms
    populate SET    114k         102k
    SET 60s 3t/4c   (not run)    1.132M
    90/10 total     (not run)    3.165M

Hybrid GET is about 12% ahead of ART-only. Hits ~100%. Python CPU on
GET climbed to ~3.6 cores, on SET to ~4.9. Populate SET is still the
ART insert plus an index write, so it did not get faster.

## 149. Hybrid keys as the default [27-08-2026]

*Was `TODO.md` entry 155.*

`hybrid_keys` now defaults to on (`configuration.h`, CONFIG `"yes"`).
The first RelWithDebInfo run was 65/67. The two failures were not
"hybrid cannot do KEYS" or "hybrid cannot report memory" - they were
the default showing up in places the earlier ART-only suite never
stressed.

TestFunctions failed because `hybridtest.py` saves keys named `"0"`
and `"1"` into the shared build directory, and `KEYS *` encodes an
integer-typed name as a RESP integer. `k.decode()` then blows up.
The function test assumed an empty space and did not flush; it does
now. Isolated, with no leftover save, it had already passed.

TestRespInfoMemory failed because `used_memory_startup` was
`min(recorded, used)` on every INFO. The recorded figure is the peak
during key-space load, which is often above what the process holds
once the temps are gone, so the min tracked `used` as the first
hybrid inserts grew the overflow hash. Redis's number does not move
after start. The baseline is now frozen the first time INFO has a
real value.

After those two, RelWithDebInfo `ctest -j1 -E 'TestInstall|TestBarchInstall'`
is 67/67. Unordered spaces, save/load, expire, range sharding,
functions, and the hybrid test itself are in that run.

## 150. Private one-shard ART for Luau [27-08-2026]

*Was `TODO.md` entry 156.*

Luau could already reach the live store through `barch.store` and
`barch.space.NAME`. Using that as a working set - an HNSW candidate
queue, any other priority queue - would persist keys, show them in
KEYS, and route through every shard.

`barch.art()` returns a space handle to a private one-shard
`key_space`: same `store_access` as a real space, so get/set/min/max
/range/count and `q[k] = v` are the same methods. It is not named, not
saved, not in the space map, and it does not call `clear_route` (that
table is global per shard number). The shard is constructed empty
(`shard(name, scratch_t)`), hybrid and ordered follow the server
defaults, and `opt_drop_on_release` deletes the arena files when the
userdata is collected. `popmin` / `popmax` are min-or-max then
remove, on every space handle.

The first constructor that skipped `start_maintain` hung in
`~key_space` on `thread_exit.wait()`. The destructor only waits if
the maintenance thread was actually started.

Covered in `test/functiontest.py`: empty, set/get/remove, numeric
order (`2` before `10`), composite distances, popmin, two trees in
one call, a second CALLF seeing nothing, range capped at 10000.

## 151. HNSW stored functions over Levenshtein [27-08-2026]

*Was `TODO.md` entry 157.*

`examples/hnsw/` is a one-shard `HNSW` space whose graph is ordinary
keys and whose walks use `barch.art()` as the candidate min-queue and
the found set. Distance is Levenshtein, so the keys are words and
short phrases.

`ADD` is already a barch command, so insert is `PUT`. The commands
are `HNSW:PUT`, `HNSW:CLOSEST`, `HNSW:TUNE`. `graph.luau` is required
by the others and has to be SETF first; `deploy.py` does that.

TUNE presets: fast (M=8, efC=16, efS=8), default (16/64/32), accurate
(32/200/100 plus the diverse-neighbour heuristic). `--demo` loaded 27
related words in 32ms and got the nearest neighbour for five typos
(`cta`→cat, `helo`→hello, and so on). `CLOSEST hello 3` was hello,
hallo, help.

## 152. Git-driven functions from a checkout [27-08-2026]

*Was `TODO.md` entry 158.*

`functions_dir` is a checkout of `.luau` files. Root files go in the
default space; each subfolder is a key space; the stem is the function
name, folded. `configuration` is skipped. SETF already refuses a bad
file; the watcher uses that as the prove step.

A sync fills a throwaway one-shard space with a multi-pass SETF so
`require` can see siblings regardless of directory order. Any failure
drops the temp space and leaves the destination alone. On success the
destination is updated as one snapshot: one-shard spaces use
begin/commit/rollback, wider spaces restore previous sources if apply
fails, then REMF names that are no longer in the tree. The managed-space
list moves only after that.

Optional `functions_git_pull` does `git fetch` + `reset --hard` with a
read-only key from `functions_git_ssh_key` (`file:` / `env:` / path).
`FUNCTIONS SYNC` runs it now; `functions_sync_ms` polls. `FUNCTIONS
STATUS` is the last result.

Covered by `test/functionsynctest.py`: load greet, refuse a broken
file without storing BROKEN, require-order AMOD/ZMOD, REMF greet,
and a function in a `hnsw/` folder.
