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
