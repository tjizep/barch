# Finish the Z* APIs against valkey's tests

Status as of 22-08-2026: phases 1 and 2 are done (DONE 107 and 108).
zset.tcl is 114 of 168 translated. The differential agrees on 287
faithful cases. Phases 3 and 4 are what is left.

---

zset.tcl is the bulk of the remaining ordered-set work. 105 of 168
cases already translated when this was written; 63 were stubs. The
differential agreed on every faithful case. The leftover is three
kinds of thing: commands we still do not have, tcl the translator
still will not read, and tests that need a second connection or
valkey internals.

Valkey itself lists 32 sorted-set commands (the json under
`src/commands/z*.json`). We register 29. Still missing, or skipped
on purpose:

- **ZREVRANK** — ZRANK from the high end. The basics test is a stub
  only because of `readraw` / `$nullres`.
- **ZREMRANGEBYRANK** — remove by index. The basics test is a stub
  because of an inner `proc`.
- **ZRANGESTORE** — copy a ZRANGE into another key. Several cases
  already translate and sit in ACCEPTED as "not implemented".
- **BZPOPMIN / BZPOPMAX** — older blocking pop. BZMPOP exists; these
  names do not. Clients still send them.
- **ZSCAN** — already decided against (DONE 57). ZRANGE pages without
  a cursor. Leave it.

geo.tcl and scan.tcl mention Z* only as setup for GEO and ZSCAN.
Neither is a new zset suite. There is no second valkey zset unit
file.

## What we will not chase

These stay stubs, same reasons as before:

- `assert_encoding`, listpack / skiplist config, `debug reload`
- `randomInt` fuzz, 100-range fuzzy tests, skiplist backlink stress
- replication streams (`assert_replication_stream`)
- MULTI/EXEC isolation (TODO 63)
- RESP3 `hello 3` and `readraw` protocol dumps (redis-py cannot parse
  the map)
- ZUNIONSTORE/ZINTERSTORE with SADD sets (no set type, already
  ACCEPTED)
- `valkey_deferring_client` + `wait_for_blocked_client` until the
  harness can hold a second connection

## Phase 1 — missing rank, remove-by-rank, and store

Done. See DONE 107.

All three reuse walks we already have. Files: `src/ordered_api.cpp`,
`src/ordered_api.h`, `test/valkeytrans/differential.py` (drop
`ZRANGESTORE*` from ACCEPTED).

**ZREVRANK** is ZRANK counted from the high end: `card - 1 - rank`,
nil when the member is missing, optional WITHSCORE like ZRANK already
has.

**ZREMRANGEBYRANK** is the index slice `zrange_by_index` already
builds, then delete those members through both keys, the score key
and the member index, the way ZREM does. Negative indexes are the
same rule as ZRANGE. Empty set or inverted range answers 0.

**ZRANGESTORE** is ZRANGE collected into memory, then written to dest
under `with_two_keys_write` (same lock order as LMOVE). Reply is the
number stored. WITHSCORES is a syntax error. LIMIT without BYSCORE
or BYLEX is a syntax error. An empty range must not create dest
(`EXISTS` 0). Wrong-type src is WRONGTYPE and must leave dest alone.
Dest replaces whatever was there.

After this, the already-translated ZRANGESTORE cases come off
ACCEPTED. BYLEX/BYSCORE store tests still stub on `assert_encoding`;
the command itself will be covered by the ones that already run.

Settle when TestValkeyDifferential still agrees, ZRANGESTORE is gone
from ACCEPTED, and a missing dest after an empty store is 0 on both
servers.

## Phase 2 — translator expansions that unlock existing commands

Done. See DONE 108.

No new Z* names. Teach `translate.py` a few more regular shapes,
regenerate `zset.json`, let valkey drop anything unfaithful.

1. **`assert {$retval == 2}` / `== 1.0`** after `set retval [r cmd]`.
   Unlocks "ZADD XX returns the number actually added" and "ZINCRBY
   return value".
2. **`catch {r ...} e` + `assert_match {*ERR*...*} $e`**, optionally
   with a last command (`exists`). Unlocks the two variadic ZADD
   parse-error tests, ZINCRBY-is-not-variadic, ZMSCORE missing
   members, ZRANGE / ZRANGESTORE invalid syntax.
3. **`create_long_zset KEY N`** for small N (the tcl uses 30). Expand
   to one DEL + one ZADD of `0 i0 … (N-1) i(N-1)`. Unlocks
   "ZRANGEBYSCORE with LIMIT", whose first half is already ordinary
   asserts.
4. **`assert_match` after catch** is the same as (2). Do not add
   `foreach` over encodings.

Leave inner `proc remrangebyscore` / `remrangebyrank` /
`remrangebylex` for phase 4. Those bodies are `proc` plus many
calls; flattening them is a bigger translator change than
implementing the command.

Settle when those stubs become translated, valkey trusts the ones
that should pass, and the differential still agrees.

## Phase 3 — BZPOPMIN / BZPOPMAX, and ZPOP's leftover index

BZMPOP already blocks, parses a timeout, and pops MIN/MAX with
COUNT. BZPOPMIN is `BZPOPMIN key [key ...] timeout` and answers
`{key member score}` rather than BZMPOP's nested array. Implement as
a thin parse in front of the same waiter ZADD already wakes
(`call_unblock`).

Same for BZPOPMAX.

While there: **ZPOPMIN/ZPOPMAX still do not delete the member
index** (`ordered_api.cpp` says so next to ZMPOP). ZREM does both
keys. A translated ZPOP test that ZADDs the same member again will
see a stale index. Fix the pop to use the same dual remove as ZREM
before adding more pop cases.

Non-blocking ZPOP tests are wrapped in `foreach {ZPOPMIN ZPOPMAX
ZMPOP_MIN …}` and `verify_zpop_response`. Expanding that foreach in
the translator is optional and only worth it after the index bug is
gone.

Settle when `BZPOPMIN z 1` on a missing key times out the same way
valkey does, and ZPOPMIN of the last member leaves no index key
behind.

## Phase 4 — remrange helpers and blocking (only if 1–3 still look thin)

**Remrange basics.** Three stubs, all "inner proc + create_zset +
command". Either:

- inline that proc shape in the translator (one-command helper, no
  loops), or
- write the steps into JSON by hand. Three reviewable cases, not
  665.

Prefer inlining if the helper is literally `create_zset; r
zremrange*;`. Hand JSON if it fights the translator.

**Blocking.** `valkey_deferring_client` tests (same key twice,
ZADD+DEL should not wake, multiple BZMPOP waiters) need a second
redis-py connection in `differential.py`. That is harness work, not
a Z* command. Do it after BZPOPMIN exists, as its own step.

## Order, and what "done" looks like

Do 1, then 2, then 3. 4 only if we still want those remrange/blocking
names in the faithful set.

Each phase:

- open one TODO.md entry before edits
- RelWithDebInfo build, pip install, `TestValkeyDifferential`
- close into DONE.md with the actual faithful count, not the
  predicted one

When this was written we were at 303 translated / 279 faithful.
Phase 1+2 should move zset stubs by a dozen or more without touching
blocking or encodings. The ACCEPTED list should shrink (ZRANGESTORE
goes), not grow.
