# Transactions across a cluster

**Status: partly built.** TODO 671 is the series, and the design is TODO 692. The six
choices it needed were made on 17-10-2026, and are listed under *Choices made*, with
what was decided against.

TODO 693 built the first part: `BEGINTX`, `COMMITTX` and `ABORTTX` on a space with one
Raft group, taking writes only (SET, SETEX, PSETEX, MSET, DEL). Here the record and the
intents go in the same group, so steps 1 and 2 below are one entry (prepare), and step
5 is part of the commit entry rather than coming after it. Readers don't look intents
up yet: the commit makes every write under a gate that reads hold, so there's nothing
half-done for them to find. A second transaction on a held key gets `TXCONFLICT`
straight away. A quiet coordinator is found by how long its transaction has been
pending (five seconds), not by a heartbeat, since the time between prepare and commit
is short.

TODO 694 took it across a split space's groups. The home group (the lowest numbered
with a write in it) holds the decision. Each group holds its own writes as intents, and
a group left pending asks the home how it ended. Readers wait while a node has a
transaction committed in only some of its groups, rather than looking intents up. See
"Client transactions" in [CLUSTERING.md](CLUSTERING.md) for how it works and what it
leaves out, and the list at the end of this file for what comes next.

TODO 695 added reads. Inside a transaction the string commands read its own writes
first, and keep each key they read from the store with a hash of what it held. Each
group's prepare checks those (choice 3, serializable) and takes a shared lock on them.
As in the design, the check is made under the intents, but it's an answer for the
node that wrote the prepare, not a state change on every member, since a key near its
expiry can read differently on two clocks.

TODO 696 added waiting and pushing, as choice 4 says: about 50 ms, then the older
transaction wins, by a start time in the id. Choice 5's heartbeat isn't built. A holder
pending a second at its home counts as quiet and can be pushed by whoever waits on it,
and the leader's five-second abort settles one nobody waits on. A coordinator that was
only slow gets TXABORTED and runs again.

## In a space without Raft

Built in TODO 450, as choice 6 says: the same commands and the same protocol, with the
node as the only member. A transaction's writes are made in its overlay and its reads
are kept, just as in a replicated space. With one member and the coordinator right there, the
prepare and the commit are one step. `COMMITTX` holds the space's read gate alone,
checks that every key read is still as it was (`TXCONFLICT` if not), and makes the
writes. A read command holds the gate shared, so it sees the transaction whole or not
at all. A space only starts taking the gate on reads once a `BEGINTX` has run in it, so
a space that never has a transaction doesn't pay for it.

In the change log, each of the transaction's writes carries its tag, in bytes 44 to 47
of the record, which used to be reserved. After the last write comes a commit marker:
an erase of a meta key, with the tag and a marker value, synced whatever
`aof_durability` says. A replay looks for the markers first, then applies the records
in log order, skipping any tagged record whose transaction has no marker. So a crash
partway through a commit replays as if it never happened. An older barchd reads the
tag bytes as reserved and the marker as removing a key that isn't there, so it applies
every write: no atomicity, but no data lost either.

- **Saves.** No save writes part of a commit. The commit counts as a run, which a whole
  space save waits for, and as a write across shards, after which no shard saves on
  its own until the next whole save.
- **A full disk.** Before its first write, `COMMITTX` reserves room in the change log
  for every write, each one's put-back and the marker (`log::reservation`, TODO 470).
  A log that can't hold the transaction refuses it before anything changes. Without
  that, `TestTxLog` saw a commit refused partway leave its first writes in memory.
- **A write that fails partway.** The writes already made are put back as the keys
  were before, tagged as well, so a replay drops both. `COMMITTX` answers an error.
- **Replicas.** A primary's replicas get the writes one by one, as for any write.
- **The first transaction in a space.** A read that was already running when the
  space's first `BEGINTX` arrived, and is still running when that transaction
  commits, didn't take the gate, so it can see part of it. Only that first one: from
  the `BEGINTX` on, every read takes the gate.

`TestTxLocal` covers this on one barchd with its change log on:
- a reader during 30 transactions never sees part of one;
- `ABORTTX` and a closed connection leave nothing;
- two clients each run INCR 50 times and the key ends at 100, and two transactions
  that both read a key before either commits get OK and `TXCONFLICT`;
- `kill -9` after `COMMITTX` keeps all of it;
- `kill -9` halfway through a commit replays none of it;
- a write that fails partway leaves the keys as they were, before and after a restart.

`TestTxLog` covers the change log under it:
- one key set, set again and deleted over three transactions, with plain writes in
  between, comes back exactly as it was;
- 100 transactions under `aof_durability=timer`, then `kill -9`: all come back. A
  `kill -9` keeps what the kernel holds, so this shows the writes were made before
  the OK, not that the fsync was;
- a torn commit marker, or a torn write in the middle of a transaction, brings back
  none of it;
- `kill -9` while SAVE runs over and over beside committing transactions: every key
  comes back from one transaction, at or past the last OK;
- a stale read: the second read of a key gives what the first did, and the commit
  answers `TXCONFLICT`;
- write skew between two keys: one of the two commits;
- SCAN, KEYS and RANGE inside a transaction, then a key added to what they walked:
  `TXCONFLICT` (they were refused until TODO 701);
- a full disk refuses the transaction whole.

Each piece was checked by switching it off:
- with the reads ungated, a reader saw a mix;
- with the replay ignoring the tags, the half commit came back half;
- with no read check, an increment was lost and both overlapping transactions
  committed.

## Pessimistic transactions

Built in TODO 719, for spaces without Raft so far. `BEGINTX PESSIMISTIC` starts a
transaction that locks each key as it uses it, instead of finding out at `COMMITTX`
that someone else got there first. Where an ordinary transaction would answer
`TXCONFLICT` and be run again, a pessimistic one waits. That's the better trade when
many clients fight over the same few keys, like a counter: each one waits its turn
instead of most of them being thrown away.

```text
BEGINTX PESSIMISTIC
GET stock:widget           # a shared lock on stock:widget
DECRBY stock:widget 1      # now an exclusive one
COMMITTX                   # writes made, every lock let go of
```

When you read a key in order to write it, use `GETFORUPDATE` (or `MGETFORUPDATE`)
instead of GET, as in SQL's `SELECT ... FOR UPDATE`. It answers the same as GET but
takes the exclusive lock straight away, so two transactions doing the same thing to
the same key take turns instead of deadlocking (see "Deadlocks" below):

```text
BEGINTX PESSIMISTIC
GETFORUPDATE stock:widget  # an exclusive lock at once
DECRBY stock:widget 1
COMMITTX
```

How it works:
- **Locks.** A read takes a shared lock on its key and a write takes an exclusive one.
  Any number of transactions can share a lock; an exclusive one is held by one
  transaction alone. A shared lock its owner holds alone turns exclusive when it writes
  the key. Locks are held until the transaction ends (strict two phase locking), by
  `COMMITTX`, `ABORTTX` or the connection closing.
- **Waiting.** A lock someone else holds is waited for, up to `tx_lock_wait_ms`
  (5000 by default). The wait holds no shard latch, so the holder can carry on and
  commit. A RESP connection parks while it waits, the way BLPOP does (TODO 721), so its
  I/O thread goes on serving the other connections on it. A lock not got in time answers `TXLOCKTIMEOUT <key> is locked by transaction
  <id>`, and from then on the transaction can only be ended. `INFO transactions` shows
  a waiting one as `waiting=a lock on <key>`, with the holder's id in `waiting_on`, and
  each transaction's locked keys in `holds` and `held`.
- **Deadlock.** A wait that would close a ring of transactions, each waiting on the
  next, isn't started: that transaction answers `TXDEADLOCK` at once, its locks go,
  and the others go on (TODO 724). See "Deadlocks" below.
- **No snapshot.** A pessimistic transaction reads the latest committed value, which
  its lock keeps still until it ends. `COMMITTX` still checks its reads, as a safety
  net, and that check shouldn't ever fail.
- **The commands.** The string reads and writes: GET, MGET, EXISTS and STRLEN lock
  shared; SET, SETNX, SETEX, PSETEX, GETSET, APPEND, INCR, DECR, INCRBY, DECRBY, MSET
  and DEL lock exclusive. GETFORUPDATE and MGETFORUPDATE read but lock exclusive
  (TODO 729). Outside a pessimistic transaction they're just GET and MGET: an
  optimistic transaction reads its snapshot with them, and a plain one doesn't wait.
- **Walks.** RANGE, COUNT, KEYS and SCAN lock the range they read (TODO 749): RANGE
  and COUNT their `[lo, hi)`, KEYS and SCAN every key in the space. See "Range locks"
  below.
- **GRAPH** locks the whole graph, shared, and a write locks it exclusive as well
  (TODO 750). See "Graphs" below.

Writes from outside the transaction:
- **A plain string write** (the commands above) waits, holding nothing, until no
  transaction holds its keys, up to `tx_lock_wait_ms`, then answers `TXLOCKTIMEOUT`.
  It waits for a shared lock too, since the transaction that read the key counts on it
  staying as it was.
- **Any other client write** that reaches a locked key, a stored function's
  `barch.store.set` outside a transaction say, is refused at the shard with `TXLOCKED`. It can't wait there,
  since it holds the shard by then. Eviction, replays and replication never meet the
  locks.
- **An ordinary transaction** that writes a key a pessimistic one holds answers
  `TXCONFLICT` at `COMMITTX`, as if the pessimistic one had committed first.

Not yet:
- **Raft spaces** refuse `BEGINTX PESSIMISTIC`. Their transactions lock keys at the
  prepare already (TODO 696), but not as the transaction runs.
- **`barch.store`'s collections** (container get, set, remove and range) are refused
  in a pessimistic transaction, since nothing locks them yet.
- **A wait inside EXEC, or from HTTP or a run,** still waits on its thread: those can't
  park. See below.

### Range locks

Built in TODO 749. A walk in a pessimistic transaction takes a shared lock on the
range it reads, a span, before it walks, and holds it to the end like any other lock.
Without one, a key written into the range by someone else would show up the second
time the transaction walked it (a phantom), and the transaction would never know.

```text
BEGINTX PESSIMISTIC
RANGE order:100 order:200  # a shared lock on [order:100, order:200)
SET order:150 x            # from another client: waits for the COMMITTX
RANGE order:100 order:200  # the same keys as before
COMMITTX                   # the range let go of, and the SET goes on
```

How it works:
- **What's locked.** RANGE and COUNT lock `[lo, hi)`, in both places a key can be
  stored (a plain key and one with the split character in it, see TODO 260). A RANGE
  with a count or LIMIT still locks the whole of `[lo, hi)`, which is a little more than
  it read. KEYS and SCAN lock every key in the space, the same as an optimistic
  transaction counts them for its phantom check. A span takes in `hi` itself too, which
  only locks a bit more than was read.
- **In the way of writes.** A span counts as a shared lock on every key inside it,
  present or not. So an exclusive lock on one of them waits, whoever wants it: another
  pessimistic transaction's write, a plain string write, which waits up to
  `tx_lock_wait_ms` or parks, a client write that reaches the shard (`TXLOCKED`), and
  an ordinary transaction's `COMMITTX` (`TXCONFLICT`). Reads aren't held up, and the
  span's own transaction writes inside it freely.
- **Waiting for writes.** A walk waits while another transaction holds an exclusive
  lock on a key inside its range, then sees that transaction's write. A RESP
  connection parks on that key, as for a key lock.
- **Deadlocks.** A transaction waiting on a span, or a span waiting on a key, is
  followed in the ring check like any other wait, so two transactions each writing
  into the other's walked range get one `TXDEADLOCK`.
- **Letting go.** The span goes when the transaction ends. Since a span's owner never
  locked the keys inside it by name, the lock table remembers which keys someone was
  told a span holds, and wakes whoever parked on them when a span goes.

A stored function's `barch.store` locks the same way: count and range lock the
`[lo, hi)` they name, and min, max, page and size lock every key.

`TestTxRangeLock` covers this on one barchd, `tx_lock_wait_ms` 1500:
- a plain SET into a walked range waits for `COMMITTX` and goes on after it, and one
  outside the range doesn't wait; the walk run again sees the same keys;
- a plain SET waiting too long gets `TXLOCKTIMEOUT`, and `ABORTTX` lets the range go;
- another pessimistic transaction's SET into the range waits for the walker;
- an ordinary transaction writing into it gets `TXCONFLICT`, and outside it commits;
- a walk waits for another transaction's write lock inside its range, then sees the
  key, and a write lock outside it doesn't hold it up;
- two transactions each writing into the other's walked range: one `TXDEADLOCK`, the
  other goes on;
- COUNT locks its range, KEYS and SCAN lock everything;
- a stored function's write into the range: `TXLOCKED`;
- four parked writers all go on when the walker aborts.

### Graphs

Built in TODO 750. GRAPH runs inside a transaction like the string commands do, on
the transaction's overlay: it sees its own writes, nobody else does until `COMMITTX`,
and `ABORTTX` drops them. Every subcommand works, BFS and DFS included. It isn't taken
inside MULTI/EXEC, which is unchanged.

```text
BEGINTX
GRAPH MKDIR /orders
GRAPH PUT /orders/100 "two widgets"
GRAPH LS /orders           # sees both
COMMITTX                   # and now everyone does
```

In an ordinary transaction:
- **Conflicts** work as for any key. A graph is stored as plain keys (`graph:` and
  `fs:`), so a plain write into a directory the transaction listed, or two
  transactions making the same path, gives `TXCONFLICT` at `COMMITTX`. A write
  somewhere else in the graph doesn't.
- **Ids** (node, edge and inode numbers) come from the space itself, not the
  overlay. Taken from the overlay, any plain write elsewhere in the graph that moved
  the counter while the transaction was open made its commit a `TXCONFLICT`. And the
  id block cache is keyed by the space's name, which an overlay reuses from a pool,
  so one transaction could be handed ids cached by an earlier one. Ids a transaction
  took and then dropped are gaps, and that's fine.
- **The layout stamp** (the `graph:layout` meta key) is only written when it isn't
  already right. Every graph read in a transaction reads it, so writing it again on
  every commit made any graph write anywhere conflict with every transaction that
  used the graph.

In a pessimistic one, every GRAPH locks the whole graph rather than the keys it
touches. Working out the keys first would take a second walk of the same paths, and
a move or copy can reach any part of the tree. So:
- **What's locked.** First the layout stamp, shared, as a sentinel, then a span over
  each of the graph's two key regions (`graph:` and `fs:`). A write (PUT, MKDIR, LINK,
  UNLINK, RM, MV, CP) also locks the sentinel exclusive. The sentinel goes first, so a
  reader waiting for a writer doesn't take the spans and get in the way of that
  writer's own `COMMITTX`.
- **Readers share it.** Several transactions can read the graph together. A writer
  waits for them, and they wait for a writer.
- **From outside.** A plain GRAPH write waits on the sentinel like a plain SET waits
  on a key: up to `tx_lock_wait_ms`, then `TXLOCKTIMEOUT`, and a RESP connection
  parks. A plain GRAPH read doesn't wait. A plain SET into the graph's keys waits on
  the spans, and an ordinary transaction's commit into them gets `TXCONFLICT`.
- **Deadlocks.** Two transactions that both read the graph and then both write it
  form a ring, and one gets `TXDEADLOCK`.

`TestTxGraph` covers this on one barchd, `tx_lock_wait_ms` 1500:
- every subcommand inside an ordinary transaction, seen inside and not outside, and
  all there after `COMMITTX`; `ABORTTX` drops it;
- a transaction's PUT while 80 plain MKDIRs go into another directory: it commits,
  and every node and edge keeps its own id (with the ids taken from the overlay it
  failed with `TXCONFLICT`);
- a plain write into a listed directory, and two transactions making one path:
  `TXCONFLICT`;
- a plain GRAPH write waits for a pessimistic transaction, or times out; a plain read
  doesn't wait; a plain SET into `graph:` waits too;
- a pessimistic writer holds up another transaction's LS, which then sees the write;
- an ordinary transaction's GRAPH commit gets `TXCONFLICT` while a pessimistic one
  holds the graph;
- two readers that both write: one `TXDEADLOCK`;
- four parked GRAPH writers all go on after `ABORTTX`.

In a Raft space (TODO 751), an ordinary transaction's GRAPH works on the leader and
on a follower; a pessimistic transaction can't open in a Raft space at all.
- **On the leader** it's just like the above.
- **On a follower** the space's id counter can't be written: that's a meta write,
  and it answers `NOTLEADER`. Before TODO 751 that made every GRAPH write in the
  transaction answer `NOTLEADER`, and `COMMITTX` still said OK with nothing written.
  Now a member that doesn't lead every group of the space takes ids in the overlay
  after all, straight from the counter with no cached block. The counter is read
  through the transaction like any key, so if a plain write moves it before
  `COMMITTX` (any MKDIR or PUT elsewhere in the graph will, once the leader's block
  runs out), the commit is a `TXCONFLICT`, never ids handed out twice. Retry it.
  What it commits moves the counter past its ids, beyond any block the leader holds.
- **Reads outside a transaction** on a follower answer `NOTLEADER`, even with
  `CLUSTER READS FOLLOWER`. That's how plain GRAPH has always behaved, not something
  transactions brought in. A transaction's own GRAPH reads work on a follower.

`TestClusterTxGraph` covers this on three nodes, under TSan too: writes seen inside
and not outside a transaction on the leader and on a follower, and on every member
after `COMMITTX`; `ABORTTX`; 80 plain MKDIRs racing a transaction's PUT, which
commits on the leader and is OK or `TXCONFLICT` on a follower, then commits once
nothing races it, every node and edge with its own id; a plain write into a listed
directory: `TXCONFLICT`; and every member's digest the same. With the follower path
turned off, seven of its checks fail.

### Waiting without holding up the I/O thread

Built in TODO 721. Connections share the server's I/O threads, given out round robin as
they're accepted. A lock wait on one of them used to hold up every other connection on
that thread, and when the lock's holder was one of them, its COMMITTX couldn't run
until the wait timed out: a deadlock that wasn't one.

Now a RESP connection that meets a busy lock parks, using what BLPOP uses:
- The command registers a wait on the key's shard and returns. The session stops
  reading, starts a timer for what's left of `tx_lock_wait_ms`, and lets go of the
  thread. Commands pipelined behind it wait their turn, as behind BLPOP.
- Letting a lock go (COMMITTX, ABORTTX, the connection closing) wakes the first
  connection parked on that key. One that gets in passes the turn to the next, so
  several shared readers all go on; one that doesn't parks again with the time it has
  left. One that gets that key but then parks on another (MGET x y with y held, say)
  passes the turn on the first key too, so whoever waits behind it there isn't left
  asleep until its timeout (TODO 728).
- Woken, the command runs again from the top. That's safe because nothing of it has
  run yet: its locks are taken before it starts. Locks it got before the busy one are
  kept.
- A lock let go of just before the wait is registered would wake nobody, so the wait
  looks once more as it's registered.
- The timer running out answers `TXLOCKTIMEOUT`, after one last try.
- A plain string write parks the same way.

What can't park still waits on its thread: a command inside EXEC (EXEC runs its
commands in one go), an HTTP handler's session, and cron, queue or sync hook runs. HTTP
and the runs have threads of their own, so only EXEC still holds up an I/O thread.

A parked connection counts in `blocked_clients`, and `INFO transactions` shows it as
`waiting=a lock on <key>` with the holder's id and the command it's waiting to run.

`TestTxLockPark` covers this with the holder and the waiter on one I/O thread (shown
first: a plain SET inside EXEC, which still waits on its thread, holds up the holder's
PING):
- the waiter parks, the holder's PING and COMMITTX are answered at once, and the waiter
  goes on with the holder's write;
- with nobody letting go, the waiter times out after 1.5 s while the holder is answered
  all along;
- a plain SET parks and goes on at the commit;
- commands pipelined behind a parked one are answered after it, in order;
- a parked connection closed: the next waiter still gets the lock, and nothing is left
  listed or counted as blocked.

The lock table is one per space, made at its first pessimistic `BEGINTX`. A shard of a
space that never has one pays a null check per write. Keys are locked in their stored
form, so a command's key and a shard's write to it name the same lock.

`TestTxPessimistic` covers this on one barchd, `tx_lock_wait_ms` 1500:
- two pessimistic INCRs of one key: the second waits, reads the first one's write, and
  both commit, no increment lost; `INFO transactions` shows it waiting on the first;
- shared locks don't wait for each other;
- a lock not got in time: `TXLOCKTIMEOUT` naming the holder, after 1.5 s, and the
  transaction can only end; of two waiting on each other, one answers `TXDEADLOCK`
  and the other goes on;
- a plain SET waits for a key read in a transaction and goes on when it commits, or
  times out; DEL too;
- a stored function's `barch.store.set` to a held key: `TXLOCKED`;
- an ordinary transaction writing a held key: `TXCONFLICT`;
- a closed connection lets its locks go;
- KEYS, RANGE, COUNT and SCAN are taken (TODO 749, see "Range locks");
- four clients, 25 pessimistic INCR transactions each: all 100 commit, count 100.

### Deadlocks

Built in TODO 724. A deadlock is a ring: A waits for a lock B holds, B for one C
holds, and so on back to A. A ring can only close when a transaction starts to wait,
so that's when it's checked. Starting from the new waiter, the lock table follows each
holder in its way to the lock that holder waits for, and so on. Getting back to the new
waiter is a ring. This runs under the lock table's own mutex, and each wait is noted
in that table before the check, so two transactions starting to wait at the same moment
can't both miss it. There's no thread and no polling, and it costs nothing until
someone has to wait.

The transaction that would close the ring gives way:

```text
TXDEADLOCK x is locked by transaction <id1>, which waits on <id2>, which waits on this one. Its locks are let go of: ABORTTX or COMMITTX ends it
```

- It answers at once instead of waiting out `tx_lock_wait_ms`.
- Its locks go straight away, so the rest of the ring goes on without waiting for its
  ABORTTX. Its writes never commit.
- Like a timeout, it can only be ended after that.
- `tx_deadlocks` in `INFO transactions` counts them.

It covers every way a transaction waits for a lock: a parked RESP session, and the
blocking wait inside EXEC, from HTTP or in a run. Two readers of one key that both
want to write it are a ring too, since each waits for the other's shared lock
(GETFORUPDATE avoids that one). A wait
that times out is taken out of the table, so a transaction that gave up isn't part of
a ring any more. Plain writes outside a transaction hold no locks, so they're never in
one.

Only transactions in one space can be in a ring, since each space has its own lock
table and a transaction locks only in the space it began in.

`TestTxDeadlock` (test/txdeadlocktest.py, `tx_lock_wait_ms` 1500) checks:
- two in a ring: the second to wait answers TXDEADLOCK at once, and the first goes on
  and commits its write;
- three in a ring, with the message naming the others in order;
- two holding a key shared, both writing it;
- a ring closed while the other waits inside EXEC, on its I/O thread;
- a chain of waits that isn't a ring: everyone waits, then goes on in turn;
- a transaction that timed out is no longer in a ring;
- `tx_deadlocks` counts four, and nothing is left waiting.

### Workloads

Built in TODO 725. `TestTxWorkload` (test/txworkloadtest.py) runs small versions of
jobs that need serializable transactions, 8 clients at once, 60 transactions each:

| Job | What has to hold afterwards |
|---|---|
| ledger: double entry transfers, an auditor reading every balance | the total never changes, no account overdrawn, the auditor always saw the total, the journal has no gaps and replays to every balance |
| auction: bids must beat the top one, money held back, outbid bidders refunded | wallets plus top bids equal what bidders brought, each item's bids rise |
| cinema: blocks of adjacent seats booked and cancelled | no seat in two bookings, each taken seat names its booking, seats sold match |
| orders: lines from stock, priced with tax, charged to credit, gapless invoices | invoices 1 to n, each adds up to the cent, revenue and credit match them, stock is opening plus received less sold |
| warehouse: picks from several bins, stock moved between bins | no bin negative, bins hold what was there less what was picked |
| on call: a doctor goes off only while another stays on | every shift always has someone on call (write skew) |

Money is whole cents everywhere, as integers, and tax is rounded in integer arithmetic,
so a lost cent fails the check. Each client retries a transaction from the top on
TXDEADLOCK, TXLOCKTIMEOUT or TXCONFLICT, with a random backoff that doubles each try,
as an application would. `TestTxWorkloadOptimistic` runs the same jobs on plain
BEGINTX. Smaller, readable versions of the same jobs are in
[examples/transactions](../examples/transactions/README.md), and `TestTxExamples` runs
them. With `--mode none`, the same code without transactions breaks every one of
them, which shows the checks catch it. The seed is printed; `TXWORKLOAD_SEED` repeats a
run.

A thing these show: a transaction that reads a key (a shared lock) and then writes it
(exclusive) deadlocks with any other doing the same on that key, since each waits for
the other's shared lock. That's how two-phase locking works, and TXDEADLOCK sorts it
out, but under heavy contention clients need the doubling backoff. With a short fixed
pause, two ledger clients lost every one of 500 tries.

Reading those keys with GETFORUPDATE takes that kind of deadlock away (TODO 729).
`TestTxWorkloadForUpdate` runs the same jobs with `--forupdate`. With seed 7 the
deadlocks across all six jobs went from 913 to 137. What's left are transactions
locking the same keys in different orders (an order's lines, a picker's route), which
only a fixed lock order avoids. The examples sort their keys where they can, and the
ledger, shop, cinema and on-call examples run without a single retry.
`TestTxForUpdate` (test/txforupdatetest.py) checks the commands on their own: as GET
and MGET outside a pessimistic transaction, GET then SET deadlocking where
GETFORUPDATE then SET takes turns and loses no update, waiting for and holding off
shared readers, and four clients doing read-then-write increments with no deadlock.

The same jobs are in Luau too (TODO 730), in
[examples/transactions/luau](../examples/transactions/luau), written with `barch.tx`
below. `TestTxExamples` runs every example both as Python and with `--luau`.

### From Luau

Built in TODO 732. A stored function has `barch.tx`:

| call | does |
|---|---|
| `barch.tx.begin(pessimistic)` | `BEGINTX`, or `BEGINTX PESSIMISTIC` when it's true |
| `barch.tx.commit()` | `COMMITTX` |
| `barch.tx.abort()` | `ABORTTX` |
| `barch.tx.open()` | whether a transaction is open on the session |
| `barch.tx.run(fn, pessimistic, ...)` | `fn(...)` as one transaction, and what it returned |

`run` begins, calls `fn`, and commits. On TXDEADLOCK, TXLOCKTIMEOUT or TXCONFLICT it
aborts and runs `fn` again from the top, up to 10 times, then raises TXRETRY so the
client can call again after a pause (a function can't sleep). Any other error aborts
and comes back as it was. That includes a timeout, which can stop `run` before its own
abort: the CALLF fails and the server aborts the transaction `run` began (TODO 743). If `fn` ends the transaction itself, with `abort()` say,
`run` returns what `fn` returned and commits nothing. Inside a transaction someone
else opened, `run` just calls `fn` in it, and the commit and any retry are theirs.

```lua
function call(src, dst, cents)
    cents = tonumber(cents)
    return barch.tx.run(function()
        local have = tonumber(barch.store.get(src, true))
        if have < cents then barch.tx.abort() return "refused" end
        barch.store.set(src, tostring(have - cents))
        barch.store.set(dst, tostring(tonumber(barch.store.get(dst, true)) + cents))
        return "moved"
    end, true)
end
```

In a pessimistic transaction `barch.store` and a space handle lock the way the
commands do: get, fetch, exists and getBufferAt take the key's shared lock, set and
remove its exclusive one. `get(key, true)` (and `sp:get(key, true)`) is the FOR UPDATE
read, taking the exclusive lock at once. Anywhere else the `true` does nothing. count
and range lock the range they read, and min, max, page and size every key, as RANGE
and KEYS do (TODO 749). Collections are still refused there.

A client can also CALLF inside its own `BEGINTX PESSIMISTIC`. The function's reads and
writes are the client's: locked until the client's `COMMITTX`, and gone on `ABORTTX`.

A function that needs a lock someone else holds parks on it, the way a client's command
does (TODO 731), through barch.call, barch.store and a space handle. Its thread goes on
serving everything else, the holder included, and when the lock is let go of the call
runs again from the top: nothing of it had run, since the lock comes first. It waits
up to `tx_lock_wait_ms` like a command, shows in `INFO transactions` as waiting on the
lock, and passes the turn on once it gets in. Where it can't park, it waits in place as
before: a nested CALLF, a Crow HTTP handler, inside `barch.store.locked`, or a call made
from a metamethod or an iterator, which Luau can't yield through.

`TestTxFunctionPark` (test/txfunctionparktest.py) checks a client holding a key and a
CALLF wanting it 0 to 9 connections apart, so that one pair shares an I/O thread: the
holder's COMMITTX answers at once and the function goes on, through barch.call,
barch.store, a space handle and an AOT function. Also the other connections on that
thread are answered while it waits, the INFO listing, TXLOCKTIMEOUT after
`tx_lock_wait_ms`, a plain SET outside a transaction, a client closing a ring with a
parked function getting TXDEADLOCK, six functions on one key each passing the turn on,
three readers going on together, and a parked function's connection closing.

`TestTxLuau` (test/txluautest.py) checks begin, open, commit and abort; run's commit,
arguments, abort in `fn` and errors; a store write waiting for another transaction's
lock; `get(k, true)` and the space handle's version holding off readers; four
functions doing read-then-write increments, deadlocking with a plain get and not with
`get(k, true)`, no increment lost either way; barch.store.count taken, its range locked; and a CALLF inside a
client's pessimistic transaction.

## Statistics

Built in TODO 706. `INFO transactions` shows, on the node it's asked:

| Field | What it counts |
|---|---|
| `tx_live` | transactions open now |
| `tx_active` | of those, the ones running a command or their COMMITTX at this moment |
| `tx_idle` | the rest of the open ones: `tx_live` less `tx_active` |
| `tx_begins` | BEGINTXs that opened one |
| `tx_commits` | COMMITTXs that answered OK |
| `tx_aborts` | ABORTTX, a COMMITTX that didn't commit, and a connection closed with one open |
| `tx_conflicts` | of the aborts, the COMMITTXs that answered TXCONFLICT |
| `tx_deadlocks` | lock waits answered with TXDEADLOCK, each one closing a ring (TODO 724) |
| `tx_expired` | snapshots let go of for being open past `tx_max_age_ms` |
| `tx_over_limit` | transactions that went past `tx_max_keys` or `tx_max_bytes` |

`tx_live` and `tx_active` are gauges: `CONFIG RESETSTAT` leaves them. The rest count
since the last reset. They count the transactions this node coordinated: in a cluster,
the node a client sent BEGINTX to. A transaction a group's leader settles for a
coordinator that's gone (TODO 694) counts on the coordinator's node, as an abort, when
its COMMITTX answers, or not at all if it never does.

### Open transactions

Built in TODO 720. After the counters, `INFO transactions` has a line for each
transaction open on the node, the oldest first, so when something times out you can
see which one isn't moving. A session with no transaction isn't listed.

`INFO transactions waiting` (TODO 723) gives the same reply, counters and all, with
only the transactions in `state=waiting` listed, numbered from `tx0`. That's the quick
way to find a stuck one: follow each one's `waiting_on` to the transaction it waits
for. A ring of them is refused with TXDEADLOCK as it forms (see "Deadlocks"), so what
you'll find here is a chain ending at one that isn't waiting. Any word other than
`waiting` after `transactions` is refused.

```text
tx0:id=000001a15f8b5672ebcf7ad2b0026716,session=client:4,space=orders,age_ms=1503,state=committing,state_ms=1502,function=,command=COMMITTX,holds=0,held=,holds_also=every shard of orders,waiting=,waiting_on=
tx1:id=000001a15f8b5672ec1d74c8fe80b4a1,session=client:5,space=orders,age_ms=1210,state=waiting,state_ms=420,function=,command=COMMITTX,holds=0,held=,holds_also=,waiting=the shards of orders while it reads its own writes,waiting_on=000001a15f8b5672ebcf7ad2b0026716
```

| Field | What it says |
|---|---|
| `id` | the transaction's id, its start time first |
| `session` | `client:N`, where N is the `id` CLIENT LIST shows; `http` for an HTTP handler's, `run` for a cron, queue, fs source or sync hook run's |
| `space` | the space it began in |
| `age_ms` | how long since BEGINTX |
| `state` | `idle` between commands, `running` a command, `committing` in COMMITTX, or `waiting` on something another holds |
| `state_ms` | how long it's been in that state |
| `function` | the stored function it's running, while it runs |
| `command` | the command it's running: the function's own, or the one the function called |
| `holds`, `held` | how many keys it holds locked, and the first 16 of them, split by `\|` |
| `holds_also` | what else it holds, like every shard of its space while a commit makes its writes |
| `waiting`, `waiting_on` | what it's waiting for, and the id of the transaction holding it, when that's a listed one |

What a transaction holds and waits on depends on the space:
- **Pessimistic.** `holds` and `held` are the keys it has locked so far (TODO 719), and
  a wait for one says `waiting=a lock on <key>`, with the holder's id.
- **No Raft.** Nothing is held until COMMITTX, which holds every shard of the space
  while it checks the reads and makes the writes. A second COMMITTX in the space waits
  for that, and says so, with the first one's id. A read command holding the commit
  gate isn't a transaction, so a wait on one has no `waiting_on`.
- **Raft.** Once a group has taken the prepare, the transaction holds the keys it
  wrote and read there, on every member, until it commits or aborts. `holds_also`
  counts the ranges it walked, which it holds too. A prepare that finds a key another
  transaction holds waits for it (TODO 696), and `waiting_on` names that transaction.

A key or name keeps its bytes, except `,`, `=`, `|`, `\` and anything not printable,
which are written `\xHH`. redis-py reads each line as a dict, like Redis's own
`slave0:` lines. A scraper that reads every field of the section as a number has to
skip them.

INFO, PING and CLIENT don't wait for a commit's gate any more, since they read no keys.
Before, `INFO transactions` in a space with a commit under way waited for it to finish.

## What this gives you

A transaction is a group of reads and writes that a client sends as one unit. Either
all of its writes land, everywhere, or none of them do. No reader ever sees some of its
writes without the rest, and two transactions that touch the same keys can't quietly
overwrite each other's work.

This works across a whole cluster. A space split over several Raft groups has a
different leader for each group, so different nodes take writes for different keys at
the same time. A transaction can touch keys in any of those groups, and any node can
run it.

It doesn't give writes on both sides of a network partition. Every step below is a
Raft write, so the side of a partition without a majority of a group's members can't
commit anything in that group until the partition heals.

## Using it

```text
BEGINTX
SET order:17 "paid"
DECRBY stock:widget 1
GET stock:widget
COMMITTX
```

- `BEGINTX` starts a transaction on the connection. The commands after it are part of
  it until `COMMITTX` or `ABORTTX`.
- Reads inside the transaction see its own writes.
- `COMMITTX` answers `OK` when the transaction has landed. It answers an error and
  lands nothing when the transaction conflicted with another one; the client runs it
  again.
- `ABORTTX`, or the connection closing, drops the transaction. Nothing it wrote was
  ever visible.
- `BEGINTX PESSIMISTIC` locks keys as it goes and waits for them instead of conflicting,
  in a space without Raft. See [Pessimistic transactions](#pessimistic-transactions).

`BEGIN`, `COMMIT` and `ROLLBACK` stay what they are: a savepoint over a whole space,
with every client's writes going back on a `ROLLBACK`. The two don't mix: a session in a
`BEGINTX` can't run `BEGIN`.

## How a transaction commits

The pieces, in barch's terms:

- A **group** is one Raft group: a space, or one run of shards in a split space. Its
  leader is the only node that writes it.
- An **intent** is a provisional write to one key: the new value (or a delete), tagged
  with the transaction's id. It's committed through the key's group like any other
  write, so a majority of that group's members hold it. Until the transaction commits,
  an intent is invisible to everyone else and works as a lock on its key.
- The **transaction record** is one small entry in one group, normally the group of the
  transaction's first key. It holds the transaction's state: `PENDING`, `COMMITTED` or
  `ABORTED`.

A transaction's writes are kept on the node running it until `COMMITTX` (choice 2).
Then:

1. **Record.** The coordinator writes the record, `PENDING`, with a heartbeat time.
2. **Intents.** It writes an intent for each key, in each key's group, through that
   group's leader. Groups are written in parallel. If a key already has another
   transaction's intent, see *When two transactions meet*.
3. **Check the reads** (choice 3). Each key the transaction read is checked in its
   group, in the same entry as that group's intents: if it changed since it was read,
   the transaction aborts.
4. **Commit.** The coordinator changes the record from `PENDING` to `COMMITTED` in one
   Raft write. That write is the commit: from this point the transaction has happened,
   whatever fails next. `COMMITTX` answers `OK` here.
5. **Resolve.** Each group then replaces the transaction's intents with plain values, in
   one entry per group. This can lag the commit; readers handle that (below).

An abort writes `ABORTED` into the record instead, and each group drops the
transaction's intents.

## What readers see

A read outside a transaction looks at the key as it always has. If the key has an
intent, it looks up that intent's transaction:

- `COMMITTED`: it reads the intent's value. The reader may resolve that group's intents
  for the transaction while it's there.
- `PENDING` or `ABORTED`: it reads the key's committed value and ignores the intent. A
  reader doesn't wait for a pending transaction, because the value it reads is the one
  the transaction will either replace in one step or leave alone.

So a reader never sees part of a committed transaction: once the record says
`COMMITTED`, every key of it reads the new value, resolved or not.

Most keys never have an intent, and a group with no open transactions skips the
lookup.

## When two transactions meet

When a transaction's intent finds another transaction's intent on the same key:

- **The other is committed or aborted.** Resolve it, then go on.
- **The other is pending and its coordinator is alive.** Wait for it, for a short time,
  then push it (choice 4).
- **The other is pending and its coordinator has gone quiet** (no heartbeat within the
  timeout). Abort it, by changing its record from `PENDING` to `ABORTED`. That change only
  happens if the record still says `PENDING`, so a coordinator that was only slow and
  commits at the same moment wins, and the abort fails.

Two transactions each waiting on a key the other holds can't both wait forever: the
younger one (by start time) is aborted, and its client runs it again.

## Failures

- **The coordinator dies before step 4.** Its record stays `PENDING` and its heartbeat
  stops. The next transaction or reader that meets one of its intents aborts it, and the
  intents are dropped.
- **The coordinator dies after step 4.** The transaction committed. Anyone who meets one
  of its intents sees `COMMITTED` and resolves it. The client never heard `OK`; it learns
  the outcome by reading, as with any write whose answer was lost.
- **A group's leader changes mid-transaction.** Intents and the record are Raft entries,
  so the new leader has them.
- **A member restarts or copies a group.** Intents, like 666's batch state, are kept with
  each snapshot and sent with it.

## Choices made

Each was decided on 17-10-2026; the first option under each is the one taken.

**1: where intents are kept.**
- *(a) Decided: a table per group, beside the shards.* Intents are kept in memory
  per group, keyed by key, and saved and sent with each snapshot, the way TODO 666 keeps
  its batch state. The stored values are untouched until resolution, so the plain read
  path doesn't change, and a group with no intents pays nothing. Intents only live for
  the length of a commit (see choice 2), so the table stays small.
- *(b) Keys in the tree, under a lead byte of their own, in the same shard as the key.*
  They go wherever the data goes with no extra work, but every read in a shard with
  intents does a second lookup, and the walks (KEYS, SCAN, EXPORT) all have to skip them.
- A flag on the stored value would be cheapest to read, but every bit of a leaf's flags
  is taken, so it means a file format change.

**2: when intents are written.**
- *(a) Decided: at COMMITTX.* The transaction's writes are kept on its node (as TODO
  450's overlay does) and become intents only at commit. Locks are held for the
  length of the commit, not the length of the client's session, so a slow client
  blocks nobody.
- *(b) As each write is sent.* A conflict shows up at the write that causes it rather
  than at commit, but every transaction holds its locks for as long as the client takes.

**3: the isolation level.**
- *(a) Decided: serializable.* The transaction remembers what it read (each key, and
  a hash of its value or "absent"), and step 3 checks those under the intents. Two
  transactions that each `INCR` the same key can never lose an increment: one of them
  aborts and runs again. barch keeps one version of each key, so this optimistic check
  is the natural way to get it.
- *(b) Read committed.* No read check. Cheaper, but two read-then-write transactions can
  overwrite each other, which is the lost update this series is meant to stop.
- Snapshot isolation would need more than one version of a key, which barch doesn't
  keep.

**4: waiting and pushing.**
- *(a) Decided: wait briefly, then the older transaction wins.* A transaction that
  meets a pending intent waits up to a short limit (say 50 ms), then the younger of the
  two is aborted. Each transaction's start time comes from its record's group leader.
- *(b) Abort straight away.* The transaction that finds the intent aborts itself and its
  client runs it again. Simplest, but busy keys mean a lot of reruns.

**5: how a quiet coordinator is detected.**
- *(a) Decided: a heartbeat in the record.* The coordinator rewrites the record's
  heartbeat time every second while it works. A record whose heartbeat is older than
  five seconds can be aborted. Uses the record group leader's clock only, so clocks on
  different nodes don't have to agree.
- *(b) The cluster's own heartbeats.* A record names its coordinator node, and a
  transaction is abortable when that node's cluster heartbeat goes stale. No extra
  writes, but it can't tell a dead coordinator from a stuck one on a live node.

**6: spaces that aren't replicated.**
- *(a) Decided: the same protocol, with the node as the only member.* One code path:
  the record, intents and resolution are local writes, and the change log gets the
  markers TODO 450 describes so a crash replays all of a transaction or none of it.
- *(b) TODO 450's overlay design as it stands.* It's already worked out for one node,
  but it's a second way of doing the same thing.

## Next: a version stack (TODO 701)

**Status: partly built.** This is the design the user gave on 17-10-2026, written up
with the gotchas found so far. Built so far, on 17-10-2026:
- **Walks through a chain of dependent spaces** (KEYS, SCAN, RANGE, COUNT, DBSIZE)
  merge the levels: each key once, the level nearest the top winning, tombs skipped.
  A chain deeper than one level used to lose keys.
- **Commands on a dependent space** read through it. INCR and its kind, SET KEEPTTL
  and the rest start from the key beneath (`shard::copy_up`), and a second DEL of a
  deleted key answers 0. Before, INCR started from 0 and KEEPTTL lost the expiry.
- **A transaction's overlay is a temporary key space** that DEPENDS on its space, and
  its commands are the ordinary builtins run there. That replaced the heap containers
  and the rewritten commands.

Not built yet: the version stack itself (layers, locks, snapshots, validation against
layers, squashing, merging, limits), iteration inside a transaction, and Raft spaces
on the stack.

### Chosen shape: before-images (20-10-2026)

The model below puts committed versions in layers *above* the base. The shape chosen to
build keeps the base always current and stores *old* versions instead, an undo log:

- **Writes go to the base, as now.** A commit, and a plain write, change the base
  directly. While any transaction is open on the space, the value a key held just
  before a change (or that it was missing) is first saved into the **before-image
  layer** of the current version, once per key per version.
- **Versions.** The space counts snapshots: BEGINTX takes the current version as its
  snapshot and starts the next one. So layer v holds what keys were before their first
  change after snapshot v was taken.
- **A transaction's read** looks in its overlay, then in the layers from its snapshot
  upward, oldest first (the first one holding the key has its value as of the
  snapshot), then in the base.
- **Validation** is the same as below: a key or a range the transaction read conflicts
  if any layer from its snapshot up holds a key there.
- **Dropping.** A layer older than every open snapshot is dropped. Nothing ever merges
  into the base.
- **Plain readers** read the base and never look at layers, so they pay nothing. The
  read gate stays, so a plain reader still sees a commit whole.

It serves open choice 1 (plain writes go into an open top layer) the other way up:
while transactions are open, a plain write is versioned by its before-image.

**Built, 20-10-2026, in spaces without Raft:** snapshots and the reads from them.
- `tx_versions` (src/tx_versions.h) is the space's: its version counter, the open
  snapshots and a layer per version. A layer is a space shaped like an overlay (the
  base's shard count and key split) with no source. `INFO transactions` shows how many
  there are as `tx_layers`.
- BEGINTX takes a snapshot holding `tx_gate` shared, so never halfway through a commit.
  ABORTTX, COMMITTX once its writes are in, and a session that goes let it go, and a
  layer older than every open snapshot goes then.
- A shard's insert, update and remove (`shard::version_prior`) save the key's leaf, or a
  tomb for nothing, in the current layer before changing it, while any snapshot is open,
  once per key per layer. A commit's writes go through the same three, so they're saved
  too. The value is kept plain, since a layer has no dictionary.
- A read in a transaction that falls through to the base (`shard::through_source`) asks
  the layers from its snapshot up first. What it finds is kept in the overlay as a first
  read is.

**Built, 20-10-2026: validation against the layers**, in spaces without Raft.
- COMMITTX checks each key the transaction read against the layers from its snapshot
  up: a key in one has changed since, and it answers TXCONFLICT. That replaced the hash
  of each value read, compared with the base, for these spaces. The hash is still kept,
  for COMMITTX to skip a key it only read.
- A clear or a load of a shard (FLUSHDB, LOAD) isn't a per-key write and leaves nothing
  in a layer, so the space counts them, and a snapshot taken before one conflicts.
- **A transaction that only reads commits** without a check: it read one moment's store.
- **The commit holds every shard of the space** for writing, from the check to its last
  write. Plain writes don't take `tx_gate`, so one could land on a key the transaction
  read after the check and before the transaction's own write there, and be lost. That
  was so with the hash check too, since TODO 450: TestTxSnapshot's INCRs from
  transactions and plain clients at once lost one or two in 750 without the hold.

**Built, 20-10-2026: walks in a transaction**, in spaces without Raft. RANGE, COUNT,
KEYS and SCAN are taken. Each shard's levels for a walk (`levels_of` in
sharded_store.cpp, and the chain in `shard::glob`) are its overlay, the snapshot's
layers oldest first, then the space, so on equal keys the overlay wins, then the key as
the snapshot saw it. A walk holds the space's shards, which keeps the layers still: KEYS,
which walks unlocked outside a transaction, holds each shard for reading inside one.
Each walk is recorded for COMMITTX: RANGE and COUNT their [lo, hi), KEYS and SCAN the
whole space. A key in a layer from the snapshot up, inside a range walked, is a phantom,
and COMMITTX answers `TXCONFLICT`. MULTI/EXEC still runs a batch with a walk in it the
old way.

**Built, 20-10-2026: Raft spaces.** A transaction's snapshot is the node it runs on's
own, its applied state at BEGINTX, and the node fills its layers as it applies the log,
since every apply goes through a shard's insert and remove. COMMITTX checks those
layers first, for an early `TXCONFLICT`; the groups' prepares decide. A key read is
checked by the hash of its value as the snapshot saw it, as before, and a range walked
by a hash of each shard's keys in it, which the shard's group compares with its own
and holds as a range lock while the transaction is pending. So open choice 6 came out
simpler than planned: a snapshot is a version on one node, not a group's applied index,
and nothing about versions goes through the log. Details in docs/CLUSTERING.md,
"Snapshots and walks". A Raft batch (a staged MSET) is applied record by record without
the gate, so a snapshot, like a plain read, can catch half of one; the prepare's hash
check then answers `TXCONFLICT` for a transaction that read it.

**Built, 20-10-2026: a script's range reads.** Inside a transaction `barch.store.range`,
`count`, `min` and `max`, and a page walk (`for row in barch.space[name]`), go through
the overlay as RANGE and COUNT do, so they read the snapshot with the transaction's own
writes on top, and each is logged for the phantom check. `min` and `max` see through the
chain: the top level holding a key decides, and a tomb there means it isn't there. `min`
counts as a walk of everything up to and including the key it found, `max` of everything
from it on, and either one that finds nothing counts as a walk of the whole space. A
script's `max` is always a function, since functions sort above every key, so in
practice it only conflicts with a SETF above it. A collection's member walk is still
refused. A function that joins a transaction runs on a follower too, since its writes
go to the overlay.

**Built, 21-10-2026: a script's container reads (TODO 713).** Inside a transaction a
script's `kind` and a member read (`barch.space[name]:container(c)[m]`) see through the
overlay to the snapshot and the space beneath. Before this they only looked at the
overlay, so a hash made before BEGINTX read as nil. A tomb in the overlay means the member
is gone, and a DEL of a collection in a transaction now tombs every member, not just the
ones it had read. A member read is logged like any key. A `kind` that finds the collection
logs a read of the first member it found there, so COMMITTX refuses it if that member
changed or went. A `kind` that finds nothing logs the collection's whole range, so it's
refused if one has been made under that name since. The commands' own type checks
(WRONGTYPE) see through the same way but don't log, since that would count three ranges
against `tx_max_keys` for each new key a transaction writes.

**Built, 20-10-2026: the limits.** `tx_max_keys` (100000) and `tx_max_bytes` (64 MB)
bound what a transaction holds: the keys it has written, deleted or read, each counted
once, and the bytes its overlay and its read log take. `tx_max_age_ms` (60000) bounds
how long it stays open. 0 turns a limit off. The command that takes it past a size
limit answers `TXABORTED`, naming the setting and how much it holds, and from then on
it can only be ended: every command in it but ABORTTX and COMMITTX answers the same,
and COMMITTX aborts it. Nothing of it is made. A GET writes its value straight to the
reply, so a GET that goes past a limit still answers, and the command after it says
so. A script's `barch.store` call that goes past one fails the script.

The age is checked at each command, and each space's maintenance thread also lets go
of a snapshot older than `tx_max_age_ms`, so the layers it held are freed while its
session sits idle. That's the point of the limit: a forgotten session can't keep the
layers growing. COMMITTX claims its snapshot before it checks against the layers, so
the maintenance thread can't take it away halfway through a commit. A command that
was already running when its snapshot expired may have read layers that were being
freed, but that transaction can't commit. `INFO transactions` counts `tx_expired`,
the snapshots let go for age, and `tx_over_limit`, the transactions that went past a
size limit.

Expiry and eviction don't leave a before-image: a key that expires after a
transaction read it doesn't make it conflict, as an expiring key doesn't trip a WATCH
in redis.

### Why

What TODO 693 to 695 built kept a transaction's own writes, the keys it read and its
gathered records in heap containers. That had three problems, the first two now
fixed by the overlay:

- **Memory.** Nothing counts it against `max_memory`, so a large transaction grows
  until barchd is OOM-killed. barch keeps temporaries in key spaces for exactly this
  reason (src/scratch_container.h, TODO 597).
- **It rebuilds what a key space already is.** `tx_value` is a value, whether it's
  there and its expiry, which is what a leaf already holds, along with compression and
  memory accounting.
- **It can't iterate.** SCAN, KEYS and RANGE are refused inside a transaction, because
  there's nothing to merge a walk of the store with.

### The model

- **Base and layers.** A space has its **base**, the store as it is now, and a
  **stack of layers** above it. A layer is a temporary key space holding one committed
  transaction's writes: a live leaf for a set, a leaf with the tomb flag for a delete.
  Each layer has a version, and newer layers are higher in the stack.
- **Tombs.** The tomb flag today means a cached miss from a foreign source. A layer
  never has a foreign source, so there it means "deleted at this version", and readers
  skip it.
- **Overlays.** A transaction's own writes go into its **overlay**, a temporary key
  space of its own, in the same form.
- **BEGINTX locks the top version.** Each version has a **lock counter**, and BEGINTX
  adds one to the top's. That's the transaction's snapshot: it sees the store as if
  the stack had been merged up to that version, and nothing committed after it.
- **COMMITTX pushes a layer.** If the transaction may commit (see *Validation*), its
  overlay becomes a new layer on top, in one step. COMMITTX and ABORTTX then unlock
  (minus one) the version the transaction locked. A closed connection must unlock too.
- **Merging into the base.** The bottom layer merges into the base only when no
  transaction holds a lock below or at it. Merging earlier would change what an older
  snapshot reads. Layers go down in order, oldest first.

### Reads

- **A point read** in a transaction looks in its overlay, then in each layer from its
  locked version down, then in the base. The first leaf it finds is the answer, and a
  tomb means the key isn't there.
- **A point read outside a transaction** does the same from the top of the stack. With
  no layers, that's the base alone, so a space with no open transactions pays nothing.
- **An iterator** (SCAN, KEYS, RANGE) takes the range from the base, then supplements
  it from each visible layer and the overlay with `lower_bound`, in key order:
  - a key only in the base is yielded;
  - a key only in a layer is new, and is yielded unless it's a tomb;
  - on equal keys the newer version wins, or is skipped if it's a tomb.
- **Shards.** A one-shard layer over a space of many shards works where the space's
  keys are ordered, since the base's walk is already in key order. Where they're hashed,
  each base shard is merged with the layer's keys that route to it, or the layers take
  the space's shard count (DEPENDS already needs equal counts).
- **Expiry.** A value that has expired in a layer reads as deleted there. It doesn't
  fall through to an older version beneath.

### One walk over shards and versions

`art::merge_iterator` merges any number of ordered iterators. On equal keys it hands
out the first iterator's and skips the rest. So one merge can cover every shard and
every visible version at once: one iterator per (shard, level), ordered level-major,
with every shard of the top layer first, then the next layer's, down to the base. A key
lives in one shard of a hashed space, so equal keys only come from different versions
of the same shard, and the newest wins. The caller skips tombs, as RANGE does. A
transaction's list stops at its locked version, so it walks its snapshot.

Two changes to `merge_iterator` first, both made on 20-10-2026:
- **A heap.** `min_current()` scans every iterator on every step: 347 shards and a few
  levels is about a thousand comparisons per key. Keyed on (key, iterator index), so a
  tie still goes to the lowest index. Then `sharded_store::range` can use it instead of
  its own heap across shards.
- **An upper bound,** so RANGE, COUNT, DBSIZE and validation's range lookups share one
  walk instead of each checking `hi`.

RANGE uses it now: one merge over every shard and every level of its chain, level-major.

RANGE's offset path (`select_start`) skips ahead by per-shard counts, which don't hold
through layers. Over layers it walks instead, as it already does for a space with a
source.

What the walk can assume depends on how the space is laid out:

1. **Hashed shards, ordered keys** (the default): the one merge above.
2. **Range sharded.** The shards split the key order, so there's no merge across shards:
   walk the shards in order and merge the levels within each. But the boundaries move as
   the space runs, so a layer's shard N stops lining up with the base's, which DEPENDS
   relies on. Options:
   - pause rebalancing while any overlay or layer exists, with layers routing through
     the base's table;
   - layers of one shard, looked up by key rather than by shard number;
   - refuse transactions there, as BEGINTX does now.

   The first keeps everything else the same and is the likely choice. DEPENDS does it
   now (TODO 716): the dependent takes the base's table at DEPENDS rather than routing
   through it, which comes to the same thing while the base holds still. BEGINTX still
   refuses a range sharded space.
3. **Unordered keys** (a hash index, no tree). There's no order to merge on.
   - Point reads on that path don't read through to a source at all, which is a gap
     in DEPENDS of its own.
   - A walk would use the shadow check SCAN uses: each level's keys, less any a higher
     level holds. That's a lookup per key per level above, in any order.

   Until then, BEGINTX refuses these too.

**Locks.** A walk holds every shard of the space (`ks_shared`), and each shard's lock
takes its whole source chain. So nothing moves under it, a value looked up again
inside it matches what the walk saw, and no layer can be pushed or merged until it's
done. A long SCAN or RANGE holds up commits and merges the way it already holds up
writers. The cost is a lock per shard per level, before the first key: with the
default shard count, hundreds. That matters when choosing shard counts:
- A space where range reads are common wants fewer shards, trading write parallelism
  for cheaper walks. That could be a per-space default, or guidance in the docs.
- A range sharded space locks only the shards its range overlaps, a contiguous run
  the routing table names, checked again under the locks (TODO 703). That's for a space
  with no source. A range sharded space can depend on another range sharded one: the
  dependent takes the source's boundaries, and the source stops rebalancing while it has
  dependents (TODO 716). A range read through such a dependent still holds every shard;
  the levels share boundaries now, so it could hold only its run of each.
- Squashing layers keeps the levels, and so the locks, down.

**Hybrid keys** have the tree as well as the hash index, so they walk like case 1.
Overlays and layers have no hybrid index of their own: a transaction's first reads are
kept straight in the tree.

### Validation (serializable)

At COMMITTX, a transaction checks the layers committed after its locked version
against what it read:

- **keys it read:** a point lookup in each of those layers;
- **ranges it scanned:** `lower_bound` into each of those layers over the range.

Any hit means another transaction changed what it saw, and it answers `TXCONFLICT`.
That catches phantoms exactly, and replaces the value hashes and the rescans. Writes
it shares with a later layer conflict the same way.

### Keeping the stack short

A long transaction pins every layer above its version, so it can't merge. That's
inherent to this design. The work goes into making version resolution cheap and
bounding the size of a transaction.

- **Squash between locks.** A layer only needs to exist on its own if a transaction
  is locked at its version. Adjacent layers with no lock between them are squashed
  into one, the newer winning and its tombs kept. They're squashed into each other, not
  into the base. Then the stack is never deeper than the number of distinct open
  snapshots plus one.
- **Skip layers on point reads.** Each layer keeps a filter of its keys (as shards have
  bloom filters) and its lowest and highest key. A point read skips a layer that can't
  hold the key, so a key no open transaction touched goes straight to the base.
- **Skip layers in iterators.** A scan only supplements from layers whose key range
  overlaps it.
- **Maybe later: one index for the stack**, from each key in an unmerged layer to its
  newest version, if measurements show the per-layer filters aren't enough.
- **Size limits, on by default.** `tx_max_keys` and `tx_max_bytes`: a write past them
  is refused inside the transaction, with an error that names the setting.
  `tx_max_age`: an open transaction older than this is aborted, so a forgotten session
  can't pin the stack for ever. Built as `tx_max_age_ms`, and the command past a size
  limit leaves the transaction able only to end, since its writes are already in the
  overlay; see "Built, 20-10-2026: the limits".

### Open choices

1. **What a plain write does while transactions are open. Chosen 20-10-2026: into an
   open top layer.** It settles TODO 699 as well. If plain writes went straight to the
   base, a transaction reading the base would see writes made after its snapshot, and
   validation, which only looks at layers committed after the snapshot, would miss them.
   So while any transaction is open on a space, a plain write goes into the top layer
   while nobody has locked it, and into a new one above it once BEGINTX has. Squashing
   keeps them few. With no transaction open, plain writes go to the base as now, so a
   space that doesn't use transactions pays nothing. The others weighed were plain
   writes showing through (read committed for those), and plain writes waiting, or
   being refused, while transactions are open.
2. **Saves.** The shard files hold the base. A save while layers are unmerged either
   writes the merged view, or the change log's checkpoint mustn't cover the records of
   layers not yet in the base.
3. **The change log.** A commit logs its writes with the tag and marker, as now
   (TODO 450). A replay rebuilds the base directly, which is fine because there are no
   snapshots after a restart.
4. **Merging while reading.** Only layers below every lock merge, so a reader that sees
   a key in both the layer and the base sees the same value in both. A layer's tombs
   have to be applied to the base before the layer goes, or a reader could see the key
   come back.
5. **Disconnects and caller copies.** The lock has to be released however a session
   ends, including through the copies a Raft write runs on (TODO 626).
6. **Raft spaces. Built 20-10-2026, not this way**: a snapshot is the node's own (see
   "Built, 20-10-2026: Raft spaces" above). The plan was: a version can be the group's applied index: every member pushes the
   same layer at the same index, so a snapshot means the same thing everywhere. A split
   space has a stack per group, so a transaction's snapshot is a version in each of
   its groups.
7. **What it replaces.** `tx_session`'s overlay and read set, the hash read check, and
   the read gate (`tx_gate` and `tx_partial`): a commit is visible all at once when its
   layer is pushed. The intents and records that the Raft prepare and commit carry
   stay as they are.

## The order it's built in

- TODO 693 (built): one group, writes only. The record, intents at commit, resolution, readers
  that check intents.
- TODO 694 (built): across a split space's groups, and coordinator failure.
- TODO 695 (built): reads and the read check, so `INCR` in two transactions never loses one.
- TODO 696 (built): waiting, pushing and deadlock.
- TODO 450 (built): spaces with no Raft.
- TODO 697 (built): stored functions on transactions (TODO 705, 707 to 709), and `MULTI`/`EXEC`
  (TODO 710).
- TODO 701: the version stack above, replacing the heap overlay, the hash read check and
  the read gate.
