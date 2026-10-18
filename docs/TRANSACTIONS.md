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
- SCAN, KEYS and RANGE are refused inside a transaction;
- a full disk refuses the transaction whole.

Each piece was checked by switching it off:
- with the reads ungated, a reader saw a mix;
- with the replay ignoring the tags, the half commit came back half;
- with no read check, an increment was lost and both overlapping transactions
  committed.

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

Two changes to `merge_iterator` first:
- **A heap.** `min_current()` scans every iterator on every step: 347 shards and a few
  levels is about a thousand comparisons per key. Keyed on (key, iterator index), so a
  tie still goes to the lowest index. Then `sharded_store::range` can use it instead of
  its own heap across shards.
- **An upper bound,** so RANGE, COUNT, DBSIZE and validation's range lookups share one
  walk instead of each checking `hi`.

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

   The first keeps everything else the same and is the likely choice.
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
- A range sharded space could lock only the shards its range overlaps, a contiguous run
  the routing table already names. Today `range` takes the whole space. Doing that
  needs care with rebalancing.
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
  can't pin the stack for ever.

### Open choices

1. **What a plain write does while transactions are open.** This is the first choice
   to make, and it settles TODO 699 as well. If plain writes go straight to the base, a
   transaction reading the base sees writes made after its snapshot. Options:
   - a plain write goes into the top layer, or a one-write layer of its own, while the
     stack isn't empty;
   - plain writes show through, so transactions are read committed for those only;
   - plain writes wait, or are refused, while transactions are open.
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
6. **Raft spaces.** A version can be the group's applied index: every member pushes the
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
- TODO 697: `MULTI`/`EXEC` and stored functions on transactions.
- TODO 701: the version stack above, replacing the heap overlay, the hash read check and
  the read gate.
