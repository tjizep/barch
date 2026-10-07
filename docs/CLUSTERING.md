# Clustering and Raft replication

**Status: phases 1 to 3 are built (TODO 610, 611, 612); the rest is a design
proposal.** That's the `cluster` space, the `CLUSTER` command, any number of key
spaces replicated with Raft and led by different nodes, snapshots, members that
join as learners, and copies between members whose page layouts differ. It's only in a
build configured with `-DBARCH_CLUSTER=ON`. Sections that describe later phases say
so, and their command names and replies may still change. See
[What phase 1 does](#what-phase-1-does) for what you can use now.

This page describes how BARCH nodes could form a cluster. A `cluster` key space
records the members and how far each one has got. Each key space opts into Raft
through the configuration space, and NuRaft keeps the members of that space in
step. A routing table built from the `cluster` space sends each command to a node
that can answer it. The goal is read after write consistency: once a write has
been acknowledged, any read routed through the cluster sees it.

## Goals

- Any key space can be replicated with Raft by setting one option, and every
  acknowledged write to it survives the loss of any minority of its members.
- The members of the cluster, when each was last seen, and how far each one has
  got in each key space, can be read like any other keys.
- A new node can join while the cluster takes writes, copy the data without
  stopping writers, and catch up from the log.
- A client that writes a key and then reads it sees its own write, whichever node
  it reads from.
- Async replication with `PUBLISH`/`PULL` keeps working for embedded instances,
  remote sites and existing setups.

## What phase 1 does

Build with `-DBARCH_CLUSTER=ON`, and give every node a `raft_port` and the
`external_host` the others reach it at. Each Raft group a node is in listens on
`raft_port` plus the group's number, so leave room above it.

```text
barchd --port 14000 --dir /data/n1 -c raft_port=15100 -c external_host=10.0.0.1
barchd --port 14000 --dir /data/n2 -c raft_port=15100 -c external_host=10.0.0.2
barchd --port 14000 --dir /data/n3 -c raft_port=15100 -c external_host=10.0.0.3
```

Then, from any client:

```text
# on the first node: start a cluster of one
CLUSTER INIT
# on each of the others: join it, through any member
CLUSTER JOIN 10.0.0.1 14000
# on the leader of the cluster group: replicate a space
USE configuration
SET orders.raft on
```

| Command | Does | Replies |
|---|---|---|
| `CLUSTER INIT` | starts a cluster with this node as its only member | `OK` |
| `CLUSTER JOIN <host> <port>` | joins the cluster that node is in. A member that isn't the leader answers with the leader's address, which `JOIN` follows | `OK` |
| `CLUSTER REMOVE <node-id>` | on the leader of the cluster group: takes a node out of the cluster, and then out of every space's group | `OK` |
| `CLUSTER INFO` | this node's id, `num`, addresses, and for each group it's in: its spaces, leader, term, commit and applied index, members | one line per item |
| `CLUSTER DIGEST <space>` | a key count and checksum of this node's own copy of a space, for comparing members | two lines |

`CLUSTER LAYOUT` answers this node's layout tag. `CLUSTER RESERVE`, `CLUSTER ADMIT`,
`CLUSTER EXPORT` and `CLUSTER HEARTBEAT` are what nodes send each
other. `CLUSTER` needs the `dangerous` category, and a node joining calls the
leader as the `default` user.

Reads and writes of a replicated space go to the leader of its group. Anywhere else
they get one of these errors:

| Error | Means | What to do |
|---|---|---|
| `NOTLEADER <host>:<port>` | another node leads the group | send the command there |
| `NOTLEADER no leader is known yet` | the group is electing one | try again shortly |
| `TRYAGAIN ...` | this node leads but is still applying what came before it, or it hasn't joined the group yet, or its copy is being rebuilt | try again shortly |
| `UNKNOWN ...` | the write reached the log and leadership changed before it was known to commit | it may or may not have happened; read it back from the new leader |

Barch's own code reads its local copy of a replicated space on any node. That's how
a space reads its options from `configuration` on a follower.

Phase 1 limits:

- Any number of spaces can be replicated. Each is a group, and a node needs a
  port for every group above `raft_port`. Every group starts on the node leading
  the `cluster` group, then hands its leadership to a preferred member: the voters
  in `num` order, group *n* the (*n* - 1)th, wrapping round. It does so once that
  member has caught up, and at most every ten seconds.
- A group snapshots its spaces every `raft_snapshot_entries` entries (default
  20000) and compacts its log behind the snapshot. A restart applies only the
  entries after the last one. See [Adding a member](#adding-a-member).
- Every member is in every group. `<space>.raft_members` isn't read yet.
- Turning `raft` off doesn't stop a space being replicated.
- A replicated space evicts nothing for memory.
- On a node that doesn't lead the `cluster` group, barch's own writes to
  `configuration` are refused like a client's. That includes what the function
  sync, cron and `--from-git` store there, so set those up on the leader.
- Node-to-node calls aren't authenticated beyond the `default` user, and the Raft
  ports have no TLS. Keep them on a private network.
- Only `barchd` runs the cluster. The Valkey module and the Python and Java
  bindings don't.

## What exists today

Three parts of the current code carry most of the weight.

**Async replication.** `PUBLISH` sends a node's shard writes to its replicas as
records (`src/rpc/server.cpp`, the `consumers` struct). Each batch carries the
sending node's id, an incarnation that changes on every restart, and a sequence.
`REPLAPPLY` on the replica (`src/repl_api.cpp`) skips batches it has seen, and
refuses a batch after a gap with `NEEDSYNC`, so a replica that missed writes needs
a full copy before it can continue. A key space takes writes from one primary.
Records are `set`, `erase` and `clear`. They carry the result of a command, so
replaying one gives the same value on every node.

**Copy on write freeze.** A save freezes a shard under its write latch, writes the
files from the frozen pages with no latch held, and merges the CoW pages back
afterwards (`src/abstract_shard.h`, `freeze_for_save_holding_lock` and
`write_frozen`). `RETRIEVE` uses the same freeze to stream a consistent copy of
every shard to another node (`send_frozen`, `receive_files`,
`install_received_holding_lock`). The page walk (`start_page_walk`,
`read_walk_page`) reads the same pages one at a time.

**Routing.** `ADDROUTE`, `ROUTE` and `REMROUTE` keep a host and port per shard
(`src/rpc/server.cpp`). A caller copies the table when it's created or switches
key space, and forwards data commands for routed shards (`src/rpc_caller.h`,
`call_route`). A route that has a network error is switched off for that caller.
The table has no leader, epoch or redirect, so a client can't tell that it reached
the wrong node.

## The libraries

| | NuRaft | libgossip |
|---|---|---|
| Purpose | Raft log replication, elections, membership changes | SWIM membership and failure detection |
| License | Apache 2.0 | MIT |
| Transport | Standalone Asio, optional TLS through OpenSSL | Asio UDP or TCP |
| Maturity | About 1.2k stars, used in production | About 7 stars, version 1.4.2 |
| Security | TLS | No TLS or node authentication yet; both are on its roadmap |
| Windows | Builds, lightly tested, no TLS | Claims MSVC 2017+ |

BARCH already builds standalone Asio and OpenSSL, so NuRaft adds no new
dependency. The `win32/` build compiles `src/` too, so both libraries have to build
there, or the cluster code has to sit behind a build option that Windows leaves off.

## The cluster key space

Every node in a cluster has a key space called `cluster`. It holds the members,
their addresses, when each was last seen, and how far each one has got in every
key space. You read it with the usual commands, such as `KEYS`, `GET` and
`HGETALL`, from any node.

### Who writes it

The `cluster` space is replicated with Raft from the start, in its own Raft group
that every member votes in. All members hold the same copy, so any node can answer
"who is in the cluster" without asking another.

Only the server writes it. It joins `repository` in `server_written()`, so no
client gets write rights there, however its ACL is set. `CLUSTER JOIN` and
`CLUSTER REMOVE` change it through the server, and each change is a committed Raft
entry.

### What it holds

Phase 1 writes these keys. Fields marked *later* are part of the design and not
written yet.

| Key | Type | Holds | Written when |
|---|---|---|---|
| `node:<id>` | hash | `num` (its server id in every Raft group), `rpc` (`host:port` for RESP and the replication protocol), `raft` (`host:raft_port`), `role` (`voter`), `joined` (ms since the epoch). *Later:* `layout` (see [Members with a different page layout](#members-with-a-different-page-layout)) | a node joins or is removed |
| `node:<id>:seen` | string | when the node last sent a heartbeat, in ms since the epoch, by the cluster leader's clock | each heartbeat |
| `node:<id>:space:<name>` | hash | `applied` (the last log index this node applied in the space's group), `term`, `leader` (`1` when this node leads the group). *Later:* `origin` and `seq` for a space replicated with `PUBLISH` | each heartbeat |
| `space:<name>` | hash | `group` (its Raft group number), `mode` (`raft`), `creator` (the `num` of the node that started the group). *Later:* `leader`, `epoch`, `commit` | the space is first replicated |

A node id is the first field `REPLNODE` already returns. It's kept in a file beside
the data, so it stays the same across restarts.

### Heartbeats

Each node sends a heartbeat every `cluster_heartbeat_ms` (default 5000). One
heartbeat carries the node's whole state: its own `seen` key and one
`node:<id>:space:<name>` entry for each space it replicates, the `cluster` and
`configuration` spaces included. A follower sends it to the leader of the
`cluster` group as `CLUSTER HEARTBEAT`, and the leader writes it.

The leader puts its own clock into `seen` before it logs the record. A record holds
a result, so every member stores the same time, and comparing two nodes' `seen`
values compares readings from one clock. A reader that compares `seen` with its
own clock should allow for skew between its clock and the leader's.

The cost is one small Raft entry per node per heartbeat for `seen`, plus one per
replicated space. Three nodes with three spaces each, every five seconds, is a bit
over two entries a second in the `cluster` group.

A node that stops sending heartbeats stops updating its `seen` key. Each data
space's own Raft group elects its leaders by itself, so a node can lose
leadership of a space before the `cluster` space shows it as stale. *Later:*
routing uses `seen` as a hint, and a node not seen for three heartbeats gets no
follower reads.

> **Recovery:** heartbeats need the `cluster` group to have a quorum. If it loses
> one, the stats stop updating and members can't be added or removed. Spaces with
> their own Raft groups keep taking writes as long as their own groups have a
> quorum.

### Gossip

With the `cluster` space in place, gossip is optional, and phase 1 doesn't use
it. It's useful for finding seed nodes and for faster failure hints in large
clusters. A node announced over gossip still has to join with `CLUSTER JOIN`
before it holds any data.

> **Security:** libgossip has no authentication. Any host that can reach the
> gossip port can announce itself or report others as failed. Keep the gossip
> port on a private network, and treat gossip data as untrusted input.

## Turning Raft on for a key space

A key space opts into Raft with a per-space option in the configuration space,
like any other per-space option:

```text
USE configuration
SET orders.raft on
SAVE
```

or, from Python:

```python
conf = barch.KeyValue("configuration")
conf.set("orders.raft", "on")
```

| Option | Type | Does | Default |
|---|---|---|---|
| `<keyspace>.raft` | `on` / `off` | replicates the space with its own Raft group | `off` |
| `<keyspace>.raft_members` | int | *later:* how many cluster members hold the space. Phase 1 puts every member in every group | `0` |

### The configuration space in a cluster

The `<keyspace>.raft` option has to read the same on every node, or one node takes
writes to a space that the others replicate. So in a cluster, the
`configuration` space is replicated with Raft too, in the same group as
`cluster`. A change to any per-space option then reaches every node as one
committed entry, in the same order everywhere.

### When the option changes

`raft` takes effect without a restart. Each node checks the `cluster` space four
times a second. At its next check, the leader of the `cluster` group records the
space in `space:<name>` and starts its group, as its creator. Every other member then binds the space to the group,
which makes it refuse clients, copies the creator's copy of the space with
`RETRIEVE`, and joins. The creator's leader then adds it.

- **Turning it on** for a space that already has data: the creator's copy is the
  one every member ends up with. Writes made to another node's copy before it
  joined are lost.
- **Turning it off:** phase 1 doesn't act on it. The space stays replicated.

> **Data safety:** turning `raft` on replaces every member's copy of the space
> with the copy on the node that leads the `cluster` group at that moment. Check
> which node that is with `CLUSTER INFO` before you turn it on.

A space replicated with Raft refuses `PUBLISH` batches through `REPLAPPLY`. Its
leader can still `PUBLISH` to replicas outside the cluster, such as an embedded
instance. A new leader is a new origin for those replicas, so after a leader
change they need a full copy with `RETRIEVE`.

## Replication with NuRaft

### Raft groups

Each key space with `raft on` has its own Raft group. That matches the rule
`REPLAPPLY` already enforces, that a key space takes writes from one primary.
Each group has its own leader, so different spaces can be led by different nodes
and write in parallel. The `cluster` and `configuration` spaces share one more
group, which every member votes in.

Membership changes start in the `cluster` group. `CLUSTER JOIN` adds the new
node there first. Then the leader of each space's group notices a cluster member
that isn't in its group and adds it as a learner with NuRaft's `add_srv`, one at a
time, makes it a voter once it has caught up, and removes one that has left the
cluster. A node's `num` is its server id in every
group.

A group listens on its own port, `raft_port` plus its number, because NuRaft gives
each server its own listener. The `cluster` group is group 0, so a node with
`raft_port` 15100 listens on 15100 for it and on 15101 for group 1.

Later, a large space could split into several groups, one per range of shards,
the way TiKV splits regions. One group for a whole node would push every write in
every space through one log, which would give away the parallel writes sharding
gives today.

### Log entries

A Raft log entry is one of the shard records `PUBLISH` already makes, after a
version byte and the incarnation of the group on the node that wrote it. On every
member, NuRaft's `state_machine::commit` hands the record to the same code
`REPLAPPLY` uses (`apply_record` in `src/repl_api.cpp`). The leader made the write
itself before logging it, so it skips entries with its own incarnation. A group
gets a new incarnation each time it starts, so after a restart a node applies the
whole log again over what its files held. Records are absolute sets, erases and
clears, so that ends where the log does.

For a space in a Raft group, the Raft log index replaces the origin, incarnation
and sequence that `REPLAPPLY` tracks. Raft already guarantees order, no gaps and
no repeats. Those spaces refuse `REPLAPPLY` batches from a `PUBLISH` stream, since
a second writer would put the members out of step.

### The write path

A record holds the result of a command, such as the new value after `INCR`, so the
leader has to run the command before it can log it. The leader makes the write
under the shard's write latch, builds the record the change log would, and hands
it to the group **while it still holds the latch**. It waits there until a quorum
has the entry. What happens next depends on the outcome:

| Outcome | What the shard does | What the client gets |
|---|---|---|
| committed | keeps the write | the command's reply |
| refused: it never reached the log (this node isn't the leader, or the group is stopping) | puts the key back the way it was, as it does when the change log refuses a write | `NOTLEADER <host>:<port>`, or the reason |
| unknown: it reached the log, and leadership changed before it was known to commit | keeps it, since it may commit yet, and marks the space for a rebuild | `UNKNOWN ...` |

Holding the latch means no reader sees a write before it commits, and no save
writes one that never did. So a node's files only ever hold committed state, and
after a crash, applying the log over them ends where the log does.

The cost is that other clients of that shard wait out each write's round trip
plus fsync. Phase 1 accepts that. A later phase can narrow the wait to the one key.

A space marked for a rebuild refuses clients with `TRYAGAIN`. The node waits for
another member to lead, copies the space from it with `RETRIEVE`, and starts its
group again as a new incarnation, so the whole log is applied over the copy. If
the node leads the group again first, its log holds the entry and every entry in
a leader's log commits, so it hands leadership to another member and rebuilds
from it. In a group of one there is nobody to disagree with, and the mark is
cleared.

A new leader takes writes only once it has applied everything an earlier leader
committed. NuRaft appends a configuration entry as a node takes over, and the
space answers `TRYAGAIN` until that entry is applied. Without this, a client
holding a shard latch while it waits for its own entry could wait on the thread
that has to take that latch to apply an earlier one.

A command that writes several shards, such as `MSET`, commits each shard's write
on its own. If leadership changes partway, some of them may be in and some not.

A write is acknowledged only after it commits. That costs one network round trip
to a quorum plus an fsync of the log on each of them.

A space replicated with Raft keeps no change log: the Raft log is its log, and a
`<space>.aof` setting is ignored with an error in the log. Its keys are evicted for
memory on no node, since each node would pick different ones. An expired key is
gone on every node at once, since its expiry is in the record, so each node sweeps
its own expired keys without a log entry.

### Where the log and Raft state live

Each group keeps its log, its term and vote, its configuration and this node's
server id in one file, `cluster/group_<n>.raft` in the data directory
(`src/cluster/group_store.h`). Two files that must agree with each other can drift
apart after a crash, and that kind of drift caused the TODO 510 bug. The change
log wasn't a fit: NuRaft needs reads by index and a tail cut after a conflict.

The file is append only. Each record has a length and a crc32c, and opening the
file cuts off a torn record at the end. Entries are synced before NuRaft counts
them as written, and so are a new term, vote or configuration.

> **Data safety:** a failed sync of a group file stops the process. After one, the
> kernel may have dropped pages it couldn't write, and a later sync can succeed
> without them, so the log may have lost entries it already acknowledged.

Every `raft_snapshot_entries` entries, a group snapshots its spaces and NuRaft
compacts its log behind the snapshot, keeping a quarter of that many entries. See
[Adding a member](#adding-a-member).

## Adding a member

A node joining copies each space with `RETRIEVE`: the `cluster` and `configuration`
spaces from the leader of the `cluster` group, and a data space from whichever node
its heartbeats say leads it, or else its creator. It then starts the group waiting
to be added. The group's leader adds it as a **learner**, which receives the log
but doesn't count towards a quorum, so a node that's slow to catch up can't slow
down commits or cost the group its majority. Once the learner is within a few
entries of the leader, the leader makes it a voter. Writers carry on throughout:
the copy is taken under the CoW freeze, and records are absolute, so applying
entries the copy already has changes nothing in the end.

### Snapshots

A snapshot at index *i* is the group's spaces as this node's data files hold them.
Every `raft_snapshot_entries` entries, NuRaft asks for one. The node saves each of
the group's spaces with `SAVE` on a thread of its own, then records *i* and the
term and configuration then in the group file. Writes hold their shard latch until
they commit, so the files hold only committed writes, and they hold at least
everything up to *i*, perhaps more. NuRaft then compacts the log behind *i*.

After a restart, the group starts applying at the entry after the last snapshot.
The files may already hold some of those entries, and applying them again changes
nothing.

A member behind the start of the leader's log, because it was down while the log
was compacted, needs a snapshot. The leader sends it one object that names the
leader. The member copies the spaces from there with `RETRIEVE`, which takes them
under the leader's CoW freeze, so the copy also holds at least *i*. It records *i*
as its own snapshot and carries on from the log. If the copy fails, NuRaft sends
the snapshot again.

The copy works only between members with the same page layout. See
[Members with a different page layout](#members-with-a-different-page-layout).

> **Risk:** while a freeze is up, `BEGIN` and other saves wait for it. A slow
> member holds the leader's freeze up for as long as its copy takes. The leader
> gives up on a snapshot after 10 minutes, and the member starts over.

## Members with a different page layout

A physical snapshot copies shard pages exactly as they sit in memory and on disk.
A member can load those pages only if its build lays them out the same way. Three
things decide that:

- **Byte order.** A big endian member reads every multi-byte field in the pages
  wrongly.
- **Type sizes and alignment.** Some types change size between platforms. For
  example, `long` is 32 bits on Windows and 64 bits on Linux.
- **Storage version.** `storage_version` in `src/constants.h` changes whenever the
  page format does, and a build only loads its own version and the one before it.
  So two members on the same CPU but different BARCH releases can be
  incompatible too, which matters during a rolling upgrade.

Raft log entries don't have this problem. A shard record is written byte by byte
in little endian (`src/aof_record.cpp`), so every build reads it the same way.
Only the snapshot step needs a second form.

### Detecting a mismatch

Each build has a layout tag: byte order, `sizeof(long)`, `sizeof(void*)` and
`storage_version`, such as `le-l8-p8-s<n>`. A node stores it in the `layout` field
of its `node:<id>` hash when it joins, and `CLUSTER LAYOUT` answers it. Before a
node copies a space, for a join, a snapshot or a rebuild, it asks the source for
its tag. The same tag means `RETRIEVE`; a different one means a logical copy. The
environment variable `BARCH_TEST_LAYOUT` replaces the tag, so a test can force the
logical path between two identical builds.

### Logical copies

The source hands over each shard a page of leaves at a time (`CLUSTER EXPORT
<space> <shard> <page>`), each leaf as a `set` record in the change log's portable
form, hex encoded. Values come over decompressed, and the dictionary stays each
node's own. The receiver clears its copy and applies the records with the same code
that applies log entries.

The walk is SCAN's: every key present for the whole walk comes over at least once,
and the source's writers and defrag carry on. A frozen view isn't needed. A key
written or erased during the walk may or may not come over, but each such write is
in the log after the snapshot the copy stands for, and applying those over the copy
ends where the log does, since records are absolute.

A logical copy is larger and slower than `RETRIEVE`, and in a cluster where every
member has the same layout it never runs.

## Routing and read after write

The routing table moves from hand set routes to a table built from cluster state.

- **Entries.** Each entry maps a key space, or a range of a range sharded space,
  to a Raft group and that group's current leader address, along with an epoch.
  Every node builds the table from the `space:<name>` and `node:<id>` keys in its
  copy of the `cluster` space, and rebuilds it when those keys change.
  The epoch goes up whenever a leader changes, a member joins or leaves, or a
  range moves.
- **Redirects.** A node that gets a command for a group it doesn't lead replies
  `-NOTLEADER <host>:<port> <epoch>`, like Redis Cluster's `MOVED`. A caller that
  sees a newer epoch refreshes its table and retries. Today a route with a network
  error is just switched off. Phase 1 replies `-NOTLEADER <host>:<port>` without
  an epoch, and the client follows it itself.
- **Default: read from the leader.** Reads and writes for a group go to its
  leader. With NuRaft's leader lease, the leader can answer reads from its own
  shards and still be linearizable. Read after write needs nothing more.
- **Optional follower reads.** Each write reply carries the log index it
  committed at. The session keeps the highest index it has seen. A follower
  answers a read only after it has applied that index, and otherwise forwards it
  or waits a short, bounded time. Within one connection this is automatic. A
  client that writes on one connection and reads on another has to pass the index
  along, so this needs a small command or `CLIENT` option.
- **Range sharded spaces.** A key's shard can change while a range sharded space
  is running (`route_moved` in `src/key_space.h`). The route epoch has to change
  when a range moves, so a caller never uses a stale range table.

## Distributed locks

Raft leadership already decides which node writes a group, so routing and
replication need no separate locks. Locks are still useful for applications, such
as the shop's order processing.

Once Raft groups exist, a lock is a few more log entries in a group:

- `LOCK <name> <owner> <ttl>` grants the lock if it's free and replies with a
  **fencing token**, the log index of the grant. A resource that receives a
  write with a token lower than one it has already seen rejects it. Without
  fencing, a client that stalls past its TTL can still write after someone else
  has the lock.
- `UNLOCK <name> <owner>` releases it.
- The leader expires locks by committing an expiry entry when a TTL runs out.
  Followers never expire a lock by their own clock, so all members agree on who
  holds it.

Redlock style locks, which take a majority of independent nodes with no shared
log, can't give fencing tokens and depend on clocks agreeing. This design avoids
them.

## Luau routing

Luau fits as a placement rule: which group a key belongs to, how to pin a
tenant's keys together, or which reads may go to followers. Two rules keep it
safe and fast.

- **Off the hot path.** The Luau function builds the routing table when the
  configuration changes. Each command then only looks the key up in the table, the
  way range routing does today with a binary search. Running Luau for every
  command would add its call cost to every request.
- **The same code on every node.** Every node has to place keys the same way, or
  writes go to the wrong group. The versioned library pin sets (TODO 593 to 596)
  already switch a whole set of libraries at once. Store the pinned version of the
  routing function in the Raft configuration, so a change reaches every node as
  one committed entry.

## Phases

0. **Spike (done, TODO 609).** Configure with `-DBARCH_CLUSTER=ON` to fetch
   NuRaft v3.0.0 and libgossip v1.4.2 and build `TestClusterSpike`. Results:
   - Both build against BARCH's asio 1.36.0 with no patches.
   - Three NuRaft servers elect a leader and commit entries. Two servers that
     join after the leader has compacted its log catch up through a logical
     snapshot sent in several objects. With the leader stopped, the other two
     elect a new one and keep committing.
   - Three gossip nodes that only know the first one find each other within a
     few ticks.
   - TSan reports nothing in seven runs.
   - Both cross-compile with MinGW and the test passes under wine. Windows needs
     two defines, which the CMake block sets: `LIBGOSSIP_STATIC_DEFINE` and an
     empty `_Printf_format_string_`. `win32/CMakeLists.txt` doesn't build them
     yet.
1. **The `cluster` space and one data space (done, TODO 610).** `CLUSTER INIT`,
   `JOIN`, `REMOVE`, `INFO` and `DIGEST`, heartbeats, and the `cluster` and
   `configuration` group. Then `raft on` for one key space. Log entries are shard
   records, writes hold the shard latch until they commit, and reads and writes go
   to the leader. A second space with `raft on` is refused and logged. See
   [What phase 1 does](#what-phase-1-does).
2. **Joining (done, TODO 611).** Snapshots through `SAVE` and `RETRIEVE`, log
   compaction, restarts from the last snapshot, and members that join as learners
   and are made voters once caught up. See [Adding a member](#adding-a-member).
3. **Many spaces and mixed layouts (done, TODO 612).** Any number of replicated
   spaces, leaders handed to preferred members, layout tags, and logical copies.
   See [Members with a different page layout](#members-with-a-different-page-layout).
4. **Routing.** Epochs, `NOTLEADER` redirects, follower reads with session
   indexes, and `seen` as the failure hint. Gossip for discovery if it's wanted.
5. **Scale out and extras.** Several groups per space, locks with fencing tokens,
   and Luau placement.

## Testing

Each phase follows the usual hardening method: write the test first, show it
fails without the change, then run it under ASan and TSan.

The tests so far, in the `short` set of a `-DBARCH_CLUSTER=ON` build:

- `TestGroupStore`: a group's file reopened after appends, cuts, compaction, a torn
  tail and a rewrite.
- `TestRaftGroup`: three groups in one process join, lose their leader, and all
  stop and start again from their files.
- `TestClusterJoin`: snapshots every 200 entries. A follower kept down while the
  log is compacted past it comes back by copying a snapshot. A restart applies only
  the entries after its snapshot. A fourth node joins while four clients write,
  becomes a voter in both groups, and ends with the same copy as the others, every
  acknowledged write in it.
- `TestCluster`: three `barchd` form a cluster and replicate a space. The space's
  leader is killed with `SIGKILL` while four clients write, and every acknowledged
  write has to be on the new leader. The killed node comes back, and every
  member's `CLUSTER DIGEST` has to match. It also checks the cluster space's
  heartbeat keys, that a client can't write `cluster`, and that a second
  replicated space gets a group of its own.
- `TestClusterMany`: three spaces led by three different nodes, a node killed with
  writers on all of them, no acknowledged write lost, and every copy matching once
  it's back. Then a fourth node with another layout tag joins under writes, copies
  all five spaces key by key, and ends with copies that match.

Cluster tests start several `barchd` processes on local ports. Faults to cover:

- kill the leader, a follower, or a learner during a snapshot
- pause a process with `SIGSTOP` to let its lease run out
- restart a node with an empty data directory
- partition the network, by dropping one node's traffic with a proxy in the test

After each fault, every acknowledged write must be readable through the cluster,
and every member must hold the same data once it settles.

Mixed layouts need members that really differ. A Windows build under Wine gives a
different type size on the same machine, and a big endian build such as s390x
under QEMU covers byte order. A test can also force a logical snapshot between two
identical builds, which checks the logical path on every CI run.

## Open questions

1. ~~Is a quorum round trip plus fsync before each write is acknowledged
   acceptable?~~ Yes (07-10-2026). Is there a need for a mode that acknowledges on
   the leader alone?
2. Should embedded instances (Python, Java) be able to join as Raft members, or
   only follow as `PUBLISH` replicas?
3. With the `cluster` space giving members and failure hints, is libgossip still
   needed, given its maturity and lack of authentication?
4. ~~Can the change log serve as NuRaft's log store?~~ No: it has its own, see
   [Where the log and Raft state live](#where-the-log-and-raft-state-live).
5. What should a client library see on `NOTLEADER`: follow the redirect itself,
   or have the node forward the command?
6. Should a node that hasn't been seen for a long time be removed from the
   cluster automatically, or only by `CLUSTER REMOVE`?
7. Is a restart the right way to apply a change to `<keyspace>.raft`, as it is
   for the other per-space options, or should there be a command that applies it
   to an open space?
8. Should mixed layouts be supported long term, or only during a rolling upgrade
   between two storage versions?
