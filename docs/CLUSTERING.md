# Clustering and Raft replication

**Status: phases 1 to 4 are built (TODO 610 to 613), and so are phase 5's locks
and splitting (TODO 614 to 616); the rest is a design proposal.** That's the `cluster` space, the `CLUSTER` command, any number of key
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

Build with `-DBARCH_CLUSTER=ON`, and give every node a `raft_port`, the
`external_host` the others reach it at, and the same `cluster_secret`. Each Raft
group a node is in listens on `raft_port` plus the group's number, so leave room
above it. See [Securing the Raft ports](#securing-the-raft-ports) for the secret,
and for TLS.

```text
barchd --port 14000 --dir /data/n1 -c raft_port=15100 -c external_host=10.0.0.1 -c cluster_secret=$SECRET
barchd --port 14000 --dir /data/n2 -c raft_port=15100 -c external_host=10.0.0.2 -c cluster_secret=$SECRET
barchd --port 14000 --dir /data/n3 -c raft_port=15100 -c external_host=10.0.0.3 -c cluster_secret=$SECRET
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

`CLUSTER ROUTES` lists every replicated space as `<space> group <n> leader
<host>:<port> epoch <term>`, from any node. `CLUSTER READS FOLLOWER` lets this
connection's reads be answered by a follower, and `CLUSTER READS LEADER` turns that
off. `CLUSTER INDEX <space>` answers the newest log index this connection has seen
in a space, and `CLUSTER AFTER <space> <index>` makes this connection read nothing
older than that index. See [Routing and read after write](#routing-and-read-after-write).
`CLUSTER LAYOUT` answers this node's layout tag. `CLUSTER RESERVE`, `CLUSTER ADMIT`,
`CLUSTER EXPORT` and `CLUSTER HEARTBEAT` are what nodes send each
other. `CLUSTER` needs the `dangerous` category, and a node joining calls the
leader as the `default` user.

Reads and writes of a replicated space go to the leader of its group. Anywhere else
they get one of these errors:

| Error | Means | What to do |
|---|---|---|
| `NOTLEADER <host>:<port> <epoch>` | another node leads the group. The epoch is the group's term; of two redirects for a space, the higher epoch is the newer | send the command there |
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
  member has caught up and only while it's answering, at most every ten seconds.
  Before TODO 628 it only looked at how far the member's log had got, as the leader
  last heard it, so a preferred member that had died was handed the group every
  ten seconds, leaving the group without a leader each time.
- A group snapshots its spaces every `raft_snapshot_entries` entries (default
  20000) and compacts its log behind the snapshot. A restart applies only the
  entries after the last one. See [Adding a member](#adding-a-member).
- Every member is in every group. `<space>.raft_members` isn't read yet.
- Turning `raft` off doesn't stop a space being replicated.
- A replicated space evicts nothing for memory.
- On a node that doesn't lead the `cluster` group, barch's own writes to
  `configuration` are refused like a client's. That includes what the function
  sync, cron and `--from-git` store there, so set those up on the leader.
- Raft messages are signed with `cluster_secret`, and `raft_tls` encrypts them.
  The calls nodes make to each other over RESP (`ADMIT` aside, which is signed
  too) are made as the `default` user without TLS. See
  [Securing the Raft ports](#securing-the-raft-ports).
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

The leader writes one heartbeat per node at a time (TODO 642). A node waits 3 seconds
for an answer and then sends again, so on slow disks, where a heartbeat's entries take
longer than that to commit, they used to pile up on the leader: RESP threads waiting on
commits, `seen` going stale, and a shutdown that sat out each one's timeout. A
heartbeat that comes while the last one from the same node is still being written is
answered at once and not written. The next one carries the node's whole state anyway.

When barchd is asked to stop, it quiesces the cluster before its server stops. It sends
and takes no more heartbeats, and refuses new Raft writes at once (`TRYAGAIN this node
is shutting down`), so the server's threads aren't left waiting on commits that a
quorum may never answer.

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
| `<keyspace>.raft_groups` | int | splits the space over this many Raft groups, `1` to `16`, each owning an even run of its shards. Read when the space is first replicated. See [Spaces split over several groups](#spaces-split-over-several-groups) | `1` |
| `<keyspace>.raft_members` | int | *later:* how many cluster members hold the space. Phase 1 puts every member in every group | `0` |
| `<keyspace>.shards` | int | the space's shard count, as for any space. For a replicated one the cluster leader fills it in when it starts replicating the space, if it isn't set; see below | `raft_shards` |

A replicated write holds its shard until it commits, so a space has as many writes
committing at once as it has shards, and the more of them there are, the more
entries share each sync of the log (TODO 631). With the default of 17 shards, 64
connections got 1,792 writes a second; with 128, 4,703. So the cluster leader
decides a replicated space's count once, when it starts replicating it, and writes
it to `<keyspace>.shards` in the configuration space before anything else about the
space, so every member opens it the same:

- `<keyspace>.shards`, if it's already set;
- else the count the space already has, if it exists on the leader, since a saved
  space can't change its count;
- else `raft_shards` (default 128).

So set `<keyspace>.raft on` before the space has any keys to get the larger count. A
member that made the space itself before it was replicated, with another count,
can't take the leader's copy: it stays out of the space's group and says so in its
log, once. Remove its copy of the space there, and it's copied again.

The count is also kept in the space's record in the `cluster` space, and a member
hands it to the space before anything opens it there. A member coming back can have
the record before its configuration catches up, since that comes with the cluster
group's snapshot or log, and a space opened in between used to get the default: 17
shards against the cluster's 128, so every copy from the leader was refused.

The cluster tests run with `raft_shards 32` (`BARCH_TEST_RAFT_SHARDS` sets another).
Copying a space to a member holds every one of its shards' latches at once, and
TSan's deadlock detector stops a process at more than 64 held by one thread.

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

A space replicated with Raft takes a `PUBLISH` stream from a primary outside the
cluster through its log; see [A primary's stream](#a-primarys-stream). Its
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

A batch entry (TODO 665) carries several records: a count after the incarnation,
then each record with its length. Every member applies a batch as it commits it, the
leader too, so all of its records are in on every member or none of them are. A
member that can't read one of its records applies none of them and is rebuilt from
the others. `FS`, `GRAPH`, `DIR`, `LOADFS` and `LOADKEYS` write this way. See
[Writes that land together](#writes-that-land-together).

A command entry (TODO 669) carries a command instead of records: the space's name,
then the command's words, laid out like a batch. Every member runs the command as it
commits, the leader too, in that space. See
[Commands every member runs](#commands-every-member-runs).

For a space in a Raft group, the Raft log index replaces the origin, incarnation
and sequence that `REPLAPPLY` tracks. Raft already guarantees order, no gaps and
no repeats. A `PUBLISH` stream from a primary outside the cluster goes into the log
too (TODO 684).

### The write path

A record holds the result of a command, such as the new value after `INCR`, so the
leader has to run the command before it can log it. How it does that depends on the
command (TODO 632).

**A single-key write** (`SET`, `DEL`, `INCR`, `EXPIRE` and the like, anything that
writes one key through the store's single-key calls) runs under the shard's write
latch, builds its record, and then takes its change back out of the tree, so the
tree only ever holds committed state. It marks its key pending and lets the latch
go while the entry commits. The group's commit thread applies the record, on the
leader exactly as on every member, in log order, and the write returns once it has.
A second write to a pending key waits for it, so writes to one key stay in order;
writes to other keys of the same shard commit alongside it.

| Outcome | What happens | What the client gets |
|---|---|---|
| committed | the commit thread applied it | the command's reply |
| refused: it never reached the log | nothing: the tree never kept it | `NOTLEADER <host>:<port>`, or the reason |
| unknown: it reached the log, and leadership changed before it was known to commit | nothing yet: if it commits, the commit thread applies it like any entry | `UNKNOWN ...` |

**A composite command** (lists, hashes, sorted sets, anything that makes several
dependent writes under one hold of the latch) applies first and holds the latch
through its commit, as every write used to, because releasing it partway would let
another command see half of it.

| Outcome | What the shard does | What the client gets |
|---|---|---|
| committed | keeps the write | the command's reply |
| refused | puts the key back the way it was | `NOTLEADER <host>:<port>`, or the reason |
| unknown | keeps it, since it may commit yet, and marks the space for a rebuild | `UNKNOWN ...` |

A composite write's commit waits for every entry before it in the log to be applied,
and the commit thread needs the shard's latch to apply a single-key write's entry.
So a composite write never holds the latch while a single-key write of that shard is
committing: it lets the latch go and waits until none is, and new single-key writes
wait behind it meanwhile. A write path that takes the latch some other way and
finds one committing answers `TRYAGAIN`.

Either way, no reader sees a write before it commits, and no save writes one that
never did. So a node's files only ever hold committed state, and after a crash,
applying the log over them ends where the log does.

On a space with 4 shards, 32 writers' entries were 2.5 to a follower's sync before
TODO 632, held to the shards a write could commit in at once, and many more after;
see the Performance section.

The wait doesn't hold up anyone else on the node (TODO 626). A connection's RESP
thread serves many connections, so a write waiting there used to stall all of them,
whatever space they used. Instead, a data write to a replicated space, and any
`CLUSTER` call, runs on a pool of 64 threads kept for calls that wait on Raft, the
same way `KEYS` runs off the RESP thread. That connection still waits its turn, so
its replies keep their order, and its follower-read settings and `CLUSTER INDEX` are
shared with the call wherever it runs. Inside `MULTI`, and `EXEC` itself, writes
still wait on the RESP thread, since they need the connection's own transaction
state. The pool's size is how many replicated writes a node has waiting at once.

A space marked for a rebuild refuses clients with `TRYAGAIN`. The node waits for
another member to lead, copies the space from it with `RETRIEVE`, and starts its
group again as a new incarnation, so the whole log is applied over the copy. If
the node leads the group again first, its log holds the entry and every entry in
a leader's log commits, so it hands leadership to another member and rebuilds
from it. It hands it over only while another voter is answering; otherwise it waits
for one to come back, and says so once (TODO 628). In a group of one there is nobody
to disagree with, and the mark is cleared.

The space stays bound to its group the whole time, from stopping the group through
the copy to starting it again, so it goes on refusing clients (TODO 621). Until
then it was unbound for the copy, and a client still sending to that node got
ordinary local writes. They were acknowledged without reaching the log, and most
were then overwritten by the copy. A writer that skips Raft is acknowledged in a
fraction of a millisecond, so a window of 250ms was enough to lose thousands.

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
file cuts off a torn record at the end. A new term, vote or configuration is
synced before it's counted as written. Entries are synced by a thread of the store's,
which syncs everything written by the time it starts, so writes that arrive during a
sync share the next one (TODO 625). NuRaft's parallel log appending is on: a leader
commits an entry once a quorum holds it durably, counting its own copy only once the
store says it's synced. A follower still answers only once its copy is synced.

> **Data safety:** a failed sync of a group file stops the process. After one, the
> kernel may have dropped pages it couldn't write, and a later sync can succeed
> without them, so the log may have lost entries it already acknowledged.

Every `raft_snapshot_entries` entries, a group snapshots its spaces and NuRaft
compacts its log behind the snapshot, keeping a quarter of that many entries. See
[Adding a member](#adding-a-member).

## Every way a replicated space changes

Built in TODO 663, after TODO 662 found a write that skipped Raft. A replicated space
stays the same on every member only if every change to it goes through Raft. That
happens in one place: `shard::raft_commit`, which three shard write paths call
(`opt_rpc_insert` for inserts, the update path, and `remove`). FLUSHDB's clear record
in `sharded_store::clear_space`, and the cluster's own control entries (a split's
hand-off), are the only other writes to a group's log. Anything else that changes a
shard changes one node's copy.

**Commands.** Every registered write command is classed in `src/raft_writes.cpp`:

- *replicated*: writes only through the paths above. The string, key, hash, list and
  ordered set writes, the TTL commands, `FLUSHDB`, `LOCK`/`UNLOCK`, `KSPACE MERGE`
  (gathered into one batch, TODO 670),
  and `FS`, `GRAPH`, `DIR`, `LOADFS` and `LOADKEYS`, which each commit one batch
  entry (TODO 665). `QUEUE` goes by the queue's own space rather than the
  connection's: a push to a queue on a replicated space is an entry in that space's
  log (TODO 682, see Queues below). `REPLAPPLY` goes by each record's space, whose
  log it goes into (TODO 684, see A primary's stream below). `FUNCTIONS SYNC` writes the spaces its
  repositories fill, each where the node leads it (TODO 683, see Repository syncs
  below).
- *logged*: changes a node's settings or what it derives from the data, not keys
  (TODO 669). The command goes in the space's log and every member runs it:
  `KSOPTIONS`, `KSPACE OPTION SET`, `TRAIN`, `DICTIONARY SET`, `COMPRESS`,
  `DECOMPRESS`, `INDEX CREATE`, `BUILD` and `DROP`, `SETF` and `REMF` (TODO 667),
  `KSPACE DEPENDS`, `RELEASE` and `DROP` (TODO 670), `BEGIN`, `COMMIT` and
  `ROLLBACK` (TODO 677), and `FOREIGN FAKE SET`, `DEL`, `FAIL`, `DELAY` and `RESET`
  and `FOREIGN_MISS` (TODO 685, see Foreign sources below).
- *server*: changes the server or the connection, not a space: `ACL`, `CONFIG`,
  `CLUSTER`, `USE`, `SELECT`, `MULTI`, `EXEC`, `START`, `STOP`, `PUBLISH`, `PULL`,
  `ADDROUTE`, `REMROUTE`, `HTTP` (this node's web server, TODO 687, see Web servers
  below), `KSPACE`'s read subcommands and `KSPACE ACL`, `DICTIONARY
  GET`, `INDEX LIST`, `CHAINS` and `FIND`, and `FUNCTIONS STATUS`, `COMMANDS`,
  `CRON`, `QUEUES` and `LIBRARIES`, and `FOREIGN FAKE QUERIES` and `PARTS`, which
  report on the node. A server command answers on any member: it needs no leader,
  even when its categories say data (TODO 685). `ACL SETUSER` and `DEL` and `KSPACE
  ACL` go in the cluster group's log (TODO 676, see Users below).
- *local*: anything not in the table. A replicated space refuses it with `ERR <CMD>
  changes this node's copy only, so a space replicated with Raft refuses it`. No
  command is local now (TODO 688 took the last, bare `KSPACE`, which is a syntax
  error and is class server); the default stays so a new write command that isn't
  classed is refused rather than written to one member, and `TestClusterWrites`
  fails on it.

`FLUSHALL` and `CLEARALL` clear every space on the node but `configuration` and, in a
cluster, `cluster`, which holds the members and groups (TODO 680). A replicated space
is cleared through its group, as `FLUSHDB` clears it, so it takes the node that leads
it: on a node that doesn't lead one of them the command answers `NOTLEADER` for that
one before clearing anything. With replicated spaces led by different nodes no node
can run it, and `FLUSHDB` in each space is the way. A space that isn't replicated is
the node's own and is cleared there only. A function run from the log can't run them.
A clear applied from the log forgets the member's cached id blocks, as the leader's
clear does.

A new write command is local until someone looks at it and lists it. `CLUSTER WRITES`
answers every write command with its class, and `unlisted` after one that's local only
by default. A command decided per subcommand gets a line for each subcommand that
differs from it, such as `INDEX/CREATE logged`. `TestClusterWrites` fails if any command is unlisted, or if a replicated one
has no sample in the test. It runs each replicated command on a leader and compares
every member's digest after it, runs each logged one and checks what every member
answers for itself, and checks each local one is refused.

`CLUSTER DIGEST` counts a hash, list or ordered set by what it holds: its fields and
values (sorted), its items, its members with their scores. Before TODO 668 it counted
only their names, so a member missing an `HSET` matched the others.

**Paths that aren't commands.**

| Path | In a replicated space |
|---|---|
| `shard::setBufferAt`'s in-place write (setBufferAt, setInt32At/64At, setF32At/64At, incrInt32ByAt/64ByAt) | goes through the write path instead (TODO 662) |
| a call to a stored function, by name or `CALLF` (TODO 672, 673) | gated like a builtin, by its header. One that may write goes in the log as a command entry, and every member runs it (see [Functions from the log](#functions-from-the-log)); `"replicate": "effects"` runs it on the leader instead, its writes replicated one by one. One with `"write": false` is a read, and can't write at all. Before 672, a call ran on whatever node it was sent to, read that node's copy without the read rules, and on a follower had its writes refused one by one |
| Luau `barch.store.*` and space-handle writes | `set`, `remove`, `setJson`, the `*At` writers and handle writes go through the write paths; `load` (`load_stream`) is refused |
| `staged` batches (FS, GRAPH, DIR, LOADFS, LOADKEYS, function syncs) | one batch entry per group; see [Writes that land together](#writes-that-land-together) |
| the FS source cache (`FS FETCH`, a read of a source-backed path) | the fetched file is a batch; its LRU entry, byte count and miss marks, and an eviction's removals, are single-key writes, so they reach every member but not as one |
| range sharding's rebalance sweep (`range_index::sweep` on the maintenance thread) | doesn't run: each member would move keys between shards on its own schedule, and in a space split over several groups a move can carry a key into another group's run of shards. A replicated range-sharded space keeps the layout its log gives it, and can become uneven |
| eviction | only expired keys, which every member removes at the same moment; no key is evicted for memory |
| `shard::remove_unlogged` | only called by `remove`, which then commits |
| defrag (`erase_page`) | moves leaves, not data |
| loading files, installing a copy (`RETRIEVE`, snapshots) | the cluster's own way of bringing a member up to date |

> **Adding a write path:** change shards only through `insert`, `update`, `remove` or
> `clear_space`, or refuse the path when `has_raft()`. A new write command goes into
> `src/raft_writes.cpp`, and a replicated one gets a sample in
> `test/clusterwritestest.py`.

### Commands every member runs

Built in TODO 669. Some writes change a node's settings, or what it builds from the
data, rather than keys: a space's options, its dictionary, how a value is stored, its
indexes. Gathering their key writes would miss the rest, so the command itself goes in
the log.

On the leader, `rpc_caller::call` sends a logged command to the space's group as a
command entry instead of running it. Every member runs it as the entry commits, in log
order, the leader too, as the server and with Raft's checks off: it is already in the
log, and what it writes is each member's own (an index's paths in `<space>_ix`, its
definition in `configuration`, the dictionary). The leader's own run is the reply the
client gets. A command that fails, fails the same way everywhere and that is its reply.
A member that can't run it at all is rebuilt from the others.

While the command commits, each of the space's shards counts it as a committing
single-key write (`abstract_shard::command_begin`). Its run may need their latches, and
a write that holds a latch through its own commit would otherwise wait for an entry
behind this one. That write answers `TRYAGAIN` instead, as it does for a single-key
write in flight (TODO 632).

A node that restarts applies its log again from its last snapshot, so it runs these
commands again. Each is safe to run twice: a second `INDEX CREATE` answers that the
index exists, `TRAIN` on a trained space answers 0, and the options, `COMPRESS` and
`DECOMPRESS` end where they already are. A new logged command has to be safe this way.

Training is deterministic, so every member trains the same dictionary from the same
`TRAIN` calls. `COMPRESS` uses each member's own dictionary, which is the same one.

A member that copies a space from another node, from a snapshot or for a rebuild,
gets the data but not the index paths. After the copy it builds each index again,
every chain its definition says is built (`pindex::rebuild`). `TestClusterIndex`
checks this. Without the rebuild, a member back from a snapshot didn't find the keys
the snapshot brought.

A space split over several groups logs the command in its first group, and that
group's members run it.

### Bulk loads

Built in TODO 681.
- `LOAD` puts back the files of the node that runs it, and `RETRIEVE` copies the space
  from another server. Run on every member they'd differ, so each is a command entry
  that only the member leading the group when it applies the entry runs. Every other
  member, applying the entry, waits until the leader has applied it, copies the space
  from the leader there and then (`group_rt::copy_replaced`, with the same `copy_from`
  a rebuild uses), and goes on from the next entry. Entries after it are records, the
  same again over the copy, or runs, which the copied `raft.ran` marker skips. The
  marker is never moved back. A rebuild instead didn't work: it applied the log again
  from the last snapshot over the copy, so records from before the LOAD brought back
  keys it had taken away, and the member went round for good.
- `RELOAD` saves each shard and loads it straight back, the data as it was, so it's
  the node's own storage: class server, run where it's sent.
- `IMPORT` replays an export's commands, each through Raft's gate, so it's
  replicated as the same commands from a client would be, not as one unit. In a
  replicated space it answers `NOTLEADER` up front on a follower and stops at the
  first command that fails.
- `UNLOAD` changes no data, but a replicated space refuses it: an entry its group
  applies while it comes back can land on files the save of the instance going away
  hasn't reached.
- A batch entry is cut at 16MB (`BARCH_TEST_BATCH_BYTES` for tests): each piece lands
  whole, in order, so a LOADFS of a big directory is several entries.
- `REPLAPPLY`, a replicated space taking writes from an outside primary: see
  [A primary's stream](#a-primarys-stream).

### Queues

Built in TODO 682. A queue kept its messages in a ring file on the node it was pushed
to, and that node's consumer handed them out, so a replicated space refused `QUEUE`.
Now a queue whose space (the declaration's `space`) is replicated keeps its messages
in that space, and every change to them goes through the space's log:

- The messages are meta keys under `queue/<name>/`: `head` and `tail` (the queue is
  empty when they're equal, and a sequence never goes backwards), `m/<seq>` for a
  message, `a/<seq>` for how many deliveries of it have started, and the dead letters
  as `d/<n>` between `dhead` and `dtail`. They travel with the space: snapshots,
  copies and rebuilds take them along, and `CLUSTER DIGEST` and `DBSIZE` count them.
  `FLUSHDB` clears them with the rest.
- A push, a delivery started, done or failed is a command entry whose first word is
  `\x01QUEUE` (`mq::log_word`). Every member applies it to its own copy, and it
  carries what it needs (the name, the sequence, `max_attempts`), so a member never
  looks at anything but the space and the entry. A push answers once a majority holds
  it in a synced log, which is more than the ring file's `each`, so a replicated
  queue's `durability` doesn't apply. On a member that doesn't lead the space a push
  answers `NOTLEADER`. A client can't send a name starting with `\x01`.
- Only the member leading the space hands messages out. Before the handler runs it
  logs the start, which counts the attempt, so a delivery that a crash or a failover
  cuts short counts the way the ring file's markers count it. Then it logs done, which
  removes the message, or failed, which dead letters it once `max_attempts` deliveries
  have started. Each checks the message is still the head, so a leader that peeked its
  copy before catching up changes nothing and looks again.
- A handler that runs from the log (the default for a function that may write) is
  called through it (`functions::call_as_logged`), so every member runs it once and
  its writes land whole. The handler and the done entry are two entries, so a leader
  lost between them leaves the message for the next leader: at least once, as before.
- A function run from the log can push to a queue on its own space: every member makes
  the same push to its copy. A queue on another space, or on one that isn't
  replicated, is refused there, since every member would push it.
- Messages a node held in its ring file from before the space was replicated are moved
  into the log by that node once it leads the space, eldest first.

A queue on a space that isn't replicated is unchanged: a ring file on the node it was
pushed to, handled there.

### Repository syncs

Built in TODO 683. A repository sync (`FUNCTIONS SYNC`, and every node's timer) writes
the spaces its repository fills, `configuration` for a package's settings and
dependencies, and `repository` for its libraries. Every node syncs on its own timer,
and only the member leading a replicated space can write it, so before this every
sync failed in part on each follower, and `FUNCTIONS` as a whole was refused in a
replicated space.

- A sync writes a replicated space only where this node leads every group of it, and
  leaves it to that member everywhere else (`led_elsewhere` in function_sync.cpp).
  The leader syncs the same repository on its own timer. That covers the functions
  and keys, `as = fs` imports, a package's settings and dependencies (`configuration`,
  the cluster group's), its libraries and pins (`repository`, if it's replicated), and
  its hooks, which run once, where their space is led. A space that isn't replicated
  is each node's own and is written there, as before. The HTTP server a package
  starts is each node's.
- What a sync wrote to a space is kept in the space as well as in memory: the meta
  key `sync.managed` holds the repository's name and the keys it wrote, so a member
  that leads the space later removes what the repository dropped. A follower also
  notes what the leader will have written, from its own checkout.
- `FUNCTIONS STATUS` shows `leader_elsewhere=<spaces>` for what the last sync left to
  other members. A manual `FUNCTIONS SYNC` writes what this node leads and, when it
  left something, answers with that space's `NOTLEADER`, so the client can send it
  there. Running it again is harmless.
- Spaces led by different nodes can be at different commits for up to one poll
  interval, since each leader fetches on its own timer.
- `FUNCTIONS SYNC` is refused in a function run from the log: every member would
  fetch.

### A primary's stream

Built in TODO 684. A barch outside the cluster can `PUBLISH` to a member, which takes
the stream with `REPLAPPLY`. `REPLAPPLY` applied records under `repl::applying`, which
is how a Raft member applies its log, so it skips Raft. The records name their own
spaces, so the gate, which looks at the connection's space, never saw them either. A
primary publishing into a replicated space wrote the receiving member's copy only, and
the members went apart without a word. The docs said such a stream was refused; it
wasn't.

- The member the primary publishes to stays the replica: it keeps the positions
  (`repl_positions.dat`) and does every check it did. The records of a replicated
  space go into that space's log as batch entries, as they came, in runs, and every
  member applies them with `repl::apply_one`, which is `REPLAPPLY`'s own apply. A
  space that isn't replicated is applied on the receiving member, as before.
- A group the receiving member doesn't lead gets its records through its leader:
  `REPLRAFT <space> <record>...` over the barch port, which the leader puts in its
  log, and the leader's answer is the answer. It doesn't forward again.
- The position moves past a run only once the log has it. A refusal that passes (an
  election, a leader out of reach, `TRYAGAIN`) answers `HOLD`, so the primary keeps the
  batch and sends it again, and what went in is skipped by sequence. Any other
  refusal is `REPLFAILED`, as before.
- A member's copy of a space from another member (joining, a rebuild, a snapshot,
  the copy after a logged LOAD or RETRIEVE) is marked `repl::raft_copy`, and RETRIEVE
  leaves the positions alone for it. Before, a member that joined took the member it
  copied from for the space's primary, and refused the real primary's stream: "a
  space can only have one".
- Records are placed and checked before they go in the log, as `REPLAPPLY` checks
  them, so a container entry from a primary with another layout is refused rather
  than sent to the members, where it would fail their apply and have them rebuilt.
- If the receiving member goes down, the primary keeps its writes and sends them
  again once it's back, as with any replica. The positions are that member's own.
- Besides records, a primary sent two kinds of plain command (TODO 686). A call to a
  stored function that declares write and data went too (TODO 188), from before a
  function's writes were records; it was an unknown command on the barch port, whose
  table has builtins only, and the sender ignores a plain command's error, so its
  writes arrived as records and nothing else. It isn't sent any more: had it run, it
  would have written twice. A cached miss makes no record, so it went as
  `FOREIGN_MISS <key>`, without its space and with the key already stored: a replica
  ran it in whatever space its session was in and encoded the key again. It goes as
  `REPLMISS <space> <key> <deadline>` now. In a replicated space that's a miss entry
  in the log, through the leader (forwarded once, marked `leader`); elsewhere it's the
  replica's own tomb. The primary doesn't wait on it: a miss lost is a key fetched
  again.

### Foreign sources

Built in TODO 685. A space with a foreign source fills a key it doesn't have from the
source on a read, and keeps a miss as a tomb leaf for `missing_ttl` seconds.

- The value a fetch brings is written like any other, so in a replicated space it
  was already committed through the log. A miss wasn't: `insert_cached_miss` puts the
  tomb in the tree directly, so it was on the leader alone, and saves and copies
  carried a tree the members didn't share. Now a miss a fetch finds in a replicated
  space goes in the log as a command entry, `[\x01MISS, key, deadline]`, with the
  leader's deadline in ms since the epoch, and every member stores the same tomb
  (`foreign::miss_from_log`). It's logged before the fetch settles, outside the
  shard's latch, since every member's run takes it. A key written meanwhile stays: a
  cached miss never covers a live key. `FOREIGN_MISS` from a client or a primary is a
  logged command too.
- Only the member leading the key's group can keep what the source answers, so a
  follower asked through a follower read doesn't ask the source: the read answers
  `NOTLEADER`. It used to ask and then fail to write the answer.
- `FOREIGN FAKE` is the test source, each node's memory. `SET`, `DEL`, `FAIL`, `DELAY`
  and `RESET` are logged, so every member's source is the same and a new leader
  reads what the old one did. `QUERIES` (this node's count) and `PARTS` answer on any
  member.
- A real source (`luau`, `mysql`, `postgres`) is outside barch; each member reaches it
  the same way, and only the leader fills from it.

### Web servers

Built in TODO 687. `HTTP START`, `STOP` and `STATUS` run, stop and report this node's
web server for a space, so they're class server and answer on any member; each node
serves on its own port. A handler runs Luau in the server's own VMs, not from the log:
its reads answer from the member's copy, and on the leader its writes are replicated
one by one, as an `"replicate": "effects"` function's are.

On a follower a write can't be made, and a handler's first one used to fail the
request with a 500, sign-on included. Now:

- a request that isn't GET or HEAD (or an OPTIONS preflight) is answered `307` to the
  same url on the leader's web server, before a handler runs. A 307 keeps the method
  and the body;
- a GET whose handler tried to write gets the same `307`. The refused write threw
  before anything was written, so the leader runs the request from the start. A
  handler that reached outside barch (fetch, mail) before its first write does that
  again on the leader;
- the leader's web server is asked of the leader, `USE <space>` then `HTTP STATUS`
  over the barch port (`repl::ask`), and kept until the leader changes or for 30
  seconds. When the leader serves no web for the space the answer is `503`.

Static files (`kind = "files"`) are read-only and answer on any member.

### Users

Built in TODO 676. A node's users, their secrets and the per-space rules `KSPACE ACL`
writes are its auth file, which nothing replicated: a user made on one node couldn't
log in on another.

- In a cluster the users are the cluster's: one meta key, `auth.users`, in the
  `cluster` space. Clients can't read it (a meta key is in no walk and no command can
  name it), so the secrets in it stay hidden, and it goes wherever that space goes:
  snapshots, joins, rebuilds.
- `CLUSTER INIT` fills it from the first node's users, so they carry on. A cluster
  from before this fills it from the cluster leader's own users on the first change.
- `ACL SETUSER`, `ACL DEL` and `KSPACE ACL ... SETUSER|DEL` go in the cluster group's
  log, after their syntax and categories are checked, and every member's run changes
  the key (`route_users_change`). `KSPACE ACL` with no space names the connection's,
  spelled out before it goes, since the run is in `cluster`. On a member that doesn't
  lead the cluster group they answer `NOTLEADER`, as `configuration` writes do.
- Everything that reads users (`AUTH`, `HELLO`, the rights a connection, an HTTP
  request or a cron job runs with) reads a copy in memory, built again from the key
  whenever the key changes. The node's own auth file stays on disk as it was, and is
  what the node uses outside a cluster.
- `get_auth` only uses the `cluster` space once it's open (`loaded_keyspace`):
  opening a space builds a caller, which reads the users, and the first version
  opened `cluster` from inside its own opening, until the stack ran out.
- `default` is the cluster's default user.

### Transactions

Built in TODO 677. A transaction is a mark on the whole space, not a client's own:
BEGIN marks it, every client's writes go on landing, ROLLBACK puts the space back to
the mark and COMMIT drops it. All three are logged commands, so every member marks and
goes back at the same place in the log, and a new leader has the mark too.

The mark is in memory. What keeps it right across restarts and copies:
- A save during a transaction writes the space as it was at BEGIN (TODO 678), the
  view a page walk in a transaction reads, so a copy gets that too.
- The group makes no snapshot while one of its spaces is in a transaction: a snapshot
  says its files hold everything up to its index, and they hold the BEGIN view. The
  last snapshot is from before BEGIN, so a member that restarts, or copies the space,
  starts from the BEGIN view and applies BEGIN and everything after it from the log,
  mark included. The log grows until the transaction ends.
- BEGIN also writes a meta key, `tx.open`, after the mark, which ROLLBACK takes away
  with the rest and COMMIT removes. A member with the key and no mark answers its
  ROLLBACK by saying so (`cluster::mark_diverged`) and is rebuilt from the others. The
  test didn't need it; it's there in case a member loses the mark some other way.

Doing this found two bugs that weren't the cluster's, on the build from before 662
too. A space saved during a transaction couldn't be loaded again (TODO 678), and a
ROLLBACK in an unordered space left its hash index from inside the transaction, so a
key the transaction added read freed memory (TODO 679). `TestTxRestart` covers both on
a single barchd.

### KSPACE across spaces

Built in TODO 670. KSPACE's subcommands name the space they change, which isn't
always the connection's, so `rpc_caller::route_kspace` finds it and routes by it,
before Raft's usual gate:
- `MERGE <from> INTO <to>` writes `<to>` through the shard write paths. In a
  replicated `<to>` the leader runs it with its writes gathered, and commits them as
  one batch (`cluster::commit_gathered`, which `staged` uses too). `<from>` is read as
  the leader holds it, so it needn't be replicated.
- `DEPENDS <dependant> ON <source>` and `RELEASE <source> FROM <dependant>` change how
  the dependant reads, which is each node's own setting: a command entry in the
  dependant's group. A read that falls through to the source reads each member's own
  source, so a source that isn't replicated can answer differently on each.
- `DROP <space>`: a command entry in the space's group; every member drops it.
- `OPTION SET`: a command entry in the connection's space's group, as before.
- `ACL`: the cluster's users, through the cluster group's log (TODO 676, see Users).

A target that isn't replicated runs here, as it always did, whatever the connection's
space is.

### Functions from the log

Built in TODO 673, from the user's suggestion. A stored function that may write was run
on the leader, and its writes committed one key at a time. That kept the members the
same, since they only ever applied the leader's results, but a function wasn't a unit:
a leader failing partway left part of it in, and two calls doing
`set(k, get(k) + 1)` could both read the same value. With 16 clients doing 1600 calls,
370 counted.

Now a call to such a function is a command entry, like the logged commands above:
every member runs it as the entry commits, in log order, and the leader's run is the
reply. The log orders the calls, so the 1600 calls count 1600, and an entry is in the
log whole or not at all. A run reads and writes the tree directly (it's applying, so
its writes aren't committed again), so it sees its own writes.

**Doing the same thing everywhere.** Every member's run has to make the same writes.
- No wall clock: the run is inline on the commit thread with no deadline, and stops
  after `function_log_insns` counted interrupts. Native frames keep the interrupt hook
  during such a run, so they're counted too.
- `math.random` is seeded from the entry's index.
- Calls that reach outside the data are refused (`foreign::refuse_from_log`): http,
  resp, mail, sql, `barch.queue` to a queue on another space or on one that isn't
  replicated, source fetches, the storage internals
  (`stats`, `freeList`, `leaves`, `nodes`, `save`, `load`), other spaces (checked on
  every use of a handle, since Luau resolves a constant `barch.space.x` once, when the
  function loads), and through `barch.call` anything that isn't a read, a replicated
  write or a logged command (`functions::refused_from_log`).
- Inside a run, `rpc_caller` skips Raft's gate: the run is already in the log, and a
  command it calls runs on this member like the rest of it.
- Module sources come from the store at the run's point in the log. A module in
  another space (another group, or the globals) is read wherever that space is on
  this member, so keep a function's modules in its own space.

`"replicate": "effects"` keeps the old path for a function that needs any of these.

**The same code, the same rights, on every member (TODO 667).** The runner makes a
caller of its own for every entry. A kept one held functions it compiled for earlier
entries, so after a SETF without RELOAD a member just restarted ran the new source
while the others ran the old: `TestClusterSideEffects` saw `A, B, A`. A new caller
compiles what the store holds at that entry. The entry also carries the caller's user
and its rights in the space, as the leader worked them out; users are each node's
own, so every member runs with those rights rather than its own idea of the user.
Before this the runs had the server's rights, so a user who could call functions but
not write the space could write it through one.

**What an install does besides the write (TODO 667).** SETF and REMF are logged
commands, so every member installs the function: the exposed-command cache, the
server's kept states, the AOT flag, the cron and queue rescans, and RELOAD's publish
all happen everywhere. A function key that arrives any other way, as a record or in a
batch (LOADKEYS, a function sync) or in a copy, runs the same after-effects
(`functions::function_keys_changed`, from `apply_record` and `copy_from`). RELOAD on a
batch path (FS PUT, LOADFS, LOADKEYS, a function sync) is a logged publish
(`functions::publish_everywhere`), so a follower's session picks the new code up too.

**Cron.** Every node's scheduler sees a job. For a replicated target space only the
node leading its group runs it, through the log as the job's user
(`functions::call_as_logged`); the others leave it.

**Once per entry.** A member that restarts applies its log again from its last
snapshot, over files that may be ahead of it. Records are absolute, so that's fine for
them, and the step 2a commands happen to be safe to repeat, except `TRAIN`, which adds
samples. A function's increment isn't. So each command entry's run writes a meta key,
`raft.ran`, holding the group's number and the entry's index, and an entry at or below
it is skipped. That only works if the files hold all of a run or none of it:
- a run counts as a cross-shard write (`note_cross_shard_write`), so no shard saves on
  its own after it, only a whole space freeze;
- a whole space freeze that finds a run under way (`key_space::log_run`) lets its
  shards go and waits for it. A run that starts after the freeze took the shards
  can't write until it lets go.
- a run that saves the space itself doesn't wait for itself: `TRAIN` keeps the
  trained dictionary durably with a whole save, from inside its run. That save holds
  part of the run, which is harmless for `TRAIN`, since running it again on a
  trained space does nothing. A new logged command that saves has to be the same.

The first version held a lock across the run and had the freeze take it before the
shards. Under load that waited on itself until NuRaft's request timeout: callers had
raised their shards' committing count, the freeze waited for it with the lock held,
and the commit thread waited for the lock. 960 calls took 324s. Nothing waits for a run
while holding anything now; the same calls take 0.8s.

`TestClusterLogFunctions` checks the count, the random values, the refusals, the cap,
and a follower saved and restarted. Without the skip, that follower came back with
1711 instead of 1650. A key-by-key copy (members with different page layouts) walks
the space without a freeze, so it can take part of a run; that's TODO 674.

### Writes that land together

Built in TODO 665. A file write is several keys: the chunks, then the inode, then the
name. When those went to Raft one key at a time, a leader that failed part way left
every member with some of them. An overwrite keeps the file's id, so its new chunks
landed under the old inode, and the file read as neither version
(`TestClusterBatchCut` shows this on the old code).

Anything that writes through `staged` now lands whole. In a replicated space, while
`staged` applies its writes the shard write paths gather them instead of committing
them: each builds its record and takes the write back out under the latch it went in
under, so no reader sees it. `staged` then sends the records as one batch entry. When
it commits, every member applies the whole batch, the leader included. A refused
batch changed nothing anywhere, so nothing needs putting back.

A stored function in the batch can require a module installed by the same batch, as
`LOADKEYS` of a directory with modules does. While the batch is being gathered,
`require` sees the batch's functions first (`functions::gathered_sources`), since the
store won't have them until the batch commits.

Other writers don't wait for a batch, so a write to one of its keys from another
client can land before or after the batch, in log order, the same on every member.
Readers can see a batch part way through its apply, on any member, as with
`staged` everywhere: the promise is that it lands whole, not that it's isolated.

**A space split over several groups (TODO 666).** It used to get one batch per group,
committed in turn, so a writer that failed between two left the earlier groups' part
in. Now a batch over several groups is rolled forward:

- Every group's log gets the same entry, `entry_version_xbatch`: the batch's id, the
  groups it's in, and all of its records. Each member applies the records of its
  group's shards. The writer sends it to each group in turn, through that group's
  leader when the writer doesn't lead it (`CLUSTER XFORWARD`), then closes it in each
  (`entry_version_xclose`).
- Once the first group has it, the batch is decided: it's never put back. If the
  writer goes before the rest have it, the leader of a group that's had it open for
  two seconds asks each of the other groups' leaders (`CLUSTER XSTATE`), sends it to
  any that hasn't got it, and closes it once all have. The writer's client hears
  `UNKNOWN` in that case; the batch lands anyway.
- A group applies an id once. Each group keeps the ids it has applied, with the index
  each landed at: the same id at the same index is the log applied again after a
  restart, and at another index a roll forward of one it already has, which is
  skipped. So a roll forward that races the writer's own entry is harmless.
- That state goes with each snapshot. It's captured before the snapshot's SAVE, so it
  never says more than the files hold, and kept in the group file (a record beside the
  snapshot's, written in the same sync). A snapshot sent to a member carries it after
  the first line of its first object, which an older build ignores. Closed ids are
  dropped after an hour; a roll forward is only asked for while one is open.
- `XFORWARD` and `XCLOSE` are signed with the cluster secret, since a forward puts
  records in a log.

**Commands from the log in a split space (TODO 689).** A command entry went in the first
group's log, and every member ran it there, so its writes to the other groups' shards
were made outside their logs, at a different point on each member against those
groups' own entries (`TestClusterSplitLog` saw three members with three digests). Now:

- a stored function that runs from the log, `SETF` and `REMF` run on the node they
  were sent to, with their writes gathered, and land as one batch across the groups,
  as above. Reads of a group the node doesn't lead are its own copy, as a follower
  read. A batch whose writes are all in one group this node doesn't lead goes through
  that group's leader the same way;
- a cached miss (TODO 685) is logged in the group that owns its key;
- commands that change settings or derived state (`KSOPTIONS`, `TRAIN`, `COMPRESS`,
  `DECOMPRESS`, `DICTIONARY SET`, `INDEX`, `KSPACE OPTION SET`, `DEPENDS`, `RELEASE`,
  `DROP`) stay logged in the first group: they write no keys of the others;
- `BEGIN`, `COMMIT`, `ROLLBACK`, `LOAD`, `RETRIEVE` and queues act on the whole space,
  and a split space has no one point in its log that every group shares. They go group
  by group instead (TODO 690):
  - `BEGIN`, `COMMIT`, `ROLLBACK`, `LOAD` and `RETRIEVE` go in every group's log, in
    turn, and each group's run acts on its own shards only, at its own point in its
    log (`cluster::run_shards`, set around the run). Members agree, since each group's
    point is the same on every member. A write racing the `BEGIN` can fall inside the
    transaction in one group and outside it in another, as a racing write can anywhere.
    `BEGIN` sent again leaves a group already in the transaction as it is, so a client
    can finish one that stopped part way.
  - Each group keeps its own "transaction open" key and its own run marker
    (`raft.ran/<group>`), in its first shard (`meta::name_in_shard`). One key for the
    space was written by every group's log, outside the log of the group whose shard
    held it.
  - A group's `ROLLBACK`, `LOAD` and `RETRIEVE` don't checkpoint the space's change log:
    one group's shards saved isn't the space at one point. A split space with a change
    log keeps the whole log until a save of the whole space.
  - `RETRIEVE` receives every shard and each group installs its own; a member that
    doesn't lead the group copies the group's shards from its leader, waiting until the
    leader has applied the entry (`CLUSTER APPLIED <group>`).
  - A queue's keys all go in one shard, its `head` key's (`name#<n>`, the smallest suffix
    that lands there), and its entries go in that shard's group's log; its consumer runs
    on that group's leader. A shard moves to another group whole, so the queue moves
    with it.
  - A command entry for a group this node doesn't lead goes through that group's leader
    (`CLUSTER XCOMMAND`, signed with the cluster secret), which answers with the
    command's error or its first result. Only in a split space: elsewhere a member that
    doesn't lead answers `NOTLEADER`, as before.

A new file's id in a split space is a write of its own, to the shard holding the space's
id counter, which only that shard's group's leader can make: an `FS PUT` of a new file
on another node answers `NOTLEADER` for it, as any write to a group led elsewhere does.
Forwarding that write would let two nodes hand out the same ids, since it reads the
counter first.

## Securing the Raft ports

Built in TODO 620. NuRaft's listener takes any connection. Before this, anyone who
could reach a Raft port could send the group a vote with a higher term and unseat
its leader, or send it entries for its log. A test showed the first: one forged
vote request moved a follower's term from 3 to 103.

**The shared secret.** Every node of a cluster is given the same `cluster_secret`.
Every Raft request carries an HMAC-SHA256 of itself, keyed with the secret, in the
metadata NuRaft lets a message have. That covers its header and each entry's term,
type and payload. A node checks it once the request has been read, and if it
doesn't match, the request goes no further and the connection is closed. Responses
are signed too, over their own header and their request's. NuRaft checks a
response's metadata before it reads the rest, so a response's context isn't
covered.

- A group won't start without a secret, so a member restarted without one doesn't
  start, and says why. `CLUSTER INIT` and `CLUSTER JOIN` are refused without it.
- `CLUSTER ADMIT`, which a joining node sends the leader over RESP, carries an
  HMAC of its arguments. The leader refuses a node that can't make one, because
  once admitted, a node is sent every entry of every group. That doesn't depend on
  the node checking signatures itself.
- A node with the wrong secret gets nothing from the others, and they get nothing
  from it. Each group's line in `CLUSTER INFO` shows `refused N` once it has
  refused a message, and the log says so for the first one and every thousandth
  after that.
- `CONFIG GET cluster_secret` answers `(set)` or `off`, never the secret, and
  setting `(set)` changes nothing, so writing back what was read is safe. The log
  shows `(a secret)` in its place.
- The secret is read when a group starts. Changing it means restarting every node
  with the new one, since nodes with different secrets can't talk.

The signature proves who sent a message and that it wasn't changed. It doesn't
hide it, and it doesn't stop an old message being sent again. Raft takes the same
entries twice without harm, and a vote for a term that has passed is refused
anyway. TLS covers both.

**TLS.** With `raft_tls on`, every Raft port speaks TLS, using
`tls_pem_certificate_chain_file` and `tls_private_key_file`. A node checks a peer's
certificate against `raft_tls_ca_file`, or against its own chain when that's
`off`, which suits one certificate shared by every node. The server side doesn't
ask a client for a certificate: the secret is what proves a client belongs. Every
node has to agree on `raft_tls`. A certificate or key that won't load stops the
group from starting, with the reason in the log.

Barch makes the TLS contexts itself rather than leaving it to NuRaft, so it can
check every certificate in them once before any handshake (TODO 629). OpenSSL fills
in a certificate's cached fields the first time it checks one. Two handshakes
verifying against the same CA at once raced on that: harmless by OpenSSL's design,
but TSan reported it in about one cluster run in three. Filled in up front, there's
nothing left to fill.

**Still open.** The calls nodes make to each other over RESP, apart from `ADMIT`,
are made as the `default` user without TLS: heartbeats, `EXPORT` and `RETRIEVE`
for copies, and `HANDOFF`. Locking down the `default` user's rights stops them.
NuRaft binds the Raft ports on every interface, so a firewall is still worth
having.

## Adding a member

A node joining copies each space with `RETRIEVE`: the `cluster` and `configuration`
spaces from the leader of the `cluster` group, and a data space from whichever node
its heartbeats say leads it, or else its creator. It then starts the group waiting
to be added. The group's leader adds it as a **learner**, which receives the log
but doesn't count towards a quorum, so a node that's slow to catch up can't slow
down commits or cost the group its majority. The leader makes the learner a voter
once it's caught up: either within a few entries of the leader, or at the index the
leader had up to 3 seconds earlier (TODO 653: a member on a slow disk, its syncs
taking a second, is always further behind than the previous tick). Under steady writes
a learner that's keeping up is always a batch or so behind, so the first rule on its
own could leave it a learner for as long as the writes go on. Writers carry on
throughout:
the copy is taken under the CoW freeze, and records are absolute, so applying
entries the copy already has changes nothing in the end.

### Snapshots

A snapshot at index *i* is the group's spaces as this node's data files hold them.
Every `raft_snapshot_entries` entries, NuRaft asks for one. The node saves each of
the group's spaces with `SAVE` on a thread of its own, then records *i* and the
term and configuration then in the group file. A single-key write is only in the
tree once it has committed (TODO 632), and a composite one holds its shard latch
until it commits, so the files hold only committed writes, and they hold at least
everything up to *i*, perhaps more. NuRaft then compacts the log behind *i*, but
not past what a member that's answering still needs (see below).

After a restart, the group starts applying at the entry after the last snapshot.
The files may already hold some of those entries, and applying them again changes
nothing.

A member behind the start of the leader's log, because it was down while the log
was compacted, needs a snapshot. The leader sends it three objects. The first names
the leader, and the member starts copying the spaces from there with `RETRIEVE`,
which takes them under the leader's CoW freeze, so the copy also holds at least *i*.
The second names it again, and the member keeps asking for it until the copy has
worked. The third carries nothing and ends the snapshot. The member then records
*i* as its own snapshot and carries on from the log.

The copy runs on a thread of the group's own (TODO 628). NuRaft holds its lock while
it hands a member a snapshot object, so a copy made there kept the member from
answering anything else for as long as it took; a leader with only that member to
make a quorum with lost its lease. Now, each time an object comes, the member starts
the copy, finds it still going, or finds it done.

The member waits on the second object rather than the first (TODO 636). While a
transfer is at its first object, the leader sends its newest snapshot each time, so
a copy that took longer than the leader took to make another snapshot was never of
the snapshot being sent. Under steady writes the member copied one snapshot after
another and never caught up: under TSan, a joining node copied 77 in a row. Past the
first object, the leader keeps to the same snapshot.

For the member to carry on from the log once its copy is done, the log after *i*
has to still be there, and so does the snapshot's own last entry, whose term NuRaft
sends along with the next. So the leader doesn't compact past a snapshot it has sent
within the last second, nor past the last entry a member holds, as long as that member has answered within the last second. The first takes
effect as the snapshot is sent. The second is worked out on the cluster's tick, up to
250ms late, and a snapshot made in that gap used to take the log a copy had just
started for. Either way, it holds back by at most 10 snapshots' worth of entries
(`raft_snapshot_entries` × 10). A member that's down, or that's further behind than that, holds nothing and
gets a newer snapshot. `CLUSTER INFO` counts the compactions held back as
`log_holds`. A copy that takes longer than the writes it would take to fill those
10 snapshots still ends behind the log. The leader then sends another snapshot.

A member reads an older leader's two-object snapshot (tagged `barch-snapshot-1`) the
old way. An older member can't read a newer leader's, so upgrade members before the
leader.

A group that's stopping (a shutdown, a rebuild, or the group being removed) starts no
new copy and no new save once it has begun to stop (TODO 638). It waits for the ones
already running, but NuRaft keeps running until the group's stop is done, and keeps
asking. Before, each time it asked during that wait it found no copy going, so it
started a second copy into the same shards as the first, and nothing waited for that
one.

If the copy fails, the member asks for the first object again, after a short pause.
If the leader has gone, the next leader sends a snapshot of its own. The copy can't
wait for the last object (TODO 627): NuRaft compacts the member's log before it
applies a snapshot, and stops the process if the apply then fails. With a single
object, a leader restarting while a member copied from it took the member down too.

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

The walk used to be SCAN's, over the live shards: a key written or erased during it
may or may not come over, and records are absolute, so applying the log over the copy
ended where the log does. That stopped being true for runs from the log (TODO 673):
a run is several writes and a marker (`raft.ran`) saying it's in, and a live walk could
carry some of a run's writes and not the rest, with the marker from before or after
them. The log applied over the copy then ran that function again over half its writes,
or skipped what the copy was missing (`TestClusterLogicalCopy` saw a node's 20 counters
split 411 and 412, once in three runs).

So the copy is taken at one point now (TODO 674). The receiver asks `CLUSTER
EXPORTSTART <space> <from> <to>`; the source waits out any run under way, as a save's
freeze does, and under the space write lock reads the shards' keys into a list in
memory, a page per shard page, and answers with an id. `CLUSTER EXPORT <space> <shard>
<page> <id>` serves the pages from that list and `CLUSTER EXPORTEND <id>` drops it (one
left behind goes after ten minutes). Writes wait only for that local walk, not for the
copy, and the source holds the space's keys a second time, in memory, while the copy
runs. A source from before this answers `EXPORTSTART` with an error, and is paged live
as before.

A logical copy is larger and slower than `RETRIEVE`, and in a cluster where every
member has the same layout it never runs.

## Routing and read after write

A client finds where a replicated space is led with `CLUSTER ROUTES`, or from the
redirect it gets when it asks the wrong node, and sends its commands there. Every
node is in every group, so any node can answer either. The client follows a
redirect itself; nodes don't forward commands.

- **Epochs.** A route's epoch is the group's Raft term. It goes up every time the
  group's leader changes, so a client with two answers for a space keeps the one
  with the higher epoch.
- **Writes** go to the leader, and get `NOTLEADER <host>:<port> <epoch>` anywhere
  else.
- **Reads go to the leader too, by default, and the leader answers them only under
  its lease.** A leader answers a read only while a quorum of voters, itself
  counted, has answered it within the last 300 ms, which is less than the shortest
  election timeout (400 ms). So no other node can have been elected leader while it
  still answers. Otherwise it answers
  `TRYAGAIN no quorum has confirmed this leader within its lease`. It works this
  out at most every 50 ms, and works it out again after a pause, so a leader that
  was stopped and resumed can't answer from before the pause. The lease counts
  from when an answer arrived, and a follower restarts its election timer when it
  sends one, after its disk sync, so the two line up.
- **Stepping down.** A leader steps down when a quorum hasn't answered it for twice
  the longest election timeout (1.6 s), not for the lease's 300 ms (TODO 641). A
  follower answers new entries only once they're synced. On a slow disk (206-239 ms
  a sync on CI's runners) the 300 ms made a leader step down, cancel the writes in
  flight (`UNKNOWN`), and leave the group without a leader for half a second. Now it
  keeps leading through that, and reads get `TRYAGAIN` until the lease is good again.
  The longer step-down guards nothing the lease doesn't: reads check the lease, and a
  write can't commit without a quorum. A leader that's really cut off is replaced by
  an election on the other side all the same; it just takes longer to notice.
- **Follower reads.** A connection that sends `CLUSTER READS FOLLOWER` can have its
  reads answered by a follower. The connection remembers, per space, the newest log
  index it has seen: the index each of its writes committed at, and the index a
  node had applied when it answered one of its reads. A follower answers a read only
  once it has applied at least that index, waiting up to 200 ms for it, and only
  while it hears from a leader. Otherwise it redirects. So a connection never reads
  anything older than what it already saw, its own writes included.
- **Across connections.** A client that writes on one connection and reads on
  another passes the index along: `CLUSTER INDEX <space>` on the first, then
  `CLUSTER AFTER <space> <index>` on the second.

> **Clocks:** the lease counts on each node's steady clock running at about the same
> rate as the others' over a few hundred milliseconds. It doesn't need the clocks to
> agree on the time.

*Later:* a range sharded space can move a key between shards while it runs
(`route_moved` in `src/key_space.h`). Once a space can span several groups, the
epoch has to change when a range moves too. Gossip for discovery isn't built; the
`cluster` space and `CLUSTER ROUTES` cover it so far.

## Spaces split over several groups

Built in TODO 615. One group per space puts every write to that space through one
leader and one log. With `<space>.raft_groups N`, set before `<space>.raft on`, the
space gets N groups. Group *k* owns the *k*-th of N even runs of the space's shards,
and is called `<space>/<k>` in routes, heartbeats and sessions. The groups' leaders
spread over the nodes like any group's, so writes to different runs commit on
different nodes in parallel.

```text
USE configuration
SET orders.raft_groups 3
SET orders.raft on
```

`CLUSTER ROUTES` lists each group with its shard run, such as
`orders/1 group 2 leader 10.0.0.2:14000 epoch 3 shards 6-11`. A client doesn't need
to know which shard a key hashes to: it sends a command to any node and follows the
redirect.

- **Writes** commit in the group of the shard they land in. A node that doesn't
  lead that group refuses the write at the shard with that group's `NOTLEADER`. A
  command that writes keys in several groups commits each one in its own group, so
  if one is refused the others may already have happened.
- **Single-key reads** are checked where they take their shard's latch, against that
  shard's group: answered on the group's leader, or redirected to it. These are
  `GET`, `GETRANGE`, `SUBSTR`, `STRLEN`, the TTL reads, and the hash, list and
  ordered set reads, except `ZDIFF`, `ZUNION`, `ZINTER` and `ZINTERCARD`.
- **Any other read**, such as `KEYS`, `SCAN`, `MGET` or `RANGE`, may touch every
  group, so the node has to be able to read all of them before it starts. In
  practice that means a connection with `CLUSTER READS FOLLOWER`. The node then
  answers once it has applied, in each group, everything the connection has seen
  there.
- **`FLUSHDB`** of a split space is refused: one record can't clear shards that
  commit in different groups.
- **Copies** for a join, a snapshot or a rebuild take only the group's own shards,
  key by key, so the other groups' shards on the node carry on following their own
  logs.
- **Indexes are per group.** `CLUSTER INDEX` and `CLUSTER AFTER` take the group's
  label, such as `orders/1`.

### Splitting a group live

Built in TODO 616. `CLUSTER SPLIT <label>`, on the leader of the `cluster` group,
halves a group's run of shards. The upper half goes to a new group, labelled with
the space's next free `<space>/<k>`, and the reply names it:
`OK orders/1 group 2`. A space in one group can be split the same way; its group
keeps its label and owns the lower half.

How the shards change hands without a write landing in both groups, or neither:

1. **Fence.** The group's leader stops taking writes for the upper half. A write
   already past that check is in the log before the next step: writes hold a shared
   lock from the check until they're in the log, and the fence waits for it.
2. **Hand-off entry.** The leader appends a control entry: these shards go to
   group *h*, whose first voters are this group's voters now.
3. **Apply.** Every member applies it at the same index. From then on the old
   group refuses reads and writes of those shards with `TRYAGAIN`, and its new run
   is written to its group file, so a restart knows it even after a snapshot. Each
   member's copy of the shards is now the same, because it's the state at that
   index.
4. **New group.** The cluster's thread on each member starts group *h* over the
   shards it already holds, with those voters, so nothing is copied, and moves the
   shards to it one latch at a time. It isn't done on the thread applying the
   entry, because a client can hold a shard latch while it waits on a later entry
   in the same log.
5. **Record.** The cluster leader lists every group's run explicitly in the
   space's record (`runs`), so a node that restarts or joins later opens the new
   layout. A group's own file wins over the record.

A group a split made is only ever started by members applying its hand-off. A node
that was never in the old group joins the new one like any group: it copies the
group's shards from its leader and catches up from its log. A node where the old
group still owns those shards waits for the hand-off instead, since joining early
would let the old group apply older writes over the copy.

Clients writing to the moving shards get `TRYAGAIN` for as long as the new group
takes to elect its first leader, typically under a second, and then `NOTLEADER`
pointing at it.

> **Ports:** a split's new group takes the next group number, so each node needs a
> free port at `raft_port` plus that number.

If the hand-off's outcome is unknown, for example because the old group's leader
died, the shards stay fenced: run `CLUSTER SPLIT` again with the same label. If the
hand-off did commit and the node you ask has applied it, the answer names the group
it made and nothing is handed off twice. If that node has applied it but hasn't
started the new group yet, the answer is `TRYAGAIN`. If the hand-off never
committed, the same shards are handed off again.

A member that applies an entry for a shard its group has already handed off counts
it as a stray, logs an error, and shows `strays N` on the group's `CLUSTER INFO`
line. The fence exists so that never happens.

*Later:* merging two groups, splitting by load rather than by hand, a shared
transport so many groups don't need many ports, and Luau placement.

## Distributed locks

Built in TODO 614. Raft leadership already decides which node writes a group, so
routing and replication need no separate locks. Applications still want them, for
example the shop's order processing. A space replicated with Raft has three lock
commands:

| Command | Does | Replies |
|---|---|---|
| `LOCK <name> <owner> <ttl_ms>` | grants the lock to `owner` for `ttl_ms` when nobody holds it, the holder's time is up, or `owner` already holds it (a renewal) | the **fencing token**, above 0, or `0` when someone else holds it |
| `UNLOCK <name> <owner>` | releases the lock if `owner` holds it | `1`, or `0` when `owner` didn't hold it |
| `LOCKINFO <name>` | who holds the lock | the owner and when its time is up, in ms since the epoch, or nil |

`LOCK` and `UNLOCK` are writes, so they go to the space's leader like any other.
`LOCKINFO` is a read. In a space without Raft all three are refused.

The fencing token is the log index the grant committed at. Indexes only go up in a
group, so a later grant always has a higher token, whoever got it and whichever node
led. Hand the token to whatever the lock protects, and have it refuse a request
with a token lower than one it has already seen. Without that, a client that stalls
past its lock's time can still write after someone else holds the lock.

A lock is a meta key in the space, so it's replicated, saved and copied with the
data, and no other command can see or change it. The leader checks and grants
under one mutex, so two clients asking at once can't both see the lock free.

> **Clocks:** the leader's clock decides when a lock's time is up. After a leader
> change the new leader's clock decides, so clocks that disagree by more than a
> lock's slack can hand it out early. Fencing tokens keep the protected resource
> safe even then.

Redlock-style locks, which take a majority of independent nodes with no shared
log, can't give fencing tokens and depend on clocks agreeing. These are one log's
entries instead.

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
   - Both build against BARCH's asio 1.36.0 with no patches. Since then, CMake
     patches two races TSan found in NuRaft's own code into the fetched source:
     a configuration change made without the server's lock in
     `flip_learner_flag` (TODO 615), and a commit's result code read outside its
     lock in `handle_cli_req_callback` (TODO 623). A NuRaft whose code isn't what
     a patch expects stops the configure rather than building unpatched.
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
4. **Routing (done, TODO 613).** Epochs on redirects, `CLUSTER ROUTES`, a lease
   on leader reads, and follower reads that never go back behind what a
   connection saw. A follower's own contact with a leader is the failure hint, not
   the `seen` key. Gossip isn't built.
5. **Scale out and extras.** Locks with fencing tokens (done, TODO 614), spaces
   split over a fixed number of groups (done, TODO 615), splitting a group live
   (done, TODO 616), then merging, a shared transport, and Luau placement.

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
- `TestClusterRoute`: a follower's redirect and every node's `CLUSTER ROUTES` name
  the leader and epoch; 300 rounds of a write on the leader and a read on a
  follower told the index, none stale; a follower with no leader refuses follower
  reads; a leader paused with `SIGSTOP` while the others elect another and take a
  write doesn't answer the old value when it resumes. Without the lease check it
  did, in 2 runs of 3.
- `TestClusterLock`: grant, refuse, renew and release; an expired lock taken by
  another owner; eight clients contending for one lock, never two holding it at
  once and every token higher than the one before (without the grant mutex there
  were 276 overlaps); a lock that survives its leader being killed.
- `TestClusterSplit`: a space split over three groups led by three nodes; a node
  answering its own groups' keys and redirecting the rest (without the shard check
  it answered all 200 itself); `KEYS` refused without follower reads and answered
  with them; `FLUSHDB` refused; a node killed under writes through redirects with
  no acknowledged write lost and equal copies once it's back.
- `TestClusterSplitLive`: a space's group split, then the new group split again,
  while four clients write; no acknowledged write lost, the three runs covering
  every shard once, routes and the space's record agreeing, equal copies, and a
  restarted node coming back with the same three groups.
- `TestClusterSplitFence`: two test settings widen the narrow cases.
  `BARCH_TEST_COMMIT_DELAY_MS=150` holds each data write that long between the
  fence check and the log, so the hand-off always lands among writes in flight.
  `BARCH_TEST_LOSE_HANDOFF` turns the first hand-off's answer into `UNKNOWN` after
  it has committed. The split runs under eight writers. The first `SPLIT` answers
  `UNKNOWN`, and running it again names the same group, with no third group made.
  No member applies a stray entry, no acknowledged write is lost, and the copies
  match. Without the fence's exclusive lock, or without the fence check, two
  members applied 3 to 7 strays in every run. Notably, no acknowledged write went
  missing and the copies still matched, so only the stray count catches it.
  Without the retry check, the second `SPLIT` failed with "isn't inside this
  group's run".
- `TestClusterLiveness`: with node 1, group 1's preferred leader, killed and
  nothing written, the group's term has to stay put for 25 seconds, with a leader at
  every check, and nobody may hand the group to node 1. Before TODO 628: 3 hand-overs,
  term 3 to 5, 4 checks without a leader. Then a member copies an 8-second snapshot
  while it and the leader are the only two up, and the leader's Raft leadership and
  term have to hold through it. Before, the leader stepped down during the copy.
- `TestClusterSnapshotSource`: a follower kept down while the leader compacts its
  log comes back and copies a snapshot, and the leader is killed mid-copy (a test
  setting, `BARCH_TEST_SNAPSHOT_COPY_DELAY_MS`, makes the copy wait long enough).
  The follower has to stay up, catch up from the next leader, and end with the same
  copy as the others. With the snapshot as one object, it aborted every time.
- `TestClusterLogHold`: a follower kept down while the leader compacts its log
  comes back and copies a snapshot, each copy held for 3 seconds, with four clients
  writing at up to ~800 writes a second to a space that snapshots every 1000 entries.
  It has to catch up while the writes go on, after one copy or two, the leader has to
  hold its log back for it, and every acknowledged write has to be there. Before TODO 636 it copied 2 to 4 times. With the log hold alone, and the
  member still waiting on the first object, it copied 3 to 7 times.
- `TestClusterStopCopy`: a follower copying a 4-second snapshot is stopped mid-copy.
  It has to start no other copy while it stops, exit cleanly, and catch up once
  started again. Before TODO 638, two more copies started while it stopped.
- `TestClusterRebuild`: the space's leader is paused with `SIGSTOP` under writes,
  so it comes back with a write whose outcome it doesn't know and rebuilds its
  copy. A client keeps writing to that node, whatever it's told. Nothing it was
  told was written may be missing, every acknowledged write has to be on the
  leader, and the copies have to match. Before the fix, every run lost writes:
  6,649 of 7,498 in one, with the group's commit index at 858.
- `TestClusterMany`: three spaces led by three different nodes, a node killed with
  writers on all of them, no acknowledged write lost, and every copy matching once
  it's back. Then a fourth node with another layout tag joins under writes, copies
  all five spaces key by key, and ends with copies that match.
- `TestClusterBulk`: LOAD on the leader leaves every member with the saved keys and
  none written since; RELOAD on a follower changes nothing; RETRIEVE from a standalone
  server gives every member its keys; IMPORT answers NOTLEADER on a follower and is
  replicated from the leader; UNLOAD is refused; a LOADFS over a 4096 byte batch bound
  lands whole on every member.
- `TestClusterFlushAll`: FLUSHALL and CLEARALL on a follower answer `NOTLEADER` and
  clear nothing; on the leader they empty the replicated space on every member and
  the leader's own space, and leave the followers' own spaces, the cluster and its
  configuration as they were.
- `TestClusterQueue`: a push on a follower answers `NOTLEADER`; with the consumer off
  every member holds a push; ten pushes are each handled once, on the leader, and
  every member holds the handler's writes; a failing handler is dead lettered on every
  member; a function run from the log pushes to its own space's queue; a leader killed
  with 40 messages waiting leaves the new leader to hand out the rest, and the old one
  catches up to the same digest; a queue on a space that isn't replicated is handled
  by the node it was pushed to.
- `TestClusterFunctionSync`: `FUNCTIONS STATUS`, `COMMANDS`, `CRON`, `QUEUES` and
  `LIBRARIES` answer on a follower in a replicated space; a repository synced on
  every node's timer lands on every member with no sync failing, and the followers
  report leaving the space to the leader; a change upstream reaches every member;
  `FUNCTIONS SYNC` answers `NOTLEADER` on a follower and `OK` on the leader; with the
  leader killed, a key dropped upstream is removed from every member by the new
  leader.
- `TestClusterReplApply`: a standalone primary publishing to a follower: strings,
  a hash, a list and deletes in the replicated space reach every member with matching
  digests, sent through the leader (`REPLRAFT`); a space that isn't replicated takes
  the stream on the receiving member only; a stored function that writes arrives
  once, and a miss on the primary's foreign source is a tomb on every member; with the space's leader killed while the
  primary writes, nothing is lost and the old leader comes back to the same digest.
- `TestClusterForeign`: a replicated space reading through the fake source: the
  source set on the leader fills a key once, and every member holds the value; a
  missing key is a cached miss on every member (a follower read of it answers nil
  from the tomb rather than asking the source) with matching digests, and a second
  read doesn't query; a follower read of an unfetched key answers `NOTLEADER` with no
  query; after a failover the new leader's source has what was set on the old one.
- `TestClusterHttp`: three nodes each serving the replicated space on its own port:
  `HTTP START` and `STATUS` on a follower; a POST to a follower is answered 307 to the
  leader's web server, and followed, its write is on every member; a GET whose
  handler writes is redirected the same way with nothing written on the follower; a
  GET that only reads answers on the follower.
- `TestClusterUsers`: the first node's own user logs in on every member after the
  others join; a user made on the cluster leader logs in on every member (and ACL
  SETUSER on a follower answers `NOTLEADER`); a `KSPACE ACL` rule set on the leader
  holds on every member; a restarted member, and one that missed 30 changes past the
  snapshot interval, have the users; `ACL DEL` reaches every member.
- `TestClusterXBatch`: a space split over two groups led by different nodes, with the
  writer pausing after the first group has a batch: an `FS PUT` whose chunks span
  both groups reads back whole on every member; a new file's `FS PUT` whose writer is
  killed in that pause is rolled forward and reads back whole on the other two, and
  the killed node comes back to the same copy.
- `TestClusterSplitLog`: a space split over two groups: a stored function bumping keys
  of both groups while clients SET the same keys leaves every member's digest the
  same (three different digests before); `SETF` from each node reaches every member;
  `BEGIN` is refused, saying why.
- `TestClusterSplitWhole`: a space split over two groups, 16 keys over both: `BEGIN`,
  writes, `ROLLBACK` leaves every member as before `BEGIN`; `BEGIN`, writes, `COMMIT`
  keeps them; a queue's ten pushes are handled with every member the same; `SAVE` on
  every node, writes, `LOAD` brings every member back; `RETRIEVE` from a standalone
  server gives every member its keys.
- `TestClusterLogicalCopy`: a fourth node with another page layout joins while a
  function from the log bumps 20 keys over and over; once it's a voter, its counters
  are equal to each other and to the leader's, and every member's digest is the same.
  With `BARCH_TEST_LIVE_EXPORT` (the old live paging) it failed once in three runs.
- `TestClusterTransactions`: BEGIN, writes from three clients past the snapshot
  interval, a follower restarted and the leader killed, then ROLLBACK on the new
  leader: every member's digest is the one from before BEGIN. And BEGIN, writes,
  COMMIT: every member keeps them.
- `TestClusterSideEffects`: a follower that had read the space's exposed commands
  answers ones a later SETF or LOADKEYS exposes; a follower's session runs the new
  code after a SETF or LOADKEYS with RELOAD on the leader; after a quiet SETF, a
  restarted member runs the same code as the others; a user without write rights
  can't write through a logged function; a cron job ticks once per tick. The old
  build failed the first four; with a kept runner caller the restarted member ran
  other code, and without the carried rights the limited user wrote.
- `TestClusterLogFunctions`: 1600 naive increments from clients on the leader and a
  follower count 1600 on every member; `math.random` agrees; a run that reads storage
  stats or another space is refused, and runs with `"replicate": "effects"`; a run
  that never ends stops at `function_log_insns`; a follower saved and restarted
  doesn't run anything twice. `TestClusterBatchCut` also cuts a function that writes
  six keys: the old code left `bbbaaa`.
- `TestClusterFunctions`: a writing function sent to a follower answers `NOTLEADER`
  before it runs, by name and through `CALLF`. A read-only one answers `NOTLEADER`
  there too unless the connection asked for follower reads, and then sees the leader's
  write. A read-only one that writes, runs `SET`, or calls a writing function is
  refused. On the old build a follower ran the writing one, and answered the read-only
  one from its own stale copy.
- `TestClusterIndex`: an index made and built on the leader answers `INDEX FIND` on
  every member, follows writes there, survives a member coming back from a snapshot,
  and a failover. Without the rebuild after a copy, the member from the snapshot
  didn't find the keys the snapshot brought.
- `TestClusterBatchCut`: an FS overwrite on the leader, with every commit held
  300ms, and the leader killed part way, once early and once late. The file has to
  read as the old version or the new one, whole, and the members have to match.
  Before TODO 665 the late kill left a file of mixed old and new chunks.

The cluster isn't in the default build, so it has a CI workflow of its own,
`.github/workflows/ubuntu24-cluster.yml`, which runs these tests (`ctest -L cluster`)
normally and under TSan. A test fails if any node it stops exits with anything but
0, so a sanitizer report or a crash on shutdown fails it even when every check
passed.

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

## Performance

Measured in TODO 624 with `test/clusterbench.py`, which starts its own `barchd` and
drives them with `memtier_benchmark`. These numbers are from a Release build on one
16-core machine, three nodes on localhost, 64-byte values, 10 seconds a run.

| | plain space | replicated, TODO 624 | replicated, after TODO 625 |
|---|---|---|---|
| SET, 1 connection | 50,562/s, p50 0.023ms | 223/s, p50 4.0ms | 313/s, p50 3.1ms |
| SET, 8 connections | 335,351/s | 421/s, p50 17ms | 1,229/s, p50 5.2ms |
| SET, 64 connections | 384,460/s | 395/s, p50 154ms | 946/s, p50 57ms |
| SET, 64 connections, pipeline 32 | 4,684,976/s | 465/s, p50 4.6s | 1,286/s, p50 1.7s |
| GET, 64 connections, pipeline 32 | 5,517,896/s | 3,011,621/s | 2,834,277/s |

GETs on a space that isn't replicated, on the leader: 325,420/s alone at 8
connections, and 52/s, with a p50 of 141ms, while 64 clients write the replicated
space (125/s, p50 52ms, after TODO 625).

What they say:

- **Replicated writes don't scale with clients.** They're flat at about 400 a
  second from 1 connection to 64 pipelined. A `fdatasync` on this disk takes
  1.65ms, about 600 a second one at a time. Each client's write is its own call to
  NuRaft's `append_entries`, which appends it to the leader's log and syncs it
  before the next can go, so the leader syncs once per write. The follower's sync
  and the network hop make up the rest of the 4ms at one connection.
  `group_store` already syncs once per batch; the batches just never hold more than
  one entry. TODO 625 moved the leader's sync to the background, so writes that
  arrive during a sync share the next one. That gave about three times as many
  writes at 8 connections. Past 8 it stops scaling, because a 16-core machine has
  8 RESP threads, and each replicated write blocks one until it commits (the next
  point).
- **A write blocks its RESP thread for the whole commit.** Every other client on
  that thread waits too, whatever space it uses. That's the 325,420/s to 52/s drop,
  and it's the same problem that stopped TODO 618.
- **Leader reads cost something.** Pipelined GETs on a replicated space were 45%
  slower than on a plain one. Unpipelined, the two were the same.

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
5. ~~What should a client library see on `NOTLEADER`?~~ The redirect, which it
   follows itself (TODO 613). Forwarding could come later as an option.
6. Should a node that hasn't been seen for a long time be removed from the
   cluster automatically, or only by `CLUSTER REMOVE`?
7. Is a restart the right way to apply a change to `<keyspace>.raft`, as it is
   for the other per-space options, or should there be a command that applies it
   to an open space?
8. Should mixed layouts be supported long term, or only during a rolling upgrade
   between two storage versions?
