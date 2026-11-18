# Tractability Ledger

This page says which parts of barchd can be checked in full and which can't yet.
Use it to pick what to harden next, and to see what a fix actually improved.

## How to read a row

Each row is one area of the code, not one file. A file can hold an area that is
checked well and one that isn't.

The level says how much of the area's behaviour a test can check exhaustively:

| Level | Meaning |
|---|---|
| 0 | Not audited. |
| 1 | Audited. Its hotspots are listed here. |
| 2 | Every hotspot has a TODO entry or a test. |
| 3 | Its decisions live in a pure core, and a test runs the core through every short order of events. |
| 4 | The shell around the core only does I/O. Every assumption the core's model makes about that I/O has its own test. |

A level goes up only when code or tests change, never on review alone.

A **hotspot** is one place in an area where a bug is likely and a test can't yet
show there isn't one. Most are shared state that two threads, or two events in some
order, can reach: a wait, a lock order, a flag that says which mode the code is in. An
audit names each one with the code it lives in and the claim that keeps it correct.
Working a hotspot means testing that claim.

A bug is likely at a hotspot because of the code's shape, not because of a count of
past bugs. Three things make the shape:
- **Many orders.** Two threads, or a push, a timeout and a hang-up, can arrive in any
  order. A test run sees one order. The bug is in an order nobody ran.
- **The rule is in prose.** "Off every list before `clear_blocks()`" or "never wait
  holding a gate" is a claim spread over several call sites. Nothing enforces it, so a
  new caller breaks it without a sign. `stream_wait` broke that way: TODO 693 added the
  `tx_gate` long after the wait was written, and no test put the two together.
- **Context lost across a hop.** State set in one place is used later in another: on a
  different thread, after a park, after a flush. What held when it was set may not hold
  when it is used. Both `erase_blocks` bugs were this.

A **core** is a header of plain decisions: no asio, no locks, no clocks, no socket
calls. `TestCoreBoundary` fails if a core includes anything else. A **shell** is the
code that does the I/O and hands the results to the core.

## State explosion

A state machine with a handful of states is easy to check by hand. The trouble is that
the states multiply. A session with six independent flags has 2^6 = 64 states. Put two
of them next to a shard latch and a timer, and the combined state is the product of
each part's states. Then each order the threads can run in is another path through
it. The number of reachable states grows exponentially in the number of parts. That
is state explosion.

It is why formal checking is hard to start, before any proof is attempted:
- **A model checker has to visit the states.** TLA+, SPIN and CBMC all enumerate or
  encode them. Past a few dozen interacting variables they run out of memory or time.
  Bounding the run (two clients, three events) keeps it finite, but then it proves
  nothing past the bound.
- **The real state is not in a form a checker reads.** It sits in flags spread over a
  class, in what is queued on a socket, in which thread holds which latch, in asio's
  handler queue, in the kernel's send buffer. Writing that down as a model is the
  work. And the model is only as true as the person who wrote it.
- **Some parts can't be modelled at all, only assumed.** The OS scheduler, TCP, the
  disk, NuRaft and Luau are outside the code. A model must say how they behave, and
  that is an assumption, not a fact.
- **The model drifts.** The code changes every week. A separate model does not change
  with it, and a model of last month's code checks nothing.

A core answers each of these in a small way. It pulls one decision out of the
explosion, so its state is small enough to run every short order. It is the code, not
a copy, so it cannot drift. And what it can't see becomes a listed assumption in the
row, with its own test at level 4. The aim is not to verify barchd. It is to shrink
the part that can't be checked, one decision at a time, and to say plainly what is
left.

## Where the state explodes

Places where many parts meet and no small model covers them yet. Each says what the
state is made of and why it resists a model. A place that also has a row in Areas is
marked with that row.

- **Session read chain** (row: Read chain modes). `asio_resp_session.h` keeps
  `write_busy`, `read_paused`, `parse_pending`, `stream_busy`, `stream_failed`,
  `read_out`, `read_for_park` and the atomic `let_go`, plus `park_gen` and the byte
  gauges. No single value said which mode the session was in. Since TODO 930 the
  chain's six flags have a mode in `park_core.h`, and 11 of 64 are legal. The stream
  flags, `let_go` and the gauges are still outside it.
- **Async batches** (row: Async batches). A batch moves from the socket's thread to a
  worker and back. A park, a hang-up or back pressure can happen on either side. The
  state is the product of the session's modes on both threads, plus whatever is
  queued in asio between them. Since TODO 931 the choice of what joins a batch is in
  `park_core.h`. Each call still runs on a copy of the session's caller, so the
  caller's state is copied once per call. The thread hops are not modelled.
- **Session collector and `let_go`** (row: Session collector). `collect_sessions` in
  `server.cpp` decides a session is gone from `let_go`, or a `peek_now` of 0 on the
  socket. That puts the kernel's socket state into the decision. The rule is spread over
  four call sites. Three earlier fixes (TODO 489, 496, 859) touched it.
- **Blocking pops across shards** (row: Block registration). A park spans one wait
  list per key, each in its own shard and under its own latch, plus the session's
  park generation and the timer. The pop handler can park again. The lists are
  shared by every client waiting on the key, so the state grows with the clients.
- **Transactions.** `tx_session.h`, `tx_locks.cpp`, `commit_gate.h`, `tx_versions`.
  Transactions keep version layers that `tx_versions::trim` drops.
  Pessimistic ones hold key locks with waits up to `tx_lock_wait_ms`. A commit waits
  on `tx_gate` for readers. The state is every open transaction's read and write
  set, times the lock table, times the layers kept. Isolation is a property of all
  orders of all of them at once, which is the standard hard case for model checkers.
- **Raft write paths** (row: Raft write paths). `src/cluster/raft_group.cpp` and
  `src/raft_writes.cpp`. Leadership changes, leases, learners joining, snapshots
  being installed, and a write whose outcome is `unknown` because leadership changed
  before it committed. Most of the state is inside NuRaft and the network. It can be
  assumed, not modelled from here.
- **Saves while writing.** `shard::end_save_view_holding_lock` and
  `wait_for_frozen_save` in `shard.cpp`. A save works from a frozen view while writes
  go on. The state is which pages the view holds, which the writers have changed
  since, and where the save has got to. With the arena's pointer compression, the
  memory layout itself is part of the state.
- **Eviction.** The `run_evict_*` functions in `shard.cpp` remove keys under memory
  pressure, while readers, transactions and saves may hold them. Which key goes
  depends on LFU counts and TTLs, so the state includes the whole key set.
- **Function sync and queues.** `function_sync.cpp` (2,500 lines) and
  `queue_service.cpp` each have their own atomics, `settled` and `stopping` flags,
  and shared slots swapped with acquire and release. Neither has been audited.
- **Luau calls.** A `CALLF` runs Luau on an io thread it shares with other
  connections. A script can call back into barchd, take locks, and raise. Luau's own
  stack and error state join the session's. It can only be assumed, like NuRaft.

What these have in common: each mixes a decision barchd owns with state it doesn't
(the kernel, asio, NuRaft, Luau, the clock). The first part can move into a core. The
second can only be named as an assumption and tested.

## The loop

Harden one hotspot at a time, in this order:

1. Audit the area and list its hotspots here.
2. Write a test that fails, and prove it fails on the current code.
3. Fix it.
4. Move the decision into a core and add it to the exhaustive test.
5. Run the short set under TSan and ASan.
6. Update the row.

Step 4 is the one that pays off: a fix closes one bug, but a core with an exhaustive
test closes every bug of that shape. Each model also runs rules known to be wrong, and
checks that it catches them. A model that catches nothing is not checking anything.

## Areas

| Area | Where | Level | Open |
|---|---|---|---|
| Park timer, wakes, park read | `asio_resp_session.h`, core `src/rpc/park_core.h` | 3 | 922 |
| Read chain modes (reading, paused, parked, batch suspended) | `asio_resp_session.h`, core `src/rpc/park_core.h` | 3 | |
| Async batches moving between threads | `asio_resp_session.h` `run_asynch_batch`, core `src/rpc/park_core.h` | 3 | |
| Streamed replies and `stream_wait` | `asio_resp_session.h`, `rpc_caller.h` `send_socket_tail` | 2 | |
| Block registration and erase across shards | `rpc_caller.h` `erase_blocks`, `transfer_rpc_blocks`, `shard.h` | 2 | |
| Session collector and `let_go` | `server.cpp` `collect_sessions` | 1 | |
| Raft write paths | `src/raft_writes.cpp` | 0 | |

## Park timer, wakes, park read

Level 3 since TODO 920. Reworked by TODO 923 and 924.

**Core.** `park_core.h` decides how long a park waits (`plan_timer`), whether a
handler's park is still current (`still_parked`), and what a wake does (`on_wake`).
It also decides the park read: when a park starts a read (`watch_now`), what a read
that comes back does (`on_arrival`), and whether the chain starts a read or takes over
the one that is out (`chain_reads`).

**How a park sees a hang-up.** A parked session keeps one real socket read out. What
it brings goes into the parser and does not run. It runs when the block answers, as
parser leftovers do. A read that ends with EOF or an error drops the blocks. There is
no peek and no `POLLRDHUP`, so the same code serves TLS and the select reactor.

A park never stops reading. A server that stops reading closes the TCP window, and
the client's FIN then waits behind its unsent data on the client's side, where no
server can see it. So a client that keeps sending past `rpc_query_buffer_limit`
(default 1 GB, as Redis's `client-query-buffer-limit`) is closed. INFO clients counts
these closes as `query_limit_drops` (TODO 924).

**Exhaustive test.** `TestParkCore` (`test/parkcoretest.cpp`) models the socket, the
read chain and a batch. It runs every order of up to two sends, a close, the block
answering plainly or into a batch, and the batch ending. It checks:
- A client that went away isn't left parked.
- A client that is still there isn't dropped.
- Nothing pipelined runs before the blocked reply.
- Nothing pipelined is left waiting once the chain is back.
- There is never more than one read out.

It catches these known-bad rules:
- Running what a park's read brought.
- Taking pipelined bytes for a disconnect (TODO 226).
- Stopping after the first bytes (TODO 919).
- The chain reading beside a park's read.
- Kept bytes not marked pending, which a batch then never runs.
- A full parser that stops the read (TODO 924).

With a limit of one request it also checks that a client past the limit is closed,
not left parked, and that no client within the limit is closed for it.

**What the models found.**
- TODO 920: the TODO 919 fix waited for the client's kernel to send a reset. A client
  that half closes never sends one, and its session stayed parked. The shell now
  drops the blocks itself.
- TODO 921 and 923: the TODO 920 model said a watch armed after an edge never fires.
  On Linux it did. asio arms with `EPOLL_CTL_MOD`, and a MOD makes the kernel report
  what is already there. So the model's arm was wrong, and the real behaviour was
  worse than the model: the TODO 919 watch re-armed on unread bytes and spun. One
  parked client with a PING behind it took 2.0 s of CPU in 2 s and woke 3362 times in
  200 round trips. The model now completes a read on whatever is in the socket.
- TODO 924: the park read used to stop at `rpc_output_high_water` (1 MB) kept. A
  client that sent 2.7 MB and closed stayed parked. The model had pinned this as open.
  A bounded buffer can't close it, which is why the park now reads on up to a limit.

**Assumptions the model makes:**
- A read armed on a socket that already holds data or a FIN completes. asio's MOD on
  arm gives this. `TestPopPark` checks it with clients that send and close at once.
- A read that ends with no bytes is a hang-up. `TestBpopThread` checks a close and a
  half close.

**Known gap.**
- Windows has not run the park read. It should work there, since it uses no
  Linux-only signal, but `test/bstartest.py` still skips its cases on win32. TODO 922.

**Still in the shell.** Which `read_out` and `read_for_park` flags the read handler
clears, and the order "add the bytes, then decide". The model assumes both. The
shell has comments that say why.

## Read chain modes

Level 3 since TODO 930.

**Core.** `park_core.h` decides what the chain does next (`continue_reading`: pause,
run what is kept, or read), whether a drained write starts it again (`resumes`), and
what follows a park's answer (`after_block`: the rest of the batch, or the chain). It
also names the mode. `mode_of` takes six flags (`read_paused`, `read_out`,
`read_for_park`, a park, `in_batch`, `pending_batch`) and returns reading, paused,
parked, batch, batch parked, or impossible. `in_batch` is new: before it, nothing said
a batch was running. 11 of the 64 combinations are legal. The rules that make the
rest illegal were kept by the shell and written nowhere. They are now comments on
`mode_of`.

**The shell's check.** Each handler on the socket's thread calls `note_mode()` first.
An impossible mode bumps `chain_mode_errors` in INFO clients and logs once. The short
set keeps it at 0, and `TestReadChain` asserts it.

**Exhaustive test.** `TestParkCore` has a model of the whole chain: sends, reads,
replies past the high water mark and their drain, parks from a read and from a
batch, answers, timeouts and batch ends. It runs every order up to a bound and
checks:
- Replies ahead of a park go out while it is parked.
- Replies never pile up past the high water mark.
- The chain never stops with work left.
- Every reply goes out, in order.
- No command runs ahead of a reply it waits for.
- Every state seen has a legal mode, and every legal mode is reached (11 of 11).

It catches these known-bad rules: holding replies behind a park, no pause, a chain
that stops, a skipped reply, forgetting the batch after a park, and reading beside a
batch.

**What the work found.**
- Replies to commands sent ahead of a blocking one were held until it answered.
  `consume_available` wrote them into `stream`, and the park returned before the
  send. A pipeline of SET, GET, BLPOP got nothing until a push. Found by hand while
  writing the model's rules.
- A BLPOP parked inside a batch lost its answer. The value was popped and never sent.
  `call_blocks` cleared the caller's results but not `reply_sent`. A sync SET's
  "+OK" left it set, and `write_result` took the pop's answer as sent. Found by
  `TestReadChain`. The model could not see it: it has no notion of who wrote a reply.
- The first draft of `mode_of` allowed 16 combinations. The model reached 11. The
  other 5 had the chain's own read out while paused, parked or in a batch. No path
  makes them, so `mode_of` now calls them impossible.
- `do_callback_into_socket_context` had no callers. It is gone.

**Assumptions:**
- Handlers on the socket's thread run one at a time, so the six flags are read
  between steps. A flag written from a worker would break this. None is today.
- The model treats a reply as bytes. Who writes them (`reply_sent`, `reply_out`) is
  outside it. `TestReadChain` covers the batch park; other paths that write a reply
  straight to the socket have only the end-to-end tests.

## Async batches

Level 3 since TODO 931.

**What it is.** `consume_available` reads a run of requests. Once one is
asynchronous, it and every request after it in the read join a batch, so replies
leave in order. `run_asynch_batch` runs the calls on workers, one after another.
Each call has a copy of the session's `rpc_caller`, made when the batch is read.

**The bug.** A call that changes the connection changes only its copy. The calls
after it were copied before it ran, and the session never sees the change. KEYS,
then USE, then SET put the SET in the old space. HELLO 3 behind a KEYS left later
replies in RESP2. MULTI behind a KEYS queued nothing. Under MULTI, KEYS opened a
batch and was queued on its copy, so EXEC never ran it.

**Core.** `park_core.h`:
- `changes_connection(name)`: the commands that change the connection. MULTI,
  EXEC, DISCARD, WATCH, UNWATCH, USE, SELECT, AUTH, HELLO, CLIENT, RESET, READONLY,
  READWRITE, BEGINTX, ABORTTX, COMMITTX.
- `holds(batch_open, changes)`: such a command read while a batch is open waits.
  The session keeps it in `held` and sets `parse_pending`. When the batch ends,
  `continue_reading` runs what is kept, and the held command runs first, on the
  session's caller.
- `joins_batch(batch_open, asynch, buffering)`: once a batch is open, every command
  joins it. Under MULTI none opens one, so a queued command lands on the session's
  caller, where EXEC finds it.

**Exhaustive test.** `TestParkCore` models a connection with a space, a MULTI flag
and a queue, and six commands: GET, KEYS (asynchronous), USE a, USE b, MULTI and
EXEC. It runs every pipeline of one to five commands, 9330 in all. Each runs once
one at a time and once through the core's rules, with batch calls on copies. The
replies and the final state must match. It searches by length, so it reports the
shortest pipeline that differs. It catches:
- the old rules (KEYS, USE a);
- a USE that joins the batch (KEYS, USE a);
- KEYS under MULTI on a copy (MULTI, KEYS);
- a list without MULTI and EXEC (KEYS, MULTI).

**Live test.** `TestBatchState` sends each case on a raw socket. USE, HELLO and
MULTI behind KEYS, KEYS under MULTI, and a BLPOP parked in the space a held USE
chose. All fail on the 17-11 build and pass now.

**What the work found.**
- The bug above. Found by reading `run_params`.
- HGETALL answers an array under RESP3, not a map. It is a separate quirk. The test
  uses ZSCORE, which answers `,1` in RESP3 and `$1 1` in RESP2.
- `write_then` calls its continuation even when the write fails. A batch keeps
  running for a client that has gone. The other failure paths (`stream_out`,
  `send_socket_tail`, `stream_flush`) return. Not fixed; it wastes work but writes
  nothing wrong.

**Assumptions:**
- The list in `changes_connection` is kept by hand. A new command that changes the
  connection must be added to it. Nothing checks that.
- A Luau function that changes the connection, run from a batch, is not covered.
- The model has one queue and one space. AUTH, HELLO and CLIENT are taken to behave
  like USE: state on the caller, read by later calls.

## Streamed replies and `stream_wait`

Level 2 since TODO 925.

**What it is.** KEYS over RESP runs on a worker and sends its reply through
`stream_out`. Past `rpc_stream_high_water` queued, the worker waits in `stream_wait`
for the client to take some. A client that takes nothing for `rpc_client_max_wait_ms`
is closed. At 0 the wait has no end.

**What the audit found.** The hotspot was listed as a wait holding
`socket_write_mutex` and `glob_queue`. The `glob_queue` half was stale: since TODO 491
KEYS walks first and writes after `art::glob` returns. But the write was still made
inside `rpc_caller::call`, and once a space has had a transaction, a read command
runs there holding the space's `tx_gate` shared (TODO 693). A commit waits for the
gate's readers to go. So a client that stopped reading held up every COMMITTX in its
space, for 30 s by default, or for ever at 0.

The wait under the gate needs bytes queued ahead of KEYS: a pipelined reply between
`rpc_stream_high_water` (256 KB) and `rpc_output_high_water` (1 MB) that the socket's
send buffer can't take. On loopback the send buffer grows past that, so it never
showed there. To a real client, with a segment size near 1460, it does.
`TestStreamGate` sets `TCP_MAXSEG` 536 on its stalled client to get the same.

**The fix.** `write_socket_array` puts the encoded reply in `socket_tail` on the
caller. The session sends it with `send_socket_tail` after `call()` returns, holding
nothing but its own `socket_write_mutex`. The socket's thread never takes that mutex.

**The check.** The lock order argument is now a rule that runs: `stream_wait` is never
entered holding a commit gate or a latch. `commit_gate` counts the gates each thread
holds, and `latch_t` already lists its holders per thread. A wait that breaks the rule
bumps `stream_wait_locked` in INFO clients and logs once. `TestStreamGate` and
`TestKeysStall` check it is 0. With the write put back inside the call, it read 3.

**Why not level 3.** There is no decision left to move into a core. The fix removed
the wait from inside the call rather than choosing when it may happen. What the row
needs for level 3 is a model of the async batch, which is its own row.

**Assumptions:**
- `socket_write_mutex` is taken only by this session's workers, never by its socket
  thread. Read from the code. Nothing checks it.
- `bind_socket_writer` sends the session's `stream` first if it isn't empty. That
  buffer belongs to the socket's thread. During a batch the read chain is down, so
  nothing writes it then, and the batch moves it into `ctx->stream` before it starts.
  TSan has not reported it.

## Block registration and erase across shards

Level 2 since TODO 926.

**What it is.** A blocking pop stamps each key it waits on with its space and shard
index (`add_block`). `transfer_rpc_blocks` puts the session on each key's wait list
in that shard, under the shard's latch. A push wakes the first waiter on the list and
takes it off. When the park ends, `erase_blocks` takes the session off every list,
shard by shard under each latch, then calls `clear_blocks()`.

**What the audit found.** The erase argument holds. The session is off every list
before `clear_blocks()` runs, and a waker reads the blocks only under the latch of a
list that still holds the session. Two bugs sat next to it:
- `space:CMD` sets the space for one call. The keys were registered in that space,
  but the wake ran the pop after the session had put its own space back. So the pop
  took a value from the connection's own space, a space the client never named.
  BLPOP, BRPOP, BLMPOP, BZPOPMIN and BZMPOP all did it.
- FLUSHDB and FLUSHALL cleared every shard's wait lists along with the data. A pop
  parked before the flush never saw the push after it. It timed out with nil, or
  waited for ever at 0.

**The fix.** The caller keeps `block_space`, the space its blocks were made in.
`call_blocks` runs the wake in that space and puts the old one back after. The flush
leaves the wait lists alone: they are clients, not data.

**The check.** INFO clients has `blocked_keys`: one per session per key on a wait
list. Every change to a list moves it. `TestBlockSpace` checks it and
`blocked_clients` are 0 after each way a park ends: answered, answered on one key of
three, a key named twice, timed out, two waiters and one value, a hang-up, and a
hang-up from a cross-space park. It also checks each cross-space pop answers from the
space it named, and a pop parked before a flush is answered. On the old code the five
pops answered from the wrong space and the flush case hung.

**Why not level 3.** The decisions are small: add once, remove every copy, wake the
first. Neither bug was in them. Both were in which space and which data the shell
used. A core of the list rules would not have seen either. The gauge is the check
that would: it is the invariant, read from the running server.

**Assumptions:**
- A block handler that parks again (`block_again`, `park_on_lock`, a second
  `add_block`) names one key. `erase_blocks` takes the latest set only, so a handler
  that swapped in several keys from another shard could leave the old ones behind.
  True of every handler today. Nothing checks it but the gauge in `TestBlockSpace`.
- A block's shard index finds the same shard at erase as at registration. A range
  sharded space can move the key to index mapping, but both sides use the stored
  index, not the key.

## Hotspots not yet worked

From the `asio_resp_session.h` audit (18-11-2026):

- **Collector and `let_go`.** Three earlier fixes (TODO 489, 496, 859) touched who
  lets a session go and when. The rule is "let_go, or a peek of 0", spread over four
  call sites.
