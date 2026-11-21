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
  four call sites. Three earlier fixes (TODO 489, 496, 859) touched it. Since TODO
  932 the rule is `collects` in `park_core.h`, and every read that ends the chain
  sets `let_go`. The kernel's socket state is in the model as a peek result.
  Since TODO 933 the peek counts only for a session nothing else holds. Since
  TODO 934 a reset peeks 0 on Windows too (`peek_core.h`).
- **Blocking pops across shards** (row: Block registration). A park spans one wait
  list per key, each in its own shard and under its own latch, plus the session's
  park generation and the timer. The pop handler can park again. The lists are
  shared by every client waiting on the key, so the state grows with the clients.
- **Transactions** (row: Transactions: the commit gate, which covers the gate only).
  `tx_session.h`, `tx_locks.cpp`, `commit_gate.h`, `tx_versions`.
  Transactions keep version layers that `tx_versions::trim` drops.
  Pessimistic ones hold key locks with waits up to `tx_lock_wait_ms`. A commit waits
  on `tx_gate` for readers. The state is every open transaction's read and write
  set, times the lock table, times the layers kept. Isolation is a property of all
  orders of all of them at once, which is the standard hard case for model checkers.
- **Raft write paths** (row: Raft write paths: the gate, which covers the gate only). `src/cluster/raft_group.cpp` and
  `src/raft_writes.cpp`. Leadership changes, leases, learners joining, snapshots
  being installed, and a write whose outcome is `unknown` because leadership changed
  before it committed. Most of the state is inside NuRaft and the network. It can be
  assumed, not modelled from here.
- **Saves while writing** (row: Saves while writing: the save view). `shard::end_save_view_holding_lock` and
  `wait_for_frozen_save` in `shard.cpp`. A save works from a frozen view while writes
  go on. The state is which pages the view holds, which the writers have changed
  since, and where the save has got to. With the arena's pointer compression, the
  memory layout itself is part of the state.
- **Eviction** (row: Eviction: the sweep rules). The `run_evict_*` functions in `shard.cpp` remove keys under memory
  pressure, while readers, transactions and saves may hold them. Which key goes
  depends on read bits and TTLs, so the state includes the whole key set.
- **Function sync and queues** (row: Function sync and queues: the wake and post rules). `function_sync.cpp` (2,500 lines) and
  `queue_service.cpp` each have their own atomics, `settled` and `stopping` flags,
  and shared slots swapped with acquire and release. Neither has been audited.
- **Luau calls** (row: Luau calls: the park rules). A `CALLF` runs Luau on an io thread it shares with other
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

## Bounded-state module splits

Large translation units mix locks, timers, parsing, and domain calls in one file.
Before auditing or extracting a core from one, split it into bounded-state modules
using this template (established on the Luau driver in TODO 929):

1. **One narrow internal header (`*_internal.h`).** Hold shared internal structs and
   plain free-function declarations in an internal `detail` namespace. Pass typed
   pointers across files, with no `void*` tunnels and no new `std::function` vtables
   or raw function-pointer indirections.
2. **One owner per kind of state.** Separate concurrency and preemption (mutexes,
   condition variables, timers, parking), compilation or caching, domain bindings,
   and a thin public facade with fallback stubs.
3. **A per-file boundary rule.** State what each file may not touch and check it:
   - Compiler and cache files hold no mutexes, condition variables, or park calls.
   - Binding files hold no mutexes and talk to the scheduler only through its public
     park hooks and explicit context flags.
   - Scheduler files contain no storage, filesystem, graph, or SQL query logic.
4. **Two-phase extraction.** Move bindings behind the internal header first and run
   the module's test suite. Then separate compilation and scheduling and run the
   suite again under RelWithDebInfo, ASan, and TSan. Keep runtime behavior, error
   prefixes, stack balances, and concurrency invariants unchanged.

## Areas

| Area | Where | Level | Open |
|---|---|---|---|
| Park timer, wakes, park read | `asio_resp_session.h`, core `src/rpc/park_core.h` | 3 | |
| Read chain modes (reading, paused, parked, batch suspended) | `asio_resp_session.h`, core `src/rpc/park_core.h` | 3 | |
| Async batches moving between threads | `asio_resp_session.h` `run_asynch_batch`, `write_then`, core `src/rpc/park_core.h` | 3 | |
| Streamed replies and `stream_wait` | `asio_resp_session.h` `stream_out`, `stream_wait`, `stream_next`, `rpc_caller.h` `send_socket_tail`, core `src/stream_core.h` | 3 | |
| Block registration and erase across shards | `rpc_caller.h` `erase_blocks`, `transfer_rpc_blocks`, `call_blocks`, `shard.h`, core `src/block_core.h` | 3 | |
| Dropped spaces: files and holders | `keyspace_api.cpp` `is_drop`, `shard.cpp` `~shard`, `key_space.cpp` `get_keyspace`, `asio_resp_session.h` `run_params`, core `src/drop_core.h` | 3 | |
| Cluster log words: the users seed | `cluster_api.cpp` `run_logged`, `auth_api.cpp` `route_users_change`, `seed_cluster_users`, core `src/log_words_core.h` | 3 | |
| Session collector and `let_go` | `server.cpp` `collect_sessions`, `socket_peek.h` `peek_now`, cores `src/rpc/park_core.h`, `src/peek_core.h` | 3 | |
| Raft write paths: the gate | `rpc_caller.h` `call`, `src/raft_writes.cpp`, core `src/raft_gate.h` | 3 | |
| Transactions: the commit gate | `commit_gate.h`, core `src/commit_gate_core.h` | 3 | |
| Saves while writing: the save view | `shard.cpp` `freeze_for_save_holding_lock`, `write_frozen`, `send_frozen`, core `src/save_view_core.h` | 3 | |
| Function sync and queues: the wake and post rules | `function_sync.cpp` `start_function_sync`, `queue_service.cpp` `consumer`, core `src/wake_core.h` | 3 | |
| Eviction: the sweep rules | `shard.cpp` `run_evict_*`, `run_sweep_*`, `evict_logged`, core `src/eviction_core.h` | 3 |  |
| Luau calls: the park rules | `luau_scheduler.cpp` `pump_slices`, `park_call`, `park_yield`, `complete_call`, core `src/luau_park_core.h` | 3 | |
| RESP3 reply shapes | `hash_api.cpp` `HGETALL`, `HRANDFIELD`, `ordered_api.cpp` `zrange`, `zpop_flat`, `caller.h` `start_pair`, `rpc_caller.h` `nested_result`, core `src/pair_shape_core.h` | 3 || |
| Sorted sets: score moves | `ordered_api.cpp` `ZINCRBY`, core `src/score_move_core.h` | 3 | |
| Container scans: pages | `ordered_api.cpp` `ZSCAN`, core `src/scan_page_core.h` | 3 | |
| Names: number keys | `conversion.cpp` `convert`, `keys.cpp` `encoded_key_as_reply`, `ordered_api.cpp` `score_bound`, core `src/number_name_core.h` | 3 | |

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

**Windows - TODO 922.** asio uses IOCP on Windows, not epoll. The park read uses no
Linux-only signal, so it should end the same way. `test/bstartest.py` skipped its park
cases on win32 until TODO 922. All eight cases were then run from Linux against a
MinGW barchd.exe under wine, and passed twice. The skip is gone, so Windows CI runs
them in-process. Wine is not Windows: the real check is that CI run.

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
  uses ZSCORE, which answers `,1` in RESP3 and `$1 1` in RESP2. Fixed in TODO 936:
  see "RESP3 reply shapes" below.
- `write_then` calls its continuation even when the write fails. A batch keeps
  running for a client that has gone. The other failure paths (`stream_out`,
  `send_socket_tail`, `stream_flush`) return. Fixed in TODO 935: see "A batch's
  writes" below. It did more than waste work: a SET behind the failed write ran.

**Assumptions:**
- The list in `changes_connection` is kept by hand. A new command that changes the
  connection must be added to it. Nothing checks that.
- A Luau function that changes the connection, run from a batch, is not covered.
- The model has one queue and one space. AUTH, HELLO and CLIENT are taken to behave
  like USE: state on the caller, read by later calls.

**A batch's writes - TODO 935.** A worker writes each call's reply in four parts:
- what was carried in from before the batch;
- what the call streamed (`send_socket_tail`);
- the rest of the stream (`stream_flush`);
- the reply itself (`write_then`).

The first three stopped the batch when they failed. `write_then` ran its
continuation whatever its write did, so the next call ran for a client that had
reset. A write among those calls ran too, with nobody to see its reply.

The fix:
- `park::batch_after_write(written, blocked)` decides at each of the four parts:
  the next call, a park, or stop.
- A failed write stops the batch. `write_then` shuts the socket and sets `let_go`,
  as `stream_next` does since TODO 943. A call parks only once its reply is written.

`TestParkCore` runs every batch of one to four calls, each plain or blocking,
against a reset before each of its writes or none: 452 batches. It checks:
- no call runs after a failed write;
- a failed write lets the client go;
- a call parks only after its reply is out;
- with no reset, every call runs up to the first block.

It catches:
- the old `write_then`;
- no write checked;
- a block that parks after its write failed;
- a block that stops the batch;
- a batch that stops after its first call.

`TestBatchWrite` pipelines `KEYS * COUNT` and a SET. A hold
(`BARCH_TEST_BATCH_WRITE_HOLD_FILE`) stops the worker before `write_then`, and the
client resets. On the old code the SET ran. Now it doesn't, and the session is let
go. A live client's batch still runs to its end.

## Streamed replies and `stream_wait`

Level 2 since TODO 925. Level 3 since TODO 943.

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

**The second audit (TODO 943).** It covered `stream_out`, `stream_flush`,
`stream_wait`, `stream_next` and its completions, `abandon_stream`, and the async
batch's returns when a stream fails. A script's `barch.call` uses a caller with no
socket writer, so it never streams. It found one hole.
- **A failed write was never let go.** A client that resets while the server holds
  input it has not read fails the write that is out. The completion only marked the
  stream failed. The worker saw that, gave up, and returned. The read chain is down
  for the batch, so no read saw the error, and nothing set `let_go`. A reset socket
  with unread input peeks as that input for ever, not as 0, so the collector kept the
  session and its fd. Linux does this even after a send gets `ECONNRESET`. That was
  checked by hand.

It changed one rule on the way. A stream that failed by a timeout, with a write half
done, wrote the rest. Now `stream_next` starts nothing once the stream failed.

**Fix.** A write's error completion shuts the socket and sets `let_go`, as the
timeout in `stream_wait` does. It runs on the socket's thread.

**Failing test.** `TestStreamReset` stalls a client on a pipelined MGET and KEYS *,
as `TestStreamGate` does, until INFO says a worker waits on it. The client then
pipelines 100 PINGs, which stay unread, and resets with `SO_LINGER` 0. On the old
code the worker stopped waiting, but `redis_sessions` stayed at 2 past the 60 s
backstop. Now it drops back to 1.

**Core.** `stream_core.h`:
- `wait_step(failed, backlog, below)` says whether the worker goes, waits, or gives
  up;
- `posts_next(busy)` says whether queuing posts `stream_next`;
- `next(failed, written_all, pending_empty)` says whether `stream_next` writes the
  rest, swaps in what is queued, or stops;
- `written(error, failed, backlog, length)` gives a completion's backlog, progress,
  net error, failure and `let_go`.

**Exhaustive test.** `TestStream` runs a worker that queues one to three pieces of
one to three bytes, with the high water at 2, then flushes. It runs against every
order of the posted `stream_next`, write completions that take one byte, take all of
it or fail, a client reset, a timeout with its posted shutdown, and a server stop.
The reset, the timeout and the stop are each tried with and without. That is 153,642
states.

It checks:
- the worker never hangs, and one that finishes had every byte taken;
- the backlog is what is queued plus what is unwritten;
- a worker queues only under the high water;
- one write or posted `stream_next` at a time, and none once the stream failed;
- a wait times out only when the client took nothing;
- one net error for a stream;
- a failed stream, with the server not stopping, is let go and left idle.

It catches 11 mutants. Two are the code before TODO 943: a failed write that does not
let go, and a failed stream that writes the rest.

**Assumptions:**
- `socket_write_mutex` is taken only by this session's workers, never by its socket
  thread. Read from the code. Nothing checks it.
- `bind_socket_writer` sends the session's `stream` first if it isn't empty. That
  buffer belongs to the socket's thread. During a batch the read chain is down, so
  nothing writes it then, and the batch moves it into `ctx->stream` before it starts.
  TSan has not reported it.
- `cv.wait` with a predicate wakes on every change made under `stream_mut` with a
  `notify_all`. The model takes the predicate as the wake and does not model the cv.
- A write on a socket that was shut down completes with an error. Linux does. On
  Windows a cancel does the same for the timeout's shutdown (TODO 599). The error
  completion's own shutdown has no write out to cancel.
- The collector frees on `let_go` alone and only drops its reference, so a batch
  still running keeps the session alive. That is the TODO 496 rule.

## Block registration and erase across shards

Level 3 since TODO 944. Level 2 since TODO 926.

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

**The second audit (TODO 944).** It covered the park across a space that goes away.
UNLOAD and KSPACE DROP take the space out of the registry and wake every waiter in it
(`fail_foreign_flights`). The shard vector is fixed once the space is built, so a
stored shard index can't go stale. Add once and remove all make the add again on a
retry safe. It found two holes and one wrong literal.
- **A woken BLPOP parked on the dead space.** The wake ran the pop in the old space.
  Its list was still empty, so the pop asked to park again, on the old space's
  lists. A push goes to the space opened again under the name, so nothing woke it.
  At timeout 0 it waited for ever.
- **The caller kept the park's space after the answer.** `block_space` lived until
  the next park. An idle client that once blocked in a dropped space held it. The
  last holder of a dropped space deletes its files, so closing that client deleted
  the files of a space made again under the same name.
- **CALLF's wake key did not start with `\x01`.** In `"\x01function.wake."` the
  escape takes the `f` as a hex digit, so the key started with `0x1f`. Nothing read
  the first byte before this fix.

**Fix.**
- `flush_keyspace` marks the space unloaded before it wakes the waiters.
- After the handler, `call_blocks` refuses a retry in an unloaded space with an
  error, if any key is one a write finds by name. A CALLF parks again on its own
  wake key: its job owns the session's state, and wakes it through a pointer it
  kept. Answering it early is the TODO 874 hazard.
- `call_blocks` drops `block_space` once the park is over: answered, refused, or a
  retry with no blocks left. A timeout erases the blocks before the handler runs, so
  the space has to live until then.
- The wake key literal is split so it starts with `\x01`.

**Failing test.** `TestBlockUnload`:
- a BLPOP at 0 in a space that is then unloaded, with a push from a new connection.
  On the old code no reply came in the 60 s backstop and a key stayed blocked. Now
  it gets an error and `blocked_keys` is 0;
- a client that parked with `DR:BLPOP` and was answered. Then `KSPACE DROP DR`, a key
  set and saved in the new `DR`, and the client closes. On the old code the key was
  gone after a restart. Now it is there;
- a CALLF parked on an HTTP request held by a latch, in a space that is then
  unloaded. It still answers what its job returns.

Each piece of the fix was taken out in turn: the check and the reset, the check, the
reset, the CALLF exemption, and the literal. Each fails the test.

**Core.** `block_core.h`:
- `adds(already_there)`, `drops_list(left)` and `wakes(waiting)`: the list rules.
  `shard.h` calls them;
- `by_name(key)`: a key a write finds by name, not the server's own `\x01` key;
- `after(retry, space_gone, any_by_name)`: answer, park again, or refuse;
- `keeps_space(what, has_blocks)`: whether the caller keeps the park's space.

`call_blocks` calls the last three.

**Exhaustive test.** `TestBlock` runs two clients, each a BLPOP that names its key
twice or a CALLF on its wake key. It runs them against every order of parks, two
pushes, two unloads, the CALLF job's end, and wakes, over three generations of the
space. That is 4631 states.

It checks:
- a session is on a list at most once, and only while it is parked there;
- a list is kept only while someone waits on it;
- no BLPOP is parked in a space that went;
- an answered client holds no space, and a parked one holds the space it parked in;
- nothing is refused in a space that is still open;
- no CALLF is answered while its job runs;
- at the end, no BLPOP is parked with something to pop, and no CALLF after its job
  ended.

It catches 10 mutants. Two are the code before TODO 944: a retry that always parks
again, and a caller that keeps the space.

**Not fixed, noted:**
- A connection that ran USE on a dropped space holds it too. Fixed in TODO 945: see
  "Dropped spaces: files and holders".
- `"\x01AUTHSEED"` in `auth_api.h` is really `0x1a` then `UTHSEED`, by the same
  escape rule. Fixed in TODO 946: see "Cluster log words: the users seed".

**Assumptions:**
- A block handler that parks again (`block_again`, `park_on_lock`, a second
  `add_block`) names one key. `erase_blocks` takes the latest set only, so a handler
  that swapped in several keys from another shard could leave the old ones behind.
  True of every handler today. Nothing checks it but the gauge in `TestBlockSpace`.
- A block's shard index finds the same shard at erase as at registration. A range
  sharded space can move the key to index mapping, but both sides use the stored
  index, not the key.

## Dropped spaces: files and holders

Level 3 since TODO 945.

**What it is.** KSPACE DROP takes a space out of the registry and marks its shards to
delete their files when the last holder lets go. Anything can hold the space: a
connection that chose it with USE, a park, a call in flight. The next open of the
name builds a new space from the files on disk. A shard deletes its files by name,
not by the space it belongs to.

**What the audit found.** Three holes, one cause. A connection that ran USE on the
space held it after the drop.
- Its close deleted the files of a space made again under the name. The new space's
  saved keys were gone after a restart.
- While it stayed open, the files stayed. A new open of the name loaded the dropped
  keys back.
- Its commands ran in the dead space. A write went where nobody reads. A BLPOP
  parked where no push reaches.

**The fix.**
- The drop deletes the files there and then (`is_drop`).
- `key_space.cpp` counts the opens of a name since its drop (`note_drop`,
  `opens_since_drop`). The last holder deletes the files only if nothing opened the
  name since. Then the files can only be a late save of the dropped space.
- At the start of each command, `run_params` moves a connection whose space went to
  the space under the name now. Not inside an open transaction: its overlay sits on
  the space it began in.

**Failing test.** `TestUseDrop`. A raw connection runs `USE DR` and writes a key.
Another saves and drops `DR`, then writes and saves `kept` in the new `DR`. It checks:
- the new `DR` doesn't hold the dropped key. On the old code it did;
- the connection's next write is seen in the new `DR`. On the old code it wasn't;
- its BLPOP is answered by a push to `DR`. On the old code no reply came;
- after it closes and the server restarts, `kept` is there. On the old code it was
  gone.

Each piece of the fix was taken out in turn: the delete at drop, the release guard,
and the follow. Each fails the test.

**Core.** `drop_core.h`:
- `deletes_at_drop()`: the drop deletes the files;
- `deletes_at_release(opens_at_drop, opens_now)`: the last holder deletes only if
  nothing opened the name since;
- `follows_name(space_gone, in_transaction)`: whether a connection moves.

**Exhaustive test.** `TestDrop` runs one name, a connection that chose it, and
another that names it with `space:CMD`. It runs them against every order of writes,
a save, a transaction, a close, two drops, an unload, and opens, over four
generations of the space. That is 15054 states.

It checks:
- a space under the name never holds a key written in a dropped space;
- no file of a dropped space is left under the name;
- a release never deletes the files of a space that was not dropped;
- a write outside a transaction never lands in a space that went;
- the connection's space doesn't change inside a transaction.

It catches 5 mutants. One is the code before TODO 945: no delete at the drop, a
release that always deletes, and a connection that never moves.

**Assumptions:**
- A save that renames into place after the delete at drop could bring the files
  back. A save holds no lock that the drop excludes. The release delete covers it
  only if nothing opened the name in between.
- On Windows a mapped file may not delete at the drop. The release delete covers it
  on the same terms.
- A holder mid-call, such as a long CALLF that calls SAVE, still writes the dead
  space.
- Only the RESP session follows the name. HTTP and the other transports are not
  checked.
- Commands queued by MULTI keep the space they were queued in.
- Scratch and overlay shards keep the old rule. They are not dropped by name.

## Cluster log words: the users seed

Level 3 since TODO 946.

**What it is.** A Raft log entry is the user, its rights, and the words of a command.
The server logs entries of its own: a RELOAD's publish, a queue's push or settle, a
fetch's miss, and the cluster's first users, the seed. Their first word starts with
`\x01` and the user is empty. `rpc_caller::call` refuses a client's call to any name
that starts with `\x01`. `run_logged` takes an entry whose first word is the seed
word as the seed.

**What the audit found.** The seed word was written `"\x01AUTHSEED"`. The escape
takes the `A` as a hex digit, so the word was `0x1a` then `UTHSEED`. The gate checks
for `\x01`, so a client could name it. A client with SETF rights in a Raft space
stored a function of that name and called it.
- With no arguments, every member failed the entry: "a users seed entry outside a
  cluster, or with 1 words". Every member counted an apply failure and rebuilt the
  space.
- With one argument, every member ran it as the seed. After INIT that does nothing.
  The function never ran.

The other server words (`\x01PUBLISH`, `\x01QUEUE`, `\x01MISS`, `\x01none`) are
spelled right.

**The fix.**
- The seed word is two literals, `"\x01" "AUTHSEED"`, so `\x01` stays one byte.
  `auth_api.h` asserts it matches the core.
- `run_logged` reads the old spelling as the seed only from an entry with an empty
  user. Logs written before the fix still hold it, from the server. From a client it
  is the stored function the client named.
- The test knob `old_seed` (`BARCH_TEST_FAULT`) makes a node write the old spelling,
  as a node from before the fix did.

**Failing test.** `TestAuthSeed`, on a three-node cluster. It stores a function named
`0x1a UTHSEED` in a Raft space and calls it. It checks:
- with no arguments, it answers as the function and its write is there. On the old
  code it answered an unknown command error;
- every member holds the same digest and none failed the entry. On the old code all
  three counted a failure and rebuilt;
- with one argument it answers as the function, and the users are as they were;
- the first node's own user logs in on every member, with the seed in the new
  spelling and, under `old_seed`, in the old.

Each piece of the fix was taken out in turn in a cluster build: the old word written
again with no user check, the user check alone, and the old spelling not read. Each
fails the test.

**Core.** `log_words_core.h`:
- `seed_word`, `old_seed_word`;
- `server_word(w)`: a word no client can call;
- `from_server(user)`: an entry the server logged;
- `is_seed(word, user)`: the new spelling from anyone, the old from the server only.

**Exhaustive test.** `TestLogWords` runs every writer against every word: the server
logging each of its words, the server before the fix logging the old seed, and three
users calling the server words, the old spelling, names close to them, and plain
commands. That is 44 entries.

It checks:
- a client can't call a server word;
- a client's entry that the gate lets through is never the seed;
- every seed the server logged, in either spelling, is the seed;
- no other server entry is the seed.

It catches 4 mutants. One is the code before TODO 946: the word alone decided.

**Assumptions:**
- An empty user appears only in server entries. A client's `rpc_caller::user` starts
  as `default` and is never empty. Only the runner's `set_acl` sets an empty one.
- A mixed-version cluster still has old members. They take a client's call to the
  old name for the seed, so the hole stays open until every member runs the fix.
- SETF accepts a name that starts with `\x01`. Nobody can call it. Noted, not fixed.
- `TestAuthSeed` needs a cluster build. It SKIPs in the regular trees and does not
  run under a sanitizer.

## Session collector and `let_go`

Level 3 since TODO 932. The held rule since TODO 933.

**What it is.** The collector thread sweeps the session lists. It drops a session on
`let_go`, or when a peek of its socket reads 0: the peer's FIN or reset has been
read. It only drops its own reference, so a handler still out keeps the session
alive. A session nothing will run again lives only through the collector.

**The bug.** A TLS session reads through the ssl stream. A bad record or a
close_notify ends the read, but the TCP stays open, so the peek reads nothing. No
path set `let_go`. The session, its fd and its CLIENT LIST line stayed for as long
as the client stayed connected. The same thing TODO 859 fixed for a failed
handshake, one step later.

**Core.** `park_core.h`:
- `on_read_end(eof, mid_request)`: whether the end counts in `net_errors`, and
  that the socket is shut. Every read that ends the chain shuts the socket once
  what is queued is out (`when_sent`), and sets `let_go`.
- `collects(let_go, held, peek)`: the collector's rule. `server.cpp` calls it.
  `held` is a use count above 1: a read, a write or a worker's call holds the
  session. A held session goes on `let_go` only. The peek is for one nothing
  holds - TODO 933.

`peek_core.h` (TODO 934): `answer(n, fail)` turns recv's count, or the kind of its
error, into the peek. A socket that is reset, aborted, shut or no longer connected
answers 0. `socket_peek.h` sorts the error into a kind on each platform.

**Exhaustive test.** `TestParkCore` models a client with up to four moves: a
request, a FIN, a reset, and over TLS a bad record or a close_notify. The session
reads and writes in every order with them, 11828 states over TCP and TLS. Where the
session has no step left, it checks:
- a session nothing will run again is collected;
- a session its client still uses is not;
- every request read before the end is answered, unless the client reset.

In every state, since the collector may run at any time, it checks that a running
session stays in the list - TODO 933.

It catches:
- the old code (TLS: bad record, then the read);
- a close_notify left open (TLS: close_notify, then the read);
- a shut before the replies are out (TCP: req, FIN, two reads);
- a collector that drops every session;
- the rule before TODO 933, where a peek of 0 drops a held session (TCP: FIN).

**Live test.** `TestSessionEnd`: a bad TLS record and a close_notify with the TCP
left open both fail on the old code, each waiting out its failsafe, and pass now.
A TCP half close after a pipeline, a BLPOP parked at a half close, and a reset are
guards. They passed before and after.

`TestHalfCloseStop` (TODO 933): a client stalls a streamed KEYS until a worker waits
in `stream_wait`, with `rpc_client_max_wait_ms` at 0. It then shuts its write side.
After two collector sweeps (INFO clients `collector_sweeps`) the session must still
be in CLIENT LIST, and SIGTERM must end barchd. On the old code the session left
the list and barchd did not stop. A half close with nothing out still leaves the
list. That part is a guard.

`TestSocketPeek` (TODO 934) checks `answer` over every input, with three mutants,
then peeks real loopback sockets: nothing sent, a byte, a FIN after a byte, a reset,
and the session's own shutdown. A wait is a select with no timeout. Under wine the
old peek read -1 on every call after a reset. Linux read -1 with ECONNRESET, then 0.
Both read 0 at once now. `win32/CMakeLists.txt` builds it as `socketpeektest.exe`,
run by hand, since `run_tests.py` runs Python tests only.

**Assumptions:**
- The peek reads 0 once a FIN or reset is read and nothing is left unread. Checked
  on Linux and under wine, not on Windows itself.
- After the session's own shutdown, Linux peeks 0. Under wine select does not mark
  the socket readable, so the peek says EAGAIN. Every path that shuts a session
  sets `let_go` too, so the collector does not need that peek.
- With every read end setting `let_go`, the peek is a backup. It still catches a
  session whose chain stopped some other way with a dead peer.
- A use count of 1 under `session_latch` cannot rise again. Every copy is made
  from the list under the latch, or from a handler that holds one. A count read as
  2 just as it falls to 1 only keeps the session one sweep longer.
- A held session reaches `let_go`. Every read end sets it, and so do the stream
  and write failure paths. A session that held itself for good would stay in the
  list. Its fd was never freed in that case anyway, since the collector only drops
  its own reference.
- On loopback the FIN is in the server's socket once the client's shutdown
  returns. The live test relies on that.

## Raft write paths: the gate

Level 3 since TODO 937.

**What it is.** `rpc_caller::call` decides what a command does in a space replicated
with Raft. It refuses it, sends it into the log, refuses it as NOTLEADER, runs it as
a write, or runs it as a read under the read rules. The inputs are the command's
categories, its class in `raft_writes`, whether it is a stored function and how
that runs, and the space's groups. `routes_itself` commands, runs from the log and
the in-process caller skip the gate.

**The bug.** INDEX is registered as read, write and keys. It has no data category.
`raft_writes::of` gave its FIND, LIST and CHAINS the class server. A server command
skips the leader check and the read rules both. So a follower answered INDEX FIND
from its own copy, which may lag, while its GET answered NOTLEADER. An INDEX word
the table did not know was server too, so it failed open.

**The fix.** A fifth class, `reads`: a subcommand of a write command that only reads
the space. INDEX FIND, LIST and CHAINS have it, and so does any INDEX word not in
the table. The gate treats it as a read of the space. CLUSTER WRITES lists it. A
function run from the log may call it, as it may call GET.

**Core.** `raft_gate.h`. `gate(gate_in)` takes plain values and returns a step and
whether to gather the writes into one batch. `rpc_caller.h` fills `gate_in`, calls
it, and does what the step says.

**Exhaustive test.** `TestRaftGate` runs all 25600 inputs. It checks:
- a local write is refused;
- a logged command goes into the log, into every group's log, or (SETF and REMF in a
  split space) is gathered;
- a function that runs from the log does, or gathers in a split space;
- a write to the space on a node that does not lead its one group is refused,
  unless a transaction runs it, it is COMMITTX, or a function joining a transaction;
- a read of the space always goes under the read rules;
- a server command runs with no check;
- nothing gathers in a space of one group.

It catches:
- the old code: a read subcommand taken as server;
- no leader check;
- a logged command run on one node;
- a local command let run.

It also checks the table: INDEX's words and an unknown one, and that CLUSTER WRITES
lists them as `of()` decides them.

**Live test.** `TestClusterIndex`: a follower without follower reads answers INDEX
FIND, LIST and CHAINS with NOTLEADER, as its GET does. All three failed on the old
code, each answering from the follower's copy.

**Assumptions:**
- The shell fills `gate_in` right. Nothing but the cluster tests checks that the
  categories, the class and the leader flag it reads are the ones the command uses.
- The leader flag is read once, before the command runs. A leadership change after
  that is caught at the shard, which checks its own group on every write.
- Other per-subcommand tables still fail open to server: DICTIONARY (not SET),
  FUNCTIONS (not SYNC) and FOREIGN FAKE's reads. None of their read words reads
  the space's data today. A new one that does needs a `reads` entry.
- No regular build tree has `BARCH_CLUSTER=ON`, so the cluster tests run only in a
  tree made for them.
- Leadership, leases, snapshots and `unknown` outcomes are inside NuRaft. Not
  modelled here.

## Transactions: the commit gate

Level 3 since TODO 938.

**What it is.** Each space has a `commit_gate`. Read commands and BEGINTX hold it
shared. A commit that touches some shards holds it in commit mode. A commit that
needs the whole space holds it alone. Shared holders go together, and so do commit
holders, but never one of each. An alone holder goes with nobody. A shared holder
comes in whenever no commit or alone holder is in, so a thread can take it shared
twice. A commit does not come in while a shared or alone holder waits.

**The audit.** It covered the gate, `shard_claims`, the lock table in `tx_locks`,
the lock waits in `tx_api.cpp`, and the snapshot book in `tx_versions`. It found no
hole:
- a timed-out lock wait clears through `tx_lock_timed_out`, and a parked one clears
  on abort through `release_locks`;
- a deadlock ring is found through waiting writers and spans;
- a layer is dropped only when it is older than every open snapshot;
- a commit is seen whole or not at all: read commands hold the gate shared, and a
  commit holds it while its writes go in.

One gap is accepted by design. A snapshot can expire while a command runs. That
command may then read some keys from their layers and later keys from the base.
The transaction finds out at its next command, and COMMITTX's `claim` fails, so it
aborts. Nothing it read commits.

**Core.** `commit_gate_core.h`. `may_enter(counts, mode)` says who may come in.
`wakes(counts, mode)` says whether one leaving must wake the waiters. The gate
keeps the counts under its mutex and waits on its condition.

**Exhaustive test.** `TestCommitGate` runs one to four threads, each with a script
from six, through every order the gate allows. That is 1554 choices of scripts. A
thread that cannot come in sleeps until a leaver wakes everyone. Nothing wakes by
itself, so a missed wake leaves threads asleep for good. It checks:
- a reader and a commit are never in together, and an alone holder is alone;
- no order ends with threads asleep that nobody will wake;
- a commit never comes in while a reader or an alone holder waits;
- a thread holding it shared takes it shared again without waiting.

It catches:
- the last reader out waking nobody;
- the last commit out waking only commits;
- a reader let in beside a commit;
- an alone holder let in beside a commit;
- a commit let in past waiting readers;
- a reader kept out while an alone holder waits, which deadlocks a read inside a
  read.

**Assumptions:**
- A condition variable may wake a thread for nothing. The model never does, which
  is the harder case: a missed wake shows as a stuck state.
- Readers come first, so a steady stream of readers can keep an alone holder out.
  That is the old `std::shared_mutex` behaviour and is kept.
- A thread holding commit mode never takes the gate shared. Nothing checks that
  but the code paths.
- `shard_claims`, the lock table and the snapshot book are audited but have no core.
  The lock table and the snapshot book keep maps and sets, which a core may not
  include.

**A hole found later (TODO 955).** The audit missed the pessimistic read check. A
commit holds the shards its read log names. The check then routed each read key
again, by the key's own hash. A container's keys live in its name's shard, so for
those it searched a shard the commit did not hold. It found nothing there and
refused, so a pessimistic DEL of a list, hash or ordered set never committed. It
also read-locked that shard while holding write latches, so two commits could each
wait on the other's until the lock wait ran out. Now `tx_hash_now` searches the
shard the read was kept in. The cluster prepare check does the same.
`TestTxContainerRead` covers it. There is no core: the choice of shard is one
line. That every shard the check reads is held rests on the read log and the held
set being built from the same walk.

## Saves while writing: the save view

Level 3 since TODO 939.

**What it is.** A save freezes a view of a shard under the write latch. It records
the arena state and borrows the main arenas' pages, and later writes go to CoW
pages. It then lets the latch go. `write_frozen` takes the view under a shared
latch and writes the files from it under `save_load_mutex`. It takes the latch
again to end the view and merge the CoW pages. A clear or a load moves the
generation and drops the view. A BEGIN waits while a view is up. A second save
waits too, and tries again when the view goes.

**The audit.** It covered the freeze, the write, the end, `send_frozen`, clear,
load, BEGIN and defrag. Defrag is safe while a view is up: `pop_last` returns while
a CoW map is up, and `release_free_pages` returns while pages are borrowed.

It found one hole. `save()` lets the latch go between its freeze and
`write_frozen`. A clear in that gap drops its view. Another save can then freeze a
new view before the first gets to `write_frozen`, which took whatever view was up.
Both saves wrote from that view. The first to end it merged the CoW pages while the
second still read the borrowed pages. A whole-space save or a replica send could
be the second.

**Fix.** A view has a `claimed` flag. The first writer to take it sets it. A second
writer gets no view and returns false. That writer's own view went with the clear,
so it has nothing to write.

**Failing test.** `TestSaveViewShard` freezes one view and runs two `write_frozen`
calls. A test hook stops both between the take and the write. Before the fix both
wrote. Now one does. It needs no timing.

**Core.** `save_view_core.h`:
- `on_freeze(view_up, transacted)` gives busy, direct or frozen;
- `may_take(view_up, claimed)` says whether a writer takes the view;
- `may_write(generation_taken, generation_now, log_first, refused)` says whether
  it writes;
- `ends_view(up, mine)` says whether it ends the view that is up;
- `may_begin(view_up)` says whether a BEGIN starts.

**Exhaustive test.** `TestSaveView` runs one to four threads, each a save, a clear,
a BEGIN or a put, through every order. That is 340 choices of threads. A write
branches on whether the change log reached the disk first. It checks:
- a save writes from live pages, and from CoW maps that are still its own;
- a save never writes ahead of the change log;
- a freeze never takes a transaction's CoW maps;
- no thread waits on a view nobody will end, and no view stays up at the end.

It catches:
- a writer taking a view another has, which is the rule before the fix;
- a freeze over a view that is up;
- a freeze inside a transaction;
- a write after a clear;
- a write ahead of the log;
- a save ending whatever view is up;
- a save never ending its view;
- a BEGIN over a view that is up.

**Assumptions:**
- A clear and a load take `save_load_mutex` and the latch, so the model runs them
  as one step. The write step holds the mutex, so they never overlap it.
- A put with a view up goes to CoW pages and leaves the view as it froze. The
  arena code does that; the model does not check it.
- `wait_for_frozen_save` waits at most 50 ms and then looks again. The model treats
  it as a wait that only the view going ends, which is the harder case.
- A whole-space save that loses a shard's view to a lone save counts that shard as
  failed, the same as when a clear drops its view.

**A writer that throws (TODO 953).** The model's writers never throw. `send_frozen`
did, when a RETRIEVE's socket write failed. The throw skipped the end of the view
it had claimed. The sender then ran `send_frozen(nullptr)` to let the view go, and
that found the view claimed and left it up. Every later freeze of the shard waited
for it, and the remote barchd did not stop on SIGTERM. `send_frozen` now ends its
view in a guard's destructor, so a throw ends it too. CI's TestRetrieve hung on it.

**Left open.** `write_frozen` ends its view after the writes, not in a guard. A
throw from `write_pair` leaves the view up in the same way. Nothing seen throws
there yet.

## Eviction: the sweep rules

Level 3 since TODO 940.

**What it is.** Each maintenance tick under memory pressure, each sweep picks one
page of leaves, copies it, and evicts keys from the copy through `evict_logged`.
The random, volatile-random, volatile-ttl, clock and expiry sweeps pick a random
page. The clock sweep serves LRU and LFU (TODO 456). `evict_logged` writes the change log and
the replication stream, and keeps a before-image for an open snapshot.

**The audit.** It covered every sweep, `may_evict`, `evict_logged` and the paths
that hold a key while a sweep runs:
- a transaction: the sweep runs on the maintenance thread and doesn't check key
  locks, by design (TODO 719). Its eviction goes in the change log, and a ROLLBACK
  truncates the log back to the BEGIN mark (DONE 508);
- a save view: `art::erase` goes through modify, so it writes to CoW pages and the
  view keeps the key;
- a reader: every sweep holds the shard's unique latch (`storage_release` or
  `unique_latch`), so no reader is inside the shard while it evicts;
- a Raft space: only expired keys go (TODO 610);
- function, meta, file, graph and id keys: `may_evict` keeps them.

It found one hole. The random pick drew from `[1, max_allocated_page_num()]`. A
shard with no pages has a max of 0, and `[1, 0]` is undefined for
`uniform_int_distribution`. Memory is counted for the whole server, so an empty
shard sweeps when the others are full. A plain build drew a garbage page, which
`get_page_buffer` turned into a no-op. With `_GLIBCXX_ASSERTIONS` it aborted.
Three sweeps had it.

It also found two things it did not fix:
- LFU takes nothing. `get_lru_page` always returns an empty page. That is TODO 456,
  a design decision.
- A sweep checks the threshold once and evicts a whole page per tick. It does not
  stop when memory drops under the limit part way. That is by design.

**Fix.** `page_range` returns no range for no pages, and the three sweeps return.
volatile-lfu now takes keys with an expiry, like the other volatile policies. It
read the leaf's volatile bit, which no command sets. LFU takes nothing yet, so this
changes nothing today.

**Failing test.** No end-to-end test fails: the build has no assertions, and the
plain build's garbage draw hides as a no-op. The proof is the model's mutant for
the old range, which `TestEviction` catches, and a short program that aborts with
`_GLIBCXX_ASSERTIONS` on `dist(1, 0)`.

**Core.** `eviction_core.h`:
- `under_pressure(memory, threshold)` says whether a sweep runs;
- `page_range(max_page)` gives the pages a random pick draws from, or none;
- `takes(policy, key)` says whether a sweep takes a key, from five facts: deleted,
  kept by `may_evict`, has an expiry, expired, read since the clock passed;
- `on_clock(policy)` says whether a policy runs on the clock: LRU and LFU;
- `ttl_cut(n)` gives the last of `n` sorted candidates volatile-ttl takes;
- `raft_may_evict(expired)` says whether a Raft space evicts a key.

**Exhaustive test.** `TestEviction` runs every policy against every key state, every
page of up to five keys at expiries 0 to 3 through volatile-ttl, and every page
count from 0 to 8. It checks:
- no sweep takes a deleted or kept key;
- the volatile policies take only keys with an expiry;
- LRU and LFU run on the clock and never take a read key;
- the expiry sweep and a Raft space take only expired keys;
- volatile-ttl takes the nearer half and nothing later than a key it leaves;
- a random pick never draws from an empty range or page 0.

It catches 13 mutants, the old page range and LFU off the clock among them.

**Assumptions:**
- `may_evict` is right about which keys belong to a set. The core takes its answer
  as one fact.
- The page copy holds the leaves as they were. A key written after the copy is
  judged by its old state. A key evicted that way was written under pressure, and
  the change log records it.
- `l->expired()` implies `l->is_expiry()`. The model skips the state where it
  doesn't.

**LFU - TODO 456.** Both LFU policies took nothing. Their sweep read
`get_lru_page`, a stub that returns an empty page. Past the cap, writes were
refused. Now they run on the LRU clock: `run_sweep_lru_keys` serves all four
policies, and `apply_lru_options` stamps the read bit for LFU too. It is not a
frequency count. A key read once since the last pass is kept as long as one read
a thousand times. A count needs a byte on every leaf and a new file format, and
the leaf's flags byte is full.

The test for it found a second bug. `hash_add_leaf` read the new leaf through
`const_leaf()`, which sets the read bit. So every key SET wrote looked read. The
first pass over a page took nothing and cleared every bit, and the second took
everything. A read between the two passes was the only thing that told keys
apart. That broke the rule in `node_impl.cpp` that a new key starts cold (TODO
307). It reads through `peek_leaf()` now. `ApplyEvictionType` also called
`apply_lru_options` before it set the LFU flags, so it read the old ones. It
calls it last now.

`TestVolatileEvict` runs both LFU policies. Half the keys with an expiry are read
before the cap drops. The first time the count drops, every key read must still
be there. Only a second pass over a page takes one, so this holds however many
pages were swept. On the old code both policies left all 400 keys. With the LFU
fix and the old `hash_add_leaf`, no drop left a key standing: one pass cleared
the bits, the next took all.

## Function sync and queues: the wake and post rules

Level 3 since TODO 941.

**What it is.** The function sync poller is one thread. Each pass it reads the
repository list, runs the ones that are due or asked for, and sleeps on a
condition variable until the next is due. FUNCTIONS SYNC, a configuration write, a
`functions_sync_ms` change and a stop each set a flag and notify it.

The queue consumer lives on a strand of a server's io_context. A publish, a settle
or a Raft entry wakes it. A handler runs on the function pool, and its count goes
back to the strand when it ends. `server::start` and `server::stop` stop the
consumer without waiting, then destroy the io_context.

**The audit.** It covered:
- the poller's flags and its wait, its schedule, start and stop;
- the run lock `mu` and the `in_sync` check (TODO 583), and `hook_running`;
- the consumer's `stopping`, `abandoned` and `pending`, its token, timer and strand;
- a delivery's `settled` flag and the reply's `once` flag. Each is one exchange, and
  the one that wins settles. Both hold.

It found two holes. Both are a check split from what it allows.
- **The poller's wake.** Every notifier set its flag and notified without the
  wait's mutex. `wait_for` checks its predicate under the mutex and then sleeps. A
  flag and notify between the two were lost, and the poller slept the whole wait,
  up to the idle minute. A stop at shutdown then waited for it.
- **The consumer's post.** A finished handler checked `abandoned`, then posted its
  count to the strand. A wake checked `stopping`, then posted. A restart could set
  the flag, return, and destroy the io_context in between. The post then landed on
  a destroyed context.

It also found two smaller things:
- The wait read `kick_names`, which `request_repo_sync` writes under `state_mu`,
  while holding only `mu`. Nothing calls `request_repo_sync` today, so it didn't
  race. A name with no repository would have kept the wait from ever sleeping.
- `mq::start` loads the slot and then stores it, so two at once would make two
  consumers. Its callers are serial: barchd's start runs before a server exists, and
  `server::start` holds `srv_mut`. Not fixed.

**Fix.**
- **Function sync:** the flags that wake the poller are set under `wake_mu` by
  `wake_poller`. The poller waits under `wake_mu`. The wait reads `kick_named`, an
  atomic the poller clears each pass, instead of the set.
- **Queue consumer:** it has a `post_mu`. A wake and a completion check and post
  under it. Stop sets `stopping` and `abandoned` under it. `asio::post` never runs
  the handler inline, so nothing waits under the lock.
- **The schedule:** moved into the core unchanged. It keeps whole milliseconds, one
  value per pass, in place of a steady clock time point.

**Failing test.** No end-to-end test fails on the old code. Both windows are inside
a call: inside `wait_for`, and between a check and `asio::post`. A test can't stop a
thread there without a timing guess. The proof is the model's two mutants that run
the old protocols, which `TestWake` catches.

**Core.** `wake_core.h`:
- `poller_wakes(running, kick_all, kick_named, kick_rescan)` says whether the poller
  stops waiting;
- `plan(repo, schedule, now, index)` says whether a repository runs this pass and
  how long the poller may sleep for it. `shorter` folds the waits;
- `may_post(poster, stopping, abandoned)` says whether a wake or a completion posts.

**Exhaustive test.** `TestWake` runs:
- the poller over two repositories, each at 0, 150, 300 or 500 ms, enabled or not,
  with or without an ask and an interval change, for two seconds;
- every order of the poller's check and sleep against a stop or a kick;
- every order of a completion and a wake against stop's two flags and the
  io_context going.

It checks:
- a repository runs when it is due, never before and never after;
- an ask runs that pass, and interval 0 runs only when asked;
- a disabled repository never runs, and two first runs never share a pass;
- a changed interval holds from the change, and no sleep is 0;
- no notify is lost;
- no post lands after the io_context goes;
- a completion before stop gives up keeps its count.

It catches 16 mutants. Among them are the two old protocols, TODO 583's wait, TODO
788's rounding and TODO 584's changed interval.

**Assumptions:**
- A notify reaches a thread asleep on the condition variable, and `wait_for` checks
  its predicate under the mutex before it sleeps. That is the standard's contract.
- Stop always sets `abandoned` in the model. The code sets it only when something is
  still out. With nothing out, nothing can post.
- The model's poller wakes exactly at its wait. A late wake only runs a due
  repository late, by the same amount.
- `asio::post` of a handler to a live io_context doesn't fail, and destroying the
  context destroys handlers it never ran.

## Luau calls: the park rules

Level 3 since TODO 942.

**What it is.** `CALLF` starts a Luau call in `function_api.cpp`. The first slice runs
on the io thread. After that the call goes to the function pool, and the session
waits on its wake key. `pump_slices` in `luau_scheduler.cpp` resumes the call's
coroutine and acts on what comes back: a return, an error, or a yield.
Since TODO 929, the old `luau_driver.cpp` is split across `luau_internal.h`,
`luau_scheduler.cpp` (execution, slicing, parking, preemption), `luau_compiler.cpp`
(compilation, caching, `require`, service specs, with no mutexes or parks),
`luau_bindings.cpp` (storage, `fs`, `graph`, `tx`, `uuid`, with no mutexes), and a
thin `luau_driver.cpp` facade.

A yield is one of two things. A budget yield means the slice is used up, and the
call goes back on the pool. A park means a C function waits on something that is not
Lua: a fetch, a mail send, a RESP call, a script lock. `park_call` marks the call
parked, the C function starts its work, and `park_yield` yields. `complete_call`
runs when the work ends. The pump and the completion meet under the park's lock,
through its `done` and `waiting` flags, so either may come first.

**The audit.** It covered:
- `finish_job`, `finish_raised` and `pump_call`, and the stop of TODO 873 and 874;
- the stop raised again at each interrupt, and the deadline watch for hookless
  native frames;
- the session's `finished` and `can_wake` recheck and its block registration;
- `script_lock_park`'s fire and time up;
- `start_stored` and the states given back after the call ends;
- the four park sites: fetch, mail, RESP and the script lock.

It found one hole.
- **A raise after `park_call`.** Luau turns a `std::exception` from a C function into
  a Luau error, and a script's `pcall` catches it. Between `park_call` and
  `park_yield` a raise can come from an allocation in the pool post, the request
  start or the RESP client, or from a failed thread start in the lock timer. The
  call then ran on, still marked parked. The pump took its next budget yield for the
  park's. With no completion coming, the call waited forever. Its wait ran out, the
  session told it to stop, and then waited for it with no timer. The client never got
  an answer. If the work had started, its completion resumed the call at the budget
  yield and pushed the park's values there.

It also noted one thing not fixed:
- `wait_for`'s bounded wait for a server-state call gives up and lets the call run
  on, with no stop set. The call ends on its own deadline.

**Fix.** `park_yield` marks its yield in the call's `run_ctx`, `park_yielded`. The
pump reads and clears the mark at every yield. A call marked parked whose yield has
no mark has a stale park. The pump drops it and takes the yield as a budget yield.
The stale park's work may still finish. It finds `waiting` false and resumes
nothing.

**Failing test.** `TestParkRaise` sets `BARCH_TEST_FAULT=park`, a test knob in
`src/test_fault.h` that raises at the end of `park_call`. A script catches the
raise from `http.request(...):get()` with `pcall`, then loops past its inline slice.
On the old code the call never answered and the test hung until it was killed. Now
it answers `caught test fault park 12500002500000`, twice on the same connection.

**Core.** `luau_park_core.h`:
- `yield(parked, park_yielded, done)` says whether the pump waits, resumes now,
  takes a budget yield, or drops a stale park first;
- `next_slice(inline_only, http, overtime)` says where a budget yield's next slice
  runs: on this thread, on the pool, or on the overtime pool;
- `complete(done, waiting)` says whether a completion is ignored, recorded, or
  recorded and resumes the call.

**Exhaustive test.** `TestLuauPark` runs every script of up to three steps. A step
is a park, a raise before the work starts, a raise after it starts, or a budget
yield. Each script runs on the pool, on the calling thread and in overtime. Each
started park gets two completions. Every order of the pump's steps, the
completions' steps and the pool's runs is tried.

It checks:
- every call finishes;
- a park's values go to that park's yield, and only once its work is done;
- no call is resumed by two at once, or after it finished;
- a call on the calling thread never goes on the pool, and one in overtime goes on
  the overtime pool.

It catches 13 mutants. Among them are the old rule, a mark never set, a mark kept
past its yield, no `waiting` flag, and a second completion that resumes again.

**Assumptions:**
- Luau's `pcall` catches a `std::exception` thrown from a C function, and the
  coroutine can go on after it. That is how `ldo.cpp` works today.
- A yield made by `park_yield` reaches the pump as `LUA_YIELD` or `LUA_BREAK`, with
  no Lua run in between.
- Each started park's work calls `complete_call` at least once. A call whose work
  never ends is bounded only by that work's own timeout, not by the stop.
- The model runs one call. Calls that share a Luau state are kept apart by the
  state's own hold, which this ledger does not cover.

## RESP3 reply shapes

Level 2 since TODO 936. Level 3 since TODO 948.

**The bug.** HGETALL built its reply with `start_array`. A RESP3 client got a flat
array of fields and values. Redis sends a map. CONFIG GET, HELLO and KSRESIDENT
already used `start_map`. RESP2 has no map, so there a map is written as the flat
array it was.

**The fix.**
- HGETALL opens its reply with `start_map` and closes it with `end_map`.
- `nested_result` in `rpc_caller.h` unwraps a map or a set as well as an array.
  `append_flat` uses it. The cluster reads each node's record with a local HGETALL
  and `append_flat`. With an array-only check it would have got one map value
  back, not the pairs. This was found by reading the code. No run showed it.

**No core at first.** A reply's shape is fixed per command and protocol. There is
no order of events to model. TODO 948 added one anyway, for the pairs below,
because the same open and close now sit in eight places.

**Live test.** `TestResp3Map` reads replies on raw sockets and keeps each wire type.
- Under RESP3, HGETALL opens with `%` and pairs each field with its value.
- Under RESP2, HGETALL is still a flat array.
- Under RESP3, CONFIG GET is still a map.

The `%` check fails on the code before the fix and passes now.

**What the survey found** (RESP3 on a raw socket, against Redis's documented replies):
- HGETALL: fixed.
- HRANDFIELD WITHVALUES, ZRANGE, ZRANGEBYSCORE, ZREVRANGE and ZRANDMEMBER WITH
  SCORES: a flat array. Redis 7 sends an array of two-element arrays under RESP3.
  ZPOPMIN with a count may do the same. Left open as TODO 948.
- HGETALL of a missing key: `$-1` in RESP2 and `_` in RESP3. Redis sends an empty
  array and an empty map. Left open as TODO 948.
- HSCAN, ZRANK WITHSCORE and ZMSCORE match Redis.

**Pairs - TODO 948.** Valkey 8.1 and Redis 8 were run side by side with barchd on
raw sockets. Under RESP3 a list of pairs nests each pair: `*[*[$a ,1] *[$b ,2.5]]`.
That holds for ZRANGE in every form, ZRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYSCORE,
ZRANDMEMBER and HRANDFIELD with a count, ZUNION, ZINTER and ZDIFF, and ZPOPMIN and
ZPOPMAX with a count. A pop without a count answers one pair, and that stays flat:
`*[$a ,1]`. ZMPOP and BZPOPMIN already matched. barchd sent every one flat.

- `pair_shape_core.h` picks the layout: nested when the protocol is 3 or more and
  the reply is a list. `open_pair` and `close_pair` take the same layout, so an
  array is only closed where one was opened.
- `caller.h` `start_pair(list)` and `end_pair(list)` call it with `get_protocol()`.
  The commands call them around each pair. `zpop_flat` passes `list` as whether a
  count was given.
- HGETALL of a missing key, or one that fails `key_ok`, now answers an empty
  map: `*[]` under RESP2 and `%[]` under RESP3. It answered a null.

Luau scripts and the HTTP API run on fresh `rpc_caller`s at protocol 2, so a
script still sees flat pairs. The valkey module's `vk_caller` reports 2 too.
Neither changed. The Raft copy of a caller keeps the protocol, so a replayed
write answers in the session's shape.

**Core test.** `TestPairShape` runs protocols 0 to 4, a list or a lone pair, and
0 to 3 pairs into a builder that writes shape strings. It checks each shape and
that every array closed was opened. It catches five mutants: nesting under RESP2,
nesting a lone pair, nesting only at exactly 3, a close that always closes, and a
close that never closes.

**Live test.** `TestResp3Pairs` runs 23 commands under RESP2 and RESP3 and
compares each wire shape with Valkey's. It also calls a function from a RESP3
connection and checks it sees four flat values and an empty table. On the old
code 16 cases and the function check failed.

**Left open.**
- ZSCAN is not a command (TODO 951). Done; see "Container scans: pages".
- ZINCRBY by 0 deletes the member (TODO 950). The survey found it. It is not a
  shape bug.
- A forwarded cluster reply and the valkey module stay on RESP2 shapes.

**Assumptions:**
- Luau, wasm and the HTTP API read a map's elements the way they read an array's,
  so a script sees the same flat table. `luau_bindings.cpp` `push_variable` does this.
- A remote node reached over RESP2 still gets a flat array.

## Sorted sets: score moves

Level 3 since TODO 950.

**What it is.** A sorted set member has two keys. The score key holds the encoded
score and the name, so a range walks members in score order. The member index
maps the name to its score key. ZINCRBY moves a member: it writes the new score
key, points the index at it, then removes the old key through the iterator.

**The bug.** When the new score encodes the same as the old, the new key is the
old key. The write did nothing, and the remove took the only copy. The index
still pointed at it. So ZCARD dropped, ZRANGE lost the member, and ZSCORE still
answered the old score. The next ZINCRBY found no member and wrote a new one at
the increment. ZREM could not remove it either. An increment of 0 or -0 did
this, and so did one too small to change the double: 1e-300 on 1, or 1 on
1e300. The TODO 948 survey found it.

**The fix.** `score_move_core.h` `plan(old_key, new_key)` says keep when the
encoded keys are equal, and replace when they differ. It compares keys, not
doubles, because 0 and -0 compare equal and can encode apart. On keep, ZINCRBY
writes nothing, removes nothing, and answers the score.

**Core test.** `TestScoreMove` models the score map and the member index. It
starts two members at every pair of scores in {-inf, -1, -0, 0, 1, 1e300, inf}
and runs every sequence of up to three increments of {0, -0, 1e-300, 1, -1, inf}:
92,316 steps. After each it checks both members are in the map, the index points
at a key that is there, and that key encodes the answer. A nan is refused and
changes nothing. It catches three mutants: always replace (the old code), always
keep, and keep when the doubles compare equal.

**Live test.** `TestZincrbySame` runs ZINCRBY by 0, -0, 1e-300 from 1, and 1 from
1e300. It checks the reply, ZCARD, ZRANGE and ZSCORE, that a later ZINCRBY by 1
adds to the score, and that ZREM removes the member. On the old code 14 of its
25 checks failed.

**Assumptions:**
- ZADD does not share the bug. It was run with the same score, with INCR 0, and
  with XX and the same score, and kept the member each time.
- The model's encoding tells 0 and -0 apart, the strictest case. If
  `comparable_key` folds them, the real keys only match more often, and keep is
  still right.

## Container scans: pages

Level 3 since TODO 951.

**What it is.** `ZSCAN key cursor [MATCH pattern] [COUNT n] [NOSCORES]` walks one
sorted set a page at a time. It walks the member index, so members come in name
order. A page takes COUNT members, before MATCH, and saves the first member it
does not take. The next call starts at that member, inclusive. The cursor is an
iteration id, as for HSCAN and SCAN.

**The bug.** There was no ZSCAN. barchd answered an unknown command. The TODO 948
survey found it.

**The fix.** `ordered_api.cpp` `ZSCAN` parses its options the way HSCAN does, and
refuses a string key with WRONGTYPE. `scan_page_core.h` `page` says take or
resume for each entry, and `resume_inclusive` says where the next page starts.

**Why the member index.** The score keys are in score order. A score change moves
a member's score key. A walk over score keys would miss a member whose score
drops behind the saved key, and SCAN promises to report every member there
throughout. A name does not change, so the member index cannot move under the
walk.

**Wire shape.** Valkey's reply is flat under RESP2 and RESP3: the cursor, then
member and score pairs. A score is a bulk string under both, written as
`fmt::format("{}", score)`, so `inf`, `-1` and `2.5`. COUNT 0 and an unknown
option give `-ERR syntax error`. A bad cursor gives `-ERR invalid cursor`. A
missing key gives an empty page with cursor 0.

**Core test.** `TestScanPage` models four members and a walk over their keys. It
starts from every order of the scores {1, 2, 3, 4}, at COUNT 1 and 2, and runs
every sequence of up to three changes between pages: nothing, remove, add, or
set a score to -1, 0 or 5. That is 466,752 iterations. Each must end, report
every member there throughout, and report nothing that never existed. It catches
three mutants: a walk in score order, a next page that starts after the saved
key, and a page that never takes.

**Live test.** `TestZscan` checks nine shapes against Valkey under RESP2 and RESP3,
the arity error and WRONGTYPE. It runs full iterations over 203 members at COUNT
7, with MATCH and NOSCORES, and checks each score against ZSCORE. Then a second
connection drops every score by 1000 between calls, and no member may be missed.
On the old code every ZSCAN call failed with an unknown command.

**Left open.**
- Member and field names that look like numbers come back as numbers (TODO 952).
  Done; see "Names: number keys".
- barch has no SSCAN, and no SADD.

**Assumptions:**
- Valkey ignores COUNT on a small set and answers it in one page. barchd honours
  COUNT. Both are allowed; a client loops until the cursor is 0.
- A member added during an iteration may or may not be reported. SCAN allows that.

## Names: number keys

Level 3 since TODO 952.

**What it is.** barch stores a key, member or field that is a number as a number
key, so `2`, `10` and `-3` sort in numeric order. `conversion.cpp` `convert`
decides which names are numbers.

**The bug.** `convert` stored any text that parsed as a number. So `010` and `10`
were one key: SET 010 v, then GET 10, answered v. `1e3` came back as `1000`,
`1.50` as `1.5` and `-0` as `0`. Two integers past 2^63 became one double. The
reply was wrong too: an integer name came back as `:10`, and a real one under
RESP3 as `,1.5`. Valkey keeps the bytes and answers a bulk string.

**The fix.** A name is a number only when the number prints back as the same
text. `number_name_core.h` `choose` decides; `convert` parses and prints with
fmt. Every other name is a string. Replies print a number name with
`conversion::number_text`, which is the same fmt print, as a bulk string;
`keys.cpp` `encoded_key_as_reply` does this for KEYS, SCAN and every container
reply.

**Two more paths.** ZINCRBY read its member with `noint`, so member `10` became
the real 10.0, a second member beside the integer 10 that ZADD wrote. BYLEX
bounds did the same, so `[10` never found member `10`. Both now read names the
way ZADD does. Score bounds are not names: `ordered_api.cpp` `score_bound` reads
them as scores, so `1e0` still means 1.

**Scores are not names.** WITHSCORES, ZPOPMIN, ZPOPMAX, BZPOPMIN, BZPOPMAX and ZRANK
WITHSCORE pushed a stored score with `push_encoded_key`. Once names became bulk
strings, RESP3 scores did too, and `TestResp3Pairs` caught it. `ordered_api.cpp`
`push_score_key` pushes a stored score with `push_double`, as ZSCORE does: a bulk
string under RESP2, a double under RESP3.

**Core test.** `TestNumberName` runs `choose` over every text of up to five
characters from `0 1 9 - + . e`, and the edges: 2^63 and its neighbours, `-0`,
`inf`, `nan`. That is 19,619 texts. No two may share a stored form, a number must
print back as its text, `noint` must give no integer, and `10`, `-3`, `0`, `1.5`
must stay numbers. It catches three mutants: the old rule, a rule that checks
only the integer print, and a rule that prefers a real to an integer.

**Live test.** `TestNumericName` uses the names `010`, `10`, `1e3`, `1.50`, `+1`,
`-0`, 2^63 and 2^63+1 as keys, members and fields, under RESP2 and RESP3. It
checks KEYS, SCAN, GET, DBSIZE, ZRANGE, ZSCAN, ZSCORE, ZCARD, HKEYS, HSCAN, HGET
and HLEN against Valkey's answers. It also checks ZINCRBY, ZRANGEBYLEX and score
bounds written as `1e0`. On the old code 30 checks failed.

**Left open.**
- Data saved before TODO 952 keeps its old number keys. A name saved as `010` is
  the integer 10 and answers to `10` only.
- A key with a leading space, such as ` 1`, loses the space. The composite key
  split reads spaces as separators.
- No live test checks the RESP3 score shape of BZPOPMIN, BZPOPMAX or ZRANK
  WITHSCORE. They take the ZSCORE path, which `TestNumericName` checks.
- `-inf` and `-nan` print back as themselves, so they are number names. Their
  order among other numbers is the double order.

**Assumptions:**
- `fmt::format("{}", x)` gives the shortest text that reads back as `x`, so each
  number has one canonical text.
- A script that reads a name sees the text. `rpc_caller.h` `push_encoded_key` is
  the one place a call puts a name in its reply. `TestNumericName` does not run a
  script.

## Hotspots not yet worked

From the `asio_resp_session.h` audit (18-11-2026):

- None. Every hotspot from that audit has a row at level 2 or more.
