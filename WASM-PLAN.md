# Stored WASM functions, built the way the Luau ones are

TODO 819. A design, not code yet. Nothing here is built.

The idea: a stored function can be a WebAssembly module instead of a Luau script, and
from the outside nothing else changes. Same commands, same keys, same ACL, same limits,
same Raft rules. A client calling `PLACE_ORDER` can't tell which language it's in, and
a Luau function can call a WASM one through `barch.call` (and the other way round)
because both resolve through `barch::functions::resolve`.

What WASM brings that Luau doesn't: code in C, Zig, Rust, Go or AssemblyScript (how
an author builds a module is their business; barch only ever sees the bytes), with a
plain 64-bit integer type and deterministic arithmetic. What it costs: no `require`, no tables, and a host API that
passes bytes rather than Lua values.

---

## 1. Storage: the same function key

A WASM function is an ordinary `art::tfunction` key, like a Luau one. No new lead byte,
no new commands, no second key for metadata.

- `SETF name <bytes>` stores a module. The value starts with the WASM magic
  `\0asm` followed by the version, and that's how everything downstream tells the two
  apart. Luau source can never start with a NUL byte, so the check is exact.
- Only the binary form is stored. WAMR has no parser for the WAT text form, and
  adding one (wabt) would put a text parser on the write path for little gain.
- `GETF` returns the bytes. RESP bulk strings carry binary fine.
- Persistence, the change log, replication, `EXPORT`, no eviction, no TTL: all
  unchanged, because they already treat a function key as a function key whatever is
  in it.

The settings a Luau function puts in its `--@barch` comment go in a **custom section
named `barch`** holding the same JSON:

```json
{"deadline_ms": 5000, "slice_insns": 200000, "write": false, "replicate": "effects"}
```

Custom sections are ignored by every engine, the same way Luau ignores a comment, so
the module is still a normal module and the bytes are still the whole record. This
keeps to the rule from the 510 bug: one key holds everything, no sidecar to drift out
of step. `read_function_meta` gets a second branch that walks the module's sections itself
(a few lines, no engine needed) and reads the `barch` one instead of the comment, and everything that uses `function_meta` (`may_write`, `replication_of`,
`function_limit`) works unchanged.

`--!native` and `AOT` mean nothing here, since phase 1 only interprets.
`SETF ... AOT` is accepted and ignored so scripts that pass it don't break.

## 2. The engine

**WAMR (WebAssembly Micro Runtime), plain C, pinned at `WAMR-2.4.5`.** No Rust and no
cargo anywhere in the build. Wasmtime was the first pick and is out: even its prebuilt
C API is a Rust library built with cargo, and a cargo dependency tree is a supply
chain risk. The other C and C++ engines don't do better: wasm3 is in maintenance
mode, WasmEdge brings LLVM and more, and V8 is enormous. None of them can pause a
running call either.

WAMR builds with CMake from `FetchContent`, like Luau. Phase 1 uses its fast
interpreter only. Its JIT and AOT modes need LLVM, so they're left out, which means a
WASM function runs interpreted, at roughly Luau's speed rather than native speed.
That's fine to start with; the value is in the languages, not the speed.

How it lines up with what barch already relies on for Luau:

| barch needs | Luau today | stock WAMR | WAMR with a small patch |
|---|---|---|---|
| end a slice and give the worker back | interrupt + `lua_yield` | no | yes, on a fiber |
| park on I/O and resume later | coroutine | no | yes, on a fiber |
| stop a runaway at the deadline | interrupt reads the CPU clock | instruction cap only | yes |
| count work the same on every Raft member | counted interrupts | instruction cap, exact | same |
| cap memory | allocator hook | max memory pages | same |

What stock WAMR gives is
`wasm_runtime_set_instruction_count_limit`, an exact count that **ends** the call with
"instruction limit exceeded" when it hits zero. It can't pause one, and it never
looks at a clock. `wasm_runtime_terminate` only sets an error flag, which a tight loop
doesn't check unless WAMR's thread manager is built in. So with stock WAMR, a call
holds its worker until it finishes, and the only thing that stops a loop is a
count of instructions.

The patch that closes the gap is small. Both interpreters count down in one macro,
`CHECK_INSTRUCTION_LIMIT`. Instead of raising at zero, it calls a hook barch installs:

```c
if (instructions_left == 0) {
    instructions_left = barch_wasm_budget_out(exec_env);  /* may switch fibers */
    if (instructions_left == 0) { wasm_set_exception(module, "instruction limit exceeded"); goto got_exception; }
}
```

The hook does what `function_interrupt` does for Luau: checks the CPU time deadline,
and if the slice is used up, switches the call's fiber back to the worker, which
queues it again. When it's resumed, the hook returns a fresh slice. A run from the
log never switches and just returns what's left of `function_log_insns`, so the cap
stays exact.

Each call then runs on its own fiber stack, so a host import that waits on I/O can
switch out the same way. Two WAMR settings follow from resuming on a different pool
thread: the native stack boundary is set per call to the fiber's stack
(`wasm_runtime_set_native_stack_boundary`), and hardware bound checks are off
(`WAMR_DISABLE_HW_BOUND_CHECK=1`), because they catch bad accesses with a signal
handler that finds the call through thread local state. Software bound checks cost
a little speed.

The patched file is carried as a copy, the way Crow's is, not as a `PATCH_COMMAND`.

It goes behind `option(ADD_WASM ... OFF)`, next to `ADD_LUAU`, and off by default
until it's proven. With it off, `SETF` of a module answers
`-ERR FUNCTION wasm not built in` instead of storing something that can't run.

All engine code lives in one file, `src/foreign/wasm_driver.cpp`, the way Luau is kept
to `luau_driver.cpp`. Nothing outside it includes a WAMR header, so swapping the
engine later touches one file.

## 3. What a module looks like

A module exports:

- `call() -> i32` (required). The entry point, same name as Luau's. Returns 0 on
  success, anything else is an error whose text was set with `reply_error`.
- `arity` (optional), an exported `i32` global. Same meaning as the Luau global:
  `2` exactly two, `-2` at least two, absent anything. Checked before the module runs.
- `memory`, its linear memory.

It imports what it needs from a host module named `barch`. A module importing anything
else (including WASI), or a `barch` name that doesn't exist, is refused at `SETF`.
That has to be checked by barch (`wasm_runtime_get_import_count` and
`wasm_runtime_get_import_type`), because WAMR on its own loads a module with an
unknown import and only fails when it's called. No WASI means no files, no clock, no
environment, no random, which is the same sandbox Luau has with no `os`, `io` or
`debug`.

### Passing data: handles, not pointers into the guest

The host never calls into the guest to allocate. Every value the host has for the guest
(an argument, a command's reply, a stored value) sits in a per-call table and the guest
gets an `i32` handle to it. The guest asks how long it is and copies it out into its own
memory. That works with any language's allocator, or none.

```text
;; arguments
arg_count() -> i32
arg_len(i: i32) -> i32
arg_read(i: i32, dst: i32) -> i32             ;; copies arg i to dst, answers bytes

;; any value the host holds
val_kind(h: i32) -> i32                       ;; nil, string, int, double, array, error, tomb
val_len(h: i32) -> i32                        ;; bytes, or element count for an array
val_read(h: i32, dst: i32) -> i32
val_int(h: i32) -> i64
val_double(h: i32) -> f64
val_at(h: i32, i: i32) -> i32                 ;; element of an array, as a handle
val_free(h: i32)                              ;; optional; all handles go when the call ends

;; the reply
reply_nil()
reply_int(v: i64)
reply_double(v: f64)
reply_string(src: i32, len: i32)
reply_array(n: i32)                           ;; then n more reply_* calls
reply_error(src: i32, len: i32)
```

Integers are `i64` both ways, so the Luau `integer` vs `number` trap doesn't exist here.
A tomb is its own kind, so a cached source miss stays different from nil, same as
`barch.tomb`.

A bad pointer or length (outside the guest's memory) traps the call. It never reads or
writes host memory.

### What it can reach

Phase 1 gives the core every function needs. The rest follows Luau's surface name for
name, so the docs can show both side by side.

```text
call(argc: i32, argv: i32) -> i32             ;; barch.call: argv is argc (ptr, len) pairs, answers a handle
store_get(k: i32, klen: i32) -> i32           ;; barch.store.get, handle (nil/tomb/string)
store_set(k: i32, klen: i32, v: i32, vlen: i32) -> i32
store_remove(k: i32, klen: i32) -> i32
store_exists(k: i32, klen: i32) -> i32
store_count(lo, lolen, hi, hilen) -> i64
store_range(lo, lolen, hi, hilen, limit: i64, offset: i64) -> i32   ;; array handle of keys
space_open(name, len) -> i32                  ;; barch.space.NAME; handle 0 is the running space
space_get/set/remove/exists/count/range(sp, ...)  ;; the store_ ones, with a space handle first
space_call(sp, argc, argv) -> i32             ;; sp:call(...)
tx_begin(pessimistic: i32) -> i32
tx_commit() -> i32
tx_abort() -> i32
tx_open() -> i32
time() -> i64                                 ;; barch.time
uuid(dst: i32) -> i32                         ;; barch.uuid, writes 36 bytes
http_request(method, url, urllen, headers, hlen, body, blen, timeout_ms) -> i32
log(level: i32, src: i32, len: i32)
```

Each one goes through the same `store_access` and `command_runner` the Luau bindings
use, built by `store_for`, `tx_aware` and `runner_for` in `function_api.cpp`. So ACL, the
`function` category filter on walks, `"write": false`, transactions and foreign fills
behave exactly as they do from Luau, without a second copy of any of those checks.

Later phases add `store_locked`, the in-place number helpers (`incr_int64_by_at` and
friends), JSON helpers, `resp_*`, `mail_send` and `queue_push`. (`log` isn't there
yet either.) Each of those is a thin wrapper over the C++
the Luau binding already calls.

A small C header (`examples/wasm/barch.h`) wraps the raw imports, so nobody writes
`val_read` by hand. It's an example, not part of the server, and nothing in barch's
build compiles it.

## 4. Running a call

It goes through the same path as a Luau call: `CALLF` or the function's own name,
`resolve`, the function pool, `call_limits`, `function_done`. The driver picks Luau or
WASM from the first bytes of the source. Everything above that line stays as it is.

**Compiling.** `SETF` loads the module with WAMR before writing, checks its imports
and its `barch` section, so a module that won't load, imports something it shouldn't,
or has a bad section is refused, same as a Luau script that won't compile. A loaded
`wasm_module_t` can be instantiated many times, so there's one per function per
process, keyed by `compiled_key(space, name)`, not one per session. WAMR keeps
pointers into the bytes it loaded from, so the cache owns a copy of them next to the
module. It's dropped and rebuilt under the same rules: `compile_epoch`,
`publish_compiled`, `RELOAD`. Nothing is written to disk; restarts load again.

**Instances.** One instance per session per space, kept between calls, the same
lifetime a Luau state has. A module's globals and memory survive from one call to the
next on a connection, like Luau globals do. Each instance has its own exec env.

**Slices and the deadline.** This is where the patch from section 2 matters.

- *Phase 1, stock WAMR.* A call runs to the end on its worker, like a Luau call with
  `inline_only`. What stops a runaway is an instruction cap, a new setting
  `function_wasm_insns` (per space override, default 1,000,000,000, which also fits in
  the `int` WAMR takes). Running past it answers `FUNCTION timeout`. The CPU time
  deadline can't be enforced yet, because nothing reads a clock during the run.
- *Phase 2, patched.* Each call gets a fiber and the budget hook. The slice is
  `function_slice_insns`, the CPU time deadline, `function_wall_factor` and
  `function_max_depth` all work as they do for Luau, through the same `run_ctx`. When
  a slice runs out the fiber switches back, the worker goes back to the pool, and the
  call is queued again.

**Parking.** Phase 2 too. A host import that waits on I/O (`http_request`, `resp_*`,
`mail_send`) starts the work and switches its fiber out, and `complete_call` resumes
it on a pool thread, the same contract `park_call` has. Where Luau can't park (an HTTP
route handler, `inline_only`) the import waits in place, with its timeout cut to
what's left of the ceiling, same as Luau.

**Memory.** Each instance's linear memory is capped with
`wasm_runtime_instantiation_args_set_max_memory_pages`, from a new setting
`function_wasm_max_mb` (per space override like the others, default 64). Growing past
it fails the `memory.grow`, which the guest sees as an allocation failure. What
instances hold goes in a new `statistics::wasm_bytes`, shown in `INFO memory` as
`used_memory_wasm` next to `used_memory_luau`.

**Errors.** A trap (out of bounds, unreachable, divide by zero, stack overflow) ends
the call with `-ERR FUNCTION <name> trap: <reason>`. The instance is thrown away
afterwards, since a trap can leave its memory half-updated, and the next call on that
session makes a new one. Running out of instructions answers `FUNCTION timeout`,
exactly as Luau does.

## 5. Raft and the log

Nothing changes in the routing. `may_write` and `replication_of` read `function_meta`,
and the meta now comes from the custom section, so a WASM function lands in the same
three cases: read only, logged (the default), or effects.

A run from the log has to do the same thing on every member. WASM is a good fit:

- The instruction count is exact, so `function_log_insns` becomes the instruction
  limit and every member stops at the same instruction. No clock is read. WAMR takes
  an `int`, so a setting above 2,147,483,647 is refused for WASM rather than wrapped.
- Threads and relaxed SIMD aren't built into WAMR here, so nothing can race or round
  differently.
- Floating point results are the same on every member except the bit pattern of a
  NaN, which can differ between x86 and ARM. A cluster of one architecture is fine.
  A mixed one could store different NaN bytes if a function writes a raw NaN; that's
  written down as a known limit rather than fixed, since WAMR has no switch for it.
- `time()` and `uuid()` answer from `from_log`, the same as `barch.time` and
  `barch.uuid`. There's no other source of time or randomness in the sandbox.

Imports that reach outside the data call `refuse_from_log` with the same names Luau
uses. Since a module's imports are known at compile time, `SETF` can also warn when a
logged function imports `http_request` at all. That's a warning, not a refusal,
because the call might never be made.

## 6. Transports, cron and queues

Luau declares a resp, http, cron or queue `transport()` by returning a table of
functions from running the chunk. WASM does it in the `barch` section instead, naming
exports:

```json
{"transport": {"kind": "resp",
  "methods": {"GETNAME": {"export": "get_name", "categories": ["read"], "arity": 1}}}}
```

That's static, so barch can read it without running anything, which is simpler than
the Luau path. `cron_jobs()` and `queue_declarations()` learn to read it alongside the
Luau form. Phase 3 built it for resp, cron and queue. An http transport isn't there
yet.

## 7. What's left out

- **`require`.** A module is linked when it's built, so there's nothing to load at run
  time. Shared code is either compiled in, or called with `call("CALLF", ...)`.
- **The vector and JSON libraries.** A WASM module brings its own. `barch.vec`'s
  kernels could be imported later if someone wants them.
- **The component model / WIT.** Nicer types, but WAMR doesn't support it. The handle ABI above is small enough to wrap in WIT later without
  changing the server side much.

## 8. Phases

1. **Store and call, stock WAMR.** `ADD_WASM` fetching WAMR 2.4.5 (fast interpreter,
   no LLVM, no thread manager, no WASI). `SETF`/`GETF`/`REMF`/`CALLF` with module
   detection, import checks, the `barch` section in `read_function_meta`, the handle
   ABI, `call`, `store_get/set/remove/exists`, the instruction cap, the memory cap,
   the shared loaded module and `RELOAD`. Calls run to the end on their worker. Test
   modules are built by a small Python helper that writes the binary format, so no
   compiler is needed to run the tests.
2. **The patch, slices and parking.** The budget hook in the copied interpreter
   files, fibers, the CPU time deadline, parking, then transactions,
   `store_range`/`store_count`, `time`/`uuid`, space handles, and the C header
   example.
3. **Cluster and transports.** Runs from the log (exact cap, the refusals),
   transports in the custom section, locked regions, `.wasm` files in a function
   repository and in `LOADKEYS`.
4. **Windows and docs.** WAMR under `win32/` (watch the 32-bit `long` in anything
   touching `i64`, and fibers there are the Windows fiber API), and a "Stored WASM
   Functions" section in the docs next to the Luau one.

### What phase 1 built, and where it differs from the plan

Phase 1 is in `src/foreign/wasm_driver.cpp`, tested by `test/wasmfunctiontest.py`
(`TestWasmFunctions`, only in an `ADD_WASM` build). These are the places where the
code ended up different from the plan above, or where the plan didn't say:

- **Start code is refused.** WAMR runs a start section and an exported
  `__post_instantiate` while instantiating, before any instruction cap is set, so
  `SETF` refuses a module with either one.
- **A command's error is a value.** `call` answers an error handle (kind 5) when the
  command fails, so the guest can decide what to do with it. Only the refusals (from
  the log, `"write": false`, not authorized) end the call.
- **Kinds have fixed numbers:** 0 nil, 1 string, 2 int, 3 double, 4 array, 5 error,
  6 tomb, 7 bool.
- **Reply limits.** `reply_array` nests at most 64 deep, and a call that returns
  before its arrays are filled fails. A call holds at most 1,048,576 live handles.
- **Where a call runs.** It goes onto the function pool like a Luau call and runs to
  the end there. It runs on the calling thread when the limits say `inline_only` or
  it's a run from the log.
- **Runs from the log** already use `function_log_insns` as the cap, so every member
  stops at the same instruction. The rest of section 5 is still phase 3.
- **The loaded module cache** is process wide, keyed by `compiled_key`, holding weak
  pointers. It reuses a loaded module only when the bytes are the same, so nothing
  depends on the epoch to throw an old one away.
- **Instances follow the Luau state they hang off.** They live in `function_states`,
  so they're per connection, or per user for calls from the server pool. A plain
  `SETF` or `REMF` leaves an instance a session already has, exactly like a compiled
  Luau function; `RELOAD` is what makes the next call build a new one.
- **The messages.** `SETF ... AOT` on a module answers "AOT is for Luau functions",
  and a module in a build without `ADD_WASM` answers
  `FUNCTION wasm not built in (ADD_WASM)`.

Left for later: per space overrides for `function_wasm_insns` and
`function_wasm_max_mb`, `statistics::wasm_bytes` in `INFO memory`, the CPU time
deadline (done in phase 2, below), and transport methods (a non-empty entry
answers `no such method`).

### What phase 2 has built so far

The slices and the deadline (TODO 821). Again, where the code differs from the plan:

- **A patch script, not a copied file.** `cmake/wamr_budget_hook.cmake` edits WAMR's
  `wasm_interp_fast.c` on every configure, the same way `crow_tcp_nodelay.cmake`
  patches Crow. It's idempotent, and it fails the configure if the macro it rewrites
  has changed. Only the fast interpreter is built, so it's the only file touched. The
  change is small: when the instruction count runs out, the interpreter calls
  `barch_wasm_budget_hook`, a pointer barch sets, and carries on with the count it
  returns, or traps as before on zero.
- **The hook runs every 10,000 instructions.** Each time it checks the deadline, and
  it hands out at most what's left of the slice and the cap, so the cap stays exact
  (a run from the log still stops at the same instruction everywhere).
- **Fibers are ucontext,** 4 MB stacks mapped with `MAP_NORESERVE` and a guard page,
  up to 64 kept for reuse. WAMR is told each fiber's stack with
  `wasm_runtime_set_native_stack_boundary`, and the switches carry the ASan and TSan
  fiber annotations.
- **WAMR is built without ASan's fake stacks.** WAMR's overflow check compares a
  local's address with that boundary, and a fake stack puts the local on the heap, so
  every call on a fiber trapped with "native stack overflow" under ASan.
  `cmake/wamr/CMakeLists.txt` turns use-after-return off for WAMR's files only.
- **Where a call runs.** The first 20,000 instructions run on the thread that asked,
  as a Luau call's first slice does. After that it gets `function_slice_insns` a
  slice, and between slices it goes back on the pool. A call with `inline_only`, one
  from the log, and one called from inside another WASM call (which is already on a
  fiber) run to the end where they are, without a fiber of their own.
- **The deadline is Luau's.** `function_deadline_ms` counts the call's CPU time,
  `function_wall_factor` sets the wall ceiling, and a function's `barch` section can
  change both and the slice, the same as a `--@barch` header. It's a small struct in
  the WASM driver with the same rules as `run_ctx`, using the same two clocks
  (`call_steady_ns`, `call_thread_cpu_ns`), rather than `run_ctx` itself, which is
  mostly Luau.
- **`function_wasm_insns` is still there,** as a total cap on top of the deadline.
- **`wasm_slices` in INFO memory** counts slices that went back on the pool, next to
  `luau_slices`.

The slices and the deadline are DONE 773, and they ran clean under ASan and TSan.

Then the rest of phase 2 (TODO 822):

- **Parking.** A call on a fiber of its own that has to wait leaves the fiber, and
  the thread goes back to other work. Whatever wakes it puts the call back on the
  pool. That's two kinds of wait so far:
  - `http_request`. It goes through the same HTTP client as Luau's `http.request`
    (cofetch), and the answer wakes the call.
  - A key lock that another session's pessimistic transaction holds. That's
    reached through `call` (a SET, say) or through a store call inside a
    pessimistic transaction of the call's own, as in Luau. The native is tried
    again from the top when the lock goes, the same way a Luau call is.

  `wasm_parks` in INFO memory counts them.
- **No exception crosses WAMR.** A native's work runs inside a try that catches
  everything, the lock-busy unwind included, and turns it into a trap with the
  message. `lock_wait` gained an `on_fiber` flag, so `can_park` says yes to a WASM
  call without looking for a Luau stack.
- **Where it can't park, it waits in place.** That's a call made from inside
  another call, an inline-only one, or one run from the log. It waits on its
  thread, the same as a Luau call that can't park. While it waits on HTTP, its
  clock is stopped and the wait is cut short at the wall ceiling.
- **Transactions are commands.** `tx_begin`, `tx_commit` and `tx_abort` run
  BEGINTX, COMMITTX and ABORTTX, as `barch.tx` does, so a TXCONFLICT comes back as
  an error value the module can retry on. `tx_open` says whether one is open. The
  open question, whether a parked call can hold a transaction across slices,
  settled itself: yes. The transaction belongs to the session, not to whichever
  thread runs the slice, so nothing had to change for it. The test opens a
  pessimistic transaction, writes, runs over many slices, parks on a request, then
  commits.
- **Spaces are opened by name.** `space_open` answers a small handle, kept for the
  rest of the call, through the same `open_space` the Luau handle uses. So the
  caller's rights in that space apply, and a run from the log can only name its own
  space.
- **`store_range` takes an i64 limit,** not the i32 the plan had, with Luau's
  rules: 0 or past 10,000 means 10,000, and a negative offset is an error.
- **`examples/wasm/barch.h`** declares all the imports for clang's wasm32 target,
  and `hits.c` is an example function. Nothing here has a wasm32 clang, so the
  example hasn't been compiled. A script checked the header's names and types
  against the driver's table instead.

### What phase 3 built

Phase 3 is TODO 823. Where it differs from the plan:

- **Runs from the log.** A WASM call that may write is one log entry, and every
  member runs it on a fresh instance. `function_log_insns` caps it, and the cap
  counts instructions, so every member stops on the same one. `http_request`,
  another space and commands that reach outside the data are refused from the log,
  unless the function says `{"replicate": "effects"}`.
  `test/clusterwasmlogtest.py` checks all of it on three nodes.
- **Transports.** `{"transport": {...}}` (or `"service"`) in the `barch` section,
  with kind `resp`, `cron` or `queue`. `http` is refused for now.
  - A resp method names an export shaped `() -> i32`. It gets the call's arguments,
    replies the way `call` does, and its `categories` and `arity` are checked the
    way a Luau method's are, ACL included.
  - Cron and queue specs take the same keys the Luau ones do. They're declared the
    usual way, with `configuration:SETF cron/jobs/<name>` or `queues/<name>`.
  - A resp module still has to export `call`, as a Luau one does. A module that only
    declares a cron job or a queue doesn't.
- **Locked regions.** `store_locked(k, klen, fn, ctx)` holds the shard `k` lives on
  (the whole space when `klen` is 0) and calls the module's function at table slot
  `fn` with `ctx`. `store_shard_number` and `store_has_lock` come with it.
  - **The table lookup is a second piece of the patch.** WAMR's own
    `wasm_runtime_call_indirect` checks nothing about the function's type, so the
    patch script adds `barch_wasm_table_func`. It answers table 0's entry, or
    nothing for an empty slot or an import, and barch checks it's `(i32) -> i32`
    before calling it.
  - **A region never leaves its fiber.** The shard hold is per thread, so inside a
    region a slice doesn't go back on the pool and a lock wait doesn't park. For
    the same reason `call`, the `tx_` calls, `http_request` and a nested region are
    all refused. A write to a key on another shard is refused too.
  - **A region gets one slice of instructions** (the function's slice, or
    `function_slice_insns`), then it's stopped with "locked region ran too long". It
    also still counts against the call's cap. What's left of its last grant when
    it returns isn't handed back, but that's the same on every member, so a run
    from the log is still exact.
  - Every way out (an error, a trap, running out) lets the lock go. The tests check
    that from a second client after each one.
- **`.wasm` files** in a function repository and in `LOADKEYS` become stored
  functions named after their stem, the same as `.luau` ones. That's only in a build
  with WASM. Without it they stay plain keys, as they always were, so a repository
  that ships `.wasm` files as data still loads. A `foo.luau` next to a `foo.wasm` is
  refused as two files for one function.
- **No SETF warning** for a logged function that imports `http_request`. There's no
  way for SETF to warn yet. The call is refused when it runs from the log instead.

Testing follows the usual rules: test first and see it fail on the old code, no sleeps
or timing thresholds (force interleavings with latches), and an ASan and TSan run for
the slice, park and trap paths, since those are where a fiber and a pool thread meet.

## 9. Open questions

Settled on 04-11-2026: no Rust or cargo dependency (so WAMR, not Wasmtime), one
instance per session, no WAT on the write path, and barch patches WAMR's interpreter
for the budget hook (phase 2). The patch is a script that fails the configure if
WAMR's code has moved, so it gets checked on each WAMR upgrade.

1. **The memory cap's default.** 64 MB per instance, times sessions, times spaces can
   add up. Maybe it needs a total across all instances too, the way the arena has one.
