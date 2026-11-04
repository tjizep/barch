# Stored functions in WebAssembly

barch runs a stored function written in WebAssembly the same way it runs a Luau
one: `SETF name <module bytes>`, then call it by name or with `CALLF`. A module
reaches the server only through the `barch` imports. There's no WASI, so no files,
clock or environment beyond what those imports give it. See
[WASM-PLAN.md](../../WASM-PLAN.md) for how a call runs.

- [barch.h](barch.h) declares every import, with a few helpers for strings and
  numbers.
- [hits.c](hits.c) counts hits on a page in a pessimistic transaction.

## Building a module

Any compiler that targets `wasm32` works. With clang and its `wasm-ld`, and no libc:

```bash
clang --target=wasm32 -O2 -nostdlib -Wl,--no-entry -Wl,--export=call -o hits.wasm hits.c
```

Then store it and call it:

```bash
redis-cli -x SETF hits < hits.wasm
```

```bash
redis-cli HITS /home
```

What barch checks at `SETF`:

- the module exports `call`, taking nothing and answering an `i32`: 0 for success,
  anything else after `barch_reply_error` (a module that only declares a cron job or
  a queue doesn't need one);
- it exports its memory as `memory` (wasm-ld does by default);
- it imports nothing but the `barch` names in barch.h, with those types;
- it has no start section.

An optional `arity` export, an `i32` global, has barch check the argument count
before the call runs, as Luau's `arity` does. In C, `--export=arity` on a variable
exports its address, not its value, so check `barch_arg_count()` in `call` instead,
as hits.c does.

Settings go in a custom section named `barch` holding JSON, the same keys a Luau
`--@barch` header takes, e.g. `{"write": false}`.

## How a call runs

- A long call runs in slices and goes back on the pool between them, so it doesn't
  hold an I/O thread.
- A call waiting on `barch_http_request`, or on a key another transaction has
  locked, parks: it holds no thread until it's woken. A transaction it opened
  stays open through slices and parks.
- A call made from inside another function runs straight through where it is. It
  still waits on I/O and locks, but it blocks its thread while it does. So does a
  run from a Raft log, which can't use `barch_http_request` at all unless the
  function replicates its effects (`{"replicate": "effects"}`).
- `barch_time` and `barch_uuid` answer the log entry's time in a run from the log,
  so every member gets the same answer.

## Loading from a directory

`LOADKEYS` and a function repository (`functions_dir`, or a git repository) take
`.wasm` files the same way they take `.luau` ones: `hits.wasm` becomes the function
`HITS`. Everything else in the directory becomes a key.

## Transports

A `transport` in the `barch` section serves a module the way a Luau `transport()`
does. The resp kind names exports as commands:

```json
{"transport": {"kind": "resp",
  "methods": {"GETNAME": {"export": "get_name", "categories": ["read"], "arity": 1}}}}
```

Each export is `int32_t name(void)`, like `call`, and reads its arguments with
`barch_arg_read`. A user needs the method's categories to run it.

`cron` and `queue` take the same keys as the Luau ones (`every` or `cron`, `space`,
`call`, `user`, and `name` for a queue). They're declared with `configuration:SETF
cron/jobs/<name>` or `queues/<name>`.

## Locked regions

`BARCH_LOCKED(key, len, fn, ctx)` holds the shard the key lives on (the whole space
when `len` is 0) while `fn(ctx)` runs, then answers what `fn` answered. It's
`barch.store.locked` for WASM. Inside it, stick to the store calls on keys in that
shard. `barch_call`, transactions, HTTP and a second region are refused, and so is
a key on another shard. A region gets one slice of instructions.

```c
static int32_t bump(int32_t ctx) {
    int32_t h = barch_store_get("n", 1);
    char buf[21];
    int32_t len = barch_from_int(barch_to_int(h) + ctx, buf);
    barch_store_set("n", 1, buf, len);
    return 0;
}

int32_t call(void) {
    BARCH_LOCKED("n", 1, bump, 1);
    return 0;
}
```
