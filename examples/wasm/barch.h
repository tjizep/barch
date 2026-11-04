/*
 * barch.h - the `barch` imports a stored WASM function can use, for C and C++
 * compiled to wasm32 with clang. See README.md here for how to build a module and
 * WASM-PLAN.md at the top of the repo for how barch runs it.
 *
 * It's an example, not part of the server: nothing in barch's build compiles it.
 * The raw imports are declared first, under the same names barch exports, then a
 * few helpers so a function doesn't have to juggle pointers and lengths by hand.
 *
 * How values come back: anything barch hands the module (a command's reply, a
 * stored value, a list of keys) is held on the host side and the module gets an
 * int32_t handle to it. Ask its kind and length, then copy it out with val_read.
 * Handles go away when the call ends, so val_free is only for long loops.
 *
 * Errors: a store or space call that fails ends the call with the error, the way
 * a Luau error does. `call`, `space_call` and the tx_ ones are the exception: a
 * command's error comes back as a BARCH_ERROR value, so a TXCONFLICT can be retried.
 */
#ifndef BARCH_WASM_H
#define BARCH_WASM_H

#include <stdint.h>

#define BARCH_IMPORT(name) __attribute__((import_module("barch"), import_name(#name)))

/* what val_kind answers */
enum {
    BARCH_NIL = 0,
    BARCH_STRING = 1,
    BARCH_INT = 2,
    BARCH_DOUBLE = 3,
    BARCH_ARRAY = 4,
    BARCH_ERROR = 5,
    BARCH_TOMB = 6,     /* a cached miss from a foreign source, not the same as nil */
    BARCH_BOOL = 7,
};

/* the method numbers http_request takes */
enum {
    BARCH_GET = 0,
    BARCH_POST = 1,
    BARCH_PUT = 2,
    BARCH_PATCH = 3,
    BARCH_DELETE = 4,
};

/* argv for barch_call and barch_space_call: one of these per word */
typedef struct {
    const char* at;
    int32_t len;
} barch_word;

/* ---- the call's arguments ---- */
BARCH_IMPORT(arg_count) int32_t barch_arg_count(void);
BARCH_IMPORT(arg_len) int32_t barch_arg_len(int32_t i);
/* copies argument i to dst, answers how many bytes */
BARCH_IMPORT(arg_read) int32_t barch_arg_read(int32_t i, char* dst);

/* ---- values barch holds for the module ---- */
BARCH_IMPORT(val_kind) int32_t barch_val_kind(int32_t h);
/* bytes for a string or error, elements for an array */
BARCH_IMPORT(val_len) int32_t barch_val_len(int32_t h);
BARCH_IMPORT(val_read) int32_t barch_val_read(int32_t h, char* dst);
BARCH_IMPORT(val_int) int64_t barch_val_int(int32_t h);
BARCH_IMPORT(val_double) double barch_val_double(int32_t h);
/* element i of an array, as a handle of its own */
BARCH_IMPORT(val_at) int32_t barch_val_at(int32_t h, int32_t i);
BARCH_IMPORT(val_free) void barch_val_free(int32_t h);

/* ---- the reply: exactly one per call, an array counts its elements as more ---- */
BARCH_IMPORT(reply_nil) void barch_reply_nil(void);
BARCH_IMPORT(reply_int) void barch_reply_int(int64_t v);
BARCH_IMPORT(reply_double) void barch_reply_double(double v);
BARCH_IMPORT(reply_string) void barch_reply_string(const char* s, int32_t len);
/* then n more reply_ calls fill it */
BARCH_IMPORT(reply_array) void barch_reply_array(int32_t n);
/* set it, then return non-zero from call() */
BARCH_IMPORT(reply_error) void barch_reply_error(const char* s, int32_t len);

/* ---- commands, as barch.call ---- */
BARCH_IMPORT(call) int32_t barch_call(int32_t argc, const barch_word* argv);

/* ---- the running space's store, as barch.store ---- */
BARCH_IMPORT(store_get) int32_t barch_store_get(const char* k, int32_t klen);
/* 1 when written */
BARCH_IMPORT(store_set) int32_t barch_store_set(const char* k, int32_t klen, const char* v, int32_t vlen);
/* 1 when there was something to remove */
BARCH_IMPORT(store_remove) int32_t barch_store_remove(const char* k, int32_t klen);
BARCH_IMPORT(store_exists) int32_t barch_store_exists(const char* k, int32_t klen);
BARCH_IMPORT(store_count) int64_t barch_store_count(const char* lo, int32_t lolen, const char* hi, int32_t hilen);
/* an array of keys, at most `limit` (0 or past 10000 means 10000), skipping `offset` */
BARCH_IMPORT(store_range)
int32_t barch_store_range(const char* lo, int32_t lolen, const char* hi, int32_t hilen, int64_t limit, int64_t offset);

/*
 * ---- locked regions, as barch.store.locked ----
 * store_locked holds the shard `k` lives on (the whole space when klen is 0), calls
 * the module's own function `fn` with `ctx`, and answers what it answers. `fn` is
 * an index into the module's function table, which is what clang gives a function
 * pointer, so BARCH_LOCKED below is the easy way to call it. The function has to be
 * `int32_t fn(int32_t ctx)`.
 *
 * Keep the region short and inside its shard: it can't use barch_call, the tx_
 * calls or barch_http_request, can't open another region, and writing a key on
 * another shard is an error. It has a slice's worth of instructions, then it's
 * stopped.
 */
BARCH_IMPORT(store_locked) int32_t barch_store_locked(const char* k, int32_t klen, int32_t fn, int32_t ctx);
BARCH_IMPORT(store_shard_number) int64_t barch_store_shard_number(const char* k, int32_t klen);
/* 1 when this call holds the shard `k` lives on */
BARCH_IMPORT(store_has_lock) int32_t barch_store_has_lock(const char* k, int32_t klen);

#define BARCH_LOCKED(k, klen, fn, ctx) \
    barch_store_locked((k), (klen), (int32_t) (uintptr_t) (int32_t (*)(int32_t)) (fn), (ctx))

/*
 * ---- other key spaces, as barch.space.NAME ----
 * space_open answers a handle for a space that already exists; opening the same
 * one again answers the same handle. Handle 0 is the running space, so the
 * space_ calls with 0 are the store_ ones.
 */
BARCH_IMPORT(space_open) int32_t barch_space_open(const char* name, int32_t len);
BARCH_IMPORT(space_get) int32_t barch_space_get(int32_t sp, const char* k, int32_t klen);
BARCH_IMPORT(space_set)
int32_t barch_space_set(int32_t sp, const char* k, int32_t klen, const char* v, int32_t vlen);
BARCH_IMPORT(space_remove) int32_t barch_space_remove(int32_t sp, const char* k, int32_t klen);
BARCH_IMPORT(space_exists) int32_t barch_space_exists(int32_t sp, const char* k, int32_t klen);
BARCH_IMPORT(space_count)
int64_t barch_space_count(int32_t sp, const char* lo, int32_t lolen, const char* hi, int32_t hilen);
BARCH_IMPORT(space_range)
int32_t barch_space_range(int32_t sp, const char* lo, int32_t lolen, const char* hi, int32_t hilen, int64_t limit,
                          int64_t offset);
/* a command run in that space, as sp:call(...) */
BARCH_IMPORT(space_call) int32_t barch_space_call(int32_t sp, int32_t argc, const barch_word* argv);

/*
 * ---- transactions, as barch.tx ----
 * BEGINTX, COMMITTX and ABORTTX, answered the way barch_call answers. A
 * transaction stays open across slices and while the call waits on I/O.
 */
BARCH_IMPORT(tx_begin) int32_t barch_tx_begin(int32_t pessimistic);
BARCH_IMPORT(tx_commit) int32_t barch_tx_commit(void);
BARCH_IMPORT(tx_abort) int32_t barch_tx_abort(void);
BARCH_IMPORT(tx_open) int32_t barch_tx_open(void);

/* ---- the clock and ids, as barch.time and barch.uuid: the log's in a run from it ---- */
BARCH_IMPORT(time) int64_t barch_time(void);
/* writes a 36 character UUIDv7 to dst, answers 36 */
BARCH_IMPORT(uuid) int32_t barch_uuid(char* dst);

/*
 * ---- outbound HTTP, as http.request ----
 * `headers` are "Name: value" lines, split on '\n'. timeout_ms 0 means 30 s.
 * Answers [status, body, [name, value, ...]], or a BARCH_ERROR value when nothing
 * answered. Needs the caller to have the outbound category. While it waits, the
 * call is parked and holds no thread.
 */
BARCH_IMPORT(http_request)
int32_t barch_http_request(int32_t method, const char* url, int32_t urllen, const char* headers, int32_t hlen,
                           const char* body, int32_t blen, int32_t timeout_ms);

/* ---- helpers ---- */

static inline int32_t barch_strlen(const char* s) {
    int32_t n = 0;
    while (s[n])
        ++n;
    return n;
}

static inline barch_word barch_w(const char* s) {
    barch_word w = {s, barch_strlen(s)};
    return w;
}

/* copies a string value into buf if it fits, answers its length, or -1 if it doesn't */
static inline int32_t barch_read(int32_t h, char* buf, int32_t size) {
    int32_t n = barch_val_len(h);
    if (n > size)
        return -1;
    return barch_val_read(h, buf);
}

static inline void barch_reply_cstr(const char* s) {
    barch_reply_string(s, barch_strlen(s));
}

/* sets the error and answers 1, so `return barch_fail("ERR ...");` ends call() */
static inline int32_t barch_fail(const char* s) {
    barch_reply_error(s, barch_strlen(s));
    return 1;
}

/* the decimal digits of a string value, as a number; 0 for anything else */
static inline int64_t barch_to_int(int32_t h) {
    char buf[24];
    int32_t n = barch_read(h, buf, (int32_t) sizeof buf);
    int64_t v = 0;
    int neg = n > 0 && buf[0] == '-';
    for (int32_t i = neg; i < n; ++i) {
        if (buf[i] < '0' || buf[i] > '9')
            return 0;
        v = v * 10 + (buf[i] - '0');
    }
    return neg ? -v : v;
}

/* v in decimal into buf (at least 21 bytes), answers the length */
static inline int32_t barch_from_int(int64_t v, char* buf) {
    char tmp[21];
    int32_t n = 0;
    uint64_t u = v < 0 ? 0 - (uint64_t) v : (uint64_t) v;
    do {
        tmp[n++] = (char) ('0' + u % 10);
        u /= 10;
    } while (u);
    int32_t at = 0;
    if (v < 0)
        buf[at++] = '-';
    while (n)
        buf[at++] = tmp[--n];
    return at;
}

#endif
