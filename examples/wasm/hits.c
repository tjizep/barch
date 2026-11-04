/*
 * hits.c - a stored WASM function that counts hits on a page.
 *
 *   HITS <page>   ->  [hits so far, when it was first seen (ms)]
 *
 * The count and the first-seen time are two keys, read and written in one
 * pessimistic transaction, so two callers hitting the same page at once can't
 * both read the old count. Build it with the command in README.md, then
 *
 *   redis-cli -x SETF hits < hits.wasm
 *   redis-cli HITS /home
 */
#include "barch.h"

static char key[256];
static char num[24];

/* key = prefix + page, answers its length, or -1 if the page is too long */
static int32_t make_key(const char* prefix, const char* page, int32_t plen) {
    int32_t n = 0;
    while (prefix[n]) {
        key[n] = prefix[n];
        ++n;
    }
    if (plen > (int32_t) sizeof key - n)
        return -1;
    for (int32_t i = 0; i < plen; ++i)
        key[n + i] = page[i];
    return n + plen;
}

__attribute__((export_name("call"))) int32_t call(void) {
    static char page[200];
    if (barch_arg_count() != 1)
        return barch_fail("ERR HITS takes one page");
    if (barch_arg_len(0) > (int32_t) sizeof page)
        return barch_fail("ERR page name too long");
    int32_t plen = barch_arg_read(0, page);

    // a conflict or lock timeout comes back as an error value, not a trap
    int32_t began = barch_tx_begin(1);
    if (barch_val_kind(began) == BARCH_ERROR)
        return barch_fail("ERR couldn't start a transaction");

    int32_t klen = make_key("hits:", page, plen);
    if (klen < 0) {
        barch_tx_abort();
        return barch_fail("ERR page name too long");
    }
    int64_t hits = barch_to_int(barch_store_get(key, klen)) + 1;
    barch_store_set(key, klen, num, barch_from_int(hits, num));

    // the first-seen time, set once; barch_time is the log's own in a run from it
    klen = make_key("seen:", page, plen);
    int64_t seen = barch_to_int(barch_store_get(key, klen));
    if (seen == 0) {
        seen = barch_time();
        barch_store_set(key, klen, num, barch_from_int(seen, num));
    }

    int32_t done = barch_tx_commit();
    if (barch_val_kind(done) == BARCH_ERROR) {
        static char why[200];
        int32_t n = barch_read(done, why, (int32_t) sizeof why);
        if (n < 0)
            return barch_fail("ERR commit failed");
        barch_reply_error(why, n);
        return 1;
    }

    barch_reply_array(2);
    barch_reply_int(hits);
    barch_reply_int(seen);
    return 0;
}
