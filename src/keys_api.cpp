//
// Created by teejip on 8/1/26.
//
// The key API: the commands that operate on keys in the current key space -
// GET/SET/ADD/REM and everything around them (bounds, ranges, counters, scans,
// glob matching, expiry). They were carved out of barch.cpp, which now keeps the
// module wiring, key space administration, replication and stats.
//

#include <ranges>
#include <cctype>
#include <cstring>
#include <cmath>
#include <shared_mutex>

#include "keys_api.h"
#include "sharded_store.h"
#include "barch_apis.h"
#include "sastam.h"
#include "value_type.h"
#include "variable.h"
#include "conversion.h"
#include "composite.h"
#include "glob.h"
#include "keys.h"
#include "keyspec.h"
#include "keyspace_locks.h"
#include "spaces_spec.h"
#include "configuration.h"
#include "statistics.h"
#include "dictionary_compressor.h"
#include "lzr_log.h"
#include "module.h"
#include "caller.h"
#include "vk_caller.h"
#include "art/art.h"
#include "art/iterator.h"

extern "C" {
#include "../external/include/valkeymodule.h"
}

template<typename IntT>
static int BarchModifyInteger(caller& call,const arg_t& argv, IntT by) {

    if (argv.size() < 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto converted = conversion::as_composite(k);

    int r = -1;
    IntT l = IntT();
    // `present` separates a key that was there from one that was not. shard::update
    // answers false for both a missing key and an updater that declined, so without
    // this the decline was read as a miss and the insert below overwrote a value that
    // simply was not a number
    bool present = false;
    numeric_status why = numeric_status::updated;
    auto updater = [&](const art::node_ptr &value) -> art::node_ptr {
        if (value.null()) {
            return nullptr;
        }
        present = true;
        auto val = leaf_numeric_update(l, value, by, why);
        if (!val.null()) {
            r = 0;
        }
        return val;
    };
    // update-or-create: the miss and the insert that follows it must not race, so
    // both run under one write lock on the owning shard
    barch::sharded_store store(call.kspace());
    store.with_key_write(converted.get_value(), [&](const barch::shard_ptr& t) {
        if (!t->update(converted.get_value(), updater)) {
            if (present) {
                return; // there, but not a number we can add to. leave it alone
            }
            l += by;
            Variable s = l;
            auto fc = [&](const art::node_ptr &) -> void {};
            t->opt_insert({},converted.get_value(), {s.s()}, true, fc);
            r = 0;
        }
    });
    if (r == 0) {
        return call.push_int(l);
    }
    if (present && why == numeric_status::overflowed) {
        return call.push_error("increment or decrement would overflow");
    }
    return call.push_error("value is not an integer or out of range");
}

extern "C" {
/* B.RANGE <startkey> <endkey> <count>
*
* Return a list of matching keys, lexicographically between startkey
* and endkey. No more than 'count' items are emitted. */

int RANGE(caller& call, const arg_t& argv) {

    if (argv.size() < 3 || argv.size() > 4)
        return call.wrong_arity();

    /* Parse the count argument. */
    long long count = -1;
    if (argv.size() == 4)
        count = conversion::as_variable(argv[3]).i();

    auto k1 = argv[1];
    auto k2 = argv[2];

    if (key_ok(k1) != 0)
        return call.key_check_error(k1);
    if (key_ok(k2) != 0)
        return call.key_check_error(k2);

    auto c1 = conversion::as_composite(k1);
    auto c2 = conversion::as_composite(k2);

    barch::sharded_store store(call.kspace());
    call.start_array();
    // TODO: replace this with streaming api to reduce memory
    store.range(c1.get_value(), c2.get_value(), count, [&](art::value_type k) {
        call.push_encoded_key(k);
    });
    call.end_array();
    return 0;
}
int cmd_RANGE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {

    vk_caller caller;

    return caller.vk_call(ctx, argv, argc, ::RANGE);
}
int COUNT(caller& call, const arg_t& argv) {

    if (argv.size() != 3)
        return call.wrong_arity();

    auto k1 = argv[1];
    auto k2 = argv[2];

    if (key_ok(k1) != 0)
        return call.key_check_error(k1);
    if (key_ok(k2) != 0)
        return call.key_check_error(k2);

    auto c1 = conversion::as_composite(k1);
    auto c2 = conversion::as_composite(k2);
    barch::sharded_store store(call.kspace());
    return call.push_int(store.count(c1.get_value(), c2.get_value()));
}
int cmd_COUNT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {

    vk_caller caller;

    return caller.vk_call(ctx, argv, argc, ::COUNT);
}

/* B.KEYS
*
* match against all keys using a glob pattern
* */
static int glob_command(caller& call, const arg_t& argv, bool by_value) {

    if (argv.size() < 2 || argv.size() > 4)
        return call.wrong_arity();

    art::keys_spec spec(argv);
    if (spec.parse_keys_options() != call.ok()) {
        return call.wrong_arity();
    }
    std::mutex vklock{};
    std::atomic<int64_t> replies = 0;
    art::value_type pattern = argv[1];
    barch::sharded_store store(call.kspace());

    if (spec.count) {
        store.glob(spec, pattern, by_value, [&](const art::leaf& unused(l)) -> bool {
            ++replies;
            return true;
        });
        return call.push_ll(replies);
    }
    /* Reply with the matching items. */
    call.start_array();
    store.glob(spec, pattern, by_value, [&](const art::leaf& l) -> bool {
        std::lock_guard lk(vklock); // worker threads call in here concurrently
        if (0 != call.push_encoded_key(l.get_key())) {
            return false;
        }
        ++replies;
        return true;
    });
    call.end_array();
    return call.ok();
}

int KEYS(caller& call, const arg_t& argv) {
    return glob_command(call, argv, false);
}
int cmd_KEYS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, KEYS);
}
/* B.VALUES
* i.e. VALUES *ZZZ* COUNT
* match against all values using a glob pattern
*
* the pattern is matched against the values, but the reply is the keys that hold
* them - VALUES 3 answers with the key whose value is "3", not with "3". this is
* intended: it is the inverse lookup KEYS cannot do. COUNT replaces the reply with
* the number of matches.
* */
int VALUES(caller& call, const arg_t& argv) {
    return glob_command(call, argv, true);
}
int cmd_VALUES(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, VALUES);
}
/* B.SET <key> <value>
 *
 * Set the specified key to the specified value. */
int SET(caller& call,const arg_t& argv) {
    if (argv.size() < 3)
        return call.wrong_arity();


    auto k = argv[1];
    auto v = argv[2];

    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto sp = call.kspace();
    auto converted = conversion::as_composite(k);
    auto key = converted.get_value();
    art::key_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.syntax_error();
    }
    spec.hash = !sp->opt_ordered_keys;

    // SET ... GET answers with the value that was there before, so the callback has to
    // read it off the node it is handed. It used to assign `key`, which meant GET
    // replied with the key being written rather than the old value. The bytes are
    // copied because the leaf can be replaced by the insert that follows
    std::string previous;
    bool had_previous = false;
    auto fc = [&](const art::node_ptr &existing) -> void {
        if (spec.get && !existing.null()) {
            auto cl = existing.const_leaf();
            auto vt = cl->get_value();
            if (cl->is_compressed()) {
                vt = dictionary::decompress(vt);
            }
            previous.assign(vt.chars(), vt.size);
            had_previous = true;
        }
    };

    art::key_options opts = spec;
    const auto& compressed = dictionary::compress(v);
    if (!compressed.empty()) {
        statistics::value_bytes_compressed += compressed.size;
        opts.set_compressed(true);
        v = compressed;
    }

    barch::sharded_store store(sp);
    bool stored = true;
    if (spec.nx || spec.xx) {
        // NX and XX never reached storage: key_options carries no such flag and the
        // key_spec conversion drops them, so both were parsed and then ignored, and a
        // SET NX would happily overwrite a key that was already there. The test the
        // condition names has to be made under the same write lock as the insert that
        // follows it, or two callers both find the key absent and both write
        stored = false;
        store.with_key_write(key, [&](const barch::shard_ptr& t) {
            art::node_ptr existing = t->search(key);
            bool present = !existing.null();
            if (spec.get && present) {
                auto cl = existing.const_leaf();
                auto vt = cl->get_value();
                if (cl->is_compressed()) {
                    vt = dictionary::decompress(vt);
                }
                previous.assign(vt.chars(), vt.size);
                had_previous = true;
            }
            if ((spec.nx && present) || (spec.xx && !present)) {
                return; // the condition refused it; nothing is written
            }
            t->insert(opts, key, v, true, fc);
            stored = true;
        });
    } else {
        store.insert(opts, key, v, true, fc);
    }

    if (spec.get) {
        // with GET the reply is the old value whether or not the condition let the
        // write through, which is what redis does
        if (had_previous) {
            return call.push_vt(art::value_type{previous});
        } else {
            return call.push_null();
        }
    }
    if (!stored) {
        return call.push_null();
    }
    return call.push_simple("OK");
}


int cmd_SET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, SET);
}

unused(
static int BarchModifyDouble(caller& call,const arg_t& argv, double by) {

    if (argv.size() < 2)
        return call.wrong_arity();
    auto k = argv[1];
    art::key_spec spec;
    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::as_composite(k);
    barch::sharded_store store(call.kspace());
    auto t = store.write_locked(converted.get_value());
    int r = -1;
    double l = 0;
    auto updater = [&](const art::node_ptr &value) -> art::node_ptr {
        if (value.null()) {
            return nullptr;
        }
        auto val = leaf_numeric_update(l, value, by);
        if (!val.null()) {
            r = 0;
        }
        return val;
    };

    if (!t->update(converted.get_value(), updater)) {

    }
    if (r == 0) {
        return call.double_(l);
    } else {
        return call.null();
    }
}
)
int INCR(caller& call, const arg_t& argv) {
    ++statistics::incr_ops;
    // (int64_t) matters: a bare 1 deduces IntT as int, so INCR ran on 32 bits and
    // refused any value above INT32_MAX - redis's INCR is 64 bit signed. INCRBY was
    // always right because it parses its argument into a long long. See DONE 39
    return BarchModifyInteger(call, argv, (int64_t) 1);
}
int cmd_INCR(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, INCR);
}
int INCRBY(caller& call, const arg_t& argv) {
    ++statistics::incr_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    long long by = 0;

    if (!conversion::to_ll(argv[2], by)) {
        return call.push_error("not a valid integer");
    }
    auto arg2 = argv;
    arg2.pop_back();
    return BarchModifyInteger(call, arg2, by);
}

int UINCRBY(caller& call, const arg_t& argv) {
    ++statistics::incr_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    uint64_t by = 0;

    if (!conversion::to_ui64(argv[2], by)) {
        return call.push_error("not a valid integer");
    }
    auto arg2 = argv;
    arg2.pop_back();
    return BarchModifyInteger(call, arg2, by);
}
int _APPEND(caller& call, const arg_t& argv, bool pre) {
    ++statistics::set_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    auto k = argv[1];
    auto v = argv[2];
    art::key_spec spec;
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    long long r = v.size;
    auto converted = conversion::as_composite(k);
    auto fc = [&](art::node_ptr) -> void {
    };
    // read-modify-write on one key: the whole body holds a single write lock
    int reply = call.ok();
    barch::sharded_store store(call.kspace());
    store.with_key_write(converted.get_value(), [&](const barch::shard_ptr& t) {
    auto n = t->search(converted.get_value());
    if (n.is_leaf) {
        auto leaf = n.const_leaf();
        art::key_options opts = leaf->options();

        auto ov = leaf->get_value();
        if (leaf->is_compressed()) {
            ov = dictionary::decompress(ov); // the decompression may fail (perhaps panic because format is broken)
        }
        r += ov.size;// the decompressed length is used

        // threadsafe, non-re-entrant, faster
        thread_local heap::vector<uint8_t> s;
        s.clear();
        if (pre) {
            s.insert(s.end(), v.begin(), v.end());
            s.insert(s.end(), ov.begin(), ov.end());
        }else {
            s.insert(s.end(), ov.begin(), ov.end());
            s.insert(s.end(), v.begin(), v.end());
        }
#if 0 // compression on append can get really slow - so we leave it
        const auto& compressed = dictionary::compress({s.data(),s.size()});
        if (!compressed.empty()) {
            statistics::value_bytes_compressed += compressed.size;
            opts.set_compressed(true);
            v = compressed;
        }else {
            v = {s.data(),s.size()};
        }
#else
        v = {s.data(),s.size()};
#endif

        if (converted.get_value().size + v.size > maximum_allocation_size) {
            reply = call.push_int(0);
            return;
        }

        t->opt_insert( opts, converted.get_value(), v, true, fc);
        reply = call.push_ll(r);

    }else {
        art::key_options options;

        if (!t->opt_insert(options, converted.get_value(), v, true, fc)) {
            reply = call.push_error("key value not added");
            return;
        }
        reply = call.push_ll(r);

    }
    });
    return reply;
}
int APPEND(caller& call, const arg_t& argv) {
    return _APPEND(call, argv, false);
}
int PREPEND(caller& call, const arg_t& argv) {
    return _APPEND(call, argv, true);
}

int cmd_PREPEND(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PREPEND);
}

int cmd_APPEND(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, APPEND);
}

int cmd_INCRBY(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, INCRBY);
}

int cmd_UINCRBY(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, INCRBY);
}

int DECR(caller& call, const arg_t& argv) {
    ++statistics::decr_ops;
    return BarchModifyInteger(call, argv, (int64_t) -1);
}

int cmd_DECR(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, DECR);
}

int DECRBY(caller& call, const arg_t& argv) {
    ++statistics::incr_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    int64_t by = 0;

    if (!conversion::to_i64(argv[2], by)) {
        return call.wrong_arity();
    }

    return BarchModifyInteger(call,argv, -by);
}
int UDECRBY(caller& call, const arg_t& argv) {
    ++statistics::incr_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    uint64_t by = 0;

    if (!conversion::to_ui64(argv[2], by)) {
        return call.wrong_arity();
    }

    return BarchModifyInteger(call,argv, -by);
}
int cmd_UDECRBY(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, UDECRBY);
}
int cmd_DECRBY(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, DECRBY);
}

int MSET(caller& call, const arg_t& argv) {
    // pairs only: an odd list used to walk off the end of argv and surface as the
    // out_of_range from small_vector rather than as a wrong arity
    if (argv.size() < 3 || (argv.size() % 2) == 0)
        return call.wrong_arity();
    int r = call.ok();
    barch::sharded_store store(call.kspace());
    for (size_t n = 1; n < argv.size(); n += 2) {
        auto k = argv[n];
        auto v = argv[n + 1];

        if (key_ok(k) != 0) {
            r |= call.push_null();
            continue;
        }

        auto converted = conversion::as_composite(k);
        art::key_spec spec; //(argv, argc);
        auto fc = [&](art::node_ptr) -> void {
        };
        // one lock per key, as before: MSET has never been atomic across keys
        store.with_key_write(converted.get_value(), [&](const barch::shard_ptr& t) {
            t->insert( spec, converted.get_value(), v, true, fc);
        });

    }

    if (r != call.ok()) {
        return call.push_error("one or more keys were rejected");
    }
    return call.push_simple("OK");
}
int cmd_MSET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, MSET);
}
/* B.ADD <key> <value>
 *
 * Add the specified key only if its not there, with specified value. */
int ADD(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto k = argv[1];
    auto v = argv[2];

    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto fc = [](art::node_ptr) -> void {
    };
    auto converted = conversion::as_composite(k);

    art::key_spec spec(argv);
    barch::sharded_store store(call.kspace());
    store.add(spec, converted.get_value(), v, fc);

    return call.push_simple("OK");
}

int cmd_ADD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ADD);
}
/* B.GET <key>
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int GET(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto converted = conversion::as_composite(k);
    barch::sharded_store store(call.kspace());
    int r = call.ok();
    bool found = store.search(converted.get_value(), [&](const art::node_ptr& n) {
        auto cl = n.const_leaf();
        auto vt = cl->get_value();
        if (cl->is_compressed()) {
            vt = dictionary::decompress(vt);
        }
        r = call.push_vt(vt);
    });
    return found ? r : call.push_null();
}
/* B.SCAN <key>
 *
 * Return the next key in semi-allocation order, or a null reply if the key
 * is not defined or its the last key. */
int SCAN(caller& call, const arg_t& argv) {
    art::scan_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.syntax_error();
    }
    barch::sharded_store store(call.kspace());
    // a cursor is dropped when the scan runs out of shards, so one that is followed to
    // the end costs nothing. An abandoned one stays until the connection closes, and it
    // holds a page buffer, so a connection is only allowed so many at a time -
    // max_scan_iterators, which CLIENT INFO reports against as iters and iters-mem, and
    // CLIENT CLEAR_ITERS lets a client reclaim
    auto id = spec.scan_id;
    auto cursor = call.get_iteration(id);
    if (!cursor) {
        auto max_iterations = barch::get_max_scan_iterators();
        if (max_iterations && call.iteration_count() >= max_iterations) {
            return call.push_error("too many open SCAN cursors, finish one or use CLIENT CLEAR_ITERS");
        }
        cursor = call.create_iteration();
        if (!store.open_scan(*cursor)) {
            return call.push_error("invalid shard count");
        }
    }
    call.push_string(std::to_string(cursor->id));
    call.start_array();
    bool complete = store.scan(*cursor, spec, [&](art::value_type key) -> bool {
        call.push_encoded_key(key); // it throws so it's ok
        return call.results_count() < spec.count;
    });
    call.end_array();
    if (complete) {
        call.erase_iteration(cursor->id);
        call.set_string(0, "0");
    }
    return call.ok();
}
int cmd_SCAN(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    thread_local vk_caller call; // there's only one thread in redis/valkey
    return call.vk_call(ctx, argv, argc, SCAN);
}
/**
 * return the LENGTH of a key
 * @param call
 * @param argv
 * @return 0 if all is ok
 */
int LENGTH(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto converted = conversion::as_composite(k);
    barch::sharded_store store(call.kspace());
    int r = call.ok();
    bool found = store.search(converted.get_value(), [&](const art::node_ptr& n) {
        auto cl = n.const_leaf();
        auto vt = cl->get_value();
        if (cl->is_compressed()) {
            vt = dictionary::decompress(vt);
        }
        r = call.push_ll(vt.size);
    });
    return found ? r : call.push_null();
}
int cmd_GET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, GET);
}

int TTL(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto converted = conversion::as_composite(k);
    barch::sharded_store store(call.kspace());
    int reply = call.ok();
    bool answered = false;
    // not store.search: that treats a tombstone as absent, and TTL reports one as
    // present with no expiry.
    // the two negatives follow redis: -1 is present with no expiry, -2 is no such key.
    // they used to be the other way round, which quietly inverted every client's check
    store.with_key_read(converted.get_value(), [&](const barch::shard_ptr& t) {
        art::node_ptr r = t->search(converted.get_value());
        if (r.null()) {
            return;
        }
        answered = true;
        auto l = r.const_leaf();
        if (l->is_expiry()) {
            long long e = (l->expiry_ms() - art::now())/1000;
            reply = call.push_ll(e);
        } else {
            reply = call.push_ll(-1);
        }
    });
    return answered ? reply : call.push_ll(-2);

}
int cmd_TTL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, TTL);
}
int EXISTS(caller& call, const arg_t& argv) {
    if (argv.size() < 2)
        return call.wrong_arity();
    barch::sharded_store store(call.kspace());
    // the count of keys that exist, counting duplicates, as redis does. it used to be
    // a single boolean that was true only when every named key was present, which
    // answers a different question and cannot be told apart from "exactly one of one"
    int64_t found = 0;
    for (size_t i = 1; i < argv.size(); ++i) {
        auto k = argv[i];
        if (key_ok(k) != 0)
            return call.key_check_error(k);
        auto converted = conversion::as_composite(k);
        if (store.exists(converted.get_value())) {
            ++found;
        }
    }
    return call.push_ll(found);
}
int cmd_EXISTS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, EXISTS);
}

int EXPIRE(caller& call, const arg_t& argv) {
    if (argv.size() < 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto converted = conversion::as_composite(k);
    barch::sharded_store store(call.kspace());
    int reply = call.ok();
    bool answered = false;
    // read, decide, then write: has to hold one lock across all three
    store.with_key_write(converted.get_value(), [&](const barch::shard_ptr& t) {
        art::node_ptr r = t->search(converted.get_value());
        if (r.null()) {
            return;
        }
        art::key_expire_spec spec(argv);
        if (spec.parse_options() != call.ok()) {
            answered = true;
            reply = call.syntax_error();
            return;
        }

        auto l = r.const_leaf();
        if (spec.nx) {
            if (l->is_expiry()) return;
        } else if (spec.xx) {
            if (!l->is_expiry()) return;
        } else if (spec.gt) {
            if (spec.ttl + art::now() < l->expiry_ms()) return;
        } else if (spec.lt) {
            if (spec.ttl + art::now() > l->expiry_ms()) return;
        }
        auto updater = [&t,spec](const art::node_ptr &leaf) -> art::node_ptr {
            if (leaf.null()) {
                return leaf;
            }
            auto l = leaf.const_leaf();
            if (art::now() + spec.ttl == 0) {
                barch::log({"why"});
            }
            return art::make_leaf(t->get_ap(), l->get_key(),
                l->get_value(),
                art::now() + spec.ttl, l->is_volatile(),
                l->is_compressed());
        };
        answered = true;
        reply = t->update(l->get_key(), updater) ? call.push_ll(1) : call.push_ll(-2);
    });
    return answered ? reply : call.push_ll(-1);
}
int cmd_EXPIRE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, EXPIRE);
}
/* B.MGET <keys>
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int MGET(caller& call, const arg_t& argv) {

    if (argv.size() < 2)
        return call.wrong_arity();
    int responses = 0;
    barch::sharded_store store(call.kspace());
    call.start_array();
    for (size_t arg = 1; arg < argv.size(); ++arg) {
        auto k = argv[arg];
        if (key_ok(k) != 0) {
            call.push_null();
        } else {
            auto converted = conversion::as_composite(k);
            // not store.search: MGET has never decompressed, nor skipped tombstones,
            // the way GET does. left as it was rather than quietly aligned
            store.with_key_read(converted.get_value(), [&](const barch::shard_ptr& t) {
                art::node_ptr r = t->search(converted.get_value());
                if (r.null()) {
                    call.push_null();
                } else {
                    auto vt = r.const_leaf()->get_value();
                    call.push_vt(vt);
                }
            });
            ++responses;
        }
    }
    call.end_array();
    return call.ok();
}
int cmd_MGET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, MGET);
}
/* B.MINIMUM
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int MIN(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    barch::sharded_store store(call.kspace());
    int ok = call.ok();
    if (!store.minimum([&](art::value_type k) { ok = call.push_encoded_key(k); })) {
        ok = call.push_null();
    }
    return ok;
}
int cmd_MIN(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc,MIN);
}

/* B.MAXIMUM
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int MAX(caller& call, const arg_t& ) {
    barch::sharded_store store(call.kspace());
    int ok = call.ok();
    if (!store.maximum([&](art::value_type k) { ok = call.push_encoded_key(k); })) {
        ok = call.push_null();
    }
    return ok;
}
int cmd_MAX(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, MAX);
}
/* B.LB <key>
 * return first key not less than parameter in 1st slot
 *
 */
int LB(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::as_composite(k);
    barch::sharded_store store(call.kspace());
    int ok = call.ok();
    if (!store.lower_bound(converted.get_value(), [&](art::value_type f) { ok = call.push_encoded_key(f); })) {
        ok = call.push_null();
    }
    return ok;
}
int cmd_LB(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, LB);
}

/**
 * upper bound
 * @param call contains call context to place results
 * @param argv 1st param is key for upperbound
 * @return 0 if no error with key thats the upper bound (first larger than key in argv
 */
int UB(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::as_composite(k);
    barch::sharded_store store(call.kspace());
    int ok = call.ok();
    if (!store.upper_bound(converted.get_value(), [&](art::value_type f) { ok = call.push_encoded_key(f); })) {
        ok = call.push_null();
    }
    return ok;
}
int cmd_UB(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, UB);
}

/* B.RM <key>
 *
 * remove the value associated with the key and return the key if such a key existed. */
int REM(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto k = argv[1];

    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::as_composite(k);
    int r = 0;
    auto fc = [&r,&call](art::node_ptr n) -> void {
        if (n.null()) {
            r = call.push_null();
        } else {
            auto vt = n.const_leaf()->get_value();
            r = call.push_vt(vt);
        }
    };

    barch::sharded_store store(call.kspace());
    store.remove(converted.get_value(), fc);

    return r;
}
/* DEL <key> [key ...]
 *
 * redis's DEL: takes any number of keys and answers with how many were actually
 * removed. It used to be registered as another name for REM, which takes one key and
 * answers with the value it removed - so a client calling DEL got a bulk string where
 * its parser wanted an integer. REM keeps its own behaviour; this is the compatible one.
 */
int DEL(caller& call, const arg_t& argv) {
    if (argv.size() < 2)
        return call.wrong_arity();
    barch::sharded_store store(call.kspace());
    int64_t removed = 0;
    for (size_t i = 1; i < argv.size(); ++i) {
        auto k = argv[i];
        if (key_ok(k) != 0)
            return call.key_check_error(k);
        auto converted = conversion::as_composite(k);
        auto fc = [&removed](art::node_ptr n) -> void {
            if (!n.null()) ++removed;
        };
        store.remove(converted.get_value(), fc);
    }
    return call.push_ll(removed);
}
int cmd_REM(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, REM);
}
}

/* the key commands as the valkey module sees them. barch.cpp calls this from
 * ValkeyModule_OnLoad, the same way it calls add_hash_api and add_ordered_api.
 *
 * SCAN, VALUES, UB and LENGTH have handlers above but are deliberately absent
 * here: they were never registered as module commands, only reachable over RESP.
 */
int add_keys_api(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_CreateCommand(ctx, NAME(SET), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(APPEND), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(PREPEND), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(EXPIRE), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(INCR), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(DECR), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(INCRBY), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(DECRBY), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(UINCRBY), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(UDECRBY), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(MSET), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ADD), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(GET), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(EXISTS), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(TTL), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(MGET), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(KEYS), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(LB), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(REM), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(RANGE), "readonly", 1, 2, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(COUNT), "readonly", 1, 2, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(MAX), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(MIN), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}

/* the key commands as a RESP client sees them. FIRST, NEXT and DEL are aliases kept
 * for compatibility, and point at the same handlers as LB, UB and REM. */
void register_keys_api(function_map& r) {
    r["SET"] = {::SET,{"write","keys","data"}};
    r["APPEND"] = {::APPEND,{"write","keys","data"}};
    r["PREPEND"] = {::PREPEND,{"write","keys","data"}} ;
    r["KEYS"] = {::KEYS,{"read","keys","data"}, true};
    r["VALUES"] = {::VALUES,{"read","keys","data"}, true};
    r["INCR"] = {::INCR,{"write","keys","data"}};
    r["INCRBY"] = {::INCRBY,{"write","keys","data"}};
    r["UINCRBY"] = {::UINCRBY,{"write","keys","data"}};
    r["DECR"] = {::DECR,{"write","keys","data"}};
    r["DECRBY"] = {::DECRBY,{"write","keys","data"}};
    r["UDECRBY"] = {::UDECRBY,{"write","keys","data"}};
    r["COUNT"] = {::COUNT,{"read","keys","data"}};
    r["EXISTS"] = {::EXISTS,{"read","keys","data"}};
    r["EXPIRE"] = {::EXPIRE,{"write","keys","data"}};
    r["MSET"] = {::MSET,{"write","keys","data"}};
    r["ADD"] = {::ADD,{"write","keys","data"}};
    r["GET"] = {::GET,{"read","keys","data"}};
    r["SCAN"] = {::SCAN,{"read","keys","data"}};
    r["LENGTH"] = {::LENGTH,{"read","keys","data"}};
    r["MGET"] = {::MGET,{"read","keys","data"}};
    r["MIN"] = {::MIN,{"read","keys","data"}};
    r["MAX"] = {::MAX,{"read","keys","data"}};
    r["LB"] = {::LB,{"read","keys","data"}};
    r["UB"] = {::UB,{"read","keys","data"}};
    r["FIRST"] = r["LB"]; // alias
    r["NEXT"] = r["UB"];
    r["REM"] = {::REM,{"write","keys","data"}};
    r["DEL"] = {::DEL,{"write","keys","data"}};
    r["RANGE"] = {::RANGE,{"read","keys","data"}, true};
    r["TTL"] = {::TTL,{"read","keys","data"}};
}
