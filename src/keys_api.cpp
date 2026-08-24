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
#include <cstdio>
#include <shared_mutex>

#include "keys_api.h"
#include <algorithm>
#include <random>
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
#include "foreign/foreign.h"
#include <unordered_set>
#include <chrono>
#include "key_type.h"
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

/**
 * Refuse a string command when the name is being used as a list, hash or ordered set.
 *
 * Called with the owning shard already locked, so the answer cannot change under it. See
 * key_type.h for why the type is observed rather than stored, and for what this does not
 * catch.
 */
static bool wrong_type_here(barch::sharded_store& store, art::value_type name) {
    return barch::kind_of(store, name) == barch::key_kind::container;
}

template<typename IntT>
static int BarchModifyInteger(caller& call,const arg_t& argv, IntT by) {

    if (argv.size() < 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto converted = call.kspace()->encode_key(k);

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
    if (wrong_type_here(store, k)) {
        return call.push_error(barch::wrong_type_message());
    }
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
/**
 * May this connection be told that this key exists?
 *
 * A stored function is an ordinary key, so every walk meets one - KEYS, SCAN, RANGE,
 * the bounds and RANDOMKEY all would. Someone who cannot call a function or read its
 * source has no business learning which ones there are, so for them the range is not
 * there at all. See TODO 98.
 *
 * Anything that is not a function key is visible on the terms it always was; this
 * decides nothing else.
 */
static bool visible_key(caller& call, art::value_type key) {
    if (!key.size || key.bytes[0] != art::tfunction)
        return true;
    static const size_t fn = get_category_map().at("function");
    const auto& acl = call.get_acl();
    return fn < acl.size() && acl[fn];
}


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

    auto c1 = call.kspace()->encode_key(k1);
    auto c2 = call.kspace()->encode_key(k2);

    barch::sharded_store store(call.kspace());
    call.start_array();
    // TODO: replace this with streaming api to reduce memory
    store.range(c1.get_value(), c2.get_value(), count, [&](art::value_type k) {
        if (visible_key(call, k))
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

    auto c1 = call.kspace()->encode_key(k1);
    auto c2 = call.kspace()->encode_key(k2);
    barch::sharded_store store(call.kspace());
    return call.push_int(store.count(c1.get_value(), c2.get_value()));
}
int cmd_COUNT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {

    vk_caller caller;

    return caller.vk_call(ctx, argv, argc, ::COUNT);
}

/* B.KEYS
*
* match a glob against key names and reply with the matching keys.
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

    // A container is answered by name, once - and the cost of working that out is kept
    // off the keys that are not containers.
    //
    // Three things were being paid for on every key the walk touched: a name was decoded
    // from it whether or not it had one, a string was built to carry the answer, and the
    // reply lock was taken even when only counting, which left the glob's worker threads
    // queueing on one mutex instead of scanning.
    //
    //   - a key that is not a container is recognised by its first byte and pushed as it
    //     is stored: no decode, no temporary. In an ordinary store that is most keys.
    //   - the name is decoded once and reused, rather than once to dedupe and again to
    //     decide how to push it.
    //   - counting locks only when it has something to remember, so ordinary keys count
    //     through the atomic and the threads stay threads.
    //
    // What remains is a set of the container names already answered: one entry per
    // collection rather than per key, and every one of them is in the reply as well, so it
    // is the same order of memory the answer already costs.
    //
    // "the same order as the answer" is still unbounded if the answer is, so the walk
    // watches the ceiling as it goes. heap::string_set allocates through the tracking
    // allocator, so the set is already counted in get_total_memory() - which is
    // heap::allocated, everything the process holds - and no separate accounting is
    // needed. Stopping the walk is what art::glob already does when max_count is reached,
    // so a short answer is a shape callers of this command can already get.
    //
    // The ceiling is max_memory_bytes, the same number eviction triggers on, deliberately.
    // Both mean the process is at its limit, so a reply stops being built at the moment
    // the store would start shedding data, and there is nothing further to configure. The
    // cost of the check when no limit is set - the default is UINT64_MAX - is one atomic
    // read per new container name, and nothing at all per ordinary key.
    heap::string_set named;
    const uint64_t memory_ceiling = barch::get_max_module_memory();
    bool stopped_early = false;
    auto count_one = [&](const art::leaf& l) -> bool {

        auto key = l.get_key();
        // skipped here and in the emit passes below on exactly the same terms, or the
        // *N header and the body it introduces stop agreeing
        if (!visible_key(call, key))
            return true;
        if (!key.size || !art::is_container_lead(*key.bytes)) {
            ++replies;
            return true;
        }
        std::string name = encoded_container_name(key);
        if (name.empty()) return true;          // an ordered set's member index
        std::lock_guard lk(vklock);
        if (named.emplace(std::move(name)).second) {
            ++replies;
            // only a new name grows the set, so that is the only place the ceiling can
            // be crossed by this walk
            if (get_total_memory() >= memory_ceiling) {
                stopped_early = true;
                return false;
            }
        }
        return true;
    };
    if (spec.count) {
        store.glob(spec, pattern, by_value, count_one);
        if (stopped_early) {
            barch::err({"KEYS stopped at the memory ceiling; the count is short",
                        __FILE__, __LINE__});
        }
        return call.push_ll(replies);
    }
    // KEYS over RESP writes each key to the socket as it is found, so the
    // reply does not sit in Variables. RESP2 needs *N first, so the walk
    // runs twice: once to count, then once to send. VALUES stays on the
    // result stack until it gets the same path.
    if (!by_value && call.can_write_socket()) {
        barch::sharded_store::glob_pages pages;
        store.glob(spec, pattern, by_value, count_one, nullptr, &pages);
        if (stopped_early) {
            barch::err({"KEYS stopped at the memory ceiling; the count is short",
                        __FILE__, __LINE__});
        }
        const int64_t n = replies.load();
        if (!call.write_socket_array((size_t) n)) {
            return call.push_error("failed to write KEYS header");
        }
        named.clear();
        replies = 0;
        stopped_early = false;
        store.glob(spec, pattern, by_value, [&](const art::leaf& l) -> bool {
            if (replies >= n) return false;
            auto key = l.get_key();
            if (!visible_key(call, key))
                return true;
            if (!key.size || !art::is_container_lead(*key.bytes)) {
                Variable item = encoded_key_as_variant(key);
                std::lock_guard lk(vklock);
                if (!call.write_socket(item)) return false;
                ++replies;
                return true;
            }
            std::string name = encoded_container_name(key);
            if (name.empty()) return true;
            std::lock_guard lk(vklock);
            if (!named.emplace(name).second) return true;
            std::string bulk = "$";
            bulk += name;
            if (!call.write_socket(Variable{std::move(bulk)})) return false;
            ++replies;
            if (get_total_memory() >= memory_ceiling) {
                stopped_early = true;
                return false;
            }
            return true;
        }, &pages, nullptr);
        Variable pad{nullptr};
        while (replies < n) {
            if (!call.write_socket(pad)) {
                // a header already went out; an error here would be a second reply
                return call.ok();
            }
            ++replies;
        }
        if (stopped_early) {
            barch::err({"KEYS stopped at the memory ceiling; the reply is short",
                        __FILE__, __LINE__});
        }
        return call.ok();
    }
    /* Reply with the matching items. */
    call.start_array();
    store.glob(spec, pattern, by_value, [&](const art::leaf& l) -> bool {
        auto key = l.get_key();
        if (!visible_key(call, key))
            return true;
        if (!key.size || !art::is_container_lead(*key.bytes)) {
            std::lock_guard lk(vklock); // worker threads call in here concurrently
            if (0 != call.push_encoded_key(key)) {
                return false;
            }
            ++replies;
            return true;
        }
        std::string name = encoded_container_name(key);
        if (name.empty()) return true;              // an ordered set's member index
        std::lock_guard lk(vklock);
        if (!named.emplace(name).second) {
            return true;                            // this collection is already answered
        }
        if (0 != call.push_vt(art::value_type{name})) {
            return false;
        }
        ++replies;
        if (get_total_memory() >= memory_ceiling) {
            stopped_early = true;
            return false;
        }
        return true;
    });
    call.end_array();
    if (stopped_early) {
        barch::err({"KEYS stopped at the memory ceiling; the reply is short",
                    __FILE__, __LINE__});
    }
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
* match a glob against each key's value, then reply with the key.
* VALUES 3 replies with the key that holds "3", not with "3" itself.
* COUNT replaces the reply with the number of matches.
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
    // A leaf holds the key and the value together and has to fit in a page. The insert
    // already refuses a pair that does not - it throws, art::insert catches it, logs it and
    // answers false - but SET never looked at that answer, so an oversized value was
    // acknowledged with OK and stored nothing. Silent loss on a write that said it worked
    // is the worst way to be wrong, so the size is judged here where it can be reported.
    if (!fits_in_leaf(call.kspace()->encode_key(k).get_value().size, v.size)) {
        return call.push_error(too_large_message());
    }
    auto sp = call.kspace();
    auto converted = call.kspace()->encode_key(k);
    auto key = converted.get_value();
    art::key_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return spec.bad_expire
                   ? call.push_error("invalid expire time in 'set' command")
                   : call.syntax_error();
    }
    spec.hash = !sp->opt_ordered_keys;

    // What redis does with a name that holds a list, hash or ordered set is not one rule
    // but two, and they were both wrong here. `SET k v` replaces the collection - that is
    // an ordinary overwrite and it answers OK. `SET k v GET` refuses with WRONGTYPE and
    // changes nothing, because the GET half has to return a string and there is not one.
    // barch used to write the plain key either way and leave the collection sitting under
    // the same name, so the name held both: LRANGE then answered WRONGTYPE and ZRANGE
    // still answered the members. See DONE 126
    {
        barch::sharded_store type_store(call.kspace());
        if (barch::kind_of_container(type_store, k) != barch::container_kind::none) {
            if (spec.get) {
                return call.push_error(barch::wrong_type_message());
            }
            barch::remove_container(type_store, k);
        }
    }

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
    // deliberately no type check: redis's SET replaces whatever the name held, including
    // a list or a hash, and the tests rely on it - every other string command refuses
    // instead. Replacing means removing, though, or the collection stays reachable
    // through its own commands beside the new value
    // deliberately no type check and no removal here. redis's SET replaces whatever the
    // name held, including a list, and string.tcl relies on that - every other string
    // command refuses instead. Removing the collection it replaces needs to tell a
    // collection from an ordinary key reliably, which the prefix probe cannot do: a plain
    // key is itself a composite split on a separator, so `SET "1.1 b"` matched the prefix
    // of `{1.1, a}` and deleted it. See TODO 53
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

    auto converted = call.kspace()->encode_key(k);
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
    // exactly the key. BarchModifyInteger cannot check this for us - DECRBY hands it an
    // argv of three and INCRBY one of two - so `INCR k anything` used to be accepted and
    // the extra word ignored, where redis answers wrong number of arguments
    if (argv.size() != 2)
        return call.wrong_arity();
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
        return call.push_error("value is not an integer or out of range");
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
        return call.push_error("value is not an integer or out of range");
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
    // a name already holding a list, hash or ordered set is the wrong type for a string
    // write, and redis refuses rather than replacing it. Without this SET answered OK and
    // the collection was gone - silent data loss on a command that said it worked. GET
    // had the check all along, which is why `SET k v` then `LRANGE k` answered WRONGTYPE
    // and looked like the read was at fault. See DONE 126
    {
        barch::sharded_store type_store(call.kspace());
        if (wrong_type_here(type_store, k)) {
            return call.push_error(barch::wrong_type_message());
        }
    }
    long long r = v.size;
    auto converted = call.kspace()->encode_key(k);
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
// compression on append can get really slow - so we leave it. The value is stored back
// uncompressed and stays that way until something rewrites it whole; that is a latency
// measure rather than an oversight, since compressing here puts the entire value through
// the dictionary on every append. SETRANGE makes the same trade
#if 0
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
/* SETRANGE <key> <offset> <value>
 *
 * Overwrite the value from `offset` with `value`, growing it if it has to. A key that is
 * not there counts as an empty value, and an offset past the end is filled with zero
 * bytes rather than refused - both as redis does, and the zero fill is why an offset can
 * be used to build a value out of order.
 *
 * Nothing is created when the value to write is empty and the key does not exist, so
 * `SETRANGE missing 0 ""` answers 0 and leaves the space alone rather than adding an
 * empty key nobody asked for.
 *
 * Modelled on _APPEND above: the read, the splice and the write are one write lock on the
 * owning shard, because anything else lets two callers each extend a value from the same
 * starting point and lose one of the writes.
 */
int SETRANGE(caller& call, const arg_t& argv) {
    ++statistics::set_ops;
    if (argv.size() != 4)
        return call.wrong_arity();
    auto k = argv[1];
    auto v = argv[3];
    if (key_ok(k) != 0)
        return call.key_check_error(k);

    long long offset = 0;
    if (!conversion::to_ll(argv[2], offset)) {
        return call.push_error("value is not an integer or out of range");
    }
    if (offset < 0) {
        return call.push_error("offset is out of range");
    }
    if ((size_t) offset + v.size > maximum_allocation_size) {
        return call.push_error("string exceeds maximum allowed size");
    }

    auto converted = call.kspace()->encode_key(k);
    auto fc = [&](art::node_ptr) -> void {
    };
    int reply = call.ok();
    barch::sharded_store store(call.kspace());
    if (wrong_type_here(store, k)) {
        return call.push_error(barch::wrong_type_message());
    }
    store.with_key_write(converted.get_value(), [&](const barch::shard_ptr& t) {
        auto n = t->search(converted.get_value());
        art::key_options opts;
        art::value_type ov{"", 0};
        const bool existed = n.is_leaf;
        if (existed) {
            auto leaf = n.const_leaf();
            opts = leaf->options();
            ov = leaf->get_value();
            if (leaf->is_compressed()) {
                ov = dictionary::decompress(ov);
            }
        } else if (v.size == 0) {
            // nothing to write and nothing there: do not bring a key into being
            reply = call.push_ll(0);
            return;
        }

        // threadsafe, non-re-entrant, as _APPEND does
        thread_local heap::vector<uint8_t> s;
        s.clear();
        s.insert(s.end(), ov.begin(), ov.end());
        if ((size_t) offset > s.size()) {
            s.resize((size_t) offset, 0);   // the gap is zero bytes, not spaces
        }
        if ((size_t) offset + v.size > s.size()) {
            s.resize((size_t) offset + v.size, 0);
        }
        std::copy(v.begin(), v.end(), s.begin() + offset);

        art::value_type written{s.data(), s.size()};
        if (converted.get_value().size + written.size > maximum_allocation_size) {
            reply = call.push_error("string exceeds maximum allowed size");
            return;
        }
        // a compressed leaf was decompressed above and is stored back uncompressed. That
        // is deliberate and it is a latency measure, not an oversight: compressing on
        // every partial write puts the whole value through the dictionary each time, and
        // a value being built by repeated SETRANGE would pay that on each call. It stays
        // uncompressed until something rewrites it whole. _APPEND makes the same trade,
        // and has the recompressing version next to it behind an #if 0
        opts.set_compressed(false);
        // opt_insert answers true when the key was *added*, so a false here is the normal
        // result of replacing a value that was already there - only a new key that failed
        // to appear is worth reporting, which is the distinction _APPEND makes too
        if (!t->opt_insert(opts, converted.get_value(), written, true, fc) && !existed) {
            reply = call.push_error("key value not added");
            return;
        }
        reply = call.push_ll((long long) s.size());
    });
    return reply;
}

/* GETRANGE <key> <start> <end>   (SUBSTR is the old name for it)
 *
 * The substring between two inclusive offsets. A negative offset counts back from the
 * end, so -1 is the last byte. Offsets outside the value are clamped rather than refused,
 * and a range that ends up empty - or a key that is not there - answers with an empty
 * string, not nil. Both of those are redis's behaviour and both are relied on: callers
 * use GETRANGE k 0 -1 to read a whole value without knowing its length.
 */
int GETRANGE(caller& call, const arg_t& argv) {
    ++statistics::get_ops;
    if (argv.size() != 4)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    long long start = 0, end = 0;
    if (!conversion::to_ll(argv[2], start) || !conversion::to_ll(argv[3], end)) {
        return call.push_error("value is not an integer or out of range");
    }
    auto converted = call.kspace()->encode_key(k);
    barch::sharded_store store(call.kspace());
    if (wrong_type_here(store, k)) {
        return call.push_error(barch::wrong_type_message());
    }
    int reply = call.ok();
    bool found = store.search(converted.get_value(), [&](const art::node_ptr& n) {
        auto cl = n.const_leaf();
        auto vt = cl->get_value();
        std::string held;
        if (cl->is_compressed()) {
            auto d = dictionary::decompress(vt);
            held.assign(d.chars(), d.size);
        } else {
            held.assign(vt.chars(), vt.size);
        }
        const long long n_bytes = (long long) held.size();
        long long from = start < 0 ? n_bytes + start : start;
        long long to = end < 0 ? n_bytes + end : end;
        if (from < 0) from = 0;
        if (to >= n_bytes) to = n_bytes - 1;
        if (n_bytes == 0 || from > to) {
            reply = call.push_vt(art::value_type{"", 0});
            return;
        }
        std::string part = held.substr((size_t) from, (size_t) (to - from + 1));
        reply = call.push_vt(art::value_type{part});
    });
    if (!found) {
        return call.push_vt(art::value_type{"", 0});   // empty, not nil
    }
    return reply;
}

/* GETDEL <key>
 *
 * The value, and the key is gone afterwards. One write lock covers the read and the
 * remove, so nothing can read the value between them and believe it is still there.
 */
int GETDEL(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    // reads a string, so a name holding a collection is the wrong type rather than a
    // miss - this used to answer nil. See DONE 126
    {
        barch::sharded_store type_store(call.kspace());
        if (wrong_type_here(type_store, k)) {
            return call.push_error(barch::wrong_type_message());
        }
    }
    auto converted = call.kspace()->encode_key(k);
    barch::sharded_store store(call.kspace());
    int reply = call.ok();
    bool had = false;
    std::string held;
    store.with_key_write(converted.get_value(), [&](const barch::shard_ptr& t) {
        auto n = t->search(converted.get_value());
        if (!n.is_leaf) {
            return;
        }
        auto cl = n.const_leaf();
        auto vt = cl->get_value();
        if (cl->is_compressed()) {
            auto d = dictionary::decompress(vt);
            held.assign(d.chars(), d.size);
        } else {
            held.assign(vt.chars(), vt.size);
        }
        had = true;
        t->remove(converted.get_value());
    });
    if (!had) {
        return call.push_null();
    }
    reply = call.push_vt(art::value_type{held});
    return reply;
}

/* GETEX <key> [EX s | PX ms | EXAT unix-s | PXAT unix-ms | PERSIST]
 *
 * The value, and the expiry is changed on the way past. With no option the expiry is left
 * exactly as it was - GETEX with nothing after the key is a plain GET, not a PERSIST.
 */
int GETEX(caller& call, const arg_t& argv) {
    ++statistics::get_ops;
    if (argv.size() < 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    // reads a string, so a name holding a collection is the wrong type rather than a
    // miss - this used to answer nil. See DONE 126
    {
        barch::sharded_store type_store(call.kspace());
        if (wrong_type_here(type_store, k)) {
            return call.push_error(barch::wrong_type_message());
        }
    }

    bool persist = false, change = false;
    int64_t when = 0;
    if (argv.size() > 2) {
        auto opt = argv[2].to_string();
        for (auto& ch : opt) ch = (char) toupper((unsigned char) ch);
        if (opt == "PERSIST") {
            if (argv.size() != 3) return call.syntax_error();
            persist = true;
            change = true;
        } else {
            if (argv.size() != 4) return call.syntax_error();
            long long given = 0;
            if (!conversion::to_ll(argv[3], given)) {
                return call.push_error("value is not an integer or out of range");
            }
            bool secs = (opt == "EX" || opt == "EXAT");
            bool rel = (opt == "EX" || opt == "PX");
            if (opt != "EX" && opt != "PX" && opt != "EXAT" && opt != "PXAT") {
                return call.syntax_error();
            }
            if (!art::expiry_ms(given, secs, rel, when)) {
                return call.push_error("invalid expire time in 'getex' command");
            }
            change = true;
        }
    }

    auto converted = call.kspace()->encode_key(k);
    barch::sharded_store store(call.kspace());
    bool had = false;
    std::string held;
    store.with_key_write(converted.get_value(), [&](const barch::shard_ptr& t) {
        auto n = t->search(converted.get_value());
        if (!n.is_leaf) {
            return;
        }
        auto cl = n.const_leaf();
        auto vt = cl->get_value();
        if (cl->is_compressed()) {
            auto d = dictionary::decompress(vt);
            held.assign(d.chars(), d.size);
        } else {
            held.assign(vt.chars(), vt.size);
        }
        had = true;
        if (!change) {
            return;
        }
        auto updater = [&t, persist, when](const art::node_ptr &leaf) -> art::node_ptr {
            if (leaf.null()) return leaf;
            auto l = leaf.const_leaf();
            return art::make_leaf(t->get_ap(), l->get_key(), l->get_value(),
                                  persist ? 0 : when, l->is_volatile(), l->is_compressed());
        };
        t->update(converted.get_value(), updater);
    });
    if (!had) {
        return call.push_null();
    }
    return call.push_vt(art::value_type{held});
}

/* SETEX <key> <seconds> <value> and PSETEX <key> <milliseconds> <value>
 *
 * SET with an expiry that is not optional. redis refuses a non positive time here rather
 * than storing a key that is already dead, which is the only thing separating these from
 * `SET k v EX n`.
 */
static int SETEX_(caller& call, const arg_t& argv, bool millis) {
    ++statistics::set_ops;
    if (argv.size() != 4)
        return call.wrong_arity();
    auto k = argv[1];
    auto v = argv[3];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    long long given = 0;
    if (!conversion::to_ll(argv[2], given)) {
        return call.push_error("value is not an integer or out of range");
    }
    int64_t deadline = 0;
    if (given <= 0 || !art::expiry_ms(given, !millis, true, deadline)) {
        return call.push_error("invalid expire time in 'setex' command");
    }
    if (!fits_in_leaf(call.kspace()->encode_key(k).get_value().size, v.size)) {
        return call.push_error(too_large_message());
    }
    // SETEX is a SET with a deadline, so it replaces a collection of the same name the
    // same way - and has to take it away rather than write beside it. See DONE 126
    {
        barch::sharded_store type_store(call.kspace());
        if (barch::kind_of_container(type_store, k) != barch::container_kind::none) {
            barch::remove_container(type_store, k);
        }
    }
    auto converted = call.kspace()->encode_key(k);
    art::key_options opts;
    opts.set_expiry(deadline);
    auto fc = [&](const art::node_ptr &) -> void {};
    const auto& compressed = dictionary::compress(v);
    if (!compressed.empty()) {
        statistics::value_bytes_compressed += compressed.size;
        opts.set_compressed(true);
        v = compressed;
    }
    barch::sharded_store store(call.kspace());
    store.insert(opts, converted.get_value(), v, true, fc);
    return call.push_simple("OK");
}
int SETEX(caller& call, const arg_t& argv) {
    return SETEX_(call, argv, false);
}
int PSETEX(caller& call, const arg_t& argv) {
    return SETEX_(call, argv, true);
}

/* LCS <key1> <key2> [LEN] [IDX] [MINMATCHLEN n] [WITHMATCHLEN]
 *
 * The longest common subsequence of two values - subsequence, not substring, so the
 * characters have to be in order but need not be adjacent.
 *
 * Plain, it answers with the subsequence itself. LEN answers with its length instead.
 * IDX answers with where the matches are: a map of `matches` and `len`, where each match
 * is the range in key1 and the range in key2, both inclusive, listed from the end of the
 * strings backwards - which is the order the traceback produces and the order redis
 * reports. MINMATCHLEN drops the short ones and WITHMATCHLEN adds each match's length.
 *
 * The table is (n+1)*(m+1) uint32, as redis's is, so the memory is the product of the two
 * lengths. That is worth knowing before calling it on large values; redis has the same
 * cost and the same caveat in its documentation.
 */
int LCS(caller& call, const arg_t& argv) {
    ++statistics::get_ops;
    if (argv.size() < 3)
        return call.wrong_arity();
    bool want_len = false, want_idx = false, with_match_len = false;
    long long min_match_len = 0;
    for (size_t i = 3; i < argv.size(); ++i) {
        auto opt = argv[i].to_string();
        for (auto& ch : opt) ch = (char) toupper((unsigned char) ch);
        if (opt == "LEN") {
            want_len = true;
        } else if (opt == "IDX") {
            want_idx = true;
        } else if (opt == "WITHMATCHLEN") {
            with_match_len = true;
        } else if (opt == "MINMATCHLEN") {
            if (i + 1 >= argv.size()) return call.syntax_error();
            if (!conversion::to_ll(argv[++i], min_match_len)) {
                return call.push_error("value is not an integer or out of range");
            }
        } else {
            return call.syntax_error();
        }
    }
    if (want_len && want_idx) {
        return call.push_error("If you want both the length and indexes, please just use IDX.");
    }

    // read both values first; the comparison itself needs no lock
    std::string a, b;
    barch::sharded_store store(call.kspace());
    auto read_one = [&](art::value_type key, std::string& into) -> void {
        if (key_ok(key) != 0) return;
        auto converted = call.kspace()->encode_key(key);
        store.search(converted.get_value(), [&](const art::node_ptr& n) {
            auto cl = n.const_leaf();
            auto vt = cl->get_value();
            if (cl->is_compressed()) {
                auto d = dictionary::decompress(vt);
                into.assign(d.chars(), d.size);
            } else {
                into.assign(vt.chars(), vt.size);
            }
        });
    };
    read_one(argv[1], a);
    read_one(argv[2], b);

    const size_t n = a.size(), m = b.size();
    if ((uint64_t) n * (uint64_t) m > (uint64_t) maximum_allocation_size) {
        return call.push_error("string too long for LCS");
    }
    // one row per character of a, one column per character of b
    heap::std_vector<uint32_t> table;
    table.assign((n + 1) * (m + 1), 0);
    auto at = [&](size_t i, size_t j) -> uint32_t& { return table[i * (m + 1) + j]; };
    for (size_t i = 1; i <= n; ++i) {
        for (size_t j = 1; j <= m; ++j) {
            if (a[i - 1] == b[j - 1]) {
                at(i, j) = at(i - 1, j - 1) + 1;
            } else {
                at(i, j) = std::max(at(i - 1, j), at(i, j - 1));
            }
        }
    }
    const uint32_t total = (n && m) ? at(n, m) : 0;
    if (want_len) {
        return call.push_ll((long long) total);
    }

    // walk back from the corner. Equal characters extend the current run; anything else
    // steps whichever way the table came from, which is where a run ends
    std::string result;
    struct match { long long a_start, a_end, b_start, b_end; };
    heap::std_vector<match> matches;
    size_t i = n, j = m;
    long long run_a_end = -1, run_b_end = -1;
    while (i > 0 && j > 0) {
        if (a[i - 1] == b[j - 1]) {
            result.push_back(a[i - 1]);
            if (run_a_end < 0) {
                run_a_end = (long long) i - 1;
                run_b_end = (long long) j - 1;
            }
            --i; --j;
            if (i == 0 || j == 0 || a[i - 1] != b[j - 1]) {
                matches.push_back({(long long) i, run_a_end, (long long) j, run_b_end});
                run_a_end = run_b_end = -1;
            }
        } else if (at(i - 1, j) > at(i, j - 1)) {
            --i;
        } else {
            --j;
        }
    }
    std::reverse(result.begin(), result.end());
    if (!want_idx) {
        return call.push_vt(art::value_type{result});
    }

    // RESP2 flattens this to an array of four; RESP3 sends it as a map
    call.start_map();
    call.push_simple("matches");
    call.start_array();
    for (const auto& mt : matches) {
        if (mt.a_end - mt.a_start + 1 < min_match_len) continue;
        call.start_array();
        call.start_array();
        call.push_ll(mt.a_start);
        call.push_ll(mt.a_end);
        call.end_array();
        call.start_array();
        call.push_ll(mt.b_start);
        call.push_ll(mt.b_end);
        call.end_array();
        if (with_match_len) {
            call.push_ll(mt.a_end - mt.a_start + 1);
        }
        call.end_array();
    }
    call.end_array();
    call.push_simple("len");
    call.push_ll((long long) total);
    call.end_map();
    return call.ok();
}

/* SETNX <key> <value>
 *
 * Set only if the key is absent, answering 1 when it was written and 0 when it was not.
 * `SET key value NX` does the same work; this is the older spelling, and the difference
 * is the reply - an integer here, a simple string or nil there.
 */
int SETNX(caller& call, const arg_t& argv) {
    ++statistics::set_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    auto k = argv[1];
    auto v = argv[2];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    // a name holding a collection is a name that is taken. NX means "only if absent", so
    // this is 0 rather than an error or an overwrite - which is what redis answers
    {
        barch::sharded_store type_store(call.kspace());
        if (barch::kind_of_container(type_store, k) != barch::container_kind::none) {
            return call.push_ll(0);
        }
    }
    if (!fits_in_leaf(call.kspace()->encode_key(k).get_value().size, v.size)) {
        return call.push_error(too_large_message());
    }
    auto converted = call.kspace()->encode_key(k);
    bool stored = false;
    auto fc = [&](const art::node_ptr &) -> void {};
    art::key_options opts;
    const auto& compressed = dictionary::compress(v);
    if (!compressed.empty()) {
        statistics::value_bytes_compressed += compressed.size;
        opts.set_compressed(true);
        v = compressed;
    }
    barch::sharded_store store(call.kspace());
    // the test and the write are one lock, or two callers both find it absent
    store.with_key_write(converted.get_value(), [&](const barch::shard_ptr& t) {
        if (!t->search(converted.get_value()).null()) {
            return;
        }
        t->insert(opts, converted.get_value(), v, true, fc);
        stored = true;
    });
    return call.push_ll(stored ? 1 : 0);
}

/* GETSET <key> <value>
 *
 * Write the value and answer with the one it replaced, nil when there was none. `SET key
 * value GET` is the same thing; this is the older spelling and redis still documents it.
 */
int GETSET(caller& call, const arg_t& argv) {
    ++statistics::set_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    auto k = argv[1];
    auto v = argv[2];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    if (!fits_in_leaf(call.kspace()->encode_key(k).get_value().size, v.size)) {
        return call.push_error(too_large_message());
    }
    {
        barch::sharded_store type_store(call.kspace());
        if (wrong_type_here(type_store, k)) {
            return call.push_error(barch::wrong_type_message());
        }
    }
    auto converted = call.kspace()->encode_key(k);
    bool had = false;
    std::string previous;
    auto fc = [&](const art::node_ptr &) -> void {};
    art::key_options opts;
    const auto& compressed = dictionary::compress(v);
    if (!compressed.empty()) {
        statistics::value_bytes_compressed += compressed.size;
        opts.set_compressed(true);
        v = compressed;
    }
    barch::sharded_store store(call.kspace());
    store.with_key_write(converted.get_value(), [&](const barch::shard_ptr& t) {
        auto n = t->search(converted.get_value());
        if (n.is_leaf) {
            auto cl = n.const_leaf();
            auto ov = cl->get_value();
            if (cl->is_compressed()) {
                auto d = dictionary::decompress(ov);
                previous.assign(d.chars(), d.size);
            } else {
                previous.assign(ov.chars(), ov.size);
            }
            had = true;
        }
        t->insert(opts, converted.get_value(), v, true, fc);
    });
    if (!had) {
        return call.push_null();
    }
    return call.push_vt(art::value_type{previous});
}

/* STRLEN <key>
 *
 * The length of the value in bytes. Not quite an alias for LENGTH: LENGTH answers nil for
 * a key that is not there and this answers 0, which is what redis does and what callers
 * that compare the reply to a number rely on.
 */
int STRLEN(caller& call, const arg_t& argv) {
    ++statistics::get_ops;
    if (argv.size() != 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto converted = call.kspace()->encode_key(k);
    barch::sharded_store store(call.kspace());
    if (wrong_type_here(store, k)) {
        return call.push_error(barch::wrong_type_message());
    }
    long long length = 0;
    store.search(converted.get_value(), [&](const art::node_ptr& n) {
        auto cl = n.const_leaf();
        auto vt = cl->get_value();
        if (cl->is_compressed()) {
            vt = dictionary::decompress(vt);
        }
        length = (long long) vt.size;
    });
    return call.push_ll(length);
}

/* MSETNX <key> <value> [key value ...]
 *
 * Write every pair, but only if not one of the keys is already there - all of them or
 * none. That is the whole point of the command and it is why this holds a write lock on
 * every shard while it works, rather than one lock per key the way MSET does. MSET has
 * never been atomic across keys and says so; this one cannot afford not to be.
 *
 * The cost is real: it stops writes to the whole key space for the duration. It is only
 * defensible because MSETNX is a small, occasional call - a caller reaching for it with a
 * thousand pairs is using the wrong command.
 */
int MSETNX(caller& call, const arg_t& argv) {
    ++statistics::set_ops;
    if (argv.size() < 3 || (argv.size() % 2) == 0)
        return call.wrong_arity();
    barch::sharded_store store(call.kspace());
    bool any_present = false;
    // one pass to look, one to write, both under the same set of locks
    store.each_shard_write([&](const barch::shard_ptr& t) {
        if (any_present) return;
        for (size_t n = 1; n < argv.size(); n += 2) {
            auto k = argv[n];
            if (key_ok(k) != 0) continue;
            auto converted = call.kspace()->encode_key(k);
            if (store.shard_for(converted.get_value()) != t) continue;
            if (!t->search(converted.get_value()).null()) {
                any_present = true;
                return;
            }
        }
    });
    if (any_present) {
        return call.push_ll(0);
    }
    auto fc = [&](const art::node_ptr &) -> void {};
    store.each_shard_write([&](const barch::shard_ptr& t) {
        for (size_t n = 1; n < argv.size(); n += 2) {
            auto k = argv[n];
            if (key_ok(k) != 0) continue;
            auto converted = call.kspace()->encode_key(k);
            if (store.shard_for(converted.get_value()) != t) continue;
            art::key_options opts;
            t->insert(opts, converted.get_value(), argv[n + 1], true, fc);
        }
    });
    return call.push_ll(1);
}

/* RANDOMKEY
 *
 * Some key, or nil when the space is empty.
 *
 * "Some" is doing work in that sentence. This picks a shard at random from the ones that
 * hold anything, then walks a bounded number of steps into it, so the answer varies but
 * is not uniform over the key space - keys near the start of a shard come up more often,
 * and a shard with ten keys is as likely as one with ten thousand. redis's own RANDOMKEY
 * is approximate too, for its own reasons, so this is a difference of shape rather than
 * of kind. Making it uniform means knowing each shard's size and sampling in proportion,
 * which is a walk this command does not otherwise need.
 */
int RANDOMKEY(caller& call, const arg_t& argv) {
    ++statistics::get_ops;
    if (argv.size() != 1)
        return call.wrong_arity();
    barch::sharded_store store(call.kspace());
    heap::std_vector<barch::shard_ptr> holding;
    store.each_shard_read([&](const barch::shard_ptr& t) {
        if (t->get_size() > 0) holding.push_back(t);
    });
    if (holding.empty()) {
        return call.push_null();
    }
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    auto& picked = holding[rng() % holding.size()];
    // how far to walk into it. Bounded so a large shard does not turn one call into a
    // long iteration; the bias that introduces is described above
    enum { max_steps = 64 };
    size_t steps = (size_t) (rng() % std::min<uint64_t>(picked->get_size(), max_steps));
    int reply = call.ok();
    // seeded from the shard's own minimum. art::iterator's one argument form finds the
    // minimum but never fills its trace, so it walks nothing - TODO 31 - and an empty
    // value_type as the second argument does the same, which is what made an earlier
    // version of this answer with the global minimum every single time
    auto first = picked->tree_minimum();
    if (!first.is_leaf) {
        return call.push_null();   // emptied between the count above and here
    }
    art::iterator it(picked, first.const_leaf()->get_key());
    art::value_type found = it.ok() ? it.key() : art::value_type{};
    for (size_t i = 0; i < steps && it.ok(); ++i) {
        it.next();
        if (it.ok()) {
            found = it.key();      // keep the last good one; a short shard just stops early
        }
    }
    if (found.empty()) {
        return call.push_null();
    }
    reply = visible_key(call, found) ? call.push_encoded_key(found) : call.push_null();
    return reply;
}

/**
 * Move or copy the value at one key to another, under both keys' locks.
 *
 * The lock order is sharded_store's, which is by shard number - see keyspace_locks.h for
 * why the caller does not get to choose it. `replace` says whether an existing
 * destination may be overwritten; `keep_source` separates COPY from RENAME.
 *
 * Answers: 1 moved or copied, 0 the destination was in the way, -1 no such source.
 */
static int move_value(barch::sharded_store& store, art::value_type from,
                      art::value_type to, bool replace, bool keep_source) {
    int result = -1;
    store.with_two_keys_write(from, to, [&](const barch::shard_ptr& sf,
                                            const barch::shard_ptr& st) {
        auto n = sf->search(from);
        if (!n.is_leaf) {
            result = -1;
            return;
        }
        if (!replace && !st->search(to).null()) {
            result = 0;
            return;
        }
        auto cl = n.const_leaf();
        art::key_options opts = cl->options();
        // the bytes are copied out before anything is written, because writing to the
        // destination can move or free the leaf being read when both are on one shard
        std::string held(cl->get_value().chars(), cl->get_value().size);
        auto fc = [&](const art::node_ptr &) -> void {};
        st->insert(opts, to, art::value_type{held}, true, fc);
        if (!keep_source) {
            sf->remove(from);
        }
        result = 1;
    });
    return result;
}

/* RENAME <key> <newkey>
 *
 * The value moves and the old name is gone. An existing destination is overwritten, and a
 * source that is not there is an error rather than a quiet nothing - which is the one
 * place RENAME differs from the rest of the key commands, and it is redis's choice.
 */
int RENAME(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto from = argv[1];
    auto to = argv[2];
    if (key_ok(from) != 0) return call.key_check_error(from);
    if (key_ok(to) != 0) return call.key_check_error(to);
    auto cf = call.kspace()->encode_key(from);
    auto ct = call.kspace()->encode_key(to);
    barch::sharded_store store(call.kspace());
    int r = move_value(store, cf.get_value(), ct.get_value(), true, false);
    if (r < 0) {
        return call.push_error("no such key");
    }
    return call.push_simple("OK");
}

/* RENAMENX <key> <newkey>
 *
 * As RENAME, but it will not overwrite. 1 when the name was taken, 0 when the destination
 * was already there. A missing source is still an error.
 */
int RENAMENX(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto from = argv[1];
    auto to = argv[2];
    if (key_ok(from) != 0) return call.key_check_error(from);
    if (key_ok(to) != 0) return call.key_check_error(to);
    auto cf = call.kspace()->encode_key(from);
    auto ct = call.kspace()->encode_key(to);
    barch::sharded_store store(call.kspace());
    int r = move_value(store, cf.get_value(), ct.get_value(), false, false);
    if (r < 0) {
        return call.push_error("no such key");
    }
    return call.push_ll(r);
}

/* COPY <source> <destination> [DB <n>] [REPLACE]
 *
 * The value is written under the second name and stays under the first. 1 when it was
 * copied, 0 when the destination existed and REPLACE was not given. A source that is not
 * there answers 0 rather than an error, unlike RENAME.
 *
 * DB selects the destination key space by number, the same mapping SELECT uses, so it
 * honours `db_number_prefix`. Copying into another space cannot use the two key shard
 * helper - the keys are in different spaces, not different shards - so it takes the two
 * spaces through ks_two instead, which is the same rule one level up.
 */
int COPY(caller& call, const arg_t& argv) {
    if (argv.size() < 3)
        return call.wrong_arity();
    auto from = argv[1];
    auto to = argv[2];
    if (key_ok(from) != 0) return call.key_check_error(from);
    if (key_ok(to) != 0) return call.key_check_error(to);
    bool replace = false;
    long long db = -1;
    for (size_t i = 3; i < argv.size(); ++i) {
        auto opt = argv[i].to_string();
        for (auto& ch : opt) ch = (char) toupper((unsigned char) ch);
        if (opt == "REPLACE") {
            replace = true;
        } else if (opt == "DB") {
            if (i + 1 >= argv.size()) return call.syntax_error();
            if (!conversion::to_ll(argv[++i], db)) {
                return call.push_error("value is not an integer or out of range");
            }
            if (db < 0) return call.push_error("DB index is out of range");
        } else {
            return call.syntax_error();
        }
    }
    auto cf = call.kspace()->encode_key(from);
    auto ct = call.kspace()->encode_key(to);

    if (db < 0) {
        barch::sharded_store store(call.kspace());
        int r = move_value(store, cf.get_value(), ct.get_value(), replace, true);
        return call.push_ll(r < 0 ? 0 : r);
    }

    auto here = call.kspace();
    auto there = barch::get_keyspace(db == 0 ? "" : barch::get_db_number_prefix() + std::to_string(db));
    if (here == there) {
        barch::sharded_store store(here);
        int r = move_value(store, cf.get_value(), ct.get_value(), replace, true);
        return call.push_ll(r < 0 ? 0 : r);
    }
    ks_two held(here, ks_mode::shared, there, ks_mode::unique);
    barch::sharded_store src(here);
    barch::sharded_store dst(there);
    // the shards are addressed directly. ks_two is already holding every shard lock in
    // both spaces, and sharded_store's own search and insert take a shard lock of their
    // own - asking for one that is already held waits for a lock this thread will never
    // release. shard_for only routes, it does not lock, which is what makes it usable here
    auto sf = src.shard_for(cf.get_value());
    auto st = dst.shard_for(ct.get_value());
    if (!sf || !st) {
        return call.push_ll(0);
    }
    int result = 0;
    auto n = sf->search(cf.get_value());
    if (n.is_leaf) {
        if (replace || st->search(ct.get_value()).null()) {
            auto cl = n.const_leaf();
            art::key_options opts = cl->options();
            std::string value(cl->get_value().chars(), cl->get_value().size);
            auto fc = [&](const art::node_ptr &) -> void {};
            st->insert(opts, ct.get_value(), art::value_type{value}, true, fc);
            result = 1;
        }
    }
    return call.push_ll(result);
}

/* MOVE <key> <db>
 *
 * The value moves to another database, and is gone from this one. 1 when it moved, 0 when
 * the key was not here or was already there - redis will not overwrite with MOVE, and
 * says nothing about which of the two reasons it was.
 */
int MOVE(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0) return call.key_check_error(k);
    long long db = 0;
    if (!conversion::to_ll(argv[2], db)) {
        return call.push_error("value is not an integer or out of range");
    }
    if (db < 0) {
        return call.push_error("DB index is out of range");
    }
    auto here = call.kspace();
    auto there = barch::get_keyspace(db == 0 ? "" : barch::get_db_number_prefix() + std::to_string(db));
    if (here == there) {
        return call.push_error("source and destination objects are the same");
    }
    auto converted = call.kspace()->encode_key(k);
    ks_two held(here, ks_mode::unique, there, ks_mode::unique);
    barch::sharded_store src(here);
    barch::sharded_store dst(there);
    // direct on the shards, for the reason COPY gives above
    auto sf = src.shard_for(converted.get_value());
    auto st = dst.shard_for(converted.get_value());
    if (!sf || !st) {
        return call.push_ll(0);
    }
    auto n = sf->search(converted.get_value());
    if (!n.is_leaf || !st->search(converted.get_value()).null()) {
        return call.push_ll(0);
    }
    auto cl = n.const_leaf();
    art::key_options opts = cl->options();
    std::string value(cl->get_value().chars(), cl->get_value().size);
    auto fc = [&](const art::node_ptr &) -> void {};
    st->insert(opts, converted.get_value(), art::value_type{value}, true, fc);
    sf->remove(converted.get_value());
    return call.push_ll(1);
}

/* INCRBYFLOAT <key> <increment>
 *
 * Add a floating point amount to a key, creating it at that amount when it is absent.
 * The reply is a bulk string rather than a double, and redis trims it: 3.0 comes back as
 * "3", not "3.0", because the value is stored as text and read back the same way.
 *
 * A value that is not a number is refused and left alone, as the integer forms have done
 * since DONE 37.
 */
int INCRBYFLOAT(caller& call, const arg_t& argv) {
    ++statistics::incr_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    double by = 0;
    if (!conversion::to_double(argv[2], by)) {
        // "+inf" and "nan" parse as numbers for some readers and not others, so the
        // text is checked directly - redis distinguishes "not a float" from "a float we
        // will not add", and the tests match on the second message
        std::string given = argv[2].to_string();
        for (auto& ch : given) ch = (char) tolower((unsigned char) ch);
        if (given.find("inf") != std::string::npos || given.find("nan") != std::string::npos) {
            return call.push_error("increment would produce NaN or Infinity");
        }
        return call.push_error("value is not a valid float");
    }
    if (std::isnan(by) || std::isinf(by)) {
        return call.push_error("increment would produce NaN or Infinity");
    }
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto converted = call.kspace()->encode_key(k);

    barch::sharded_store store(call.kspace());
    if (wrong_type_here(store, k)) {
        return call.push_error(barch::wrong_type_message());
    }
    double l = 0;
    bool present = false;
    numeric_status why = numeric_status::updated;
    auto updater = [&](const art::node_ptr &value) -> art::node_ptr {
        if (value.null()) {
            return nullptr;
        }
        present = true;
        return leaf_numeric_update(l, value, by, why);
    };
    int r = -1;
    store.with_key_write(converted.get_value(), [&](const barch::shard_ptr& t) {
        if (t->update(converted.get_value(), updater)) {
            r = 0;
            return;
        }
        if (present) {
            return;   // there, but not a number - leave it where it is
        }
        l = by;
        std::string held = numeric_to_text(l);
        auto fc = [&](const art::node_ptr &) -> void {};
        t->opt_insert({}, converted.get_value(), art::value_type{held}, true, fc);
        r = 0;
    });
    if (r != 0) {
        return call.push_error("value is not a valid float");
    }
    if (std::isnan(l) || std::isinf(l)) {
        return call.push_error("increment would produce NaN or Infinity");
    }
    // as text, the way redis renders it: seventeen significant digits, and a whole
    // number without a fraction, so 1.5 + 1.5 is "3" rather than "3.0". Clients compare
    // this reply as a string
    std::string text = numeric_to_text(l);
    return call.push_vt(art::value_type{text});
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
    if (argv.size() != 2)
        return call.wrong_arity();
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

    // a value that will not parse is not a wrong argument *count* - this answered with an
    // arity error, which tells the caller to look at the wrong thing entirely
    if (!conversion::to_i64(argv[2], by)) {
        return call.push_error("value is not an integer or out of range");
    }

    return BarchModifyInteger(call,argv, -by);
}
int UDECRBY(caller& call, const arg_t& argv) {
    ++statistics::incr_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    uint64_t by = 0;

    if (!conversion::to_ui64(argv[2], by)) {
        return call.push_error("value is not an integer or out of range");
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
    // every pair is measured before any is written. MSET answers OK or an error, with no
    // room in that reply to say "most of them" - so a pair that cannot fit stops the whole
    // command rather than leaving the caller with a success and a gap
    for (size_t n = 1; n + 1 < argv.size(); n += 2) {
        if (key_ok(argv[n]) == 0
            && !fits_in_leaf(call.kspace()->encode_key(argv[n]).get_value().size, argv[n + 1].size)) {
            return call.push_error(too_large_message());
        }
    }
    for (size_t n = 1; n < argv.size(); n += 2) {
        auto k = argv[n];
        auto v = argv[n + 1];

        if (key_ok(k) != 0) {
            r |= call.push_null();
            continue;
        }
        // MSET replaces whatever the name held, collections included, which is what redis
        // does - it answers OK and the list is gone. See DONE 126
        if (barch::kind_of_container(store, k) != barch::container_kind::none) {
            barch::remove_container(store, k);
        }

        auto converted = call.kspace()->encode_key(k);
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
    auto converted = call.kspace()->encode_key(k);

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
    auto converted = call.kspace()->encode_key(k);
    barch::sharded_store store(call.kspace());
    int r = call.ok();
    auto key = converted.get_value();
    // Search first. A live string is the common case, and kind_of used to lock
    // and look the same key up again before we got here. A collection lives
    // under a different prefix and may be on another shard, so that probe
    // only runs on a miss - see TODO 59
    bool found = store.search(key, [&](const art::node_ptr& n) {
        auto cl = n.const_leaf();
        auto vt = cl->get_value();
        if (cl->is_compressed()) {
            vt = dictionary::decompress(vt);
        }
        r = call.push_bulk(vt);
    });
    if (found) return r;
    if (barch::kind_of_container(store, k) != barch::container_kind::none) {
        return call.push_error(barch::wrong_type_message());
    }
    if (!call.kspace()->has_foreign())
        return call.push_null();
    if (call.is_collecting_exec())
        return call.push_error("FOREIGN GET inside MULTI is not supported");
    art::node_ptr leaf;
    store.with_key_read(key, [&](const barch::shard_ptr& t) {
        leaf = t->local_leaf(key);
    });
    if (!leaf.null() && leaf.cl()->is_tomb() && !leaf.cl()->expired()) {
        ++statistics::foreign_misses;
        return call.push_null();
    }
    return barch::foreign::point_get(call, key);
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
    // as with KEYS, a container is reported by its name rather than once per entry. The
    // set only spans this call: SCAN promises that everything present throughout the
    // iteration is reported at least once, and explicitly allows repeats, so a container
    // whose entries straddle two calls may be named in both - see TODO 59
    std::unordered_set<std::string> named;
    bool complete = store.scan(*cursor, spec, [&](art::value_type key) -> bool {
        std::string name = encoded_container_name(key);
        if (!name.empty()) {
            if (!named.emplace(name).second) {
                return call.results_count() < spec.count;
            }
            call.push_vt(art::value_type{name});
        } else if (key.size && art::is_container_lead(*key.bytes)) {
            return call.results_count() < spec.count;   // the member index
        } else {
            if (!visible_key(call, key))
                return call.results_count() < spec.count;
            call.push_encoded_key(key); // it throws so it's ok
        }
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
    auto converted = call.kspace()->encode_key(k);
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

/**
 * TTL, PTTL, EXPIRETIME and PEXPIRETIME are one read with four ways of reporting it.
 *
 * The two negatives follow redis: -1 is present with no expiry, -2 is no such key.
 *
 * not store.search: that treats a tombstone as absent, and TTL reports one as present
 * with no expiry.
 *
 * The deadline forms answer the stored number directly. They used to rebuild it from the
 * time remaining, because expiry was measured against a clock that started when the
 * machine did and the stored number was not a unix time; since DONE 55 it is one.
 *
 * Both second forms round to the nearest second rather than truncating, which is what
 * redis does - `addReplyLongLong(c, output_ms ? ttl : ((ttl+500)/1000))`. PX 1600 then
 * TTL answers 2, not 1, and a deadline at x.6 seconds reports x+1. Truncating made TTL
 * one low as soon as any time at all had passed, which is why the valkey case that sets
 * a timeout twice kept flapping. See DONE 113.
 */
enum class ttl_report { seconds, millis, deadline_seconds, deadline_millis };

static int ttl_query(caller& call, const arg_t& argv, ttl_report form) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto converted = call.kspace()->encode_key(k);
    barch::sharded_store store(call.kspace());
    int reply = call.ok();
    bool answered = false;
    store.with_key_read(converted.get_value(), [&](const barch::shard_ptr& t) {
        art::node_ptr r = t->search(converted.get_value());
        if (r.null()) {
            return;
        }
        answered = true;
        auto l = r.const_leaf();
        if (!l->is_expiry()) {
            reply = call.push_ll(-1);
            return;
        }
        long long left = l->expiry_ms() - art::now();
        // an overdue key that is still here answers 0, not a negative - redis clamps
        // before it reports, and without this a key one second past its deadline would
        // answer -1, which means something else entirely
        if (left < 0) left = 0;
        switch (form) {
            case ttl_report::seconds:
                reply = call.push_ll((left + 500) / 1000);
                break;
            case ttl_report::millis:
                reply = call.push_ll(left);
                break;
            case ttl_report::deadline_seconds:
                reply = call.push_ll((l->expiry_ms() + 500) / 1000);
                break;
            case ttl_report::deadline_millis:
                reply = call.push_ll(l->expiry_ms());
                break;
        }
    });
    return answered ? reply : call.push_ll(-2);
}

int TTL(caller& call, const arg_t& argv) {
    return ttl_query(call, argv, ttl_report::seconds);
}
int PTTL(caller& call, const arg_t& argv) {
    return ttl_query(call, argv, ttl_report::millis);
}
int cmd_PTTL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PTTL);
}
int EXPIRETIME(caller& call, const arg_t& argv) {
    return ttl_query(call, argv, ttl_report::deadline_seconds);
}
int cmd_EXPIRETIME(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, EXPIRETIME);
}
int PEXPIRETIME(caller& call, const arg_t& argv) {
    return ttl_query(call, argv, ttl_report::deadline_millis);
}
int cmd_PEXPIRETIME(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PEXPIRETIME);
}

/**
 * PERSIST - the key keeps its value and loses its deadline.
 *
 * Answers 1 when there was an expiry to remove and 0 when the key is missing or had none,
 * which is the distinction redis makes and the reason it is not simply a write.
 */
int PERSIST(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto converted = call.kspace()->encode_key(k);
    barch::sharded_store store(call.kspace());
    bool removed = false;
    store.with_key_write(converted.get_value(), [&](const barch::shard_ptr& t) {
        auto n = t->search(converted.get_value());
        if (!n.is_leaf) return;
        auto l = n.const_leaf();
        if (!l->is_expiry()) return;
        auto updater = [&t](const art::node_ptr &leaf) -> art::node_ptr {
            if (leaf.null()) return leaf;
            auto ol = leaf.const_leaf();
            return art::make_leaf(t->get_ap(), ol->get_key(), ol->get_value(),
                                  0, ol->is_volatile(), ol->is_compressed());
        };
        removed = t->update(converted.get_value(), updater);
    });
    return call.push_ll(removed ? 1 : 0);
}
int cmd_PERSIST(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PERSIST);
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
        // a name holding a collection has no plain key, so looking only for that answered
        // 0 for a hash - and EXISTS is the command redis expects a caller to ask with
        auto converted = call.kspace()->encode_key(k);
        auto key = converted.get_value();
        if (store.exists(key)
            || barch::kind_of_container(store, k) != barch::container_kind::none) {
            ++found;
            continue;
        }
        if (argv.size() == 2 && call.kspace()->has_foreign()) {
            if (call.is_collecting_exec())
                return call.push_error("FOREIGN EXISTS inside MULTI is not supported");
            art::node_ptr leaf;
            store.with_key_read(key, [&](const barch::shard_ptr& t) {
                leaf = t->local_leaf(key);
            });
            if (!leaf.null() && leaf.cl()->is_tomb() && !leaf.cl()->expired()) {
                ++statistics::foreign_misses;
                return call.push_ll(0);
            }
            return barch::foreign::point_exists(call, key);
        }
    }
    if (argv.size() > 2 && call.kspace()->has_foreign()) {
        if (call.is_collecting_exec())
            return call.push_error("FOREIGN EXISTS inside MULTI is not supported");
        return barch::foreign::exists_many(call, argv);
    }
    return call.push_ll(found);
}
int cmd_EXISTS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, EXISTS);
}

/**
 * EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT differ only in the units the caller writes and
 * whether the number is a duration or a moment. Everything after that - the NX, XX, GT and
 * LT conditions, the deadline check, the rewrite of the leaf - is the same, so it is
 * written once and the two axes are passed in.
 */
static int expire_command(caller& call, const arg_t& argv, bool millis, bool absolute,
                          const char *named) {
    if (argv.size() < 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto converted = call.kspace()->encode_key(k);
    barch::sharded_store store(call.kspace());
    int reply = call.ok();
    bool answered = false;
    // read, decide, then write: has to hold one lock across all three
    store.with_key_write(converted.get_value(), [&](const barch::shard_ptr& t) {
        art::node_ptr r = t->search(converted.get_value());
        // redis answers 0 or 1. A missing key is 0, not the -1 TTL uses for "no expire"
        if (r.null()) {
            answered = true;
            reply = call.push_ll(0);
            return;
        }
        art::key_expire_spec spec(argv);
        spec.millis = millis;
        spec.absolute = absolute;
        if (spec.parse_options() != call.ok()) {
            answered = true;
            reply = spec.bad_expire
                        ? call.push_error(("invalid expire time in '"
                                           + std::string(named) + "' command").c_str())
                        : (!spec.reason.empty() ? call.push_error(spec.reason.c_str())
                                                : call.syntax_error());
            return;
        }

        auto l = r.const_leaf();
        auto refuse = [&]() {
            answered = true;
            reply = call.push_ll(0);
        };
        // a deadline that has already passed removes the key, which is what redis does
        // with EXPIRE k -1 and with any EXPIREAT in the past
        if (spec.ttl <= art::now()) {
            answered = true;
            reply = t->remove(converted.get_value()) ? call.push_ll(1) : call.push_ll(0);
            return;
        }
        if (spec.nx) {
            if (l->is_expiry()) { refuse(); return; }
        } else if (spec.xx) {
            if (!l->is_expiry()) { refuse(); return; }
        } else if (spec.gt) {
            // no expire is an infinite TTL, so GT never applies and LT always does
            if (!l->is_expiry() || spec.ttl <= l->expiry_ms()) { refuse(); return; }
        } else if (spec.lt) {
            if (l->is_expiry() && spec.ttl >= l->expiry_ms()) { refuse(); return; }
        }
        auto updater = [&t,spec](const art::node_ptr &leaf) -> art::node_ptr {
            if (leaf.null()) {
                return leaf;
            }
            auto l = leaf.const_leaf();
            return art::make_leaf(t->get_ap(), l->get_key(),
                l->get_value(),
                spec.ttl, l->is_volatile(),
                l->is_compressed());
        };
        answered = true;
        reply = t->update(l->get_key(), updater) ? call.push_ll(1) : call.push_ll(0);
    });
    return answered ? reply : call.push_ll(0);
}

int EXPIRE(caller& call, const arg_t& argv) {
    return expire_command(call, argv, false, false, "expire");
}
int PEXPIRE(caller& call, const arg_t& argv) {
    return expire_command(call, argv, true, false, "pexpire");
}
int EXPIREAT(caller& call, const arg_t& argv) {
    return expire_command(call, argv, false, true, "expireat");
}
int PEXPIREAT(caller& call, const arg_t& argv) {
    return expire_command(call, argv, true, true, "pexpireat");
}
int cmd_PEXPIRE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PEXPIRE);
}
int cmd_EXPIREAT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, EXPIREAT);
}
int cmd_PEXPIREAT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PEXPIREAT);
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
    if (call.kspace()->has_foreign()) {
        if (call.is_collecting_exec())
            return call.push_error("FOREIGN MGET inside MULTI is not supported");
        return barch::foreign::mget(call, argv);
    }
    int responses = 0;
    barch::sharded_store store(call.kspace());
    call.start_array();
    for (size_t arg = 1; arg < argv.size(); ++arg) {
        auto k = argv[arg];
        if (key_ok(k) != 0) {
            call.push_null();
        } else {
            auto converted = call.kspace()->encode_key(k);
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
    if (!store.minimum([&](art::value_type k) {
            ok = visible_key(call, k) ? call.push_encoded_key(k) : call.push_null();
        })) {
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
    // a user who may not see functions gets the largest key below the range rather
    // than a null: the range is the top of the key order, so `maximum` always lands
    // in it when the space holds one. See TODO 98 F4
    static const uint8_t fn_start[] = {art::tfunction, 0x00};
    const bool hide = !visible_key(call, art::value_type{fn_start, sizeof fn_start});
    bool any = hide
        ? store.maximum_below(art::value_type{fn_start, sizeof fn_start},
                              [&](art::value_type k) { ok = call.push_encoded_key(k); })
        : store.maximum([&](art::value_type k) { ok = call.push_encoded_key(k); });
    if (!any) {
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

    auto converted = call.kspace()->encode_key(k);
    barch::sharded_store store(call.kspace());
    int ok = call.ok();
    if (!store.lower_bound(converted.get_value(), [&](art::value_type f) {
            ok = visible_key(call, f) ? call.push_encoded_key(f) : call.push_null();
        })) {
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

    auto converted = call.kspace()->encode_key(k);
    barch::sharded_store store(call.kspace());
    int ok = call.ok();
    if (!store.upper_bound(converted.get_value(), [&](art::value_type f) {
            ok = visible_key(call, f) ? call.push_encoded_key(f) : call.push_null();
        })) {
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

    auto converted = call.kspace()->encode_key(k);
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
        auto converted = call.kspace()->encode_key(k);
        bool gone = false;
        auto fc = [&gone](art::node_ptr n) -> void {
            if (!n.null()) gone = true;
        };
        store.remove(converted.get_value(), fc);
        // and whatever the name held as a list, hash or ordered set. Without this a
        // deleted list stayed behind under its own keys, and the name kept answering as
        // a container - which made GETRANGE on it report a wrong type after a DEL
        if (barch::remove_container(store, k) > 0) {
            gone = true;
        }
        if (gone) ++removed;
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

    if (ValkeyModule_CreateCommand(ctx, NAME(PTTL), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(PEXPIRE), "write", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(EXPIREAT), "write", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(PEXPIREAT), "write", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(EXPIRETIME), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(PEXPIRETIME), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(PERSIST), "write", 1, 1, 0) == VALKEYMODULE_ERR)
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
    r["SETRANGE"] = {::SETRANGE,{"write","keys","data"}};
    r["GETRANGE"] = {::GETRANGE,{"read","keys","data"}};
    // SUBSTR is the name GETRANGE had before redis 2.0 and is still accepted
    r["SUBSTR"] = {::GETRANGE,{"read","keys","data"}};
    r["GETDEL"] = {::GETDEL,{"write","keys","data"}};
    r["GETEX"] = {::GETEX,{"write","keys","data"}};
    r["SETEX"] = {::SETEX,{"write","keys","data"}};
    r["PSETEX"] = {::PSETEX,{"write","keys","data"}};
    r["LCS"] = {::LCS,{"read","keys","data"}};
    r["SETNX"] = {::SETNX,{"write","keys","data"}};
    r["GETSET"] = {::GETSET,{"write","keys","data"}};
    r["STRLEN"] = {::STRLEN,{"read","keys","data"}};
    r["MSETNX"] = {::MSETNX,{"write","keys","data"}};
    r["RANDOMKEY"] = {::RANDOMKEY,{"read","keys","data"}};
    r["RENAME"] = {::RENAME,{"write","keys","data"}};
    r["RENAMENX"] = {::RENAMENX,{"write","keys","data"}};
    r["COPY"] = {::COPY,{"write","keys","data"}};
    r["MOVE"] = {::MOVE,{"write","keys","data"}};
    r["INCRBYFLOAT"] = {::INCRBYFLOAT,{"write","keys","data"}};
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
    // park via has_blocks, like GET. is_asynch would copy the caller and
    // take EXEC off the MULTI that queued it.
    r["EXISTS"] = {::EXISTS,{"read","keys","data"}};
    r["EXPIRE"] = {::EXPIRE,{"write","keys","data"}};
    r["PEXPIRE"] = {::PEXPIRE,{"write","keys","data"}};
    r["EXPIREAT"] = {::EXPIREAT,{"write","keys","data"}};
    r["PEXPIREAT"] = {::PEXPIREAT,{"write","keys","data"}};
    r["MSET"] = {::MSET,{"write","keys","data"}};
    r["ADD"] = {::ADD,{"write","keys","data"}};
    r["GET"] = {::GET,{"read","keys","data"}};
    r["FOREIGN"] = {barch::foreign::FAKE,{"write","keys","data"}};
    r["FOREIGN_MISS"] = {barch::foreign::MISS,{"write","keys","data"}};
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
    r["PTTL"] = {::PTTL,{"read","keys","data"}};
    r["EXPIRETIME"] = {::EXPIRETIME,{"read","keys","data"}};
    r["PEXPIRETIME"] = {::PEXPIRETIME,{"read","keys","data"}};
    r["PERSIST"] = {::PERSIST,{"write","keys","data"}};
}
