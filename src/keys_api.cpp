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
#include "logger.h"
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
    auto converted = conversion::as_composite(k);
    auto t = call.kspace()->get(converted.get_value());
    storage_release release(t);

    art::key_spec spec;
    if (key_ok(k) != 0)
        return call.key_check_error(k);


    int r = -1;
    IntT l = IntT();
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
        l += by;
        Variable s = l;
        auto fc = [&](const art::node_ptr &) -> void {};
        t->opt_insert({},converted.get_value(), {s.s()}, true, fc);
        r = 0;
    }
    if (r == 0) {
        return call.push_int(l);
    } else {
        return call.push_error("invalid integer or overflow");
    }
}

/**
 * create a merged iterator from the shard and it's sources
 * @param shard abstract_shard that has the dependencies
 * @param lower the lower bound to start the iterators at
 * @return a merge iterator with 1 or more iterators
 */
art::merge_iterator make_merged(const barch::shard_ptr& shard, art::value_type lower) {
    auto s  = shard->sources();

    art::merge_iterator m({art::iterator(shard,lower),art::iterator(s,lower)});

    return m;
}
extern "C" {
/* B.RANGE <startkey> <endkey> <count>
*
* Return a list of matching keys, lexicographically between startkey
* and endkey. No more than 'count' items are emitted. */

int RANGE(caller& call, const arg_t& argv) {

    int r = 0;
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
    /* Reply with the matching items. */

    auto ks = call.kspace();
    ks_shared kss(ks->source());
    ks_shared ksl(ks);
    auto collect = [&]() -> heap::std_vector<art::value_type>{
        int64_t striation_counter = 0;
        heap::std_vector<art::value_type> usorted;
        heap::vector<art::merge_iterator> iters;
        heap::unordered_set<size_t> active; // set must be ordered
        for (const auto& shard : ks->get_shards()) {
            // iterate the 'striations'
            auto t = shard;
            auto i = make_merged(t,c1.get_value());
            if (i.ok()) {
                active.insert(t->get_shard_number());
            }
            iters.push_back(i);
        }
        art::value_type list_max;
        while (!active.empty()) {
            bool has_first = false; // key in striation
            for (auto shard : active) {
                auto& i = iters[shard];
                if (i.current().cl()->is_tomb()) {
                    if (!i.next()) {
                        active.erase(shard);
                    }
                }else {
                    auto k = i.key();
                    if (k >= c1.get_value() && k < c2.get_value()) {

                        if (k > list_max) {
                            if (!has_first) {
                                // this may or may not increment striation counter - it's an optimization to get correct results quicker
                                striation_counter = std::max<int64_t>(usorted.size(), striation_counter);
                                has_first = true;
                            }
                            list_max = k;
                        }
                        usorted.push_back(k);
                        if (!i.next()) {
                            active.erase(shard);
                        }
                    }else {
                        active.erase(shard);
                    }
                }
            }

            if (count > 0 && striation_counter >= count) {
                // we can return early because we are certain the list contains the globally first count entries
                // although the list is at most count*shard_count large
                break;
            }
            ++striation_counter;
        }
        return usorted;
    };
    auto sorted = collect();
    std::sort(sorted.begin(), sorted.end()); // sort must happen inside the lock
    call.start_array();
    for (auto&k : sorted) {
        call.push_encoded_key(k);// TODO: replace this with streaming api to reduce memory
        if (--count == 0) break;
    }
    call.end_array();

    /* Cleanup. */
    return r;
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
    long long count = 0;
    auto c1 = conversion::as_composite(k1);
    auto c2 = conversion::as_composite(k2);
    auto space_cnt = [c1,c2](const barch::key_space_ptr& spce, size_t shard)-> long long {
        if (!spce) return 0;
        long long count = 0;
        auto t = spce->get(shard);
        read_lock release(t);

        art::iterator i(t,c1.get_value());
        art::iterator j(t,c2.get_value());
        if (i.ok() && !j.ok()) {
            j.last(); // last key in the range
            ++count;
        }
        if (i.ok() && j.ok()) {
            auto c = i.fast_distance(j) ;
            count += c; // they're all unique ?
        }
        return count;
    };
    auto s = call.kspace();
    for (const auto& shard : s->get_shards()) {
        count += space_cnt(s, shard->get_shard_number());
        count += space_cnt(s->source(), shard->get_shard_number());
    }
    return call.push_int(count);
}
int cmd_COUNT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {

    vk_caller caller;

    return caller.vk_call(ctx, argv, argc, ::COUNT);
}

/* B.KEYS
*
* match against all keys using a glob pattern
* */
int KEYS(caller& call, const arg_t& argv) {

    if (argv.size() < 2 || argv.size() > 4)
        return call.wrong_arity();

    art::keys_spec spec(argv);
    if (spec.parse_keys_options() != call.ok()) {
        return call.wrong_arity();
    }
    std::mutex vklock{};
    std::atomic<int64_t> replies = 0;
    auto cpat = argv[1];
    art::value_type pattern = cpat;
    // this operation does not need to lock since pages are copied onto a working page
    // before glob is processed (leaves are only valid within the callback since memory is reused)
    if (spec.count) {
        auto ks = call.kspace();
        for (const auto& shard : ks->get_shards()) {
            shard->glob(spec, pattern, false, [&](const art::leaf & unused(l)) -> bool {
                ++replies;
                return true;
            });
        }
        return call.push_ll(replies);
    } else {
        /* Reply with the matching items. */
        call.start_array();
        auto ks = call.kspace();
        for (const auto& shard : ks->get_shards()) {
            shard->glob(spec, pattern, false, [&](const art::leaf &l) -> bool {
                std::lock_guard lk(vklock); // because there's worker threads concurrently calling here
                if (0 != call.push_encoded_key(l.get_key())) {
                    return false;
                };
                ++replies;
                return true;
            });
        }
        call.end_array();
    }
    return call.ok();
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

    if (argv.size() < 2 || argv.size() > 4)
        return call.wrong_arity();

    art::keys_spec spec(argv);
    if (spec.parse_keys_options() != call.ok()) {
        return call.wrong_arity();
    }
    std::mutex vklock{};
    std::atomic<int64_t> replies = 0;
    auto cpat = argv[1];
    art::value_type pattern = cpat;
    if (spec.count) {
        auto ks = call.kspace();
        for (const auto& shard : ks->get_shards()) {
            shard->glob(spec, pattern, true, [&](const art::leaf & unused(l)) -> bool {
                ++replies;
                return true;
            });
        }
        return call.push_ll(replies);
    } else {
        /* Reply with the matching items. */
        call.start_array();

        auto ks = call.kspace();
        for (const auto& shard : ks->get_shards()) {
                shard->glob(spec, pattern, true, [&](const art::leaf &l) -> bool {
                std::lock_guard lk(vklock); // because there's worker threads concurrently calling here
                if (0 != call.push_encoded_key(l.get_key())) {
                    return false;
                };
                ++replies;
                return true;
            });
        }
        call.end_array();

    }
    return call.ok();
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
    auto t = sp->get(key);
    art::key_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.syntax_error();
    }
    spec.hash = !sp->opt_ordered_keys;

    art::value_type reply{"", 0};
    auto fc = [&](const art::node_ptr &) -> void {
        if (spec.get) {
            reply = key;
        }
    };

    art::key_options opts = spec;
    const auto& compressed = dictionary::compress(v);
    if (!compressed.empty()) {
        statistics::value_bytes_compressed += compressed.size;
        opts.set_compressed(true);
        v = compressed;
    }

    storage_release l(t);
    t->opt_insert(opts, key, v, true, fc);

    if (spec.get) {
        if (reply.size) {

            return call.push_encoded_key(reply);
        } else {
            return call.push_null();
        }
    } else {
        return call.push_simple("OK");
    }
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
    auto t = call.kspace()->get(converted.get_value());
    storage_release release(t);
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
    return BarchModifyInteger(call, argv, 1);
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
    auto t = call.kspace()->get(converted.get_value());
    storage_release release(t);
    auto fc = [&](art::node_ptr) -> void {
    };
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
            return call.push_int(0);
        }

        t->opt_insert( opts, converted.get_value(), v, true, fc);

    }else {
        art::key_options options;

        if (!t->opt_insert(options, converted.get_value(), v, true, fc)) {
            return call.push_error("key value not added");
        }


    }
    return call.push_ll(r);
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
    return BarchModifyInteger(call, argv, -1);
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
    if (argv.size() < 3)
        return call.wrong_arity();
    int r = call.ok();
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
        auto t = call.kspace()->get(converted.get_value());
        storage_release release(t);
        t->insert( spec, converted.get_value(), v, true, fc);

    }

    call.push_bool(r == call.ok());
    return call.ok();
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
    auto t = call.kspace()->get(converted.get_value());

    art::key_spec spec(argv);
    storage_release release(t);
    t->insert(spec, converted.get_value(), v, false, fc);

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
    auto ckey = converted.get_value();
    auto t = call.kspace()->get_ref(ckey);
    auto src = t->sources();

    if (!src && t->has_static_bloom_filter() && !t->is_bloom(ckey)) {
        return call.push_null();
    }
    ref_read_lock release(t);

    auto r = t->search(ckey);
    if (r.null()) {
        return call.push_null();
    } else {
        if (r.cl()->is_tomb()) {
            return call.push_null();
        }
        auto cl = r.const_leaf();
        auto vt = cl->get_value();
        if (cl->is_compressed()) {
            vt = dictionary::decompress(vt);
        }
        return call.push_vt(vt);
    }
}

static void push_page(caller& call, const barch::shard_ptr& shard, const art::scan_spec &spec, caller::iteration_ptr iteration) {
    if (!shard) throw_exception<std::runtime_error>("null shard");

    barch::shard_ptr src = shard->sources();
    barch::shard_ptr dest = iteration->space->get(shard->get_shard_number());
    bool is_source =  shard == dest->sources();
    read_lock release(is_source ? dest : nullptr);
    if (is_source && !dest) throw_exception<std::runtime_error>("push_key: dest not found");

    auto push_key = [&](const art::leaf* l) {
        if (is_source) {
            if (!dest->is_present(l->get_key())) {
                call.push_encoded_key(l->get_key());
            }
        }else {
            call.push_encoded_key(l->get_key());// it throws so it's ok
        }
    };

    auto update_iter = [&](const art::leaf* l, uint32_t pos) {
        iteration->pos = pos + l->next_leaf();
        if (call.results_count() >= spec.count) {
            return false;
        }
        return true;
    };

    if (iteration->pos < iteration->bytes && iteration->page > 0) {
        if (spec.is_match) {
            std::string tmp;
            art::page_iterator_ptr(iteration->buffer.data(), iteration->buffer.size(), [&](const art::leaf *l, uint32_t pos) -> bool {
                if (l->is_tomb()||l->expired()) return true;
                art::value_type td;
                if (art::tstring == *l->key())
                {
                    td = l->get_clean_key();
                    // get_clean_key steps over the leading type byte but keeps the stored
                    // length, so the trailing terminator is still on the end - leaving it
                    // there stops any pattern anchored at the end of the key from matching
                    if (td.size) --td.size;
                }else {
                    tmp = encoded_key_as_string(l->get_key());
                    td = tmp;
                }

                if (1 == glob::stringmatchlen(spec.glob_expr, td, 0)) {
                    push_key(l);
                }
                return update_iter(l, pos);
             },iteration->pos);
        }else {
            art::page_iterator_ptr(iteration->buffer.data(), iteration->buffer.size(), [&](const art::leaf *l, uint32_t pos) -> bool {
                if (l->is_tomb()||l->expired()) return true;
                push_key(l);
                return update_iter(l, pos);
            },iteration->pos);
        }
    }
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
    // an iteration is dropped when the scan runs out of shards, so one that is followed
    // to the end costs nothing. An abandoned one stays until the connection closes, and
    // it holds a page buffer, so a connection is only allowed so many at a time -
    // max_scan_iterators, which CLIENT INFO reports against as iters and iters-mem, and
    // CLIENT CLEAR_ITERS lets a client reclaim
    auto id = spec.scan_id;
    auto iteration = call.get_iteration(id);
    if (!iteration) {
        auto max_iterations = barch::get_max_scan_iterators();
        if (max_iterations && call.iteration_count() >= max_iterations) {
            return call.push_error("too many open SCAN cursors, finish one or use CLIENT CLEAR_ITERS");
        }
        iteration = call.create_iteration();
        auto space = call.kspace();
        iteration->space = space;
        for (size_t i = 0; i < space->get_shard_count(); ++i) {
            auto s = space->get(i);
            iteration->shards.push_back(s);
            if (s->sources()) {
                iteration->shards.push_back(s->sources());
            }
        }
        if (space->source() && iteration->shards.size() != 2*space->get_shard_count()) {
            return call.push_error("invalid shard count");
        }

    }
    call.push_string(std::to_string(iteration->id));
    call.start_array();
    for (; !iteration->shards.empty();) {
        auto t = iteration->shards.back();

        if (t->get_size() == 0) {
            iteration->page = 0;
            iteration->shards.pop_back();
            continue;
        }

        iteration->shard = t->get_shard_number();
        do {
            if (iteration->bytes == 0) {
                iteration->buffer.clear();
                read_lock release(t);
                iteration->bytes = t->page(iteration->page, iteration->buffer);
                iteration->pos = 0;
            }
            push_page(call, t, spec, iteration);
            if (call.results_count() >= spec.count) {
                // todo: if we're exactly on max_count and there are no more pages then there will be one additional call
                call.end_array();
                return call.ok();
            }
            iteration->page = t->next_page(iteration->page);
            iteration->pos = 0;
            iteration->bytes = 0;
            iteration->buffer.clear();
        } while (iteration->page > 0);

        iteration->shards.pop_back();
    }

    if (iteration->shards.empty()) {
        call.erase_iteration(iteration->id);
        call.end_array();
        call.set_string(0, "0");
        return call.ok();
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
    auto ckey = converted.get_value();
    auto t = call.kspace()->get(ckey);
    auto src = t->sources();

    if (!src && t->has_static_bloom_filter() && !t->is_bloom(ckey)) {
        return call.push_null();
    }
    read_lock release(t);
    auto r = t->search(ckey);
    if (r.null()) {
        return call.push_null();
    } else {
        if (r.cl()->is_tomb()) {
            return call.push_null();
        }
        auto cl = r.const_leaf();
        auto vt = cl->get_value();
        if (cl->is_compressed()) {
            vt = dictionary::decompress(vt);
        }
        return call.push_ll(vt.size);
    }
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
    auto t = call.kspace()->get(converted.get_value());
    read_lock release(t);

    art::node_ptr r = t->search(converted.get_value());
    if (r.null()) {
        return call.push_ll(-1);
    }
    auto l = r.const_leaf();
    if (l->is_expiry()) {
        long long e = (l->expiry_ms() - art::now())/1000;
        return call.push_ll(e);
    }

    return call.push_ll(-2);

}
int cmd_TTL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, TTL);
}
int EXISTS(caller& call, const arg_t& argv) {
    if (argv.size() < 2)
        return call.wrong_arity();
    for (size_t i = 1; i < argv.size(); ++i) {
        auto k = argv[i];
        if (key_ok(k) != 0)
            return call.key_check_error(k);
        auto converted = conversion::as_composite(k);
        auto t = call.kspace()->get(converted.get_value());
        auto src = t->sources();
        if (!src && t->has_static_bloom_filter() && !t->is_bloom(converted.get_value())) {
            return call.push_bool(false);
        } else {
            read_lock release(t);
            art::node_ptr r = t->search(converted.get_value());
            if (r.null()) {
                return call.push_bool(false);
            }
        }
    }
    return call.push_bool(true);
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
    auto t = call.kspace()->get(converted.get_value());
    storage_release release(t);

    art::node_ptr r = t->search(converted.get_value());
    if (r.null()) {
        return call.push_ll(-1);
    }
    art::key_expire_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.syntax_error();
    }

    auto l = r.const_leaf();
    if (spec.nx) {
        if (l->is_expiry()) {
            return call.push_ll(-1);
        }

    } else if (spec.xx) {
        if (!l->is_expiry()) {
            return call.push_ll(-1);
        }

    } else if (spec.gt) {
        if (spec.ttl + art::now() < l->expiry_ms()) {
            return call.push_ll(-1);
        }
    } else if (spec.lt) {
        if (spec.ttl + art::now() > l->expiry_ms()) {
            return call.push_ll(-1);
        }
    }
    auto updater = [&t,spec](const art::node_ptr &leaf) -> art::node_ptr {
        if (leaf.null()) {
            return leaf;
        }
        auto l = leaf.const_leaf();
        if (art::now() + spec.ttl == 0) {
            barch::std_log("why");
        }
        return art::make_leaf(t->get_ap(), l->get_key(),
            l->get_value(),
            art::now() + spec.ttl, l->is_volatile(),
            l->is_compressed());
    };
    if (t->update(l->get_key(),updater))
        return call.push_ll(1);
    return call.push_ll(-2);
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
    call.start_array();
    for (size_t arg = 1; arg < argv.size(); ++arg) {
        auto k = argv[arg];
        if (key_ok(k) != 0) {
            call.push_null();
        } else {
            auto converted = conversion::as_composite(k);
            auto t = call.kspace()->get(converted.get_value());
            storage_release release(t);
            art::node_ptr r = t->search(converted.get_value());
            if (r.null()) {
                call.push_null();
            } else {
                auto vt = r.const_leaf()->get_value();
                call.push_vt(vt);
            }
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
static art::value_type min_fun(art::value_type the_min, art::node_ptr r) {
    if (r.is_leaf && the_min.empty()) {
        return r.const_leaf()->get_key();
    }else if (r.is_leaf && r.const_leaf()->get_key() < the_min){
        return r.const_leaf()->get_key();
    }
    return the_min;
}
int MIN(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    art::value_type the_min;
    auto ks = call.kspace();
    ks_shared kss(ks->source());
    ks_shared ksl(ks);
    for (const auto& shard : ks->get_shards()) {
        auto t = shard;
        art::node_ptr r = t->tree_minimum();
        if (!t->get_tree_size()) continue;
        the_min = min_fun(the_min, r);
    }
    int ok = call.ok();
    if (the_min.empty()) {
        ok = call.push_null();
    } else {
        ok = call.push_encoded_key(the_min);
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
    art::value_type the_max;
    auto ks = call.kspace();
    ks_shared kss(ks->source());
    ks_shared ksl(ks);
    for (const auto& shard : ks->get_shards()) {
        auto t = shard;
        if (!t->get_tree_size()) continue;
        art::node_ptr r = t->tree_maximum();
        if (!r.is_leaf) {
            continue;
        }
        auto cur = r.const_leaf()->get_key();
        if (the_max.empty()) {
            the_max = cur;
        }else if (r.is_leaf && the_max < cur){
            the_max = cur;
        }
    }
    int ok = call.ok();
    if (the_max.empty()) {
        ok = call.push_null();
    } else {
        ok = call.push_encoded_key(the_max);
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
    art::value_type the_lb;
    auto ks = call.kspace();
    ks_shared kss(ks->source());
    ks_shared ksl(ks);
    for (const auto& shard : call.kspace()->get_shards()) {
        auto t = shard;
        if (!t->get_tree_size()) continue;
        art::node_ptr r = t->lower_bound(converted.get_value());
        if (r.is_leaf && the_lb.empty()) {
            the_lb = r.const_leaf()->get_key();

        }else if (r.is_leaf && r.const_leaf()->get_key() < the_lb){
            the_lb = r.const_leaf()->get_key();
        }
    }
    int ok = call.ok();
    if (the_lb.empty()) {
        ok = call.push_null();
    } else {
        ok = call.push_encoded_key(the_lb);
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
    art::value_type the_lb;
    auto ks = call.kspace();
    ks_shared kss(ks->source());
    ks_shared ksl(ks);
    for (const auto& t : call.kspace()->get_shards()) {
        if (!t->get_tree_size()) continue;
        auto key = converted.get_value();
        art::iterator ilb(t, key);
        if (ilb.ok() && ilb.key() == key) {
            ilb.next();
        }
        if (ilb.ok()) {
            if (the_lb.empty() || ilb.key() < the_lb) {
                the_lb = ilb.key();
            }
        }
    }
    int ok = call.ok();
    if (the_lb.empty()) {
        ok = call.push_null();
    } else {
        ok = call.push_encoded_key(the_lb);
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

    auto t = call.kspace()->get(converted.get_value());
    storage_release release(t);
    t->remove(converted.get_value(), fc);

    return r;
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
