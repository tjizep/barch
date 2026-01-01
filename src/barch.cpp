//
// Created by me on 11/9/24.
//

#include "auth_api.h"
#include "barch_apis.h"
#include "art/iterator.h"
#include "rpc/redis_parser.h"
#include "vk_caller.h"
#include "keys.h"
#include "swig_api.h"
#include "thread_pool.h"
/* cdict --
 *
 * This module implements a volatile key-value store on top of the
 * dictionary exported by the modules API.
 *
 * -----------------------------------------------------------------------------
 * */
extern "C" {
#include "../external/include/valkeymodule.h"
}

#include <cctype>
#include <cstring>
#include <cmath>
#include <shared_mutex>
#include "conversion.h"
#include "art/art.h"
#include "configuration.h"
#include "keyspec.h"
#include "ioutil.h"
#include "module.h"
#include "hash_api.h"
#include "ordered_api.h"
#include "caller.h"
#include "spaces_spec.h"
#include "keyspace_locks.h"
#include "dictionary_compressor.h"

static size_t save(caller& call) {
    std::atomic<size_t> errors = 0;
    shard_thread_processor(barch::get_shard_count().size(),[&](size_t shard_num) {
        if (!call.kspace()->get(shard_num)->save(true)) {
                barch::std_err("could not save",shard_num);
                ++errors;
            }
    });
    save_auth();
    return errors;
}
static auto startTime = std::chrono::high_resolution_clock::now();
template<typename IntT>
static int BarchModifyInteger(caller& call,const arg_t& argv, IntT by) {

    if (argv.size() < 2)
        return call.wrong_arity();
    auto t = call.kspace()->get(argv[1]);
    storage_release release(t);
    auto k = argv[1];
    art::key_spec spec;
    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::convert(k);
    int r = -1;
    IntT l = 0;
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
    t->update(converted.get_value(), updater);
    if (r == 0) {
        return call.push_int(l);
    } else {
        return call.push_null();
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
        count = std::atoll(argv[3].chars());

    auto k1 = argv[1];
    auto k2 = argv[2];

    if (key_ok(k1) != 0)
        return call.key_check_error(k1);
    if (key_ok(k2) != 0)
        return call.key_check_error(k2);

    auto c1 = conversion::convert(k1);
    auto c2 = conversion::convert(k2);
    /* Reply with the matching items. */

    auto ks = call.kspace();
    ks_shared kss(ks->source());
    ks_shared ksl(ks);
    auto collect = [&]() -> heap::std_vector<art::value_type>{
        int64_t striation_counter = 0;
        heap::std_vector<art::value_type> usorted;
        heap::vector<art::merge_iterator> iters;
        heap::unordered_set<size_t> active; // set must be ordered
        for (auto shard : barch::get_shard_count()) {
            // iterate the 'striations'
            auto t = call.kspace()->get(shard);
            auto i = make_merged(t,c1.get_value());
            if (i.ok()) {
                active.insert(shard);
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
    call.end_array(0);

    /* Cleanup. */
    return r;
}
int cmd_RANGE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {

    vk_caller caller;

    return caller.vk_call(ctx, argv, argc, ::RANGE);
}
int CLIENT(caller& call, const arg_t& arg_v) {
    if (arg_v.size()<=1) {
        return call.wrong_arity();
    }
    if (arg_v[1] == "INFO") {
        std::string r = call.get_info();
        return call.push_string(r);
    }
    if (arg_v[1] == "SETINFO") {
        if (arg_v.size() == 4) {
            return call.ok();
        }
    }
    return call.syntax_error();
}
int MULTI(caller& call, const arg_t& arg_v) {
    if (arg_v.size()!=1) {
        return call.wrong_arity();
    }
    call.start_call_buffer();
    return call.ok();
}
int EXEC(caller& call, const arg_t& arg_v) {
    if (arg_v.size()!=1) {
        return call.wrong_arity();
    }
    call.finish_call_buffer();
    return 0;
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
    auto c1 = conversion::convert(k1);
    auto c2 = conversion::convert(k2);
    auto space_cnt = [c1,c2](barch::key_space_ptr spce, size_t shard)-> size_t {
        if (!spce) return 0;
        size_t count = 0;
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
    for (auto shard : barch::get_shard_count()) {
        count += space_cnt(s, shard);
        count += space_cnt(s->source(), shard);
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
    if (spec.count) {
        for (auto shard : barch::get_shard_count()) {
            call.kspace()->get(shard)->glob(spec, pattern, false, [&](const art::leaf & unused(l)) -> bool {
                ++replies;
                return true;
            });
        }
        return call.push_ll(replies);
    } else {
        /* Reply with the matching items. */
        call.start_array();

        for (auto shard : barch::get_shard_count()) {
            call.kspace()->get(shard)->glob(spec, pattern, false, [&](const art::leaf &l) -> bool {
                std::lock_guard lk(vklock); // because there's worker threads concurrently calling here
                if (0 != call.push_encoded_key(l.get_key())) {
                    return false;
                };
                ++replies;
                return true;
            });
        }
        call.end_array(replies);

    }
    return call.ok();
}
int cmd_KEYS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, KEYS);
}
/* B.VALUES
*
* match against all values using a glob pattern
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
        for (auto shard : barch::get_shard_count()) {
            call.kspace()->get(shard)->glob(spec, pattern, true, [&](const art::leaf & unused(l)) -> bool {
                ++replies;
                return true;
            });
        }
        return call.push_ll(replies);
    } else {
        /* Reply with the matching items. */
        call.start_array();

        for (auto shard : barch::get_shard_count()) {
            call.kspace()->get(shard)->glob(spec, pattern, true, [&](const art::leaf &l) -> bool {
                std::lock_guard lk(vklock); // because there's worker threads concurrently calling here
                if (0 != call.push_encoded_key(l.get_key())) {
                    return false;
                };
                ++replies;
                return true;
            });
        }
        call.end_array(replies);

    }
    return call.ok();
}
int cmd_VALUES(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, VALUES);
}
int TRAIN(caller& call, const arg_t& argv) {
    std::string d;
    for (size_t i = 1; i < argv.size(); ++i) {
        d += argv[i].to_string();
        d += " ";
    }
    return call.push_ll(dictionary::train(d));
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

    auto t = call.kspace()->get(argv[1]);
    auto converted = conversion::convert(k);
    auto key = converted.get_value();
    art::key_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.syntax_error();
    }
    spec.hash = !barch::get_ordered_keys();

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
    auto t = get_art(argv[1]);
    storage_release release(t);
    auto k = argv[1];
    art::key_spec spec;
    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::convert(k);
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

    t->update(converted.get_value(), updater);
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
int APPEND(caller& call, const arg_t& argv) {
    ++statistics::set_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    auto t = call.kspace()->get(argv[1]);
    storage_release release(t);
    auto k = argv[1];
    auto v = argv[2];
    art::key_spec spec;
    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::convert(k);
    int r = -1;
    auto updater = [&](const art::node_ptr &old) -> art::node_ptr {
        if (old.null()) {
            return nullptr;
        }
        const art::leaf *leaf = old.const_leaf();
        auto ov = leaf->get_value();
        if (leaf->is_compressed()) {
            ov = dictionary::decompress(ov);
        }
        auto& alloc = const_cast<alloc_pair&>(old.logical.get_ap<alloc_pair>());
        heap::small_vector<uint8_t, 128> s;
        s.append(ov.to_view());
        s.append(v.to_view());
        art::node_ptr l = make_leaf
        (  alloc
        ,  leaf->get_key()
        ,  {s.data(),s.size()}
        ,  leaf->expiry_ms()
        ,  leaf->is_volatile()
        // no compression by design!
        );

        if (!l.null()) {
            r = 0;
        }
        return l;
    };
    t->update(converted.get_value(), updater);
    if (r == 0) {
        return call.push_simple("OK");
    } else {
        return call.push_null();
    }
}
int PREPEND(caller& call, const arg_t& argv) {
    ++statistics::set_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    auto t = call.kspace()->get(argv[1]);
    storage_release release(t);
    auto k = argv[1];
    auto v = argv[2];
    art::key_spec spec;
    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::convert(k);
    int r = -1;
    auto updater = [&](const art::node_ptr &old) -> art::node_ptr {
        if (old.null()) {
            return nullptr;
        }
        const art::leaf *leaf = old.const_leaf();
        auto& alloc = const_cast<alloc_pair&>(old.logical.get_ap<alloc_pair>());
        heap::small_vector<uint8_t, 128> s;
        s.append(v.to_view());
        s.append(leaf->get_value().to_view());
        art::node_ptr l = make_leaf
        (  alloc
        ,  leaf->get_key()
        ,  {s.data(),s.size()}
        ,  leaf->expiry_ms()
        ,  leaf->is_volatile()
        );

        if (!l.null()) {
            r = 0;
        }
        return l;
    };
    t->update(converted.get_value(), updater);
    if (r == 0) {
        return call.push_simple("OK");
    } else {
        return call.push_null();
    }
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

        auto converted = conversion::convert(k);
        art::key_spec spec; //(argv, argc);
        art::value_type reply{"", 0};
        auto fc = [&](art::node_ptr) -> void {
        };
        auto t = call.kspace()->get(k);
        storage_release release(t);
        t->insert( spec, converted.get_value(), v, true, fc);

    }
    call.push_bool(true);
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
    auto t = call.kspace()->get(argv[1]);
    auto k = argv[1];
    auto v = argv[2];

    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto fc = [](art::node_ptr) -> void {
    };
    auto converted = conversion::convert(k);
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
    auto t = call.kspace()->get(k);
    auto converted = conversion::convert(k);
    auto ckey = converted.get_value();
    if (t->has_static_bloom_filter() && !t->is_bloom(ckey)) {
        return call.push_null();
    }
    read_lock release(t);
    art::node_ptr r = t->search(ckey);
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
    auto t = call.kspace()->get(argv[1]);
    read_lock release(t);
    auto converted = conversion::convert(k);
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

        auto t = call.kspace()->get(argv[i]);
        read_lock release(t);
        auto converted = conversion::convert(k);
        art::node_ptr r = t->search(converted.get_value());
        if (r.null()) {
            return call.push_bool(false);
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
    auto t = call.kspace()->get(argv[1]);
    read_lock release(t);
    auto converted = conversion::convert(k);
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
        return art::make_leaf(t->get_ap(), l->get_key(), l->get_value(),  art::now() + spec.ttl, l->is_volatile());
    };
    art::key_options opts{spec.ttl,true,false,false, false};
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
            auto converted = conversion::convert(k);
            auto t = call.kspace()->get(k);
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
    call.end_array(responses);
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
    for (auto shard:barch::get_shard_count()) {
        auto t = call.kspace()->get(shard);
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
int cmd_MILLIS(ValkeyModuleCtx *ctx, ValkeyModuleString **, int) {
    auto t = std::chrono::high_resolution_clock::now();
    const auto d = std::chrono::duration_cast<std::chrono::milliseconds>(t - startTime);
    return ValkeyModule_ReplyWithLongLong(ctx, d.count());
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
    for (auto shard:barch::get_shard_count()) {
        auto t = ks->get(shard);
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

    auto converted = conversion::convert(k);
    art::value_type the_lb;
    for (auto shard:barch::get_shard_count()) {
        auto t = call.kspace()->get(shard);
        t->get_latch().lock();
    }
    for (auto shard:barch::get_shard_count()) {
        auto t = call.kspace()->get(shard);
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
    for (auto shard:barch::get_shard_count()) {
        auto t = call.kspace()->get(shard);
        t->get_latch().unlock();
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

    auto converted = conversion::convert(k);
    art::value_type the_lb;
    ks_unique ulock(call.kspace());
    for (auto shard:barch::get_shard_count()) {
        auto t = call.kspace()->get(shard);
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

    auto converted = conversion::convert(k);
    int r = 0;
    auto fc = [&r,&call](art::node_ptr n) -> void {
        if (n.null()) {
            r = call.push_null();
        } else {
            auto vt = n.const_leaf()->get_value();
            r = call.push_vt(vt);
        }
    };

    auto t = call.kspace()->get(argv[1]);
    storage_release release(t);
    t->remove(converted.get_value(), fc);

    return r;
}
int cmd_REM(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, REM);
}
/* B.KSPACE
    - Key space operators:
    - `KSPACE DEPENDS {depend[e|a]nt key space} ON {source key space name} [STATIC]`
      Let a key space depend on a list of one or more source key spaces (dependant missing keys are resolved in source)
      keys are added to the dependent and not propagated to the source
    - `KSPACE RELEASE {depend[e|a]nt key space} FROM {source key space name}`
     release a source from a dependent
    - `KSPACE DEPENDANTS {key space name}`
     list the dependants
    - `KSPACE DROP {key space name}`
         list the dependants
    - `KSPACE MERGE {depend[e|a]nt key space} [TO {source key space name}]`
      Merge a dependent named key space to its sources or any other random key space
    - `KSPACE OPTION [SET|GET] ORDERED [ON|OFF]` sets the current key space to ordered or unordered, option is saved in key space shards
    - `KSPACE OPTION [SET|GET] LRU [ON|OFF|VOLATILE]` sets the current key space to evict lru
    - `KSPACE OPTION [SET|GET] RANDOM [ON|OFF|VOLATILE]` sets the current key space to evict randomly
    - `KSPACE EXIST {key space name} return `1` if space exists else `0`
 */
int KSPACE(caller& call, const arg_t& argv) {
    if (argv.size() < 3) {
        return call.wrong_arity();
    }

    art::kspace_spec parser(argv);
    if (parser.parse_options() != 0) {
        return call.syntax_error();
    }
    if (parser.is_exist) {
        return call.push_bool(barch::is_keyspace(parser.name));
    }
    if (parser.is_depends) {
        auto source = barch::get_keyspace(parser.source);
        auto dependent = barch::get_keyspace(parser.dependant);

        ks_shared shl(source);
        ks_unique ul(dependent);
        dependent->depends(source);
        return call.push_simple("OK");
    }

    if (parser.is_dependants) {
        auto dependent = barch::get_keyspace(parser.dependant); // this will throw if parameter is wrong
        auto source = dependent->source();
        if (source) {
            return call.push_string(source->get_canonical_name());
        }
        return call.push_null();
    }

    if (parser.is_merge) {
        if (parser.is_merge_default) {
            merge_options opts;
            opts.set_compressed(parser.is_merge_compress);
            call.kspace()->merge(opts);
            return call.push_simple("OK");
        }
        auto to = barch::get_keyspace(parser.source);
        auto from = barch::get_keyspace(parser.dependant);
        barch::key_space_ptr old = nullptr;
        if (to == from->source()) {
            old = to;
            from->depends(nullptr);
        }
        ks_shared shl(from);
        ks_unique ul(to);

        from->merge(to, {});
        if (old) {
            from->depends(to);
        }
        return call.push_simple("OK");
    }

    if (parser.is_release) {
        auto dependent = barch::get_keyspace(parser.dependant);
        auto source = dependent->source();
        if (!source || source->get_canonical_name() != parser.source) {
            if (!parser.source.empty())
                return call.push_error("Invalid source keyspace name");
        }
        ks_unique shl(dependent);
        ks_shared ul(source);
        dependent->depends(nullptr);
        return call.push_simple("OK");
    }
    if (parser.is_drop) {
        auto source = parser.source.empty() ? call.kspace() : barch::get_keyspace(parser.source);
        ks_unique shl(source);
        source->depends(nullptr);
        source->each_shard([](barch::shard_ptr shrd) {
            shrd->opt_drop_on_release = true;
        });
        source = nullptr;
        if (barch::unload_keyspace(parser.source))
            return call.push_simple("OK");
    }

    if (parser.is_option && parser.is_get) {
        auto spc = call.kspace();
        ks_shared ul(spc);
        if (parser.name == "ORDERED") {
            barch::shard_ptr ptr = spc->get(0ul);
            call.push_bool(ptr->opt_ordered_keys);
            return 0;
        }
        if (parser.name == "LRU") {
            barch::shard_ptr ptr = spc->get(0ul);
            call.push_bool(ptr->opt_evict_all_keys_lru);
            return 0;
        }
        if (parser.name == "RANDOM") {
            barch::shard_ptr ptr = spc->get(0ul);
            call.push_bool(ptr->opt_evict_all_keys_random);
            return 0;
        }
        return call.push_simple("OK");
    }

    if (parser.is_option && parser.is_set) {
        auto spc = call.kspace();
        ks_unique ul(spc);
        if (parser.name == "ORDERED") {
            bool on = parser.value == "ON";
            for (auto &shrd : spc->get_shards()) {
                shrd->opt_ordered_keys = on;
            }
            return call.push_simple("OK");
        }
        if (parser.name == "LRU") {
            bool on = parser.value == "ON";
            bool evict_volatile = parser.value == "VOLATILE";
            for (auto &shrd : spc->get_shards()) {
                shrd->opt_evict_all_keys_lru = on;
                shrd->opt_evict_volatile_keys_lru = evict_volatile;
            }
            return call.push_simple("OK");
        }
        if (parser.name == "RANDOM") {
            bool on = parser.value == "ON";
            bool evict_volatile = parser.value == "VOLATILE";
            for (auto &shrd : spc->get_shards()) {
                shrd->opt_evict_all_keys_random = on;
                shrd->opt_evict_volatile_keys_random = evict_volatile;
            }
            return call.push_simple("OK");
        }

    }

    return call.push_null();
}

int cmd_KSPACE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, KSPACE);
}
/* B.USE
 * @return OK.
 */
int USE(caller& call, const arg_t& argv) {
    if (argv.size() == 1) {
        call.use("");
        return call.push_simple("OK");
    }
    if (argv.size() != 2) {
        return call.wrong_arity();
    }
    auto name = argv[1].to_string();
    if (name == "0") {
        call.use("");
    }else {
        call.use(name);
    }
    return call.push_simple("OK");
}

int cmd_USE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, USE);
}

int cmd_SELECT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, USE);
}

int UNLOAD(caller& call, const arg_t& argv) {
    if (argv.size() == 1) {
        barch::unload_keyspace("");
        return call.push_simple("OK");
    }
    if (argv.size() != 2) {
        return call.wrong_arity();
    }
    barch::unload_keyspace(argv[1].to_string());
    return call.push_simple("OK");
}
int cmd_UNLOAD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, UNLOAD);
}
int KSPACE(caller& call, const arg_t& argv);
int SPACES(caller& call, const arg_t& argv) {

    if (argv.size() == 1) {
        call.start_array();
        barch::all_spaces([&call](const std::string& name, const barch::key_space_ptr& space) {
            int64_t size = 0;
            for (auto s: space->get_shards() ) {
                size += s->get_size();
            }
            call.push_values({name,size});
        });
        call.end_array(0);
    }else {
        return KSPACE(call, argv);
    }
    return call.ok();
}
int cmd_SPACES(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, SPACES);
}
/* B.SIZE
 * @return the size or o.k.a. key count.
 */
int SIZE(caller& call, const arg_t& argv) {

    if (argv.size() != 1)
        return call.wrong_arity();
    auto size = 0ll;
    auto arts = call.kspace()->get_shards();
    for (auto &t:arts) {
        storage_release release(t);
        size += (int64_t) t->get_size();
    }
    return call.push_ll(size);
}
int cmd_SIZE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, SIZE);
}
/* B.SAVE
 * saves the data to files called leaf_data.dat and node_data.dat in the current directory
 * @return OK if successful
 */
int SAVE(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    size_t errors = save(call);
    return errors ? call.push_error("some shards not saved"): call.push_simple("OK");
}

int cmd_SAVE(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, SAVE);
}
/* B.LOAD
 * loads and overwrites the data from files called leaf_data.dat and node_data.dat in the current directory
 * @return OK if successful
 */
int LOAD(caller& call, const arg_t& argv) {

    if (argv.size() != 1)
        return call.wrong_arity();
    std::vector<std::thread> loaders{barch::get_shard_count().size()};
    size_t errors = 0;
    auto ks = call.kspace();
    for (auto shard : barch::get_shard_count()) {
        loaders[shard] = std::thread([&errors,shard,&ks]() {
            if (!ks->get(shard)->load(true)) ++errors;
        });
    }
    for (auto &loader : loaders) {
        if (loader.joinable())
            loader.join();
    }
    return errors>0 ? call.push_error("some shards did not load") : call.push_simple("OK");
}
int START(caller& call, const arg_t& argv) {
    if (argv.size() > 4)
        return call.wrong_arity();
    if (argv.size() == 4 && argv[3] != "SSL") {
        return call.push_error("invalid argument");
    };
    auto interface = argv[1];
    auto port = argv[2];
    bool ssl = argv.size() == 4 && argv[3] == "SSL";
    barch::server::start(interface.chars(), atoi(port.chars()), ssl);
    return call.push_simple("OK");
}
int PUBLISH(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    Variable interface = argv[1];
    Variable port = argv[2];
    auto ks = call.kspace();
    for (auto shard: barch::get_shard_count()) {
        if (!ks->get(shard)->publish(interface.s(), port.i())) {}
    }
    return call.push_simple("OK");
}
int PULL(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    Variable interface = argv[1];
    Variable port = argv[2];
    auto ks = call.kspace();
    for (auto shard: barch::get_shard_count()) {
        if (!ks->get(shard)->pull(interface.s(), port.i())) {}
    }
    return call.push_simple("OK");
}
int STOP(caller& call, const arg_t& ) {
    if (call.get_context() == ctx_resp) {
        return call.push_error("Cannot stop server");
    }
    barch::server::stop();
    return call.push_simple("OK");
}
int PING(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto interface = argv[1];
    auto port = argv[2];
    //barch::server::start(interface.chars(), atoi(port.chars()));
    barch::repl::client cli(interface.chars(), atoi(port.chars()), 0);
    if (!cli.ping()) {
        return call.push_error("could not ping");
    }
    return call.push_simple("OK");
}
int RETRIEVE(caller& call, const arg_t& argv) {

    if (argv.size() != 3)
        return call.wrong_arity();
    Variable host = argv[1];
    Variable port = argv[2];

    for (auto shard : barch::get_shard_count()) {
        barch::repl::client cli(host.s(), port.i(), shard);
        if (!cli.load(shard)) {
            if (shard > 0) {
                auto ks = call.kspace();
                for (auto s : barch::get_shard_count()) {
                    ks->get(s)->clear();
                }
            }

            return call.push_error("could not load shard - all shards cleared");
        }
    }
    return call.push_simple("OK");
}

int cmd_PUBLISH(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PUBLISH);
}

int cmd_PULL(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PULL);
}

int cmd_LOAD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, LOAD);
}
int cmd_PING(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, PING);
}
int cmd_RETRIEVE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, RETRIEVE);
}
int cmd_START(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, START);
}
int cmd_STOP(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, STOP);
}

int BEGIN(caller& call, const arg_t& argv) {

    if (argv.size() != 1)
        return call.wrong_arity();
    for (auto shard : barch::get_shard_count()) {
        call.kspace()->get(shard)->begin();
    }
    return call.ok();
}
int cmd_BEGIN(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, BEGIN);
}

int COMMIT(caller& call, const arg_t& argv) {

    if (argv.size() != 1)
        return call.wrong_arity();
    auto ks = call.kspace();
    for (auto shard : barch::get_shard_count()) {
        ks->get(shard)->commit();
    }
    return call.push_simple("OK");
}
int cmd_COMMIT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, COMMIT);
}
int ROLLBACK(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    auto ks = call.kspace();
    for (auto shard : barch::get_shard_count()) {
        ks->get(shard)->rollback();
    }
    return call.push_simple("OK");
}
int cmd_ROLLBACK(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ROLLBACK);
}
int CLEAR(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();

    for (auto shard : call.kspace()->get_shards()) {
        shard->clear();
    }

    return call.push_simple("OK");
}

int cmd_CLEAR(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, CLEAR);
}

int CLEARALL(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    barch::all_shards([](auto& shard) {
        shard->clear();
    });


    return call.push_simple("OK");
}

int cmd_CLEARALL(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, CLEAR);
}
int KSOPTIONS(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    if (argv[1] == "SET") {
        if (argv[2] == "UNORDERED") {
            for (auto &shard : call.kspace()->get_shards()) {
                shard->opt_ordered_keys = false;
            }
            return call.push_simple("OK");
        }
        if (argv[2] == "ORDERED") {
            for (auto &shard : call.kspace()->get_shards()) {
                shard->opt_ordered_keys = true;
            }
            return call.push_simple("OK");
        }
    }


    return call.push_error("Unknown option");
}

int cmd_KSOPTIONS(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, KSOPTIONS);
}

int SAVEALL(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    barch::all_shards([](auto& shard) {
        shard->save(true);
    });

    return call.push_simple("OK");
}

int cmd_SIZEALL(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, CLEAR);
}
int SIZEALL(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();

    uint64_t size = 0;

    barch::all_shards([&](auto& shard) {
        size += shard->get_size();
    });

    return call.push_int(size);
}

int cmd_SAVEALL(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, CLEAR);
}

int STATS(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    art_statistics as = barch::get_statistics();
    auto vbytes = 0ll;
    for (auto shard : barch::get_shard_count()) {
        storage_release release(call.kspace()->get(shard));
        vbytes += call.kspace()->get(shard)->get_ap().get_nodes().get_bytes_allocated() + call.kspace()->get(shard)->get_ap().get_leaves().get_bytes_allocated();
    }
    call.start_array();
    call.push_values({"heap_bytes_allocated", get_total_memory()});
    call.push_values({"value_bytes_compressed",as.value_bytes_compressed});
    call.push_values({ "last_vacuum_time", as.last_vacuum_time});
    call.push_values({ "vacuum_count", as.vacuums_performed});
    call.push_values({ "bytes_addressable", as.bytes_allocated});
    call.push_values({ "interior_bytes_addressable", as.bytes_interior});
    call.push_values({ "leaf_nodes", as.leaf_nodes});
    call.push_values({ "size_4_nodes", as.node4_nodes});
    call.push_values({ "size_16_nodes", as.node16_nodes});
    call.push_values({ "size_48_nodes", as.node48_nodes});
    call.push_values({ "size_256_nodes", as.node256_nodes});
    call.push_values({ "size_256_occupancy", as.node256_occupants});
    call.push_values({ "leaf_nodes_replaced", as.leaf_nodes_replaced});
    call.push_values({ "pages_evicted", as.pages_evicted});
    call.push_values({ "keys_evicted", as.keys_evicted});
    call.push_values({ "pages_defragged", as.pages_defragged});
    call.push_values({ "exceptions_raised", as.exceptions_raised});
    call.push_values({ "maintenance_cycles", as.maintenance_cycles});
    call.push_values({ "shards", as.shards});
    call.push_values({ "local_calls", as.local_calls});
    call.push_values({ "max_spin", as.max_spin});
    call.push_values({"logical_allocated", as.logical_allocated});
    call.push_values({"oom_avoided_inserts", as.oom_avoided_inserts});
    call.push_values({"keys_found", as.keys_found});
    call.end_array(0);
    return 0;
}
/* B.STATISTICS
 *
 * get memory statistics. */
int cmd_STATS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, STATS);
}
int OPS(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();

    art_ops_statistics as = barch::get_ops_statistics();
    call.start_array();
    call.push_values({"delete_ops", as.delete_ops});
    call.push_values({"retrieve_ops", as.get_ops});
    call.push_values({"insert_ops", as.insert_ops});
    call.push_values({"iterations", as.iter_ops});
    call.push_values({"range_iterations", as.iter_range_ops});
    call.push_values({"lower_bound_ops", as.lb_ops});
    call.push_values({"maximum_ops", as.max_ops});
    call.push_values({"minimum_ops", as.min_ops});
    call.push_values({"range_ops", as.range_ops});
    call.push_values({"set_ops", as.set_ops});
    call.push_values({"size_ops", as.size_ops});
    call.end_array(0);
    return 0;
}

/* B.OPS
 *
 * get data structure ops. */
int cmd_OPS(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, OPS);
}

int cmd_VACUUM(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc) {

    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    size_t result = 0;
    return ValkeyModule_ReplyWithLongLong(ctx, (int64_t) result);
}

int HEAPBYTES(caller& call, const arg_t& argv) {
    //compressed_release release;
    if (argv.size() != 1)
        return call.wrong_arity();;
    auto vbytes = 0ll;
    auto ks = call.kspace();

    for (auto shard : barch::get_shard_count()) {
        auto s = ks->get(shard);
        storage_release release(s);
        vbytes += s->get_ap().get_nodes().get_bytes_allocated() + s->get_ap().get_leaves().get_bytes_allocated();
    }
    return call.push_ll( (int64_t) heap::allocated + vbytes);
}
int cmd_HEAPBYTES(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, HEAPBYTES);
}

int CONFIG(caller& call, const arg_t& argv) {
    if (argv.size() != 4)
        return call.wrong_arity();
    auto s = argv[1];
    if (strncmp("set", s.chars(), s.size) == 0 || strncmp("SET", s.chars(), s.size) == 0) {
        int r = barch::set_configuration_value(argv[2].chars(), argv[3].chars());
        if (r == 0) {
            return call.push_simple("OK");
        }

    }else {
        return call.push_error("only SET keyword currently supported");
    }
    return call.push_error("could not set configuration value");
}
/* B.CONFIG [SET|GET] <key> [<value>]
 *
 * Set the specified key to the specified value. */
int cmd_CONFIG(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, CONFIG);
}

int COMMAND(caller& call, const arg_t& params) {
    if (params.size() < 2) {
        return call.wrong_arity();
    }
    if (params[1] == "DOCS") {
        std::vector<Variable> results;
        call.start_array();
        for (auto& p: functions_by_name()) {
            call.push_simple(p.first.c_str());
            call.push_simple("function");
        }
        call.end_array(0);
        return call.push_simple("OK");
    }
    return call.push_error("unknown command");
}
/* This function must be present on each module. It is used in order to
 * register the commands into the server. */
int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **, int) {
    if (ValkeyModule_Init(ctx, "B", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

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

    if (ValkeyModule_CreateCommand(ctx, NAME(SELECT), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(USE), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(RANGE), "readonly", 1, 2, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(COUNT), "readonly", 1, 2, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(SIZE), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(SIZEALL), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(STATS), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(OPS), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(MAX), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(MIN), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(MILLIS), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(VACUUM), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(HEAPBYTES), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(START), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(STOP), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(PING), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(PUBLISH), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(PULL), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(RETRIEVE), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(CONFIG), "write", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(SAVE), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(LOAD), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(BEGIN), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(COMMIT), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ROLLBACK), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(CLEAR), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(CLEARALL), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(KSOPTIONS), "write", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (add_hash_api(ctx) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }
    if (add_ordered_api(ctx) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }
    Constants.init(ctx);
    // valkey should free this I hope
    if (barch::register_valkey_configuration(ctx) != 0) {
        return VALKEYMODULE_ERR;
    };
    if (ValkeyModule_LoadConfigs(ctx) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    auto ks = get_default_ks();
    for (auto shard : barch::get_shard_count()) {
        if (ks->get(shard) == nullptr) {
            return VALKEYMODULE_ERR;
        }
    }
    if (barch::get_server_port() > 0)
        barch::server::start("0.0.0.0",barch::get_server_port(), false);
    barch::set_configuration_value("static_bloom_filter", barch::get_static_bloom_filter() ? "on":"off");
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(void *unused_arg) {
    // TODO: destroy tree
    return VALKEYMODULE_OK;
}
}
