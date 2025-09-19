//
// Created by me on 11/9/24.
//

#include "auth_api.h"
#include "barch_apis.h"
#include "rpc/redis_parser.h"
#include "vk_caller.h"
#include "keys.h"
#include "thread_pool.h"
/* cdict --
 *
 * This module implements a volatile key-value store on top of the
 * dictionary exported by the modules API.
 *
 * -----------------------------------------------------------------------------
 * */
extern "C" {
#include "valkeymodule.h"
}

#include <cctype>
#include <cstring>
#include <cstdlib>
#include <cmath>
#include <shared_mutex>
#include "conversion.h"
#include "art.h"
#include "configuration.h"
#include <fast_float/fast_float.h>
#include <functional>
#include "keyspec.h"
#include "ioutil.h"
#include "module.h"
#include "hash_api.h"
#include "keys.h"
#include "ordered_api.h"
#include "caller.h"
#include "queue_server.h"
static size_t save() {
    std::vector<std::thread> saviors{art::get_shard_count().size()};
    size_t shard = 0;
    std::atomic<size_t> errors = 0;
    for (auto& t: saviors) {
        t = std::thread([&errors,shard]() {
            if (!get_art(shard)->save(true)) {
                art::std_err("could not save",shard);
                ++errors;
            }
        });
        ++shard;
    }
    save_auth();
    for (auto& t: saviors) {
        if (t.joinable())
            t.join();
    }
    return errors;
}
static auto startTime = std::chrono::high_resolution_clock::now();
template<typename IntT>
static int BarchModifyInteger(caller& call,const arg_t& argv, IntT by) {

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
        return call.any_int(l);
    } else {
        return call.null();
    }
}


extern "C" {
/* B.RANGE <startkey> <endkey> <count>
*
* Return a list of matching keys, lexicographically between startkey
* and endkey. No more than 'count' items are emitted. */

int RANGE(caller& call, const arg_t& argv) {

    int r = 0;
    //read_lock rl(get_lock());
    if (argv.size() != 4)
        return call.wrong_arity();

    /* Parse the count argument. */
    long long count = std::atoll(argv[3].chars());

    auto k1 = argv[1];
    auto k2 = argv[2];

    if (key_ok(k1) != 0)
        return call.key_check_error(k1);
    if (key_ok(k2) != 0)
        return call.key_check_error(k2);

    auto c1 = conversion::convert(k1);
    auto c2 = conversion::convert(k2);
    /* Reply with the matching items. */
    heap::std_vector<art::value_type> sorted{};
    for (auto shard : art::get_shard_count()) {
        auto t = get_art(shard);
        queue_consume(t->shard);
        t->latch.lock_shared();
    }
    try {
        for (auto shard : art::get_shard_count()) {
            auto t = get_art(shard);
            art::iterator i(t,c1.get_value());
            while (i.ok()) {
                auto k = i.key();
                if (k >= c1.get_value() && k < c2.get_value()) {
                    sorted.push_back(k);
                }else {
                    break;
                }
                i.next();
            }
        }
        std::sort(sorted.begin(), sorted.end());
        call.start_array();
        for (auto&k : sorted) {
            call.reply_encoded_key(k);
            if (--count == 0) break;
        }
        call.end_array(0);
    }catch (std::exception& e) {
        r = call.error(e.what());
    }
    for (auto shard : art::get_shard_count()) {
        auto t = get_art(shard);
        t->latch.unlock();
    }

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
    auto c1 = conversion::convert(k1);
    auto c2 = conversion::convert(k2);
    for (auto shard : art::get_shard_count()) {
        auto t = get_art(shard);
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
    }
    return call.any_int(count);
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
        for (auto shard : art::get_shard_count()) {
            art::glob(get_art(shard), spec, pattern, [&](const art::leaf & unused(l)) -> bool {
                ++replies;
                return true;
            });
        }
        return call.long_long(replies);
    } else {
        /* Reply with the matching items. */
        call.start_array();

        for (auto shard : art::get_shard_count()) {
            art::glob(get_art(shard), spec, pattern, [&](const art::leaf &l) -> bool {
                std::lock_guard lk(vklock); // because there's worker threads concurrently calling here
                if (0 != call.reply_encoded_key(l.get_key())) {
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

    auto t = get_art(argv[1]);
    auto converted = conversion::convert(k);
    auto key = converted.get_value();
    art::key_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.syntax_error();
    }
    if (!art::get_ordered_keys()) {
        spec.hash = true;
    }

    art::value_type reply{"", 0};
    auto fc = [&](const art::node_ptr &) -> void {
        if (spec.get) {
            reply = key;
        }
    };

    if (!spec.get && is_queue_server_running() && t->queue_size < max_process_queue_size) {
        queue_insert(t->shard, spec, key, v);
    }else {
        storage_release l(t);
        t->opt_insert(spec, key, v, true, fc);
    }



    if (spec.get) {
        if (reply.size) {

            return call.reply_encoded_key(reply);
        } else {
            return call.null();
        }
    } else {
        return call.simple("OK");
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
        return call.error("not a valid integer");
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
        return call.error("not a valid integer");
    }
    auto arg2 = argv;
    arg2.pop_back();
    return BarchModifyInteger(call, arg2, by);
}
int APPEND(caller& call, const arg_t& argv) {
    ++statistics::set_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
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
        s.append(leaf->get_value().to_view());
        s.append(v.to_view());
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
        return call.simple("OK");
    } else {
        return call.null();
    }
}
int PREPEND(caller& call, const arg_t& argv) {
    ++statistics::set_ops;
    if (argv.size() != 3)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
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
        return call.simple("OK");
    } else {
        return call.null();
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
    int responses = 0;
    int r = call.ok();
    for (size_t n = 1; n < argv.size(); n += 2) {
        auto k = argv[n];
        auto v = argv[n + 1];

        if (key_ok(k) != 0) {
            r |= call.null();
            ++responses;
            continue;
        }

        auto converted = conversion::convert(k);
        art::key_spec spec; //(argv, argc);
        art::value_type reply{"", 0};
        auto fc = [&](art::node_ptr) -> void {
        };
        auto t = get_art(k);
        storage_release release(t);
        art_insert(t, spec, converted.get_value(), v, fc);

        ++responses;
    }
    call.boolean(true);
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
    auto t = get_art(argv[1]);
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

    return call.simple("OK");
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
    auto t = get_art(argv[1]);
    read_lock release(t);
    auto converted = conversion::convert(k);
    art::node_ptr r = t->search(converted.get_value());

    if (r.null()) {
        return call.null();
    } else {
        auto vt = r.const_leaf()->get_value();
        return call.vt(vt);
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
    auto t = get_art(argv[1]);
    read_lock release(t);
    auto converted = conversion::convert(k);
    art::node_ptr r = t->search(converted.get_value());
    if (r.null()) {
        return call.long_long(-1);
    }
    auto l = r.const_leaf();
    if (l->is_expiry()) {
        long long e = (l->expiry_ms() - art::now())/1000;
        return call.long_long(e);
    }

    return call.long_long(-2);

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

        auto t = get_art(argv[i]);
        read_lock release(t);
        auto converted = conversion::convert(k);
        art::node_ptr r = t->search(converted.get_value());
        if (r.null()) {
            return call.boolean(false);
        }
    }
    return call.boolean(true);
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
    auto t = get_art(argv[1]);
    read_lock release(t);
    auto converted = conversion::convert(k);
    art::node_ptr r = t->search(converted.get_value());
    if (r.null()) {
        return call.long_long(-1);
    }
    art::key_expire_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.syntax_error();
    }

    auto l = r.const_leaf();
    if (spec.nx) {
        if (l->is_expiry()) {
            return call.long_long(-1);
        }

    } else if (spec.xx) {
        if (!l->is_expiry()) {
            return call.long_long(-1);
        }

    } else if (spec.gt) {
        if (spec.ttl + art::now() < l->expiry_ms()) {
            return call.long_long(-1);
        }
    } else if (spec.lt) {
        if (spec.ttl + art::now() > l->expiry_ms()) {
            return call.long_long(-1);
        }
    }
    auto updater = [&t,spec](const art::node_ptr &leaf) -> art::node_ptr {
        if (leaf.null()) {
            return leaf;
        }
        auto l = leaf.const_leaf();
        if (art::now() + spec.ttl == 0) {
            art::std_log("why");
        }
        return art::make_leaf(*t, l->get_key(), l->get_value(),  art::now() + spec.ttl, l->is_volatile());
    };
    art::key_options opts{spec.ttl,true,false,false};
    if (t->update(l->get_key(),updater))
        return call.long_long(1);
    return call.long_long(-2);
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
            call.null();
        } else {
            auto converted = conversion::convert(k);
            auto t = get_art(k);
            storage_release release(t);
            art::node_ptr r = art_search(t, converted.get_value());
            if (r.null()) {
                call.null();
            } else {
                auto vt = r.const_leaf()->get_value();
                call.vt(vt);
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
int MIN(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    art::value_type the_min;
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        queue_consume(t->shard);
        t->latch.lock();
    }
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        art::node_ptr r = art_minimum(t);
        if (!t->size) continue;
        if (r.is_leaf && the_min.empty()) {
            the_min = r.const_leaf()->get_key();
        }else if (r.is_leaf && r.const_leaf()->get_key() < the_min){
            the_min = r.const_leaf()->get_key();
        }
    }
    int ok = call.ok();
    if (the_min.empty()) {
        ok = call.null();
    } else {
        ok = call.reply_encoded_key(the_min);
    }
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        t->latch.unlock();
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
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        queue_consume(shard);
        t->latch.lock();
    }
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        if (!t->size) continue;
        art::node_ptr r = art::maximum(t);
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
        ok = call.null();
    } else {
        ok = call.reply_encoded_key(the_max);
    }
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        t->latch.unlock();
    }
    return ok;
}
int cmd_MAX(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, MAX);
}
/* B.LB <key>
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int LB(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::convert(k);
    art::value_type the_lb;
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        queue_consume(shard);
        t->latch.lock();
    }
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        if (!t->size) continue;
        art::node_ptr r = art::lower_bound(t, converted.get_value());
        if (r.is_leaf && the_lb.empty()) {
            the_lb = r.const_leaf()->get_key();

        }else if (r.is_leaf && r.const_leaf()->get_key() < the_lb){
            the_lb = r.const_leaf()->get_key();
        }
    }
    int ok = call.ok();
    if (the_lb.empty()) {
        ok = call.null();
    } else {
        ok = call.reply_encoded_key(the_lb);
    }
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        t->latch.unlock();
    }
    return ok;

}
int cmd_LB(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, LB);
}
int UB(caller& call, const arg_t& argv) {
    if (argv.size() != 2)
        return call.wrong_arity();
    auto k = argv[1];
    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::convert(k);
    art::value_type the_lb;
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        queue_consume(shard);
        t->latch.lock();
    }
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        if (!t->size) continue;
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
        ok = call.null();
    } else {
        ok = call.reply_encoded_key(the_lb);
    }
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        t->latch.unlock();
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
            r = call.null();
        } else {
            auto vt = n.const_leaf()->get_value();
            r = call.vt(vt);
        }
    };

    auto t = get_art(argv[1]);
    storage_release release(t);
    t->remove(converted.get_value(), fc);

    return r;
}
int cmd_REM(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, REM);
}
/* B.SIZE
 * @return the size or o.k.a. key count.
 */
int SIZE(caller& call, const arg_t& argv) {

    if (argv.size() != 1)
        return call.wrong_arity();
    auto size = 0ll;
    for (auto shard: art::get_shard_count()) {
        auto t = get_art(shard);
        storage_release release(t);
        size += (int64_t) art_size(t);
    }
    return call.long_long(size);
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
    size_t errors = save();
    return errors ? call.error("some shards not saved"): call.simple("OK");
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
    std::vector<std::thread> loaders{art::get_shard_count().size()};
    size_t errors = 0;
    for (auto shard : art::get_shard_count()) {
        loaders[shard] = std::thread([&errors,shard]() {
            if (!get_art(shard)->load()) ++errors;
        });
    }
    for (auto &loader : loaders) {
        if (loader.joinable())
            loader.join();
    }
    return errors>0 ? call.error("some shards did not load") : call.simple("OK");
}
int START(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();

    auto interface = argv[1];
    auto port = argv[2];
    barch::server::start(interface.chars(), atoi(port.chars()));
    return call.simple("OK");
}
int PUBLISH(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto interface = argv[1];
    auto port = argv[2];
    for (auto shard: art::get_shard_count()) {
        if (!get_art(shard)->publish(interface.chars(), atoi(port.chars()))) {}
    }
    return call.simple("OK");
}
int PULL(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto interface = argv[1];
    auto port = argv[2];
    for (auto shard: art::get_shard_count()) {
        if (!get_art(shard)->pull(interface.chars(), atoi(port.chars()))) {}
    }
    return call.simple("OK");
}
int STOP(caller& call, const arg_t& ) {
    if (call.get_context() == ctx_resp) {
        return call.error("Cannot stop server");
    }
    barch::server::stop();
    return call.simple("OK");
}
int PING(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto interface = argv[1];
    auto port = argv[2];
    //barch::server::start(interface.chars(), atoi(port.chars()));
    barch::repl::client cli(interface.chars(), atoi(port.chars()), 0);
    if (!cli.ping()) {
        return call.error("could not ping");
    }
    return call.simple("OK");
}
int RETRIEVE(caller& call, const arg_t& argv) {

    if (argv.size() != 3)
        return call.wrong_arity();
    auto host = argv[1];
    auto port = argv[2];

    for (auto shard : art::get_shard_count()) {
        barch::repl::client cli(host.chars(), atoi(port.chars()), shard);
        if (!cli.load(shard)) {
            if (shard > 0) {
                for (auto s : art::get_shard_count()) {
                    get_art(s)->clear();
                }
            }

            return call.error("could not load shard - all shards cleared");
        }
    }
    return call.simple("OK");
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

int cmd_BEGIN(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc) {

    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    for (auto shard : art::get_shard_count()) {
        get_art(shard)->begin();
    }
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

int cmd_COMMIT(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc) {

    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    for (auto shard : art::get_shard_count()) {
        get_art(shard)->commit();
    }
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

int cmd_ROLLBACK(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc) {
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    for (auto shard : art::get_shard_count()) {
        get_art(shard)->rollback();
    }
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

int CLEAR(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    for (auto shard : art::get_shard_count()) {
        get_art(shard)->clear();
    }
    return call.simple("OK");
}

int cmd_CLEAR(ValkeyModuleCtx *ctx, ValkeyModuleString ** argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, CLEAR);
}

int STATS(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    art_statistics as = art::get_statistics();
    auto vbytes = 0ll;
    for (auto shard : art::get_shard_count()) {
        storage_release release(get_art(shard));
        vbytes += get_art(shard)->get_nodes().get_bytes_allocated() + get_art(shard)->get_leaves().get_bytes_allocated();
    }
    call.start_array();
    call.reply_values({"heap_bytes_allocated", get_total_memory()});
    call.reply_values({"page_bytes_compressed",as.page_bytes_compressed});
    call.reply_values({ "max_page_bytes_uncompressed", as.max_page_bytes_uncompressed});
    call.reply_values({ "last_vacuum_time", as.last_vacuum_time});
    call.reply_values({ "vacuum_count", as.vacuums_performed});
    call.reply_values({ "page_bytes_uncompressed", as.page_bytes_uncompressed});
    call.reply_values({ "bytes_addressable", as.bytes_allocated});
    call.reply_values({ "interior_bytes_addressable", as.bytes_interior});
    call.reply_values({ "leaf_nodes", as.leaf_nodes});
    call.reply_values({ "size_4_nodes", as.node4_nodes});
    call.reply_values({ "size_16_nodes", as.node16_nodes});
    call.reply_values({ "size_48_nodes", as.node48_nodes});
    call.reply_values({ "size_256_nodes", as.node256_nodes});
    call.reply_values({ "size_256_occupancy", as.node256_occupants});
    call.reply_values({ "leaf_nodes_replaced", as.leaf_nodes_replaced});
    call.reply_values({ "pages_uncompressed", as.pages_uncompressed});
    call.reply_values({ "pages_compressed", as.pages_compressed});
    call.reply_values({ "pages_evicted", as.pages_evicted});
    call.reply_values({ "keys_evicted", as.keys_evicted});
    call.reply_values({ "pages_defragged", as.pages_defragged});
    call.reply_values({ "exceptions_raised", as.exceptions_raised});
    call.reply_values({ "maintenance_cycles", as.maintenance_cycles});
    call.reply_values({ "shards", as.shards});
    call.reply_values({ "local_calls", as.local_calls});
    call.reply_values({ "max_spin", as.max_spin});
    call.reply_values({"logical_allocated", as.logical_allocated});
    call.reply_values({"oom_avoided_inserts", as.oom_avoided_inserts});
    call.reply_values({"keys_found", as.keys_found});
    call.reply_values({"queue_reorders", as.queue_reorders});
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

    art_ops_statistics as = art::get_ops_statistics();
    call.start_array();
    call.reply_values({"delete_ops", as.delete_ops});
    call.reply_values({"retrieve_ops", as.get_ops});
    call.reply_values({"insert_ops", as.insert_ops});
    call.reply_values({"iterations", as.iter_ops});
    call.reply_values({"range_iterations", as.iter_range_ops});
    call.reply_values({"lower_bound_ops", as.lb_ops});
    call.reply_values({"maximum_ops", as.max_ops});
    call.reply_values({"minimum_ops", as.min_ops});
    call.reply_values({"range_ops", as.range_ops});
    call.reply_values({"set_ops", as.set_ops});
    call.reply_values({"size_ops", as.size_ops});
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

int cmd_HEAPBYTES(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc) {
    //compressed_release release;
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);;
    auto vbytes = 0ll;
    for (auto shard : art::get_shard_count()) {
        storage_release release(get_art(shard));
        vbytes += get_art(shard)->get_nodes().get_bytes_allocated() + get_art(shard)->get_leaves().get_bytes_allocated();
    }
    return ValkeyModule_ReplyWithLongLong(ctx, (int64_t) heap::allocated + vbytes);
}

int cmd_EVICT(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc) {

    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    int64_t ev = 0;
    for (auto shard : art::get_shard_count()) {
        auto t = get_art(shard);
        storage_release release(t);
        ev += art_evict_lru(t);
    }
    return ValkeyModule_ReplyWithLongLong(ctx, (int64_t)ev );
}
int CONFIG(caller& call, const arg_t& argv) {
    if (argv.size() != 4)
        return call.wrong_arity();
    auto s = argv[1];
    if (strncmp("set", s.chars(), s.size) == 0 || strncmp("SET", s.chars(), s.size) == 0) {
        int r = art::set_configuration_value(argv[2].chars(), argv[3].chars());
        if (r == 0) {
            return call.simple("OK");
        }

    }else {
        return call.error("only SET keyword currently supported");
    }
    return call.error("could not set configuration value");
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
            call.simple(p.first.c_str());
            call.simple("function");
        }
        call.end_array(0);
        return call.simple("OK");
    }
    return call.error("unknown command");
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

    if (ValkeyModule_CreateCommand(ctx, NAME(RANGE), "readonly", 1, 2, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(COUNT), "readonly", 1, 2, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(SIZE), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
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

    if (ValkeyModule_CreateCommand(ctx, NAME(EVICT), "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
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

    if (add_hash_api(ctx) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }
    if (add_ordered_api(ctx) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }
    Constants.init(ctx);
    // valkey should free this I hope
    if (art::register_valkey_configuration(ctx) != 0) {
        return VALKEYMODULE_ERR;
    };
    if (ValkeyModule_LoadConfigs(ctx) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    for (auto shard : art::get_shard_count()) {
        if (get_art(shard) == nullptr) {
            return VALKEYMODULE_ERR;
        }
    }
    barch::server::start("0.0.0.0",14000);
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(void *unused_arg) {
    // TODO: destroy tree
    return VALKEYMODULE_OK;
}
}
