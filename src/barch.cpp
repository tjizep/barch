//
// Created by me on 11/9/24.
//

#include "vk_caller.h"
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

static auto startTime = std::chrono::high_resolution_clock::now();

struct iter_state {
    ValkeyModuleCtx *ctx;
    ValkeyModuleString **argv;
    int64_t replylen;
    int64_t count;

    iter_state(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int64_t count)
        : ctx(ctx), argv(argv), replylen(0), count(count) {
    }

    iter_state(const iter_state &is) = delete;

    iter_state &operator=(const iter_state &) = delete;

    int iterate(art::value_type key, art::value_type) {
        if (key.size == 0) {
            ///
            return -1;
        }

        if (key.bytes == nullptr) {
            return -1;
        }

        if (0 != reply_encoded_key(ctx, key)) {
            return -1;
        };

        if (replylen >= count)
            return -1;

        replylen++;
        return 0;
    }
};


extern "C" {
/* CDICT.RANGE <startkey> <endkey> <count>
*
* Return a list of matching keys, lexicographically between startkey
* and endkey. No more than 'count' items are emitted. */
int cmd_RANGE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    //read_lock rl(get_lock());
    if (argc != 4)
        return ValkeyModule_WrongArity(ctx);

    /* Parse the count argument. */
    long long count;
    if (ValkeyModule_StringToLongLong(argv[3], &count) != VALKEYMODULE_OK) {
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid count");
    }
    size_t k1len;
    size_t k2len;
    const char *k1 = ValkeyModule_StringPtrLen(argv[1], &k1len);
    const char *k2 = ValkeyModule_StringPtrLen(argv[2], &k2len);

    if (key_ok(k1, k1len) != 0)
        return key_check(ctx, k1, k1len);
    if (key_ok(k2, k2len) != 0)
        return key_check(ctx, k2, k2len);

    auto c1 = conversion::convert(k1, k1len);
    auto c2 = conversion::convert(k2, k2len);

    /* Seek the iterator. */
    iter_state is(ctx, argv, count);

    /* Reply with the matching items. */
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
    //std::function<int (void *, const unsigned char *, uint32_t , void *)>
    auto iter = [](void *data, art::value_type key, art::value_type value) -> int {
        auto *is = (iter_state *) data;

        return is->iterate(key, value);
    };
    for (auto shard : art::get_shard_count()) {
        storage_release release(get_art(shard)->latch);
        art::range(get_art(shard), c1.get_value(), c2.get_value(), iter, &is);
    }
    ValkeyModule_ReplySetArrayLength(ctx, is.replylen);

    /* Cleanup. */
    return VALKEYMODULE_OK;
}

/* CDICT.RANGE <startkey> <endkey> <count>
*
* Return a count of matching keys, lexicographically or numericallyordered between startkey
* and endkey. No more than 'count' items are emitted. */
int cmd_COUNT(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    //read_lock rl(get_lock());
    if (argc != 3)
        return ValkeyModule_WrongArity(ctx);


    size_t k1len;
    size_t k2len;
    const char *k1 = ValkeyModule_StringPtrLen(argv[1], &k1len);
    const char *k2 = ValkeyModule_StringPtrLen(argv[2], &k2len);

    if (key_ok(k1, k1len) != 0)
        return key_check(ctx, k1, k1len);
    if (key_ok(k2, k2len) != 0)
        return key_check(ctx, k2, k2len);

    auto c1 = conversion::convert(k1, k1len);
    auto c2 = conversion::convert(k2, k2len);

    /* Seek the iterator. */
    iter_state is(ctx, argv, std::numeric_limits<int64_t>::max());

    /* Reply with the matching items. */
    int64_t count = 0;
    //std::function<int (void *, const unsigned char *, uint32_t , void *)>
    auto iter = [&count](void * unused(data), art::value_type unused(key), art::value_type unused(value)) -> int {
        ++count;
        return 0;
    };
    for (auto shard : art::get_shard_count()) {
        storage_release release(get_art(shard)->latch);
        art::range(get_art(shard), c1.get_value(), c2.get_value(), iter, &is);
    }
    ValkeyModule_ReplyWithLongLong(ctx, count);

    /* Cleanup. */
    return VALKEYMODULE_OK;
}

/* CDICT.KEYS
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

        /* Cleanup. */
    }
    return call.ok();
}
int cmd_KEYS(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, KEYS);
}
/* CDICT.SET <key> <value>
 *
 * Set the specified key to the specified value. */
int SET(caller& call,const arg_t& argv) {
    if (argv.size() < 3)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    auto k = argv[1];
    auto v = argv[2];

    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::convert(k);
    art::key_spec spec(argv);
    if (spec.parse_options() != call.ok()) {
        return call.syntax_error();
    }

    art::value_type reply{"", 0};
    auto fc = [&](const art::node_ptr &) -> void {
        if (spec.get) {
            reply = converted.get_value();
        }
    };

    t->insert(spec, converted.get_value(), v, true, fc);
    if (spec.get) {
        if (reply.size) {

            return call.reply_encoded_key(reply);
        } else {
            return call.null();
        }
    } else {
        return call.boolean(1);//ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    }
}
int cmd_SET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, SET);
}
static int BarchModifyInteger(caller& call,const arg_t& argv, long long by) {

    if (argv.size() < 2)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    auto k = argv[1];
    art::key_spec spec;
    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::convert(k);
    int r = call.error();
    long long l = 0;
    auto updater = [&](const art::node_ptr &value) -> art::node_ptr {
        if (value.null()) {
            return nullptr;
        }
        r = call.ok();
        return leaf_numeric_update(l, value, by);
    };
    t->update(converted.get_value(), updater);
    if (r == call.ok()) {
        return call.long_long(l);
    } else {
        return call.null();
    }
}
unused(
static int BarchModifyDouble(caller& call,const arg_t& argv, double by) {

    if (argv.size() < 2)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    auto k = argv[1];
    art::key_spec spec;
    if (key_ok(k) != 0)
        return call.key_check_error(k);

    auto converted = conversion::convert(k);
    int r = call.error();
    double l = 0;
    auto updater = [&](const art::node_ptr &value) -> art::node_ptr {
        if (value.null()) {
            return nullptr;
        }
        auto val = leaf_numeric_update(l, value, by);
        if (!val.null()) {
            r = call.ok();
        }
        return val;
    };

    t->update(converted.get_value(), updater);
    if (r == call.ok()) {
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

int cmd_INCRBY(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
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
    double by = 0;

    if (!conversion::to_double(argv[2], by)) {
        return call.wrong_arity();
    }

    return BarchModifyInteger(call,argv, -by);
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
        storage_release release(t->latch);
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
/* CDICT.ADD <key> <value>
 *
 * Add the specified key only if its not there, with specified value. */
int ADD(caller& call, const arg_t& argv) {
    if (argv.size() != 3)
        return call.wrong_arity();
    auto t = get_art(argv[1]);
    storage_release release(t->latch);
    auto k = argv[1];
    auto v = argv[2];

    if (key_ok(k) != 0)
        return call.key_check_error(k);
    auto fc = [](art::node_ptr) -> void {
    };
    auto converted = conversion::convert(k);
    art::key_spec spec(argv);
    t->insert(spec, converted.get_value(), v, false, fc);

    return call.simple("OK");
}

int cmd_ADD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    vk_caller call;
    return call.vk_call(ctx, argv, argc, ADD);
}
/* CDICT.GET <key>
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
    storage_release release(t->latch);
    auto converted = conversion::convert(k);
    art::node_ptr r = art_search(t, converted.get_value());

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
/* CDICT.MGET <keys>
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
            storage_release release(t->latch);
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
/* CDICT.MINIMUM
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int MIN(caller& call, const arg_t& argv) {
    if (argv.size() != 1)
        return call.wrong_arity();
    art::value_type the_min;
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
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

/* CDICT.MAXIMUM
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int MAX(caller& call, const arg_t& ) {
    art::value_type the_max;
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        t->latch.lock();
    }
    for (auto shard:art::get_shard_count()) {
        auto t = get_art(shard);
        if (!t->size) continue;
        art::node_ptr r = art::maximum(t);
        if (r.is_leaf && the_max.empty()) {
            the_max = r.const_leaf()->get_key();
        }else if (r.is_leaf && the_max < r.const_leaf()->get_key()){
            the_max = r.const_leaf()->get_key();
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
/* CDICT.LB <key>
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
    storage_release release(t->latch);
    art_delete(t, converted.get_value(), fc);

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
        storage_release release(t->latch);
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
    for (auto shard : art::get_shard_count()) {
        if (!get_art(shard)->save()) {
            return call.null();
        }
    }
    return call.simple("OK");
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

    for (auto shard : art::get_shard_count()) {
        if (!get_art(shard)->load()) {
            return call.null();
        }
    }
    return call.simple("OK");
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
int STOP(caller& call, const arg_t& ) {
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

int cmd_CLEAR(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc) {
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    for (auto shard : art::get_shard_count()) {
        get_art(shard)->clear();
    }
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}


/* B.STATISTICS
 *
 * get memory statistics. */
int cmd_STATS(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc) {
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);

    long row_count = 0;
    art_statistics as = art::get_statistics();
    auto vbytes = 0ll;
    for (auto shard : art::get_shard_count()) {
        storage_release release(get_art(shard)->latch);
        vbytes += get_art(shard)->get_nodes().get_bytes_allocated() + get_art(shard)->get_leaves().get_bytes_allocated();
    }
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "heap_bytes_allocated");
    ValkeyModule_ReplyWithLongLong(ctx, as.heap_bytes_allocated + vbytes);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "page_bytes_compressed");
    ValkeyModule_ReplyWithLongLong(ctx, as.page_bytes_compressed);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "max_page_bytes_uncompressed");
    ValkeyModule_ReplyWithLongLong(ctx, as.max_page_bytes_uncompressed);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "last_vacuum_time");
    ValkeyModule_ReplyWithLongLong(ctx, as.last_vacuum_time);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "vacuum_count");
    ValkeyModule_ReplyWithLongLong(ctx, as.vacuums_performed);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "page_bytes_uncompressed");
    ValkeyModule_ReplyWithLongLong(ctx, as.page_bytes_uncompressed);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "bytes_addressable");
    ValkeyModule_ReplyWithLongLong(ctx, as.bytes_allocated);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "interior_bytes_addressable");
    ValkeyModule_ReplyWithLongLong(ctx, as.bytes_interior);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "leaf_nodes");
    ValkeyModule_ReplyWithLongLong(ctx, as.leaf_nodes);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "size_4_nodes");
    ValkeyModule_ReplyWithLongLong(ctx, as.node4_nodes);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "size_16_nodes");
    ValkeyModule_ReplyWithLongLong(ctx, as.node16_nodes);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "size_48_nodes");
    ValkeyModule_ReplyWithLongLong(ctx, as.node48_nodes);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "size_256_nodes");
    ValkeyModule_ReplyWithLongLong(ctx, as.node256_nodes);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "size_256_occupancy");
    ValkeyModule_ReplyWithLongLong(ctx, as.node256_occupants);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "leaf_nodes_replaced");
    ValkeyModule_ReplyWithLongLong(ctx, as.leaf_nodes_replaced);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "pages_uncompressed");
    ValkeyModule_ReplyWithLongLong(ctx, as.pages_uncompressed);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "pages_compressed");
    ValkeyModule_ReplyWithLongLong(ctx, as.pages_compressed);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "pages_evicted");
    ValkeyModule_ReplyWithLongLong(ctx, as.pages_evicted);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "keys_evicted");
    ValkeyModule_ReplyWithLongLong(ctx, as.keys_evicted);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "pages_defragged");
    ValkeyModule_ReplyWithLongLong(ctx, as.pages_defragged);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "exceptions_raised");
    ValkeyModule_ReplyWithLongLong(ctx, as.exceptions_raised);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ++row_count;
    ValkeyModule_ReplySetArrayLength(ctx, row_count);
    return 0;
}

/* B.STATISTICS
 *
 * get memory statistics. */
int cmd_OPS(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc) {
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);

    art_ops_statistics as = art::get_ops_statistics();
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "delete_ops");
    ValkeyModule_ReplyWithLongLong(ctx, as.delete_ops);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "retrieve_ops");
    ValkeyModule_ReplyWithLongLong(ctx, as.get_ops);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "insert_ops");
    ValkeyModule_ReplyWithLongLong(ctx, as.insert_ops);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "iterations");
    ValkeyModule_ReplyWithLongLong(ctx, as.iter_ops);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "range_iterations");
    ValkeyModule_ReplyWithLongLong(ctx, as.iter_range_ops);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "lower_bound_ops");
    ValkeyModule_ReplyWithLongLong(ctx, as.lb_ops);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "maximum_ops");
    ValkeyModule_ReplyWithLongLong(ctx, as.max_ops);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "minimum_ops");
    ValkeyModule_ReplyWithLongLong(ctx, as.min_ops);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "range_ops");
    ValkeyModule_ReplyWithLongLong(ctx, as.range_ops);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "set_ops");
    ValkeyModule_ReplyWithLongLong(ctx, as.set_ops);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "size_ops");
    ValkeyModule_ReplyWithLongLong(ctx, as.size_ops);
    ValkeyModule_ReplySetArrayLength(ctx, 2);
    ValkeyModule_ReplySetArrayLength(ctx, 11);
    return 0;
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
        storage_release release(get_art(shard)->latch);
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
        storage_release release(t->latch);
        ev += art_evict_lru(t);
    }
    return ValkeyModule_ReplyWithLongLong(ctx, (int64_t)ev );
}

/* B.SET <key> <value>
 *
 * Set the specified key to the specified value. */
int cmd_CONFIG(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    if (argc != 4)
        return ValkeyModule_WrongArity(ctx);
    size_t slen;
    const char *s = ValkeyModule_StringPtrLen(argv[1], &slen);
    if (strncmp("set", s, slen) == 0 || strncmp("SET", s, slen) == 0) {
        int r = art::set_configuration_value(argv[2], argv[3]);
        if (r == VALKEYMODULE_OK) {
            return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        }
        return r;
    }
    return VALKEYMODULE_ERR;
}

/* This function must be present on each module. It is used in order to
 * register the commands into the server. */
int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **, int) {
    if (ValkeyModule_Init(ctx, "B", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(SET), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(INCR), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(DECR), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(INCRBY), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(DECRBY), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(MSET), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(ADD), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, NAME(GET), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
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
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(void *unused_arg) {
    // TODO: destroy tree
    return VALKEYMODULE_OK;
}
}
