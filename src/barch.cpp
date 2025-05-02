//
// Created by me on 11/9/24.
//

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

static auto startTime = std::chrono::high_resolution_clock::now();

struct iter_state
{
    ValkeyModuleCtx* ctx;
    ValkeyModuleString** argv;
    int64_t replylen;
    int64_t count;

    iter_state(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int64_t count)
        : ctx(ctx), argv(argv), replylen(0), count(count)
    {
    }

    iter_state(const iter_state& is) = delete;
    iter_state& operator=(const iter_state&) = delete;

    int iterate(art::value_type key, art::value_type)
    {
        if (key.size == 0)
        {
            ///
            return -1;
        }

        if (key.bytes == nullptr)
        {
            return -1;
        }

        if (0 != reply_encoded_key(ctx, key))
        {
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
int cmd_RANGE(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ValkeyModule_AutoMemory(ctx);
    compressed_release release;

    //read_lock rl(get_lock());
    if (argc != 4)
        return ValkeyModule_WrongArity(ctx);

    /* Parse the count argument. */
    long long count;
    if (ValkeyModule_StringToLongLong(argv[3], &count) != VALKEYMODULE_OK)
    {
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid count");
    }
    size_t k1len;
    size_t k2len;
    const char* k1 = ValkeyModule_StringPtrLen(argv[1], &k1len);
    const char* k2 = ValkeyModule_StringPtrLen(argv[2], &k2len);

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
    auto iter = [](void* data, art::value_type key, art::value_type value) -> int
    {
        auto* is = (iter_state*)data;

        return is->iterate(key, value);
    };

    art::range(get_art(), c1.get_value(), c2.get_value(), iter, &is);


    ValkeyModule_ReplySetArrayLength(ctx, is.replylen);

    /* Cleanup. */
    return VALKEYMODULE_OK;
}
    /* CDICT.RANGE <startkey> <endkey> <count>
    *
    * Return a count of matching keys, lexicographically or numericallyordered between startkey
    * and endkey. No more than 'count' items are emitted. */
    int cmd_COUNT(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
    {
        ValkeyModule_AutoMemory(ctx);
        compressed_release release;

        //read_lock rl(get_lock());
        if (argc != 3)
            return ValkeyModule_WrongArity(ctx);


        size_t k1len;
        size_t k2len;
        const char* k1 = ValkeyModule_StringPtrLen(argv[1], &k1len);
        const char* k2 = ValkeyModule_StringPtrLen(argv[2], &k2len);

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
        auto iter = [&count](void* unused(data), art::value_type unused(key), art::value_type unused(value)) -> int
        {
            ++count;
            return 0;
        };

        art::range(get_art(), c1.get_value(), c2.get_value(), iter, &is);

        ValkeyModule_ReplyWithLongLong(ctx, count);

        /* Cleanup. */
        return VALKEYMODULE_OK;
    }

/* CDICT.KEYS
*
* match against all keys using a glob pattern
* */
int cmd_KEYS(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ValkeyModule_AutoMemory(ctx);

    if (argc < 2 || argc > 4)
        return ValkeyModule_WrongArity(ctx);

    art::keys_spec spec(argv, argc);
    if (spec.parse_keys_options() != VALKEYMODULE_OK)
    {
        return ValkeyModule_WrongArity(ctx);
    }
    std::mutex vklock{};
    std::atomic<int64_t> replies = 0;
    size_t plen;
    const char* cpat = ValkeyModule_StringPtrLen(argv[1], &plen);
    art::value_type pattern{cpat, (unsigned)plen};
    if (spec.count)
    {
        art::glob(get_art(), spec, pattern, [&](const art::leaf& unused(l)) -> bool
        {
            ++replies;
            return true;
        });
        return ValkeyModule_ReplyWithLongLong(ctx, replies);
    }
    else
    {
        /* Reply with the matching items. */
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);


        art::glob(get_art(), spec, pattern, [&](const art::leaf& l) -> bool
        {
            std::lock_guard lk(vklock); // because there's worker threads concurrently calling here
            if (0 != reply_encoded_key(ctx, l.get_key()))
            {
                return false;
            };
            ++replies;
            return true;
        });


        ValkeyModule_ReplySetArrayLength(ctx, replies);

        /* Cleanup. */
    }
    return VALKEYMODULE_OK;
}

/* CDICT.SET <key> <value>
 *
 * Set the specified key to the specified value. */
int cmd_SET(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{

    compressed_release release;
    if (argc < 3)
        return ValkeyModule_WrongArity(ctx);
    auto *t = get_art();

    size_t klen, vlen;
    const char* k = ValkeyModule_StringPtrLen(argv[1], &klen);
    const char* v = ValkeyModule_StringPtrLen(argv[2], &vlen);

    if (key_ok(k, klen) != 0)
        return key_check(ctx, k, klen);

    auto converted = conversion::convert(k, klen);
    t->default_options.set(argv, argc);
    if (t->default_options.parse_options() != VALKEYMODULE_OK)
    {
        return ValkeyModule_WrongArity(ctx);
    }

    //write_lock wl(get_lock());
    art::value_type reply{"", 0};
    auto fc = [&](const art::node_ptr&) -> void
    {
        if (t->default_options.get)
        {
            reply = converted.get_value();
        }
    };

    art_insert(t, converted.get_value(), {v, (unsigned)vlen}, fc);

    if (t->default_options.get)
    {
        if (reply.size)
        {
            ValkeyModule_AutoMemory(ctx);
            t->default_options.clear();
            return reply_encoded_key(ctx, reply);
        }
        else
        {   t->default_options.clear();
            return ValkeyModule_ReplyWithNull(ctx);
        }
    }
    else
    {   t->default_options.clear();
        return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    }
}
static int BarchMofifyInteger(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc, long long by)
{
    compressed_release release;
    if (argc != 2)
        return ValkeyModule_WrongArity(ctx);
    size_t klen;
    const char* k = ValkeyModule_StringPtrLen(argv[1], &klen);
    art::key_spec spec;
    if (key_ok(k, klen) != 0)
        return key_check(ctx, k, klen);

    auto converted = conversion::convert(k, klen);
    //write_lock wl(get_lock());
    int r = VALKEYMODULE_ERR;
    long long l = 0;
    auto updater = [&](const art::node_ptr& value) -> art::node_ptr
    {
        if (value.null())
        {
            return nullptr;
        }
        r = VALKEYMODULE_OK;
        return leaf_numeric_update(l,value, by);
    };
    art::update(get_art(), converted.get_value(),updater);
    if (r == VALKEYMODULE_OK)
    {
        return ValkeyModule_ReplyWithLongLong(ctx, l);
    }else
    {
        return ValkeyModule_ReplyWithNull(ctx);
    }

}

int cmd_INCR(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ++statistics::incr_ops;
    return BarchMofifyInteger(ctx, argv, argc, 1);
}
int cmd_INCRBY(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ++statistics::incr_ops;
    if (argc != 3)
        return ValkeyModule_WrongArity(ctx);
    long long by = 0;

    if (ValkeyModule_StringToLongLong(argv[2], &by) != VALKEYMODULE_OK)
    {
        return ValkeyModule_WrongArity(ctx);
    }

    return BarchMofifyInteger(ctx, argv, 2, by);
}

int cmd_DECR(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ++statistics::decr_ops;
    return BarchMofifyInteger(ctx, argv, argc, -1);
}
    int cmd_DECRBY(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ++statistics::incr_ops;
    if (argc != 3)
        return ValkeyModule_WrongArity(ctx);
    long long by = 0;

    if (ValkeyModule_StringToLongLong(argv[2], &by) != VALKEYMODULE_OK)
    {
        return ValkeyModule_WrongArity(ctx);
    }

    return BarchMofifyInteger(ctx, argv, 2, -by);
}

int cmd_MSET(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ValkeyModule_AutoMemory(ctx);
    compressed_release release;
    if (argc < 3)
        return ValkeyModule_WrongArity(ctx);
    int responses = 0;
    int r = VALKEYMODULE_OK;
    for (int n = 1; n < argc; n+=2)
    {
        size_t klen, vlen;
        const char* k = ValkeyModule_StringPtrLen(argv[n], &klen);
        const char* v = ValkeyModule_StringPtrLen(argv[n + 1], &vlen);

        if (key_ok(k, klen) != 0)
        {
            r |= ValkeyModule_ReplyWithNull(ctx);
            ++responses;
            continue;
        }

        auto converted = conversion::convert(k, klen);
        art::value_type reply{"", 0};
        auto fc = [&](art::node_ptr) -> void
        {
        };

        art_insert(get_art(), converted.get_value(), {v, (unsigned)vlen}, fc);

        ++responses;
    }
    ValkeyModule_ReplyWithBool(ctx, true);
    return 0;
}

/* CDICT.ADD <key> <value>
 *
 * Add the specified key only if its not there, with specified value. */
int cmd_ADD(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ValkeyModule_AutoMemory(ctx);
    compressed_release release;
    if (argc != 3)
        return ValkeyModule_WrongArity(ctx);

    size_t klen, vlen;
    const char* k = ValkeyModule_StringPtrLen(argv[1], &klen);
    const char* v = ValkeyModule_StringPtrLen(argv[2], &vlen);

    if (key_ok(k, klen) != 0)
        return key_check(ctx, k, klen);
    auto fc = [](art::node_ptr) -> void
    {
    };
    auto* t= get_art();
    auto converted = conversion::convert(k, klen);
    t->default_options.set(argv, argc);
    write_lock w(get_lock());
    art_insert_no_replace(t, converted.get_value(), {v, (unsigned)vlen}, fc);

    return ValkeyModule_ReplyWithString(ctx,Constants.OK);
}

/* CDICT.GET <key>
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int cmd_GET(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ValkeyModule_AutoMemory(ctx);

    if (argc != 2)
        return ValkeyModule_WrongArity(ctx);
    size_t klen;
    const char* k = ValkeyModule_StringPtrLen(argv[1], &klen);
    if (key_ok(k, klen) != 0)
        return key_check(ctx, k, klen);

    compressed_release release;
    auto converted = conversion::convert(k, klen);
    art::node_ptr r = art_search(get_art(), converted.get_value());

    if (r.null())
    {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    else
    {
        auto vt = r.const_leaf()->get_value();
        // Note: replying with only the string as a long long (which is 8 bytes) doubles performance
        // i.e. like this: ValkeyModule_ReplyWithLongLong(ctx,*(long long *)vt.chars());
        //
        return ValkeyModule_ReplyWithStringBuffer(ctx,vt.chars(),vt.size);
    }
}
/* CDICT.MGET <keys>
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int cmd_MGET(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ValkeyModule_AutoMemory(ctx);
    compressed_release release;
    if (argc < 2)
        return ValkeyModule_WrongArity(ctx);
    int responses = 0;
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    for (int arg = 1; arg < argc; ++arg)
    {
        size_t klen;
        const char* k = ValkeyModule_StringPtrLen(argv[arg], &klen);
        if (key_ok(k, klen) != 0)
        {
            ValkeyModule_ReplyWithNull(ctx);
        }else
        {
            auto converted = conversion::convert(k, klen);
            //read_lock rl(get_lock());
            art::node_ptr r = art_search(get_art(), converted.get_value());
            if (r.null())
            {
                ValkeyModule_ReplyWithNull(ctx);
            }
            else
            {
                auto vt = r.const_leaf()->get_value();
                auto* val = ValkeyModule_CreateString(ctx, vt.chars(), vt.size);

                ValkeyModule_ReplyWithString(ctx, val);
            }
            ++responses;
        }

    }
    ValkeyModule_ReplySetArrayLength(ctx, responses);
    return VALKEYMODULE_OK;
}

/* CDICT.MINIMUM
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int cmd_MIN(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    ValkeyModule_AutoMemory(ctx);
    compressed_release release;
    //read_lock rl(get_lock());
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);

    art::node_ptr r = art_minimum(get_art());

    if (r.null())
    {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    else
    {
        const auto* l = r.const_leaf();
        auto found = l->get_key();
        return reply_encoded_key(ctx, found);
    }
}

int cmd_MILLIS(ValkeyModuleCtx* ctx, ValkeyModuleString**, int)
{
    auto t = std::chrono::high_resolution_clock::now();
    const auto d = std::chrono::duration_cast<std::chrono::milliseconds>(t - startTime);
    return ValkeyModule_ReplyWithLongLong(ctx, d.count());
}

/* CDICT.MAXIMUM
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int cmd_MAX(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    ValkeyModule_AutoMemory(ctx);
    compressed_release release;
    //read_lock rl(get_lock());
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);

    art::node_ptr l = art::maximum(get_art());

    if (l.null())
    {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    else
    {
        return reply_encoded_key(ctx, l.const_leaf()->get_key());
    }
}

/* CDICT.LB <key>
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int cmd_LB(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ValkeyModule_AutoMemory(ctx);
    compressed_release release;
    if (argc != 2)
        return ValkeyModule_WrongArity(ctx);
    size_t klen;
    const char* k = ValkeyModule_StringPtrLen(argv[1], &klen);
    if (key_ok(k, klen) != 0)
        return key_check(ctx, k, klen);

    auto converted = conversion::convert(k, klen);
    //read_lock rl(get_lock());

    art::node_ptr r = art::lower_bound(get_art(), converted.get_value());


    if (r.null())
    {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    else
    {
        const auto* l = r.const_leaf();
        auto k = l->get_key();
        return reply_encoded_key(ctx, k);
    }
}

/* B.RM <key>
 *
 * remove the value associated with the key and return the key if such a key existed. */
int cmd_REM(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ValkeyModule_AutoMemory(ctx);
    compressed_release release;
    if (argc != 2)
        return ValkeyModule_WrongArity(ctx);
    size_t klen;
    const char* k = ValkeyModule_StringPtrLen(argv[1], &klen);

    if (key_ok(k, klen) != 0)
        return key_check(ctx, k, klen);


    auto converted = conversion::convert(k, klen);
    int r = 0;
    auto fc = [&r,ctx](art::node_ptr n) -> void
    {
        if (n.null())
        {
            r = ValkeyModule_ReplyWithNull(ctx);
        }
        else
        {
            auto vt = n.const_leaf()->get_value();
            r = ValkeyModule_ReplyWithString(ctx, ValkeyModule_CreateString(ctx, vt.chars(), vt.size));
        }
    };
    //write_lock w(get_lock());
    auto t = get_art();

    art_delete(t, converted.get_value(), fc);

    return r;
}

/* B.SIZE
 * @return the size or o.k.a. key count.
 */
int cmd_SIZE(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    compressed_release release;
    //read_lock rl(get_lock());

    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    auto size = (int64_t)art_size(get_art());
    return ValkeyModule_ReplyWithLongLong(ctx, size);
}

/* B.SAVE
 * saves the data to files called leaf_data.dat and node_data.dat in the current directory
 * @return OK if successful
 */
int cmd_SAVE(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    if (!get_art()->save())
    {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

/* B.LOAD
 * loads and overwrites the data from files called leaf_data.dat and node_data.dat in the current directory
 * @return OK if successful
 */
int cmd_LOAD(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    compressed_release release;
    //write_lock rl(get_lock());

    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    if (!get_art()->load
        ())
    {
        return ValkeyModule_ReplyWithNull(ctx);
    }
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}
int cmd_BEGIN(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    compressed_release release;
    //write_lock rl(get_lock());

    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    get_art()->begin();
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

int cmd_COMMIT(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    compressed_release release;
    //write_lock rl(get_lock());

    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    get_art()->commit();
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}
int cmd_ROLLBACK(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    compressed_release release;
    //write_lock rl(get_lock());

    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    get_art()->rollback();
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}
int cmd_CLEAR(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    compressed_release release;
    //write_lock rl(get_lock());

    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    get_art()->clear();
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}


/* B.STATISTICS
 *
 * get memory statistics. */
int cmd_STATS(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    compressed_release release;
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);

    long row_count = 0;
    art_statistics as = art::get_statistics();
    auto vbytes = art::get_node_allocator().get_bytes_allocated() + art::get_leaf_allocation().get_bytes_allocated();
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
int cmd_OPS(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    compressed_release release;
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

int cmd_VACUUM(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    compressed_release release;
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    size_t result = art::get_leaf_allocation().vacuum();
    return ValkeyModule_ReplyWithLongLong(ctx, (int64_t)result);
}

int cmd_HEAPBYTES(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    //compressed_release release;
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);;
    auto vbytes = art::get_node_allocator().get_bytes_allocated() + art::get_leaf_allocation().get_bytes_allocated();

    return ValkeyModule_ReplyWithLongLong(ctx, (int64_t)heap::allocated + vbytes);
}

int cmd_EVICT(ValkeyModuleCtx* ctx, ValkeyModuleString**, int argc)
{
    compressed_release release;
    //write_lock w(get_lock());
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    auto t = get_art();
    return ValkeyModule_ReplyWithLongLong(ctx, (int64_t)art_evict_lru(t));
}

/* B.SET <key> <value>
 *
 * Set the specified key to the specified value. */
int cmd_CONFIG(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc)
{
    ValkeyModule_AutoMemory(ctx);
    compressed_release release;
    if (argc != 4)
        return ValkeyModule_WrongArity(ctx);
    //write_lock w(get_lock());
    size_t slen;
    const char* s = ValkeyModule_StringPtrLen(argv[1], &slen);
    if (strncmp("set", s, slen) == 0 || strncmp("SET", s, slen) == 0)
    {
        int r = art::set_configuration_value(argv[2], argv[3]);
        if (r == VALKEYMODULE_OK)
        {
            return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        }
        return r;
    }
    return VALKEYMODULE_ERR;
}

/* This function must be present on each module. It is used in order to
 * register the commands into the server. */
int ValkeyModule_OnLoad(ValkeyModuleCtx* ctx, ValkeyModuleString**, int)
{
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

    if (add_hash_api(ctx) != VALKEYMODULE_OK)
    {
        return VALKEYMODULE_ERR;
    }
    if (add_ordered_api(ctx) != VALKEYMODULE_OK)
    {
        return VALKEYMODULE_ERR;
    }
    Constants.init(ctx);
    // valkey should free this I hope
    if (art::register_valkey_configuration(ctx) != 0)
    {
        return VALKEYMODULE_ERR;
    };
    if (ValkeyModule_LoadConfigs(ctx) == VALKEYMODULE_ERR)
    {
        return VALKEYMODULE_ERR;
    }

    if (get_art() == nullptr)
    {
        return VALKEYMODULE_ERR;
    }

    if (!art::init_leaf_compression())
        return VALKEYMODULE_ERR;

    if (!art::init_node_compression())
        return VALKEYMODULE_ERR;
    try
    {
        get_art()->load();
    }catch (std::exception& e)
    {
        art::std_log(e.what());
        return VALKEYMODULE_ERR;
    }
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(void*unused_arg)
{
    art::destroy_leaf_compression();
    art::destroy_node_compression();
    return VALKEYMODULE_OK;
}
}
