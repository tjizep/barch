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
extern "C"
{
#include "valkeymodule.h"
}

#include <cctype>
#include <cstring>
#include <cmath>
#include <shared_mutex>
#include "conversion.h"
#include "art.h"
#include "configuration.h"
#include <fast_float/fast_float.h>
#include <functional>
#include "keyspec.h"

static ValkeyModuleDict *Keyspace{};
static std::shared_mutex shared{};

std::shared_mutex& get_lock()
{
    return shared;
}


static art::tree * ad {};
static auto startTime = std::chrono::high_resolution_clock::now();

/// @brief  getting an initialized art tree
/// @param ctx not used but could be 
/// @return a once initialized art_tree

static art::tree *get_art()
{
    if (ad == nullptr)
    {
        if (ad == nullptr)
        {
            ad = new (heap::allocate<art::tree>(1)) art::tree(nullptr, 0);
        }

    }
    return ad;
}


static int key_ok(const char * k, size_t klen){
    if (k == nullptr)
        return -1;

    if (klen == 0) 
        return -1;

    return 0;

}

static int key_check(ValkeyModuleCtx *ctx, const char * k, size_t klen){

    if (k == nullptr)
        return ValkeyModule_ReplyWithError(ctx, "No null keys");

    if (klen == 0) 
        return ValkeyModule_ReplyWithError(ctx, "No empty keys");


    return ValkeyModule_ReplyWithError(ctx, "Unspecified key error");
}

static int reply_encoded_key(ValkeyModuleCtx* ctx, art::value_type key){
    double dk;
    int64_t ik;
    const char * k;
    size_t kl;
    const unsigned char * enck = key.bytes;
    unsigned key_len = key.length();
    if( key_len == 10 && (*enck == conversion::tinteger || *enck == conversion::tdouble)){
        ik = conversion::enc_bytes_to_int(enck, key_len);
        if (*enck == 1) {
            memcpy(&dk, &ik, sizeof(ik));
            if (ValkeyModule_ReplyWithDouble(ctx, dk) == VALKEYMODULE_ERR) {
                return -1;    
            }
        } else {
            if (ValkeyModule_ReplyWithLongLong(ctx, ik) == VALKEYMODULE_ERR) {
                return -1;    
            }
        }
    } else if ( key_len >= 1  && *enck == conversion::tstring) { //&& *enck == 2 //it's a string

        k = (const char*) &enck[1];
        kl = key_len - 1;
        if (ValkeyModule_ReplyWithStringBuffer(ctx, k, kl) == VALKEYMODULE_ERR) {
            return -1;
        }
    } else
    {
        abort();
    }
    return 0;
}
struct iter_state {
    ValkeyModuleCtx *ctx;
    ValkeyModuleString **argv;
    int64_t replylen;
    int64_t count;

    iter_state(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int64_t count)
    :   ctx(ctx), argv(argv), replylen(0), count(count)
    {}

    iter_state(const iter_state& is) = delete; 
    iter_state& operator=(const iter_state& ) = delete;

    int iterate(art::value_type key, art::value_type) {
        if(key.size == 0) { ///
            return -1;
        }
        
        if (key.bytes == nullptr){
            return -1;
        }
        
        if(0 != reply_encoded_key(ctx, key)){
            return -1;
        };

        if (replylen >= count)
            return -1;

        replylen++;
        return 0;
    }
};
extern "C" {
    /* CDICT.KEYRANGE <startkey> <endkey> <count>
    *
    * Return a list of matching keys, lexicographically between startkey
    * and endkey. No more than 'count' items are emitted. */
    int cmd_RANGE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        ValkeyModule_AutoMemory(ctx);
        compressed_release release;

        read_lock rl(get_lock());
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
            auto * is = (iter_state*)data;
                
            return is->iterate(key, value);
            
        };

        art::range(get_art(), c1.get_value(), c2.get_value(), iter, &is);


        ValkeyModule_ReplySetArrayLength(ctx, is.replylen);

        /* Cleanup. */
        return VALKEYMODULE_OK;
    }

    /* CDICT.SET <key> <value>
     *
     * Set the specified key to the specified value. */
    int cmd_SET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        ValkeyModule_AutoMemory(ctx);
        compressed_release release;
        if (argc < 3)
            return ValkeyModule_WrongArity(ctx);
        //ValkeyModule_DictSet(Keyspace, argv[1], argv[2]);
        size_t klen,vlen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        const char *v = ValkeyModule_StringPtrLen(argv[2], &vlen);

        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        
        auto converted = conversion::convert(k, klen);
        art::key_spec spec(argv,argc);
        if (spec.parse_options() != VALKEYMODULE_OK)
        {
            return ValkeyModule_WrongArity(ctx);
        }
        write_lock wl(get_lock());
        art::value_type reply{"",0};
        auto fc = [&](art::node_ptr ) -> void
        {
            if (spec.get)
            {
                reply = converted.get_value();
            }
        };

        art_insert(get_art(), spec, converted.get_value(), {v,(unsigned)vlen}, fc);
        if (spec.get)
        {
            if (reply.size)
            {
                return reply_encoded_key(ctx, reply);
            }else
            {
                return ValkeyModule_ReplyWithNull(ctx);
            }
        }else
        {
            return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        }

    }

    /* CDICT.ADD <key> <value>
     *
     * Add the specified key only if its not there, with specified value. */
    int cmd_ADD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        ValkeyModule_AutoMemory(ctx);
        compressed_release release;
        if (argc != 3)
            return ValkeyModule_WrongArity(ctx);

        size_t klen,vlen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        const char *v = ValkeyModule_StringPtrLen(argv[2], &vlen);

        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        auto fc = [](art::node_ptr) -> void {};
        auto converted = conversion::convert(k, klen);
        art::key_spec spec(argv,argc);
        write_lock w(get_lock());
        art_insert_no_replace(get_art(), spec, converted.get_value(), {v,(unsigned)vlen},fc);

        return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    }

    /* CDICT.GET <key>
     *
     * Return the value of the specified key, or a null reply if the key
     * is not defined. */
    int cmd_GET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        ValkeyModule_AutoMemory(ctx);
        compressed_release release;
        if (argc != 2)
            return ValkeyModule_WrongArity(ctx);
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        
        auto converted = conversion::convert(k, klen);
        art::trace_list trace;
        trace.reserve(klen);
        read_lock rl(get_lock());
        art::node_ptr r = art_search(trace, get_art(), converted.get_value());
        if (r.null())
        {
            return ValkeyModule_ReplyWithNull(ctx);
        }
        else
        {
            auto vt = r.const_leaf()->get_value();
            auto *val = ValkeyModule_CreateString(ctx, vt.chars(), vt.size);

            return ValkeyModule_ReplyWithString(ctx, val);
        }
    }
    /* CDICT.MINIMUM
     *
     * Return the value of the specified key, or a null reply if the key
     * is not defined. */
    int cmd_MIN(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {   ValkeyModule_AutoMemory(ctx);
        compressed_release release;
        read_lock rl(get_lock());
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

    int cmd_MILLIS(ValkeyModuleCtx *ctx, ValkeyModuleString **, int )
    {

        auto t = std::chrono::high_resolution_clock::now();
        const auto d = std::chrono::duration_cast<std::chrono::milliseconds>(t - startTime);
        return ValkeyModule_ReplyWithLongLong(ctx,d.count());

    }

    /* CDICT.MAXIMUM
     *
     * Return the value of the specified key, or a null reply if the key
     * is not defined. */
    int cmd_MAX(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {
        ValkeyModule_AutoMemory(ctx);
        compressed_release release;
        read_lock rl(get_lock());
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
    int cmd_LB(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        ValkeyModule_AutoMemory(ctx);
        compressed_release release;
        if (argc != 2)
            return ValkeyModule_WrongArity(ctx);
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        
        auto converted = conversion::convert(k, klen);
        read_lock rl(get_lock());

        art::node_ptr r = art::lower_bound(get_art(), converted.get_value());


        if (r.null())
        {
            return ValkeyModule_ReplyWithNull(ctx);
        }
        else
        {
            const auto * l = r. const_leaf();
            auto *ms = ValkeyModule_CreateString(ctx, l->s(), l->key_len);
            return ValkeyModule_ReplyWithString(ctx, ms);
        }
    }

    /* B.RM <key>
     *
     * remove the value associated with the key and return the key if such a key existed. */
    int cmd_REM(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        ValkeyModule_AutoMemory(ctx);
        compressed_release release;
        if (argc != 2)
            return ValkeyModule_WrongArity(ctx);
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        
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
                auto vt =n.const_leaf()->get_value();
                r = ValkeyModule_ReplyWithString(ctx, ValkeyModule_CreateString(ctx, vt.chars(), vt.size));
            }

        };
        write_lock w(get_lock());
        auto t = get_art();

        art_delete(t, converted.get_value(), fc);

        return r;

    }
    /* B.SIZE
     * @return the size or o.k.a. key count. 
     */
    int cmd_SIZE(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {
        compressed_release release;
        read_lock rl(get_lock());

        if (argc != 1)
            return ValkeyModule_WrongArity(ctx);
        auto size = (int64_t)art_size(get_art());
        return ValkeyModule_ReplyWithLongLong(ctx, size);
        
    }

    /* B.STATISTICS
     *
     * get memory statistics. */
    int cmd_STATS(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {
        compressed_release release;
        if (argc != 1)
            return ValkeyModule_WrongArity(ctx);

        long row_count = 0;
        art_statistics as = art_get_statistics();
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN); 
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "heap_bytes_allocated");
        ValkeyModule_ReplyWithLongLong(ctx,as.heap_bytes_allocated);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "page_bytes_compressed");
        ValkeyModule_ReplyWithLongLong(ctx,as.page_bytes_compressed);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "max_page_bytes_uncompressed");
        ValkeyModule_ReplyWithLongLong(ctx,as.max_page_bytes_uncompressed);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "last_vacuum_time");
        ValkeyModule_ReplyWithLongLong(ctx,as.last_vacuum_time);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "vacuum_count");
        ValkeyModule_ReplyWithLongLong(ctx,as.vacuums_performed);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "page_bytes_uncompressed");
        ValkeyModule_ReplyWithLongLong(ctx,as.page_bytes_uncompressed);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "bytes_addressable");
        ValkeyModule_ReplyWithLongLong(ctx,as.bytes_allocated);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "interior_bytes_addressable");
        ValkeyModule_ReplyWithLongLong(ctx,as.bytes_interior);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "leaf_nodes");
        ValkeyModule_ReplyWithLongLong(ctx,as.leaf_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "size_4_nodes");
        ValkeyModule_ReplyWithLongLong(ctx,as.node4_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "size_16_nodes");
        ValkeyModule_ReplyWithLongLong(ctx,as.node16_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN); 
        ValkeyModule_ReplyWithSimpleString(ctx, "size_48_nodes");
        ValkeyModule_ReplyWithLongLong(ctx,as.node48_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "size_256_nodes");
        ValkeyModule_ReplyWithLongLong(ctx,as.node256_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "size_256_occupancy");
        ValkeyModule_ReplyWithLongLong(ctx,as.node256_occupants);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "leaf_nodes_replaced");
        ValkeyModule_ReplyWithLongLong(ctx,as.leaf_nodes_replaced);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "pages_uncompressed");
        ValkeyModule_ReplyWithLongLong(ctx,as.pages_uncompressed);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "pages_compressed");
        ValkeyModule_ReplyWithLongLong(ctx,as.pages_compressed);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "pages_evicted");
        ValkeyModule_ReplyWithLongLong(ctx,as.pages_evicted);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "keys_evicted");
        ValkeyModule_ReplyWithLongLong(ctx,as.keys_evicted);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "pages_defragged");
        ValkeyModule_ReplyWithLongLong(ctx,as.pages_defragged);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "exceptions_raised");
        ValkeyModule_ReplyWithLongLong(ctx,as.exceptions_raised);
        ValkeyModule_ReplySetArrayLength(ctx, 2);++row_count;
        ValkeyModule_ReplySetArrayLength(ctx, row_count);
        return 0;
    }

    /* B.STATISTICS
     *
     * get memory statistics. */
    int cmd_OPS(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {   compressed_release release;
        if (argc != 1)
            return ValkeyModule_WrongArity(ctx);

        art_ops_statistics as = art_get_ops_statistics();
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN); 
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "delete_ops");
        ValkeyModule_ReplyWithLongLong(ctx,as.delete_ops);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "retrieve_ops");
        ValkeyModule_ReplyWithLongLong(ctx,as.get_ops);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "insert_ops");
        ValkeyModule_ReplyWithLongLong(ctx,as.insert_ops);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "iterations");
        ValkeyModule_ReplyWithLongLong(ctx,as.iter_ops);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN); 
        ValkeyModule_ReplyWithSimpleString(ctx, "range_iterations");
        ValkeyModule_ReplyWithLongLong(ctx,as.iter_range_ops);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "lower_bound_ops");
        ValkeyModule_ReplyWithLongLong(ctx,as.lb_ops);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "maximum_ops");
        ValkeyModule_ReplyWithLongLong(ctx,as.max_ops);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "minimum_ops");
        ValkeyModule_ReplyWithLongLong(ctx,as.min_ops);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "range_ops");
        ValkeyModule_ReplyWithLongLong(ctx,as.range_ops);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "set_ops");
        ValkeyModule_ReplyWithLongLong(ctx,as.set_ops);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "size_ops");
        ValkeyModule_ReplyWithLongLong(ctx,as.size_ops);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplySetArrayLength(ctx, 11);
        return 0;
    }

    int cmd_VACUUM(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {
        compressed_release release;
        if (argc != 1)
            return ValkeyModule_WrongArity(ctx);
        size_t result = art::get_leaf_compression().vacuum();
        return ValkeyModule_ReplyWithLongLong(ctx, (int64_t)result);
    }

    int cmd_HEAPBYTES(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {
        //compressed_release release;
        if (argc != 1)
            return ValkeyModule_WrongArity(ctx);
        ;
        return ValkeyModule_ReplyWithLongLong(ctx, (int64_t)heap::allocated);
    }
    int cmd_EVICT(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {
        compressed_release release;
        write_lock w(get_lock());
        if (argc != 1)
            return ValkeyModule_WrongArity(ctx);
        auto t = get_art();
        return ValkeyModule_ReplyWithLongLong(ctx, (int64_t)art_evict_lru(t));
    }

    /* B.SET <key> <value>
     *
     * Set the specified key to the specified value. */
    int cmd_CONFIG(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        ValkeyModule_AutoMemory(ctx);
        compressed_release release;
        if (argc != 4)
            return ValkeyModule_WrongArity(ctx);
        write_lock w(get_lock());
        size_t slen;
        const char *s = ValkeyModule_StringPtrLen(argv[1], &slen);
        if (strncmp("set",s,slen) == 0 || strncmp("SET",s,slen) == 0 )
        {
            return art::set_configuration_value(argv[2],argv[3]);
        }
        return VALKEYMODULE_ERR;
    }

#define NAME(x) "B." #x , cmd_##x

    /* This function must be present on each module. It is used in order to
     * register the commands into the server. */
    int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **, int)
    {

        if (ValkeyModule_Init(ctx, "B", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, NAME(SET), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;
        
        if (ValkeyModule_CreateCommand(ctx, NAME(ADD), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, NAME(GET), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;
        
        if (ValkeyModule_CreateCommand(ctx, NAME(LB), "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, NAME(REM), "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, NAME(RANGE), "readonly", 1, 2, 0) == VALKEYMODULE_ERR)
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


        /* Create our global dictionary. Here we'll set our keys and values. */
        Keyspace = ValkeyModule_CreateDict(nullptr);
        if (art::register_valkey_configuration(ctx)!=0)
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

        if(!art::init_leaf_compression())
            return VALKEYMODULE_ERR;

        if(!art::init_node_compression())
            return VALKEYMODULE_ERR;

        return VALKEYMODULE_OK;
    }

    int ValkeyModule_OnUnload(void *unused_arg)
    {
        art::destroy_leaf_compression();
        art::destroy_node_compression();
        return VALKEYMODULE_OK;
    }
}