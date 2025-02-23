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
#include "conversion.h"
#include "art.h"
#include <fast_float/fast_float.h>
#include <functional>

static ValkeyModuleDict *Keyspace;

typedef std::unique_lock< std::shared_mutex >  write_lock;
typedef std::shared_lock< std::shared_mutex >  read_lock;  // C++ 14

std::shared_mutex& get_lock()
{
    static std::shared_mutex shared;
    return shared;
}


static art_tree ad = {nullptr, 0};
static auto startTime = std::chrono::high_resolution_clock::now();
/// @brief  getting an initialized art tree
/// @param ctx not used but could be 
/// @return a once initialized art_tree

static art_tree *get_art()
{

    if (ad.root == nullptr)
    {
        if (art_tree_init(&ad) != 0)
        {
            return nullptr;
        }
    }
    return &ad;
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

static int reply_encoded_key(ValkeyModuleCtx* ctx, const unsigned char * enck, size_t key_len){
    double dk;
    int64_t ik;
    const char * k;
    size_t kl;
    
    if( key_len == 9 && (*enck == conversion::tinteger || *enck == conversion::tdouble)){
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
        kl = key_len-1;
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
    ValkeyModuleDictIter *iter;
    ValkeyModuleString **argv;
    int64_t replylen;
    int64_t count;

    iter_state(ValkeyModuleCtx *ctx, ValkeyModuleDictIter *iter, ValkeyModuleString **argv, int64_t count)
    :   ctx(ctx), iter(iter), argv(argv), replylen(0), count(count)
    {}

    iter_state(const iter_state& is) = delete; 
    iter_state& operator=(const iter_state& ) = delete;

    int iterate(const unsigned char *key, uint32_t key_len, void *) {
        if(key_len == 0) { /// 
            return -1;
        }
        
        if (key == nullptr){
            return -1;
        }
        
        if(0 != reply_encoded_key(ctx, key, key_len)){
            return -1;
        };

        if (replylen >= count)
            return -1;
        if (ValkeyModule_DictCompare(iter, "<=", argv[2]) == VALKEYMODULE_ERR)
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
    int cmd_KEYRANGE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {   read_lock rl(get_lock());
        compressed_release release;
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
        iter_state is(ctx, ValkeyModule_DictIteratorStart(Keyspace, ">=", argv[1]), argv, count);
        
        /* Reply with the matching items. */
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
        //std::function<int (void *, const unsigned char *, uint32_t , void *)>
        auto iter = [](void *data, const unsigned char *key, uint32_t key_len, void *value) -> int {
            auto * is = (iter_state*)data;
                
            return is->iterate(key, key_len, value);
            
        };

        art_range(get_art(), c1.get_data(), c1.get_size(), c2.get_data(), c2.get_size(), iter, &is);


        ValkeyModule_ReplySetArrayLength(ctx, is.replylen);

        /* Cleanup. */
        ValkeyModule_DictIteratorStop(is.iter);
        return VALKEYMODULE_OK;
    }

    /* CDICT.SET <key> <value>
     *
     * Set the specified key to the specified value. */
    int cmd_SET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        compressed_release release;
        if (argc != 3)
            return ValkeyModule_WrongArity(ctx);
        ValkeyModule_DictSet(Keyspace, argv[1], argv[2]);
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        
        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        
        auto converted = conversion::convert(k, klen);
        write_lock wl(get_lock());

        art_insert(get_art(), converted.get_data(), converted.get_size(), argv[2]);

        /* We need to keep a reference to the value stored at the key, otherwise
         * it would be freed when this callback returns. */
        ValkeyModule_RetainString(nullptr, argv[2]);
        return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    }

    /* CDICT.ADD <key> <value>
     *
     * Add the specified key only if its not there, with specified value. */
    int cmd_ADD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        compressed_release release;
        if (argc != 3)
            return ValkeyModule_WrongArity(ctx);
        ValkeyModule_DictSet(Keyspace, argv[1], argv[2]);
        
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        
        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        
        auto converted = conversion::convert(k, klen);
        write_lock w(get_lock());
        art_insert_no_replace(get_art(), converted.get_data(), converted.get_size(), argv[2]);

        /* We need to keep a reference to the value stored at the key, otherwise
         * it would be freed when this callback returns. */
        ValkeyModule_RetainString(nullptr, argv[2]);
        return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    }

    /* CDICT.GET <key>
     *
     * Return the value of the specified key, or a null reply if the key
     * is not defined. */
    int cmd_GET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {

        compressed_release release;
        if (argc != 2)
            return ValkeyModule_WrongArity(ctx);
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        
        auto converted = conversion::convert(k, klen);
        trace_list trace;
        trace.reserve(klen);
        read_lock rl(get_lock());
        void *r = art_search(trace, get_art(), converted.get_data(), converted.get_size());

        auto *val = (ValkeyModuleString *)r;

        if (val == nullptr)
        {
            return ValkeyModule_ReplyWithNull(ctx);
        }
        else
        {
            return ValkeyModule_ReplyWithString(ctx, val);
        }
    }
    /* CDICT.MINIMUM
     *
     * Return the value of the specified key, or a null reply if the key
     * is not defined. */
    int cmd_MIN(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {   read_lock rl(get_lock());
        compressed_release release;
        if (argc != 1)
            return ValkeyModule_WrongArity(ctx);

        const art_leaf *r = art_minimum(get_art());

        if (r == nullptr)
        {
            return ValkeyModule_ReplyWithNull(ctx);
        }
        else
        {
            return reply_encoded_key(ctx, r->key, r->key_len);
        }
    }
    int cmd_MILLIS(ValkeyModuleCtx *ctx, ValkeyModuleString **, int )
    {

        auto t = std::chrono::high_resolution_clock::now();
        auto d = std::chrono::duration_cast<std::chrono::milliseconds>(t - startTime);

        return ValkeyModule_ReplyWithLongLong(ctx,d.count());

    }

    /* CDICT.MAXIMUM
     *
     * Return the value of the specified key, or a null reply if the key
     * is not defined. */
    int cmd_MAX(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {
        read_lock rl(get_lock());
        compressed_release release;
        if (argc != 1)
            return ValkeyModule_WrongArity(ctx);

        const art_leaf *r = art_maximum(get_art());

        if (r == nullptr)
        {
            return ValkeyModule_ReplyWithNull(ctx);
        }
        else
        {
            return reply_encoded_key(ctx, r->key, r->key_len);
        }
    }

    /* CDICT.LB <key>
     *
     * Return the value of the specified key, or a null reply if the key
     * is not defined. */
    int cmd_LB(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        compressed_release release;
        if (argc != 2)
            return ValkeyModule_WrongArity(ctx);
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        
        auto converted = conversion::convert(k, klen);
        read_lock rl(get_lock());

        void *r = art_lower_bound(get_art(), converted.get_data(), converted.get_size());

        ValkeyModuleString *val = (ValkeyModuleString *)r;

        if (val == nullptr)
        {
            return ValkeyModule_ReplyWithNull(ctx);
        }
        else
        {
            return ValkeyModule_ReplyWithString(ctx, val);
        }
    }

    /* CDICT.RM <key>
     *
     * remove the value associated with the key and return the key if such a key existed. */
    int cmd_RM(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {

        compressed_release release;
        if (argc != 2)
            return ValkeyModule_WrongArity(ctx);
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        
        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        

        auto converted = conversion::convert(k, klen);
        write_lock w(get_lock());
        auto t = get_art();
        void *r = art_delete(t, converted.get_data(), converted.get_size());

        auto *val = (ValkeyModuleString *)r;

        if (val == nullptr)
        {
            return ValkeyModule_ReplyWithNull(ctx);
        }
        else
        {
            return ValkeyModule_ReplyWithString(ctx, val);
        }
    }
    /* CDICT.SIZE
     * @return the size or o.k.a. key count. 
     */
    int cmd_SIZE(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {
        read_lock rl(get_lock());
        compressed_release release;
        if (argc != 1)
            return ValkeyModule_WrongArity(ctx);
        auto size = (int64_t)art_size(get_art());
        return ValkeyModule_ReplyWithLongLong(ctx, size);
        
    }

    /* CDICT.STATISTICS
     *
     * get memory statistics. */
    int cmd_STATISTICS(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {
        compressed_release release;
        if (argc != 1)
            return ValkeyModule_WrongArity(ctx);

        art_statistics as = art_get_statistics();
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN); 
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "heap_bytes_allocates");
        ValkeyModule_ReplyWithLongLong(ctx,as.heap_bytes_allocated);

        ValkeyModule_ReplyWithSimpleString(ctx, "bytes_addressable");
        ValkeyModule_ReplyWithLongLong(ctx,as.bytes_allocated);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "interior_bytes_addressable");
        ValkeyModule_ReplyWithLongLong(ctx,as.bytes_interior);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "leaf_nodes");
        ValkeyModule_ReplyWithLongLong(ctx,as.leaf_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "size_4_nodes");
        ValkeyModule_ReplyWithLongLong(ctx,as.node4_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "size_16_nodes");
        ValkeyModule_ReplyWithLongLong(ctx,as.node16_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN); 
        ValkeyModule_ReplyWithSimpleString(ctx, "size_48_nodes");
        ValkeyModule_ReplyWithLongLong(ctx,as.node48_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "size_256_nodes");
        ValkeyModule_ReplyWithLongLong(ctx,as.node256_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "size_256_occupancy");
        ValkeyModule_ReplyWithLongLong(ctx,as.node256_occupants);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplySetArrayLength(ctx, 9);
        return 0;
    }

    /* CDICT.STATISTICS
     *
     * get memory statistics. */
    int cmd_OPS_STATISTICS(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
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
        size_t result = get_leaf_compression().vacuum();
        return ValkeyModule_ReplyWithLongLong(ctx, (int64_t)result);
    }

    int cmd_HEAP_BYTES(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {
        compressed_release release;
        if (argc != 1)
            return ValkeyModule_WrongArity(ctx);
        ;
        return ValkeyModule_ReplyWithLongLong(ctx, (int64_t)heap::allocated);
    }

    /* This function must be present on each module. It is used in order to
     * register the commands into the server. */
    int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **, int)
    {

        if (ValkeyModule_Init(ctx, "OD", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "ODSET", cmd_SET, "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;
        
        if (ValkeyModule_CreateCommand(ctx, "ODADD", cmd_ADD, "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "ODGET", cmd_GET, "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;
        
        if (ValkeyModule_CreateCommand(ctx, "ODLB", cmd_LB, "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "ODREM", cmd_RM, "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "ODRANGE", cmd_KEYRANGE, "readonly", 1, 2, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "ODSIZE", cmd_SIZE, "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "ODSTATS", cmd_STATISTICS, "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;
        
        if (ValkeyModule_CreateCommand(ctx, "ODOPS", cmd_OPS_STATISTICS, "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "ODMAX", cmd_MAX, "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "ODMIN", cmd_MIN, "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "ODMILLIS", cmd_MILLIS, "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "ODVACUUM", cmd_VACUUM, "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "ODHEAPBYTES", cmd_HEAP_BYTES, "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        /* Create our global dictionary. Here we'll set our keys and values. */
        Keyspace = ValkeyModule_CreateDict(nullptr);
        if (get_art() == nullptr)
        {
            return VALKEYMODULE_ERR;
        }
        return VALKEYMODULE_OK;
    }
}