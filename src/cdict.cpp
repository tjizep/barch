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

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include "conversion.h"
#include "art.h"
#include <fast_float/fast_float.h>
#include <array>
#include <functional>

static ValkeyModuleDict *Keyspace;
static art_tree ad = {NULL, 0};

/// @brief  getting an initialized art tree
/// @param ctx not used but could be 
/// @return a once initialized art_tree

static art_tree *get_art(ValkeyModuleCtx *ctx)
{
    VALKEYMODULE_NOT_USED(ctx);

    if (ad.root == NULL)
    {
        if (art_tree_init(&ad) != 0)
        {
            return NULL;
        }
    }
    return &ad;
}

template <typename V>
static const uint8_t *ucast(const V *v)
{
    return reinterpret_cast<const uint8_t *>(v);
}
template <typename V>
uint8_t *ucast(V *v)
{
    return reinterpret_cast<uint8_t *>(v);
}

template <typename V>
static char *ccast(V *v)
{
    return reinterpret_cast<char *>(v);
}
static int key_ok(const char * k, size_t klen){
    if (k == NULL){
        return -1;
    }
    if (klen == 0) 
        return -1;
    if (*k == 0 || *k == 1 || *k == 2)
        return -1;
    return 0;

}

static int key_check(ValkeyModuleCtx *ctx, const char * k, size_t klen){

    if (k == NULL) 
        return ValkeyModule_ReplyWithError(ctx, "No null keys");

    if (klen == 0) 
        return ValkeyModule_ReplyWithError(ctx, "No empty keys");

    if (*k == 0 || *k == 1)
        return ValkeyModule_ReplyWithError(ctx, "Keys cannot start with a byte containing 0,1 or 2");
    return ValkeyModule_ReplyWithError(ctx, "Unspecified key error");
}
struct iter_state {
    ValkeyModuleCtx *ctx;
    ValkeyModuleDictIter *iter;
    ValkeyModuleString **argv;
    int64_t replylen;
    int64_t count;
    std::string chars;

    iter_state(ValkeyModuleCtx *ctx, ValkeyModuleDictIter *iter, ValkeyModuleString **argv, int64_t count)
    :   ctx(ctx), iter(iter), argv(argv), replylen(0), count(count), chars("")
    {}

    iter_state(const iter_state& is) = delete; 
    iter_state& operator=(const iter_state& ) = delete;

    int iterate(const unsigned char *key, uint32_t key_len, void *) {
        double dk;
        uint64_t ik;
        const char * k;
        size_t kl;
        
        if(key_len == 0) { /// 
            return -1;
        }
        
        if (key == NULL){
            return -1;
        }
        
        if((*key == 0 || *key == 1) && key_len == 9){
            ik = conversion::enc_bytes_to_int(key, key_len);
            if (*key == 1) {
                memcpy(&dk, &ik, sizeof(ik));
                chars = std::to_string(dk);
            } else {
                chars = std::to_string(ik); // its probably slow
                
            }
            k = chars.c_str();
            kl = chars.length();
        }else { //it's a string
            k = (const char*) &key[0];
            kl = key_len;
        }
        if (replylen >= count)
            return -1;
        if (ValkeyModule_DictCompare(iter, "<=", argv[2]) == VALKEYMODULE_ERR)
            return -1;
        
        if (ValkeyModule_ReplyWithStringBuffer(ctx, k, kl) == VALKEYMODULE_ERR) {
            return -1;
        };
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
    {
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
        auto c1 = conversion::convert(k1, k1len);
        auto c2 = conversion::convert(k2, k2len);
        
        /* Seek the iterator. */
        iter_state is(ctx, ValkeyModule_DictIteratorStart(Keyspace, ">=", argv[1]), argv, count);
        
        /* Reply with the matching items. */
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
        //std::function<int (void *, const unsigned char *, uint32_t , void *)>
        auto iter = [](void *data, const unsigned char *key, uint32_t key_len, void *value) -> int {
            iter_state * is = (iter_state*)data;
                
            return is->iterate(key, key_len, value);
            
        };

        art_range(get_art(ctx), c1.get_data(), c1.get_size(), c2.get_data(), c2.get_size(), iter, &is);


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
        if (argc != 3)
            return ValkeyModule_WrongArity(ctx);
        ValkeyModule_DictSet(Keyspace, argv[1], argv[2]);
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        
        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        
        auto converted = conversion::convert(k, klen);
        art_insert(get_art(ctx), converted.get_data(), converted.get_size(), argv[2]);

        /* We need to keep a reference to the value stored at the key, otherwise
         * it would be freed when this callback returns. */
        ValkeyModule_RetainString(NULL, argv[2]);
        return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    }

    /* CDICT.GET <key>
     *
     * Return the value of the specified key, or a null reply if the key
     * is not defined. */
    int cmd_GET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        if (argc != 2)
            return ValkeyModule_WrongArity(ctx);
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        
        auto converted = conversion::convert(k, klen);

        void *r = art_search(get_art(ctx), converted.get_data(), converted.get_size());

        ValkeyModuleString *val = (ValkeyModuleString *)r;

        if (val == NULL)
        {
            return ValkeyModule_ReplyWithNull(ctx);
        }
        else
        {
            return ValkeyModule_ReplyWithString(ctx, val);
        }
    }

    /* CDICT.LB <key>
     *
     * Return the value of the specified key, or a null reply if the key
     * is not defined. */
    int cmd_LB(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        if (argc != 2)
            return ValkeyModule_WrongArity(ctx);
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        
        auto converted = conversion::convert(k, klen);

        void *r = art_lower_bound(get_art(ctx), converted.get_data(), converted.get_size());

        ValkeyModuleString *val = (ValkeyModuleString *)r;

        if (val == NULL)
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
        if (argc != 2)
            return ValkeyModule_WrongArity(ctx);
        size_t klen;
        const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
        
        if (key_ok(k, klen) != 0)
            return key_check(ctx, k, klen);
        

        auto converted = conversion::convert(k, klen);

        void *r = art_delete(get_art(ctx), converted.get_data(), converted.get_size());

        ValkeyModuleString *val = (ValkeyModuleString *)r;

        if (val == NULL)
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
    int cmd_STATISTICS(ValkeyModuleCtx *ctx, ValkeyModuleString **, int argc)
    {
        if (argc != 1)
            return ValkeyModule_WrongArity(ctx);

        art_statistics as = art_get_statistics();
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN); 
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "Bytes Allocated");
        ValkeyModule_ReplyWithLongLong(ctx,as.bytes_allocated);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "Leaf Nodes"); 
        ValkeyModule_ReplyWithLongLong(ctx,as.leaf_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "Node 4 Nodes"); 
        ValkeyModule_ReplyWithLongLong(ctx,as.node4_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "Node 16 Nodes"); 
        ValkeyModule_ReplyWithLongLong(ctx,as.node16_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN); 
        ValkeyModule_ReplyWithSimpleString(ctx, "Node 48 Nodes");
        ValkeyModule_ReplyWithLongLong(ctx,as.node48_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        ValkeyModule_ReplyWithSimpleString(ctx, "Node 256 Nodes"); 
        ValkeyModule_ReplyWithLongLong(ctx,as.node256_nodes);
        ValkeyModule_ReplySetArrayLength(ctx, 2);
        ValkeyModule_ReplySetArrayLength(ctx, 6);
        return 0;
    }

    

    /* This function must be present on each module. It is used in order to
     * register the commands into the server. */
    int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc)
    {
        VALKEYMODULE_NOT_USED(argv);
        VALKEYMODULE_NOT_USED(argc);

        if (ValkeyModule_Init(ctx, "cdict", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "cdict.set", cmd_SET, "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "cdict.get", cmd_GET, "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;
        
        if (ValkeyModule_CreateCommand(ctx, "cdict.lb", cmd_LB, "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "cdict.rm", cmd_RM, "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "cdict.range", cmd_KEYRANGE, "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        if (ValkeyModule_CreateCommand(ctx, "cdict.statistics", cmd_STATISTICS, "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
            return VALKEYMODULE_ERR;

        /* Create our global dictionary. Here we'll set our keys and values. */
        Keyspace = ValkeyModule_CreateDict(NULL);
        if (get_art(ctx) == NULL)
        {
            return VALKEYMODULE_ERR;
        }
        return VALKEYMODULE_OK;
    }
}