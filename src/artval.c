//
// Created by me on 11/9/24.
//


/* artd --
 *
 * This module implements a volatile key-value store on top of the
 * dictionary exported by the modules API.
 *
 * -----------------------------------------------------------------------------
 * */

#include "valkeymodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include "art.h"

static ValkeyModuleDict *Keyspace;

/* ART_DICT.SET <key> <value>
 *
 * Set the specified key to the specified value. */
int cmd_SET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 3) return ValkeyModule_WrongArity(ctx);
    ValkeyModule_DictSet(Keyspace, argv[1], argv[2]);
    size_t klen,vlen;
    const char *k = ValkeyModule_StringPtrLen(argv[1], &klen);
    if (k == NULL) return ValkeyModule_WrongArity(ctx);
    const char * v = ValkeyModule_StringPtrLen(argv[2], &vlen);
    if (v == NULL) return ValkeyModule_WrongArity(ctx);
    printf("key: %s, value: %s\n", k, v);
    /* We need to keep a reference to the value stored at the key, otherwise
     * it would be freed when this callback returns. */
    ValkeyModule_RetainString(NULL, argv[2]);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

/* HELLODICT.GET <key>
 *
 * Return the value of the specified key, or a null reply if the key
 * is not defined. */
int cmd_GET(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) return ValkeyModule_WrongArity(ctx);
    ValkeyModuleString *val = ValkeyModule_DictGet(Keyspace, argv[1], NULL);
    if (val == NULL) {
        return ValkeyModule_ReplyWithNull(ctx);
    } else {
        return ValkeyModule_ReplyWithString(ctx, val);
    }
}

/* HELLODICT.KEYRANGE <startkey> <endkey> <count>
 *
 * Return a list of matching keys, lexicographically between startkey
 * and endkey. No more than 'count' items are emitted. */
int cmd_KEYRANGE(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 4) return ValkeyModule_WrongArity(ctx);

    /* Parse the count argument. */
    long long count;
    if (ValkeyModule_StringToLongLong(argv[3], &count) != VALKEYMODULE_OK) {
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid count");
    }

    /* Seek the iterator. */
    ValkeyModuleDictIter *iter = ValkeyModule_DictIteratorStart(Keyspace, ">=", argv[1]);

    /* Reply with the matching items. */
    char *key;
    size_t keylen;
    long long replylen = 0; /* Keep track of the emitted array len. */
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
    while ((key = ValkeyModule_DictNextC(iter, &keylen, NULL)) != NULL) {
        if (replylen >= count) break;
        if (ValkeyModule_DictCompare(iter, "<=", argv[2]) == VALKEYMODULE_ERR) break;
        ValkeyModule_ReplyWithStringBuffer(ctx, key, keylen);
        replylen++;
    }
    ValkeyModule_ReplySetArrayLength(ctx, replylen);

    /* Cleanup. */
    ValkeyModule_DictIteratorStop(iter);
    return VALKEYMODULE_OK;
}

/* This function must be present on each module. It is used in order to
 * register the commands into the server. */
int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (ValkeyModule_Init(ctx, "artd", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "artd.set", cmd_SET, "write deny-oom", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "artd.get", cmd_GET, "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "artd.keyrange", cmd_KEYRANGE, "readonly", 1, 1, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    /* Create our global dictionary. Here we'll set our keys and values. */
    Keyspace = ValkeyModule_CreateDict(NULL);

    return VALKEYMODULE_OK;
}
