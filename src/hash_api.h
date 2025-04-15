//
// Created by teejip on 4/9/25.
//

#ifndef HASH_API_H
#define HASH_API_H

#include "valkeymodule.h"
int add_hash_api(ValkeyModuleCtx* ctx);

int cmd_HSET(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HMSET(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HEXPIRE(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HEXPIREAT(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HGETEX(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HINCRBY(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HINCRBYFLOAT(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HDEL(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HGETDEL(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HTTL(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HGET(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HLEN(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HMGET(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HEXPIRETIME(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HGETALL(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HKEYS(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_HEXISTS(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);


#endif //HASH_API_H
