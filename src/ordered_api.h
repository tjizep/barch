//
// Created by teejip on 4/9/25.
//

#ifndef ORDERED_API_H
#define ORDERED_API_H
#include "valkeymodule.h"
int add_ordered_api(ValkeyModuleCtx* ctx);
int cmd_ZADD(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_ZCOUNT(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_ZCARD(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_ZDIFF(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_ZINTER(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_ZPOPMAX(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_ZPOPMIN(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_ZRANGE(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_ZREVRANGE(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_ZRANGEBYSCORE(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_ZREVRANGEBYSCORE(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_ZRANGEBYLEX(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
int cmd_ZREVRANGEBYLEX(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);

#endif //ORDERED_API_H
