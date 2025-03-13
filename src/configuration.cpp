//
// Created by linuxlite on 3/13/25.
//

#include "valkeymodule.h"
#include "configuration.h"
#include <cstdlib>
#include <cstring>
#include <string>
#define unused_arg
static int TestMatchReply(ValkeyModuleCallReply *reply, const char *str) {
    ValkeyModuleString *mystr;
    mystr = ValkeyModule_CreateStringFromCallReply(reply);
    if (!mystr) return 0;
    const char *ptr = ValkeyModule_StringPtrLen(mystr,NULL);
    return strcmp(ptr,str) == 0;
}
static bool test_configuration(ValkeyModuleCtx* ctx,const char * expected_name, const char * expected_value) {
    if (ctx == nullptr) return false;
    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleCallReply *reply;
    std::string cmd = "CONFIG GET ";
    cmd += expected_name;
    reply = ValkeyModule_Call(ctx, "CONFIG","GET", expected_name);
    if (reply == nullptr) return false;
    size_t items = ValkeyModule_CallReplyLength(reply);
    if (items != 2) return false;
    ValkeyModuleCallReply *name, *value;

    name = ValkeyModule_CallReplyArrayElement(reply,0);
    value = ValkeyModule_CallReplyArrayElement(reply,1);
    if (!TestMatchReply(name,expected_name)) return false;
    if (!TestMatchReply(value,expected_value)) return false;
    return true;
}

bool get_lru_evict_enabled(ValkeyModuleCtx* ctx)
{
    //allkeys-lru
    return test_configuration(ctx, "maxmemory-policy", "allkeys-lru");
}
bool get_lfu_evict_enabled(ValkeyModuleCtx* ctx)
{
    return test_configuration(ctx, "maxmemory-policy", "allkeys-lfu");
}
bool get_compression_enabled(ValkeyModuleCtx* ctx)
{
    return test_configuration(ctx, "cdict-compression", "zstd");
}
float get_module_memory_ratio(ValkeyModuleCtx* unused_arg)
{
    return 0.95;
}