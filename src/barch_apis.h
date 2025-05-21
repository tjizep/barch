//
// Created by teejip on 5/20/25.
//

#ifndef BARCH_APIS_H
#define BARCH_APIS_H
#include "caller.h"
extern "C" int SET(caller& call,const arg_t& argv);
extern int KEYS(caller& call, const arg_t& argv);

extern "C" int INCR(caller& call, const arg_t& argv);
extern "C" int INCRBY(caller& call, const arg_t& argv);
extern "C" int DECR(caller& call, const arg_t& argv);
extern "C" int DECRBY(caller& call, const arg_t& argv);
extern int MSET(caller& call, const arg_t& argv);
extern int ADD(caller& call, const arg_t& argv);
extern "C" int GET(caller& call, const arg_t& argv);
extern int MGET(caller& call, const arg_t& argv);
extern "C" int MIN(caller& call, const arg_t& argv);
extern "C" int MAX(caller& call, const arg_t& );
extern int LB(caller& call, const arg_t& argv);
extern "C" int REM(caller& call, const arg_t& argv);
extern "C" int SIZE(caller& call, const arg_t& argv);
extern "C" int SAVE(caller& call, const arg_t& argv);
extern "C" int LOAD(caller& call, const arg_t& argv);
extern int HEXPIREAT(caller& call, const arg_t& args);
extern int HGETEX(caller& call, const arg_t &argv);
extern int HINCRBY(caller& call, const arg_t &argv);
extern int HINCRBYFLOAT(caller& call, const arg_t &argv);
extern int HDEL(caller& call, const arg_t &argv);
extern int HGETDEL(caller& call, const arg_t &argv);
extern int HTTL(caller& call,const arg_t& argv);
extern int HGET(caller& call, const arg_t& argv);
extern int HLEN(caller& call, const arg_t& argv);
extern int HEXPIRETIME(caller& call, const arg_t& argv);
extern int HGETALL(caller& call, const arg_t& argv);
extern int HKEYS(caller& call, const arg_t& argv);
extern int HEXISTS(caller& call, const arg_t& argv);
extern int ZADD(caller& call, const arg_t &argv);
extern int ZREM(caller& call, const arg_t& argv);
extern int ZINCRBY(caller& call, const arg_t& argv);
extern int ZRANGE(caller& call, const arg_t& argv);
extern int ZCARD(caller& call, const arg_t& argv);
extern int ZDIFF(caller& call, const arg_t& argv);
extern int ZDIFFSTORE(caller& call, const arg_t& argv);
extern int ZINTERSTORE(caller& call, const arg_t& argv);
extern int ZINTERCARD(caller& call, const arg_t& argv);
extern int ZINTER(caller& call, const arg_t& argv);
extern int ZPOPMIN(caller& call, const arg_t& argv);
extern int ZPOPMAX(caller& call, const arg_t& argv);
extern int ZREVRANGE(caller& call, const arg_t& argv);
extern int ZRANGEBYSCORE(caller& call, const arg_t& argv);
extern int ZREVRANGEBYSCORE(caller& call, const arg_t& argv);
extern int ZREMRANGEBYLEX(caller& call, const arg_t& argv);
extern int ZRANGEBYLEX(caller& call, const arg_t& argv);
extern int ZREVRANGEBYLEX(caller& call, const arg_t& argv);
extern int ZRANK(caller& call, const arg_t& argv);
extern int ZFASTRANK(caller& call, const arg_t& argv);

#endif //BARCH_APIS_H
