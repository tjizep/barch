//
// Created by teejip on 5/20/25.
//

#ifndef BARCH_APIS_H
#define BARCH_APIS_H
#include "caller.h"
extern "C"{
int SET(caller& call,const arg_t& argv);
int KEYS(caller& call, const arg_t& argv);
int INCR(caller& call, const arg_t& argv);
int INCRBY(caller& call, const arg_t& argv);
int DECR(caller& call, const arg_t& argv);
int DECRBY(caller& call, const arg_t& argv);
int MSET(caller& call, const arg_t& argv);
int ADD(caller& call, const arg_t& argv);
int GET(caller& call, const arg_t& argv);
int MGET(caller& call, const arg_t& argv);
int MIN(caller& call, const arg_t& argv);
int MAX(caller& call, const arg_t& );
int LB(caller& call, const arg_t& argv);
int REM(caller& call, const arg_t& argv);
int SIZE(caller& call, const arg_t& argv);
int SAVE(caller& call, const arg_t& argv);
int LOAD(caller& call, const arg_t& argv);
int START(caller& call, const arg_t& argv);
int STOP(caller& call, const arg_t& argv);
int RETRIEVE(caller& call, const arg_t& argv);
int PING(caller& call, const arg_t& argv);
int HEXPIREAT(caller& call, const arg_t& args);
int HGETEX(caller& call, const arg_t &argv);
int HINCRBY(caller& call, const arg_t &argv);
int HINCRBYFLOAT(caller& call, const arg_t &argv);
int HDEL(caller& call, const arg_t &argv);
int HGETDEL(caller& call, const arg_t &argv);
int HTTL(caller& call,const arg_t& argv);
int HGET(caller& call, const arg_t& argv);
int HLEN(caller& call, const arg_t& argv);
int HEXPIRETIME(caller& call, const arg_t& argv);
int HGETALL(caller& call, const arg_t& argv);
int HKEYS(caller& call, const arg_t& argv);
int HEXISTS(caller& call, const arg_t& argv);
int ZADD(caller& call, const arg_t &argv);
int ZREM(caller& call, const arg_t& argv);
int ZINCRBY(caller& call, const arg_t& argv);
int ZRANGE(caller& call, const arg_t& argv);
int ZCARD(caller& call, const arg_t& argv);
int ZDIFF(caller& call, const arg_t& argv);
int ZDIFFSTORE(caller& call, const arg_t& argv);
int ZINTERSTORE(caller& call, const arg_t& argv);
int ZINTERCARD(caller& call, const arg_t& argv);
int ZINTER(caller& call, const arg_t& argv);
int ZPOPMIN(caller& call, const arg_t& argv);
int ZPOPMAX(caller& call, const arg_t& argv);
int ZREVRANGE(caller& call, const arg_t& argv);
int ZRANGEBYSCORE(caller& call, const arg_t& argv);
int ZREVRANGEBYSCORE(caller& call, const arg_t& argv);
int ZREMRANGEBYLEX(caller& call, const arg_t& argv);
int ZRANGEBYLEX(caller& call, const arg_t& argv);
int ZREVRANGEBYLEX(caller& call, const arg_t& argv);
int ZRANK(caller& call, const arg_t& argv);
int ZFASTRANK(caller& call, const arg_t& argv);
}
#endif //BARCH_APIS_H
