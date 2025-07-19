//
// Created by teejip on 5/20/25.
//

#ifndef BARCH_APIS_H
#define BARCH_APIS_H
#include "caller.h"
typedef std::function<int (caller& call, const arg_t& argv)> barch_function;
struct barch_info {
    barch_info() = default;
    barch_info(const barch_function& call, bool data = true) : call(call), data(data){}
    barch_info(const barch_info& binfo) {
        call = binfo.call;
        data = binfo.data;
        pub = binfo.pub;
        calls = (uint64_t)binfo.calls;
    }
    barch_info& operator=(const barch_info& binfo) {
        call = binfo.call;
        data = binfo.data;
        pub = binfo.pub;
        calls = (uint64_t)binfo.calls;
        return *this;
    }
    barch_function call{};
    bool data {true};
    bool pub {false};
    std::atomic<uint64_t> calls {0};
};

extern "C"{
    // Misc
    int COMMAND(caller& call,const arg_t& argv);

    // Keys
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
    int RANGE(caller& call, const arg_t& argv);
    int REM(caller& call, const arg_t& argv);

    // database
    int SIZE(caller& call, const arg_t& argv);
    int SAVE(caller& call, const arg_t& argv);
    int CLEAR(caller& call, const arg_t& argv);

    // replication+cluster
    int ADDROUTE(caller& call, const arg_t& argv);
    int ROUTE(caller& call, const arg_t& argv);
    int REMROUTE(caller& call, const arg_t& argv);
    int PUBLISH(caller& call, const arg_t& argv);
    int PULL(caller& call, const arg_t& argv);
    int LOAD(caller& call, const arg_t& argv);
    int START(caller& call, const arg_t& argv);
    int STOP(caller& call, const arg_t& argv);
    int RETRIEVE(caller& call, const arg_t& argv);
    int PING(caller& call, const arg_t& argv);

    // stats
    int OPS(caller& call, const arg_t& argv);
    int STATS(caller& call, const arg_t& argv);

    // config
    int CONFIG(caller& call, const arg_t& argv);

    // Lists
    int LBACK(caller& cc, const arg_t& args);
    int LFRONT(caller& cc, const arg_t& args);
    int LPUSH(caller& cc, const arg_t& args);
    int LPOP(caller& cc, const arg_t& args);
    int LLEN(caller& cc, const arg_t& args);

    // Hash Set
    int HSET(caller& cc, const arg_t& args);
    int HEXPIREAT(caller& call, const arg_t& args);
    int HEXPIRE(caller& call, const arg_t& args);
    //int HGETEX(caller& call, const arg_t &argv);
    int HMGET(caller& call, const arg_t& argv);
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

    // Ordered Set
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

extern std::unordered_map<std::string, barch_info>& functions_by_name();
#endif //BARCH_APIS_H
