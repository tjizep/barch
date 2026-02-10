//
// Created by teejip on 5/20/25.
//

#ifndef BARCH_APIS_H
#define BARCH_APIS_H
#include "caller.h"
typedef std::function<int (caller& call, const arg_t& argv)> barch_function;
typedef heap::string_map<size_t> catmap;
heap::vector<std::string> categories();
catmap& get_category_map();
heap::vector<bool> cats2vec(const catmap& icats);

struct barch_info {
    barch_info() = default;
    void set_cats(const std::initializer_list<const char *>& icats) {
        catmap mycats;
        for (auto c : icats) {
            mycats[c] = true;
        }
        this->cats = cats2vec(mycats);
        this->dp = get_category_map().at("data");
        this->wr = get_category_map().at("write");
    }
    barch_info(const barch_function& call, const std::initializer_list<const char *>& cats, bool asynch = false) : call(call), is_asynch(asynch) {
        set_cats(cats);
    }
    barch_info(const barch_info& binfo) = default;
    barch_info& operator=(const barch_info& binfo) = default;
    barch_info& operator=(barch_info&& binfo) = default;
    barch_info(barch_info&& binfo) = delete;
    bool is_data() const {
        return cats[dp];
    }
    bool is_write() const {
        return cats[wr];
    }
    barch_function call{};
    heap::vector<bool> cats{};
    uint64_t calls {0};
    bool is_asynch{false};
    int dp = 0;
    int wr = 0;
};
typedef heap::string_map<barch_info> function_map;
extern "C"{
    // Misc/sys
    int COMMAND(caller& call,const arg_t& argv);
    int AUTH(caller& call,const arg_t& argv);
    int ACL(caller& call,const arg_t& argv);
    int INFO(caller& call, const arg_t& argv);
    int CLIENT(caller& call, const arg_t& arg_v);
    int MULTI(caller& call, const arg_t& arg_v);
    int EXEC(caller& call, const arg_t& arg_v);
    // Keys
    int SET(caller& call,const arg_t& argv);
    int APPEND(caller& call,const arg_t& argv);
    int PREPEND(caller& call,const arg_t& argv);
    int KEYS(caller& call, const arg_t& argv);
    int VALUES(caller& call, const arg_t& argv);
    int INCR(caller& call, const arg_t& argv);
    int INCRBY(caller& call, const arg_t& argv);
    int UINCRBY(caller& call, const arg_t& argv);
    int DECR(caller& call, const arg_t& argv);
    int DECRBY(caller& call, const arg_t& argv);
    int UDECRBY(caller& call, const arg_t& argv);
    int EXISTS(caller& call, const arg_t& argv);
    int EXPIRE(caller& call, const arg_t& argv);
    int MSET(caller& call, const arg_t& argv);
    int ADD(caller& call, const arg_t& argv);
    int GET(caller& call, const arg_t& argv);
    int LENGTH(caller& call, const arg_t& argv);
    int MGET(caller& call, const arg_t& argv);
    int MIN(caller& call, const arg_t& argv);
    int MAX(caller& call, const arg_t& );
    int LB(caller& call, const arg_t& argv);
    int UB(caller& call, const arg_t& argv);
    int RANGE(caller& call, const arg_t& argv);
    int COUNT(caller& call, const arg_t& argv);
    int REM(caller& call, const arg_t& argv);
    int TTL(caller& call, const arg_t& argv);
    // database
    int USE(caller& call, const arg_t& argv);
    int UNLOAD(caller& call, const arg_t& argv);
    int SPACES(caller& call, const arg_t& argv);
    int KSPACE(caller& call, const arg_t& argv);
    // size in current keyspace
    int SIZE(caller& call, const arg_t& argv);
    // total count in the entire db
    int SIZEALL(caller& call, const arg_t& argv);
    int SAVE(caller& call, const arg_t& argv);
    int CLEAR(caller& call, const arg_t& argv);
    // save and clear all key spaces
    int CLEARALL(caller& call, const arg_t& argv);
    int SAVEALL(caller& call, const arg_t& argv);
    int KSOPTIONS(caller& call, const arg_t& argv);
    // replication+cluster
    int ADDROUTE(caller& call, const arg_t& argv);
    int ROUTE(caller& call, const arg_t& argv);
    int REMROUTE(caller& call, const arg_t& argv);
    int PUBLISH(caller& call, const arg_t& argv);
    int PULL(caller& call, const arg_t& argv);
    int LOAD(caller& call, const arg_t& argv);
    int RELOAD(caller& call, const arg_t& argv);
    int START(caller& call, const arg_t& argv);
    int STOP(caller& call, const arg_t& argv);
    int RETRIEVE(caller& call, const arg_t& argv);
    int PING(caller& call, const arg_t& argv);
    // compression
    int TRAIN(caller& call, const arg_t& argv);
    // stats
    int OPS(caller& call, const arg_t& argv);
    int STATS(caller& call, const arg_t& argv);

    // config
    int CONFIG(caller& call, const arg_t& argv);

    // Lists
    int LBACK(caller& cc, const arg_t& args);
    int LFRONT(caller& cc, const arg_t& args);
    int LPUSH(caller& cc, const arg_t& args);
    int RPUSH(caller& cc, const arg_t& args);
    int LPOP(caller& cc, const arg_t& args);
    int RPOP(caller& cc, const arg_t& args);
    int LLEN(caller& cc, const arg_t& args);
    int BLPOP(caller& cc, const arg_t& args);
    int BRPOP(caller& cc, const arg_t& args);
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

extern std::shared_ptr<function_map> functions_by_name();
#endif //BARCH_APIS_H
