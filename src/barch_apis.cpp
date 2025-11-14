#include "sastam.h"
#include "barch_apis.h"
//
// Created by teejip on 7/13/25.
//
static std::mutex& latch() {
    static std::mutex l{};
    return l;
}
catmap& get_category_map() {
    static catmap r;
    if (r.empty()) {
        std::lock_guard lock(latch());
        if (!r.empty()) return r;
        size_t at = 0;
        for (auto& c : categories()) {
            r[c] = at++;
        }
    }
    return r;
}


heap::vector<std::string> categories() {
    heap::vector<std::string> r = {"read","write","data", "stats",
        "dangerous","acl", "keyspace",
        "keys", "orderedset","hash","list","auth",
        "connection","config"};

    return r;
}
heap::vector<bool> cats2vec(const catmap& icats) {
    heap::vector<bool> cats;
    auto &catm = get_category_map();
    cats.resize(get_category_map().size());
    for (auto &c : icats) {
        if (c.first == "all") {
            for (size_t i = 0;i < cats.size();++i) {
                cats[i] = c.second != 0;
            }
            continue;
        }
        auto i = catm.find(c.first);
        if (i != catm.end()) {
            cats[i->second] = c.second != 0;
        } //ignore unknown cats
    }
    return cats;
}
function_map& functions_by_name() {
    static function_map r;
    if (r.empty()) {
        //std::lock_guard lock(latch());
        //if (!r.empty()) return r;
        r["SET"] = {::SET,{"write","keys","data"}};
        r["APPEND"] = {::APPEND,{"write","keys","data"}};
        r["PREPEND"] = {::PREPEND,{"write","keys","data"}};
        r["KEYS"] = {::KEYS,{"read","keys","data"}};
        r["VALUES"] = {::VALUES,{"read","keys","data"}};
        r["INCR"] = {::INCR,{"write","keys","data"}};
        r["INCRBY"] = {::INCRBY,{"write","keys","data"}};
        r["UINCRBY"] = {::UINCRBY,{"write","keys","data"}};
        r["DECR"] = {::DECR,{"write","keys","data"}};
        r["DECRBY"] = {::DECRBY,{"write","keys","data"}};
        r["UDECRBY"] = {::UDECRBY,{"write","keys","data"}};
        r["COUNT"] = {::COUNT,{"read","keys","data"}};
        r["EXISTS"] = {::EXISTS,{"read","keys","data"}};
        r["EXPIRE"] = {::EXPIRE,{"write","keys","data"}};
        r["MSET"] = {::MSET,{"write","keys","data"}};
        r["ADD"] = {::ADD,{"write","keys","data"}};
        r["GET"] = {::GET,{"read","keys","data"}};
        r["MGET"] = {::MGET,{"read","keys","data"}};
        r["MIN"] = {::MIN,{"read","keys","data"}};
        r["MAX"] = {::MAX,{"read","keys","data"}};
        r["LB"] = {::LB,{"read","keys","data"}};
        r["UB"] = {::UB,{"read","keys","data"}};
        r["FIRST"] = r["LB"]; // alias
        r["NEXT"] = r["UB"];
        r["REM"] = {::REM,{"write","keys","data"}};
        r["DEL"] = {::REM,{"write","keys","data"}};
        r["RANGE"] = {::RANGE,{"read","keys","data"}};
        r["TTL"] = {::TTL,{"read","keys","data"}};
        r["SIZE"] = {::SIZE,{"read"}};
        r["DBSIZE"] = {::SIZE,{"read"}};
        r["SIZEALL"] = {::SIZEALL,{"read"}};
        r["USE"] = {::USE,{"write"}};
        r["KSOPTIONS"] = {::KSOPTIONS,{"write"}};
        r["UNLOAD"] = {::UNLOAD,{"write"}};
        r["SPACES"] = {::SPACES,{"read"}};
        r["KSPACE"] = {::KSPACE,{"read","write"}};

        r["SAVE"] = {::SAVE,{"read"}};
        r["SAVEALL"] = {::SAVEALL,{"read"}};
        r["AUTH"] = {::AUTH,{"auth"}};
        r["ACL"] = {::ACL,{"write","acl"}};

        r["FLUSHDB"] = {::CLEAR,{"write","dangerous"}};
        r["CLEARALL"] = {::CLEARALL,{"write","dangerous"}};
        r["FLUSHALL"] = {::CLEAR,{"write","dangerous"}};
        r["STATS"] = {::STATS,{"read","stats"}};
        r["OPS"] = {OPS,{"read","stats"}};
        r["INFO"] = {INFO,{"read","stats"}};
        r["CLIENT"] = {CLIENT,{"read","stats"}};

        r["MULTI"] = {MULTI,{"write"}};
        r["EXEC"] = {EXEC,{"write"}};

        r["ADDROUTE"] = {::ADDROUTE,{"write","connection"}};
        r["ROUTE"] = {::ROUTE,{"read","connection"}};
        r["REMROUTE"] = {::REMROUTE,{"write","connection"}};
        r["PUBLISH"] = {::PUBLISH,{"write","connection"}};
        r["PULL"] = {::PULL,{"write","dangerous"}};
        r["LOAD"] = {::LOAD,{"write","dangerous"}};
        r["CONFIG"] = {::CONFIG,{"write","read","config"}};

        r["LBACK"] = {::LBACK,{"write","list","data"}};
        r["LFRONT"] = {::LFRONT,{"read","list","data"}};
        r["LPUSH"] = {::LPUSH,{"write","list","data"}};
        r["RPUSH"] = {::RPUSH,{"write","list","data"}};
        r["RPOP"] = {::RPOP,{"write","list","data"}};

        r["LPOP"] = {::LPOP,{"write","list","data"}};
        r["BLPOP"] = {::BLPOP,{"write","list","data"}};
        r["BRPOP"] = {::BRPOP,{"write","list","data"}};
        r["LLEN"] = {::LLEN,{"read","list","data"}};
        r["START"] = {::START,{"write","connection","data"}};
        r["STOP"] = {::STOP,{"write","connection","data"}};
        r["RETRIEVE"] = {::RETRIEVE,{"write","dangerous","data"}};
        r["PING"] = {::PING,{"read","connection","data"}};
        r["HSET"] = {::HSET,{"write","hash","data"}};
        r["HEXPIREAT"] = {::HEXPIREAT,{"write","hash","data"}};
        r["HEXPIRE"] = {::HEXPIRE,{"write","hash","data"}};
        //r["HGETEX"] = ::HGETEX;
        r["HMGET"] = {::HMGET,{"read","hash","data"}};
        r["HINCRBY"] = {::HINCRBY,{"write","hash","data"}};
        r["HINCRBYFLOAT"] = {::HINCRBYFLOAT,{"write","hash","data"}};
        r["HDEL"] = {::HDEL,{"write","hash","data"}};
        r["HGETDEL"] = {::HGETDEL,{"write","hash","data"}};
        r["HTTL"] = {::HTTL,{"read","hash","data"}};
        r["HGET"] = {::HGET,{"read","hash","data"}};
        r["HLEN"] = {::HLEN,{"read","hash","data"}};
        r["HEXPIRETIME"] = {::HEXPIRETIME,{"write","hash","data"}};
        r["HGETALL"] = {::HGETALL,{"read","hash","data"}};
        r["HKEYS"] = {::HKEYS,{"read","hash","data"}};
        r["HEXISTS"] = {::HEXISTS,{"read","hash","data"}};
        r["ZADD"] = {::ZADD,{"write","ordered","data"}};
        r["ZREM"] = {::ZREM,{"write","ordered","data"}};
        r["ZINCRBY"] = {::ZINCRBY,{"write","ordered","data"}};
        r["ZRANGE"] = {::ZRANGE,{"write","ordered","data"}};
        r["ZCARD"] = {::ZCARD,{"write","ordered","data"}};
        r["ZDIFF"] = {::ZDIFF,{"write","ordered","data"}};
        r["ZDIFFSTORE"] = {::ZDIFFSTORE,{"write","ordered","data"}};
        r["ZINTERSTORE"] = {::ZINTERSTORE,{"write","ordered","data"}};
        r["ZINTERCARD"] = {::ZINTERCARD,{"write","ordered","data"}};
        r["ZINTER"] = {::ZINTER,{"read","ordered","data"}};
        r["ZPOPMIN"] = {::ZPOPMIN,{"write","ordered","data"}};
        r["ZPOPMAX"] = {::ZPOPMAX,{"write","ordered","data"}};
        r["ZREVRANGE"] = {::ZREVRANGE,{"read","ordered","data"}};
        r["ZRANGEBYSCORE"] = {::ZRANGEBYSCORE,{"read","ordered","data"}};
        r["ZREVRANGEBYSCORE"] = {::ZREVRANGEBYSCORE,{"read","ordered","data"}};
        r["ZREMRANGEBYLEX"] = {::ZREMRANGEBYLEX,{"write","ordered","data"}};
        r["ZRANGEBYLEX"] = {::ZRANGEBYLEX,{"read","ordered","data"}};
        r["ZREVRANGEBYLEX"] = {::ZREVRANGEBYLEX,{"read","ordered","data"}};
        r["ZRANK"] = {::ZRANK,{"read","ordered","data"}};
        r["ZFASTRANK"] = {::ZFASTRANK,{"read","ordered","data"}};
    }

    return r;
}
