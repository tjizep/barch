#include "sastam.h"
#include "barch_apis.h"
//
// Created by teejip on 7/13/25.
//

std::unordered_map<std::string, barch_info>& functions_by_name() {
    static std::unordered_map<std::string, barch_info> r;
    if (!r.empty()) return r;

    r["SET"] = {::SET};
    r["KEYS"] = {::KEYS};
    r["INCR"] = {::INCR};
    r["INCRBY"] = {::INCRBY};
    r["DECR"] = {::DECR};
    r["DECRBY"] = {::DECRBY};
    r["EXISTS"] = {::EXISTS};
    r["EXPIRE"] = {::EXPIRE};
    r["MSET"] = {::MSET};
    r["ADD"] = {::ADD};
    r["GET"] = {::GET};
    r["MGET"] = {::MGET};
    r["MIN"] = {::MIN};
    r["MAX"] = {::MAX};
    r["LB"] = {::LB};
    r["REM"] = {::REM};
    r["RANGE"] = {::RANGE};
    r["TTL"] = {::RANGE};
    r["SIZE"] = {::SIZE};
    r["DBSIZE"] = {::SIZE};
    r["SAVE"] = {::SAVE,false};
    r["FLUSHDB"] = {::CLEAR,false};
    r["FLUSHALL"] = {::CLEAR,false};
    r["STATS"] = {::STATS,false};
    r["OPS"] = {OPS,false};

    r["ADDROUTE"] = {::ADDROUTE,false};
    r["ROUTE"] = {::ROUTE,false};
    r["REMROUTE"] = {::REMROUTE,false};
    r["PUBLISH"] = {::PUBLISH,false};
    r["PULL"] = {::PULL,false};
    r["LOAD"] = {::LOAD,false};
    r["CONFIG"] = {::CONFIG,false};

    r["LBACK"] = {::LBACK};
    r["LFRONT"] = {::LFRONT};
    r["LPUSH"] = {::LPUSH};
    r["LPOP"] = {::LPOP};
    r["LLEN"] = {::LLEN};
    r["START"] = {::START,false};
    r["STOP"] = {::STOP,false};
    r["RETRIEVE"] = {::RETRIEVE,false};
    r["PING"] = {::PING,false};
    r["HSET"] = {::HSET};
    r["HEXPIREAT"] = {::HEXPIREAT};
    r["HEXPIRE"] = {::HEXPIRE};
    //r["HGETEX"] = ::HGETEX;
    r["HMGET"] = {::HMGET};
    r["HINCRBY"] = {::HINCRBY};
    r["HINCRBYFLOAT"] = {::HINCRBYFLOAT};
    r["HDEL"] = {::HDEL};
    r["HGETDEL"] = {::HGETDEL};
    r["HTTL"] = {::HTTL};
    r["HGET"] = {::HGET};
    r["HLEN"] = {::HLEN};
    r["HEXPIRETIME"] = {::HEXPIRETIME};
    r["HGETALL"] = {::HGETALL};
    r["HKEYS"] = {::HKEYS};
    r["HEXISTS"] = {::HEXISTS};
    r["ZADD"] = {::ZADD};
    r["ZREM"] = {::ZREM};
    r["ZINCRBY"] = {::ZINCRBY};
    r["ZRANGE"] = {::ZRANGE};
    r["ZCARD"] = {::ZCARD};
    r["ZDIFF"] = {::ZDIFF};
    r["ZDIFFSTORE"] = {::ZDIFFSTORE};
    r["ZINTERSTORE"] = {::ZINTERSTORE};
    r["ZINTERCARD"] = {::ZINTERCARD};
    r["ZINTER"] = {::ZINTER};
    r["ZPOPMIN"] = {::ZPOPMIN};
    r["ZPOPMAX"] = {::ZPOPMAX};
    r["ZREVRANGE"] = {::ZREVRANGE};
    r["ZRANGEBYSCORE"] = {::ZRANGEBYSCORE};
    r["ZREVRANGEBYSCORE"] = {::ZREVRANGEBYSCORE};
    r["ZREMRANGEBYLEX"] = {::ZREMRANGEBYLEX};
    r["ZRANGEBYLEX"] = {::ZRANGEBYLEX};
    r["ZREVRANGEBYLEX"] = {::ZREVRANGEBYLEX};
    r["ZRANK"] = {::ZRANK};
    r["ZFASTRANK"] = {::ZFASTRANK};
    return r;
}