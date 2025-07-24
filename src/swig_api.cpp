//
// Created by teejip on 5/20/25.
//

#include "swig_api.h"
#include "barch_apis.h"
#include "keys.h"
#include "caller.h"
#include "module.h"
#include "configuration.h"
#include "swig_caller.h"

void setConfiguration(const std::string& name, const std::string& value) {
    art::set_configuration_value(name,value);
}

void setRoute(int shard, const std::string& host, int port) {
    std::vector<std::string_view> params = {"ADDROUTE", std::to_string(shard), host, std::to_string(port)};
    swig_caller sc;
    int r = sc.call(params, ADDROUTE);
    if (r == 0) {
        art::std_log("add route", host, port);
    }
}
void removeRoute(int shard) {
    std::vector<std::string_view> params = {"REMROUTE", std::to_string(shard)};
    swig_caller sc;
    int r = sc.call(params, REMROUTE);
    if (r == 0) {
        art::std_log("removed route", shard);
    }
}

Route getRoute(int shard) {
    std::vector<std::string_view> params = {"ROUTE", std::to_string(shard)};
    swig_caller sc;
    int r = sc.call(params, ROUTE);
    if (r == 0 && sc.results.size() == 2) {
        return{sc.results[0], sc.results[1]};
    }
    return {};

}


void load(const std::string &host, const std::string& port) {
    std::vector<std::string_view> params = {"RETRIEVE", host, port};
    swig_caller sc;
    int r = sc.call(params, RETRIEVE);
    if (r == 0) {
        art::std_log("loaded all shards from", host, port);
    }
}
void load(const std::string &host, int port) {
    load(host, std::to_string(port));
}
void start(const std::string &host, const std::string& port) {
    std::vector<std::string_view> params = {"START", host, port};
    swig_caller sc;
    int r = sc.call(params, START);
    if (r == 0) {
        art::std_log("started server on", host, port);
    }
}
void start(const std::string &host, int port) {
    start(host, std::to_string(port));
}
void start(const std::string& port) {
    start("127.0.0.1", port);
}
void stop() {
    std::vector<std::string_view> params = {"STOP"};
    swig_caller sc;
    int r = sc.call(params, STOP);
    if (r == 0) {
        art::std_log("stopped server");
    }
}
void ping(const std::string &host, const std::string& port) {
    std::vector<std::string_view> params = {"PING", host, port};
    swig_caller sc;
    int r = sc.call(params, PING);
    if (r != 0) {
        art::std_log("ping failed", host, port);
    }
}
void ping(const std::string &host, int port) {
    ping(host, std::to_string(port));
}
void publish(const std::string &ip, const std::string &port) {
    std::vector<std::string_view> params = {"PUBLISH", ip, port};
    swig_caller sc;
    int r = sc.call(params, PUBLISH);
    if (r != 0) {
        art::std_err("publish failed", ip, port);
    }
}
void publish(const std::string &host, int port) {
    publish(host, std::to_string(port));
}

void pull(const std::string &ip, const std::string &port) {
    std::vector<std::string_view> params = {"PULL", ip, port};
    swig_caller sc;
    int r = sc.call(params, PULL);
    if (r != 0) {
        art::std_err("publish failed", ip, port);
    }
}
void pull(const std::string &host, int port) {
    pull(host, std::to_string(port));
}

unsigned long long size()  {
    std::vector<std::string_view> params = {"SIZE"};
    swig_caller sc;
    int r = sc.call(params, ::SIZE);
    if (r == 0) {
        return sc.results.empty() ? 0 : conversion::to_int64(sc.results[0]);
    }
    return 0;
}
void save() {
    std::vector<std::string_view> params = {"SAVE"};
    swig_caller sc;
    int r = sc.call(params, ::SAVE);
    if (r == 0) {}
}
void clear() {
    for (auto shard : art::get_shard_count()) {
        get_art(shard)->clear();
    }
}
List::List() {

}

List::List(const std::string &host, int port) {
    sc.host = barch::repl::create(host,port);
}

long long List::push(const std::string &key, const std::vector<std::string> &items) {
    params = {"LPUSH", key};
    params.insert(params.end(), items.begin(), items.end());

    int r = sc.call(params, LPUSH);
    if (r != 0) {
        art::std_err("set failed", key);
    }
    return sc.results.empty() ? 0 : conversion::to_int64(sc.results[0]);

}
long long List::len(const std::string &key) {
    params = {"LLEN", key};

    int r = sc.call(params, LLEN);
    if (r != 0) {
        art::std_err("len failed", key);
    }
    return sc.results.empty() ? 0 : conversion::to_int64(sc.results[0]);

}

long List::pop(const std::string &key, long long count) {
    params = {"LPOP", key, std::to_string(count)};

    int r = sc.call(params, LPOP);
    if (r != 0) {
        art::std_err("pop failed", key);
    }
    return sc.results.empty() ? 0 : conversion::to_int64(sc.results[0]);
}
std::string List::back(const std::string &key) {
    params = {"LBACK", key};
    int r = sc.call(params, LBACK);
    if (r != 0) {
        art::std_err("back failed", key);
    }
    return sc.results.empty() ? "" : conversion::to_string(sc.results[0]);
}

std::string List::front(const std::string &key) {
    params = {"LFRONT", key};
    int r = sc.call(params, LFRONT);
    if (r != 0) {
        art::std_err("front failed", key);
    }
    return sc.results.empty() ? "" : conversion::to_string(sc.results[0]);
}

KeyValue::KeyValue() {

}
KeyValue::KeyValue(const std::string& host, int port) {
    sc.host = barch::repl::create(host,port);
}
void KeyValue::set(const std::string &key, const std::string &value) {
    params = {"SET", key, value};

    int r = sc.call(params, SET);
    if (r != 0) {
        art::std_err("set failed", key, value);
    }

}

std::string KeyValue::get(const std::string &key) const {
    params = {"GET", key};

    int r = sc.call(params, ::GET);
    if (r == 0) {
        return sc.results.empty() ? "": conversion::to_string(sc.results[0]);
    }
    return "";
}

void KeyValue::erase(const std::string &key) {
    params = {"REM", key};

    int r = sc.call(params, ::REM);
    if (r == 0) {}
}
bool KeyValue::exists(const std::string &key) {
    params = {"EXISTS", key};

    int r = sc.call(params, ::EXISTS);
    if (r == 0) {
        return sc.results.empty() ? false: sc.results[0].b();
    }
    return false;
}

bool KeyValue::expire(const std::string &key, const std::string& flag) {
    params = {"EXPIRE", key, flag};

    int r = sc.call(params, ::EXPIRE);
    if (r == 0) {
        return sc.results.empty() ? false: sc.results[0].i() == 1;
    }
    return false;
}

long long KeyValue::ttl(const std::string &key) {
    params = {"TTL", key};

    int r = sc.call(params, ::TTL);
    if (r == 0) {
        return sc.results.empty() ? 0: sc.results[0].i();
    }
    return 0;
}

Value KeyValue::min() const {
    params = {"MIN"};

    int r = sc.call(params, ::MIN);
    if (r == 0) {
        return sc.results.empty() ? "": sc.results[0];
    }
    return "";
}
Value KeyValue::max() const {
    params = {"MAX"};

    int r = sc.call(params, ::MAX);
    if (r == 0) {
        return sc.results.empty() ? "": sc.results[0];
    }
    return {""};
}

void KeyValue::incr(const std::string& key, double by) {
    std::string b = std::to_string(by);
    params = {"INCRBY",key, b};

    int r = sc.call(params, ::INCRBY);
    if (r == 0) {}
}

void KeyValue::decr(const std::string& key, double by) {
    std::string b = std::to_string(by);
    params = {"DECRBY", key,b};

    int r = sc.call(params, ::DECRBY);
    if (r == 0) {}
}

void KeyValue::incr(const std::string& key, long long by) {
    std::string b = std::to_string(by);
    params = {"INCRBY",key, b};

    int r = sc.call(params, ::INCRBY);
    if (r == 0) {}
}

void KeyValue::decr(const std::string& key, long long by) {
    std::string b = std::to_string(by);
    params = {"DECRBY", key,b};

    int r = sc.call(params, ::DECRBY);
    if (r == 0) {}
}
long long KeyValue::count(const std::string &start, const std::string &end) {
    result.clear();
    params = {"COUNT", start, end} ;
    int r = sc.call(params, ::COUNT);
    if (r == 0) {
        if (!sc.results.empty()) {
            return sc.results[0].i();
        }
    }

    return 0;
}
std::vector<Value> KeyValue::range(const std::string &start, const std::string &end, long long limit) {
    result.clear();
    params = {"RANGE", start, end, std::to_string(limit)} ;
    int r = sc.call(params, ::RANGE);
    if (r == 0) {
        for (auto& v: sc.results) {
            result.emplace_back(v);
        }
    }

    return result;
}

std::vector<Value> KeyValue::glob(const std::string &glob, unsigned long long max_) const {
    result.clear();

    if ( max_ > 0) {
        params = {"KEYS", glob, "MAX", std::to_string(max_)} ;
    }else {
        params = {"KEYS", glob} ;
    }

    int r = sc.call(params, ::KEYS);
    if (r == 0) {
        for (auto& v: sc.results) {
            result.emplace_back(v);
        }
    }

    return result;
}
size_t KeyValue::globCount(const std::string& glob) const {
    params = {"KEYS", glob, "COUNT"};

    int r = sc.call(params, ::KEYS);
    if (r == 0) {
        return sc.results.empty() ? 0: conversion::to_int64(sc.results[0]);
    }
    return 0;

}
Value KeyValue::lowerBound(const std::string& key) const {

    params = {"LB", key};

    int r = sc.call(params, ::LB);
    if (r == 0) {
        return sc.results.empty() ? "": sc.results[0];
    }
    return "";
}

Value KeyValue::upperBound(const std::string& key) const {

    params = {"UB", key};

    int r = sc.call(params, ::UB);
    if (r == 0) {
        return sc.results.empty() ? "": sc.results[0];
    }
    return "";
}

long long KeyValue::size() const {
    std::vector<std::string_view> params = {"SIZE"};
    int r = sc.call(params, ::SIZE);
    if (r == 0) {
        return sc.results.empty() ? 0 : sc.results[0].i();
    }
    return 0;
}

void load() {
    std::vector<std::string_view> params = {"LOAD"};
    swig_caller sc;
    int r = sc.call(params, ::LOAD);
    if (r != 0) {
        art::std_err("load failed");
    }
}
repl_statistics repl_stats() {
    auto ar = art::get_repl_statistics();
    repl_statistics r;
    r.key_add_recv = ar.key_add_recv;
    r.key_add_recv_applied = ar.key_add_recv_applied;
    r.key_rem_recv = ar.key_rem_recv;
    r.key_rem_recv_applied = ar.key_rem_recv_applied;
    r.instructions_failed = ar.instructions_failed;
    r.out_queue_size = ar.out_queue_size;
    r.bytes_recv = ar.bytes_recv;
    r.bytes_sent = ar.bytes_sent;
    r.insert_requests = ar.insert_requests;
    r.remove_requests = ar.remove_requests;
    r.routes_succeeded = ar.routes_succeeded;
    r.attempted_routes = ar.attempted_routes;
    r.request_errors = ar.request_errors;
    return r;
}

ops_statistics ops_stats() {
    auto t =  art::get_ops_statistics();
    ops_statistics r;
    r.delete_ops = t.delete_ops;
    r.get_ops = t.get_ops;
    r.insert_ops = t.insert_ops;
    r.iter_ops = t.iter_ops;
    r.iter_range_ops = t.iter_range_ops;
    r.lb_ops = t.lb_ops;
    r.max_ops = t.max_ops;
    r.min_ops = t.min_ops;
    r.range_ops = t.range_ops;
    r.set_ops = t.set_ops;
    r.size_ops = t.size_ops;
    return r;
}

statistics_values stats() {
    auto t = art::get_statistics();
    statistics_values r;
    r.bytes_allocated = t.bytes_allocated;
    r.bytes_interior = t.bytes_interior;
    r.exceptions_raised = t.exceptions_raised;
    r.heap_bytes_allocated = t.heap_bytes_allocated;
    r.keys_evicted = t.keys_evicted;
    r.last_vacuum_time = t.last_vacuum_time;
    r.leaf_nodes = t.leaf_nodes;
    r.leaf_nodes_replaced = t.leaf_nodes_replaced;
    r.max_page_bytes_uncompressed = t.max_page_bytes_uncompressed;
    r.node4_nodes = t.node4_nodes;
    r.node16_nodes = t.node16_nodes;
    r.node48_nodes = t.node48_nodes;
    r.node256_nodes = t.node256_nodes;
    r.node256_occupants = t.node256_occupants;
    r.page_bytes_compressed = t.page_bytes_compressed;
    r.pages_compressed = t.pages_compressed;
    r.pages_evicted = t.pages_evicted;
    r.pages_defragged = t.pages_defragged;
    r.pages_evicted = t.pages_evicted;
    r.pages_uncompressed = t.pages_uncompressed;
    r.vacuums_performed = t.vacuums_performed;
    r.maintenance_cycles = t.maintenance_cycles;
    r.shards = t.shards;
    r.local_calls = t.local_calls;
    r.logical_allocated = t.logical_allocated;
    r.oom_avoided_inserts = t.oom_avoided_inserts;
    return r;
}
configuration_values config() {
    configuration_values r;
    auto i = art::get_configuration();
    r.active_defrag = i.active_defrag;
    r.bind_interface = i.bind_interface;
    r.compression = i.compression;
    r.evict_allkeys_lfu = i.evict_allkeys_lfu;
    r.evict_allkeys_lru = i.evict_allkeys_lru;
    r.evict_allkeys_random = i.evict_allkeys_random;
    r.evict_volatile_lfu = i.evict_volatile_lfu;
    r.evict_volatile_random = i.evict_volatile_random;
    r.evict_volatile_lru = i.evict_volatile_lru;
    r.evict_volatile_ttl = i.evict_volatile_ttl;
    r.external_host = i.external_host;
    r.iteration_worker_count = i.iteration_worker_count;
    r.listen_port = i.listen_port;
    r.log_page_access_trace = i.log_page_access_trace;
    r.maintenance_poll_delay = i.maintenance_poll_delay;
    r.max_defrag_page_count = i.max_defrag_page_count;
    r.max_modifications_before_save = i.max_modifications_before_save;
    r.min_fragmentation_ratio = i.min_fragmentation_ratio;
    r.n_max_memory_bytes = i.n_max_memory_bytes;
    r.rpc_max_buffer = i.rpc_max_buffer;
    r.rpc_client_max_wait_ms = i.rpc_client_max_wait_ms;
    r.save_interval = i.save_interval;
    r.use_vmm_memory = i.use_vmm_memory;
    return r;
}
HashSet::HashSet(){}

HashSet::HashSet(const std::string &host, int port) {
    sc.host = barch::repl::create(host,port);
}
void HashSet::set(const std::string &k, const std::vector<std::string>& members) {
    params = {"HSET", k};
    params.insert(params.end(), members.begin(), members.end());
    int r = sc.call(params, ::HSET);
    if (r != 0) {
        art::std_err("set failed");
    }
}
Value HashSet::get(const std::string &k, const std::string &member) {
    params = {"HGET", k, member};
    sc.call(params, ::HGET);

    if (!sc.results.empty()) {
        return sc.results[0];
    }
    return {nullptr};
}
std::vector<Value> HashSet::mget(const std::string& k, const std::vector<std::string> &fields) {
    result.clear();
    params = {"HMGET", k};
    params.insert(params.end(), fields.begin(), fields.end());
    int r = sc.call(params, ::HMGET);
    if (r != 0) {
    }

    result.insert(result.end(), sc.results.begin(), sc.results.end());

    return result;
}

std::vector<Value> HashSet::getall(const std::string& k) {
    result.clear();
    params = {"HGETALL", k};
    sc.call(params, ::HGETALL);

    result.insert(result.end(), sc.results.begin(), sc.results.end());

    return result;
}
std::vector<Value> HashSet::expiretime(const std::string &k, const std::vector<std::string> &fields) {
    result.clear();
    params = {"HEXPIRETIME", k, "FIELDS", std::to_string(fields.size())};

    params.insert(params.end(), fields.begin(), fields.end());

    sc.call(params, ::HEXPIRETIME);
    result.insert(result.end(), sc.results.begin(), sc.results.end());

    return result;
}
Value HashSet::exists(const std::string &k, const std::string &member) {
    result.clear();
    params = {"HEXISTS", k, member};
    sc.call(params, ::HEXISTS);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    if (result.empty()) return {nullptr};
    return result[0];
}

Value HashSet::remove(const std::string &k, const std::vector<std::string> &member) {
    result.clear();
    params = {"HDEL", k};
    params.insert(params.end(), member.begin(), member.end());
    sc.call(params, ::HDEL);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    if (result.empty()) return {nullptr};
    return result[0];
}

Value HashSet::getdel(const std::string &k, const std::vector<std::string> &member) {
    result.clear();
    params = {"HGETDEL", k , "FIELDS", std::to_string(member.size())};
    params.insert(params.end(), member.begin(), member.end());
    sc.call(params, ::HGETDEL);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    if (result.empty()) return {nullptr};
    return result[0];
}
std::vector<Value> HashSet::ttl(const std::string &k, const std::vector<std::string> &member) {
    result.clear();
    params = {"HTTL", k , "FIELDS", std::to_string(member.size())};
    params.insert(params.end(), member.begin(), member.end());
    sc.call(params, ::HTTL);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}
// k+args+fields
std::vector<Value> HashSet::expire(const std::string &k, const std::vector<std::string> &args, const std::vector<std::string> &fields) {
    result.clear();
    params = {"HEXPIRE", k};
    params.insert(params.end(), args.begin(), args.end());
    params.emplace_back("FIELDS");
    params.emplace_back(std::to_string(fields.size()));
    params.insert(params.end(), fields.begin(), fields.end());

    sc.call(params, ::HEXPIRE);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}
std::vector<Value> HashSet::expireat(const std::string &k, long long exp, const std::string &flags, const std::vector<std::string> &fields) {
    result.clear();
    params = {"HEXPIREAT", k, std::to_string(exp), flags,"FIELDS", std::to_string(fields.size())};
    params.insert(params.end(), fields.begin(), fields.end());
    sc.call(params, ::HEXPIREAT);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}
Value HashSet::incrby(const std::string &k, const std::string& field, long long by) {
    result.clear();
    params = {"HINCRBY", k, field, std::to_string(by)};
    sc.call(params, ::HINCRBY);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

OrderedSet::OrderedSet() {

}

OrderedSet::OrderedSet(const std::string &host, int port) {
    sc.host = barch::repl::create(host,port);
}

Value OrderedSet::add(const std::string &k, const std::vector<std::string>& flags, const std::vector<std::string>& members) {
    result.clear();
    params = {"ZADD", k};
    params.insert(params.end(), flags.begin(), flags.end());
    params.insert(params.end(), members.begin(), members.end());
    sc.call(params, ::ZADD);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

std::vector<Value> OrderedSet::range(const std::string &k, double start, double stop, const std::vector<std::string>& flags) {
    result.clear();
    params = {"ZRANGE", k, std::to_string(start), std::to_string(stop)};
    params.insert(params.end(), flags.begin(), flags.end());
    sc.call(params, ::ZRANGE);
    if (sc.results.empty()) return {nullptr};
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}
std::vector<Value> OrderedSet::revrange(const std::string &k, double start, double stop, const std::vector<std::string>& flags) {
    result.clear();
    params = {"ZREVRANGE", k, std::to_string(start), std::to_string(stop)};
    params.insert(params.end(), flags.begin(), flags.end());
    sc.call(params, ::ZREVRANGE);
    if (sc.results.empty()) return {nullptr};
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}

Value OrderedSet::card(const std::string &k) {
    result.clear();
    params = {"ZCARD", k};
    sc.call(params, ::ZCARD);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

Value OrderedSet::popmin(const std::string &k) {
    result.clear();
    params = {"ZPOPMIN", k};
    sc.call(params, ::ZPOPMIN);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

Value OrderedSet::popmax(const std::string &k) {
    result.clear();
    params = {"ZPOPMAX", k};
    sc.call(params, ::ZPOPMAX);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

Value OrderedSet::rank(const std::string &k, double lower, double upper) {
    result.clear();
    params = {"ZFASTRANK", k, std::to_string(lower), std::to_string(upper)};
    sc.call(params, ::ZFASTRANK);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

Value OrderedSet::remove(const std::string &k, const std::vector<std::string>& members) {
    result.clear();
    params = {"ZREM", k};
    params.insert(params.end(), members.begin(), members.end());
    sc.call(params, ::ZREM);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}
std::vector<Value> OrderedSet::diff(const std::vector<std::string>& keys, const std::vector<std::string>& flags) {
    result.clear();
    params = {"ZDIFF", std::to_string(keys.size())};
    params.insert(params.end(), keys.begin(), keys.end());
    params.insert(params.end(), flags.begin(), flags.end());
    sc.call(params, ::ZDIFF);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}

Value OrderedSet::diffstore(const std::string &destkey, const std::vector<std::string>& keys) {
    result.clear();
    params = {"ZDIFFSTORE", destkey, std::to_string(keys.size())};
    params.insert(params.end(), keys.begin(), keys.end());
    sc.call(params, ::ZDIFFSTORE);
    return sc.results[0];
}

Value OrderedSet::incrby(const std::string &key, double val, const std::string &field) {
    result.clear();
    params = {"ZINCRBY", key, std::to_string(val), field};
    sc.call(params, ::ZINCRBY);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

std::vector<Value> OrderedSet::inter(const std::vector<std::string>& keys, const std::vector<std::string>& flags) {
    result.clear();
    params = {"ZINTER", std::to_string(keys.size())};
    params.insert(params.end(), keys.begin(), keys.end());
    params.insert(params.end(), flags.begin(), flags.end());
    sc.call(params, ::ZINTER);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}

Value OrderedSet::interstore(const std::string &destkey, const std::vector<std::string>& keys) {
    result.clear();
    params = {"ZINTERSTORE", destkey, std::to_string(keys.size())};
    params.insert(params.end(), keys.begin(), keys.end());
    sc.call(params, ::ZINTERSTORE);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

Value OrderedSet::intercard(const std::vector<std::string>& keys) {
    result.clear();
    params = {"ZINTERCARD", std::to_string(keys.size())};
    params.insert(params.end(), keys.begin(), keys.end());
    sc.call(params, ::ZINTERCARD);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

Value OrderedSet::remrangebylex(const std::string &key, const std::string& lower, const std::string& upper) {
    result.clear();
    params = {"ZREMRANGEBYLEX", key, lower, upper};
    sc.call(params, ::ZREMRANGEBYLEX);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}
