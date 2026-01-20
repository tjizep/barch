//
// Created by teejip on 5/20/25.
//

#include "swig_api.h"
#include "barch_apis.h"
#include "keys.h"
#include "caller.h"
#include "module.h"
#include "configuration.h"
#include "rpc_caller.h"
#include "rpc/server.h"

void setConfiguration(const std::string& name, const std::string& value) {
    barch::set_configuration_value(name,value);
}

void testKv() {
#if 1
    auto spc = barch::get_keyspace("test");
    barch::std_log("test","shard count",spc->get_shard_count());
    size_t z = 0;
    auto kv = spc->get(z);

    for (long i =0 ; i < 1000000;++i) {
        auto k = conversion::comparable_key(i);
        auto v = Variable(i);
        storage_release l(kv);
        kv->insert(k.get_value(),v.to_string(), true);
    }
#else
    KeyValue test("test");
    for (long long i = 0; i < 1000000; ++i) {
        test.set(Value(i).s(),Value(i).s());
    }
#endif

}

void setRoute(int shard, const std::string& host, int port) {

    std::vector<std::string_view> params = {"ADDROUTE", std::to_string(shard), host, std::to_string(port)};
    rpc_caller sc;
    int r = sc.call(params, ADDROUTE);
    if (r == 0) {
        barch::std_log("add route", host, port);
    }
}
void removeRoute(int shard) {
    std::vector<std::string_view> params = {"REMROUTE", std::to_string(shard)};
    rpc_caller sc;
    int r = sc.call(params, REMROUTE);
    if (r == 0) {
        barch::std_log("removed route", shard);
    }
}

Route getRoute(int shard) {
    std::vector<std::string_view> params = {"ROUTE", std::to_string(shard)};
    rpc_caller sc;
    int r = sc.call(params, ROUTE);
    if (r == 0 && sc.results.size() == 2) {
        return{sc.results[0], sc.results[1]};
    }
    return {};

}


void load(const std::string &host, const std::string& port) {
    std::vector<std::string_view> params = {"RETRIEVE", host, port};
    rpc_caller sc;
    int r = sc.call(params, RETRIEVE);
    if (r == 0) {
        barch::std_log("loaded all shards from", host, port);
    }
}
void load(const std::string &host, int port) {
    load(host, std::to_string(port));
}
void start(const std::string &host, const std::string& port) {
    std::vector<std::string_view> params = {"START", host, port};
    rpc_caller sc;
    int r = sc.call(params, START);
    if (r == 0) {
        barch::std_log("started server on", host, port);
    }
}
void start(const std::string &host, int port) {
    start(host, std::to_string(port));
}
void start(int port) {
    start("127.0.0.1", std::to_string(port));
}
void stop() {
    std::vector<std::string_view> params = {"STOP"};
    rpc_caller sc;
    int r = sc.call(params, STOP);
    if (r == 0) {
        barch::std_log("stopped server");
    }
}
void ping(const std::string &host, const std::string& port) {
    std::vector<std::string_view> params = {"PING", host, port};
    rpc_caller sc;
    int r = sc.call(params, PING);
    if (r != 0) {
        barch::std_log("ping failed", host, port);
    }
}
void ping(const std::string &host, int port) {
    ping(host, std::to_string(port));
}
void publish(const std::string &ip, const std::string &port) {
    std::vector<std::string_view> params = {"PUBLISH", ip, port};
    rpc_caller sc;
    int r = sc.call(params, PUBLISH);
    if (r != 0) {
        barch::std_err("publish failed", ip, port);
    }
}
void publish(const std::string &host, int port) {
    publish(host, std::to_string(port));
}

void pull(const std::string &ip, const std::string &port) {
    std::vector<std::string_view> params = {"PULL", ip, port};
    rpc_caller sc;
    int r = sc.call(params, PULL);
    if (r != 0) {
        barch::std_err("publish failed", ip, port);
    }
}
void pull(const std::string &host, int port) {
    pull(host, std::to_string(port));
}
long long calls(const std::string& name) {
    auto barch_functions = functions_by_name();
    auto f = barch_functions->find(name);
    if (f!=barch_functions->end()) {
        return f->second.calls.load();
    }
    return 0;
}

unsigned long long size()  {
    std::vector<std::string_view> params = {"SIZE"};
    rpc_caller sc;
    int r = sc.call(params, ::SIZE);
    if (r == 0) {
        return sc.results.empty() ? 0 : sc.results[0].to_int64();
    }
    return 0;
}
unsigned long long sizeAll()  {
    std::vector<std::string_view> params = {"SIZEALL"};
    rpc_caller sc;
    int r = sc.call(params, ::SIZEALL);
    if (r == 0) {
        return sc.results.empty() ? 0 : sc.results[0].to_int64();
    }
    return 0;
}
void save() {
    std::vector<std::string_view> params = {"SAVE"};
    rpc_caller sc;
    int r = sc.call(params, ::SAVE);
    if (r == 0) {}
}
void saveAll() {
    std::vector<std::string_view> params = {"SAVEALL"};
    rpc_caller sc;
    int r = sc.call(params, ::SAVEALL);
    if (r == 0) {}
}

bool clearAll() {
    std::vector<std::string_view> params = {"CLEARALL"};
    rpc_caller sc;
    int r = sc.call(params, ::CLEARALL);
    return r == 0;
}

bool clear() {
    std::vector<std::string_view> params = {"CLEAR"};
    rpc_caller sc;
    int r = sc.call(params, ::CLEAR);
    return r == 0;
}

List::List() {

}

List::List(const std::string &host, int port) {
    sc.host = barch::repl::create(host,port);
}

long long List::push(const std::string &key, const std::vector<std::string> &items) {
    std::unique_lock l(lock);
    params = {"LPUSH", key};
    params.insert(params.end(), items.begin(), items.end());
    barch::repl::call(params);
    int r = sc.call(params, LPUSH);
    if (r != 0) {
        barch::std_err("set failed", key);
    }
    return sc.results.empty() ? 0 : sc.results[0].to_int64();

}
long long List::len(const std::string &key) {
    std::unique_lock l(lock);
    params = {"LLEN", key};

    int r = sc.call(params, LLEN);
    if (r != 0) {
        barch::std_err("len failed", key);
    }
    return sc.results.empty() ? 0 : sc.results[0].to_int64();

}
Value List::brpop(const std::string &key, double timeout) {
    std::unique_lock l(lock);
    params = {"BRPOP", key, std::to_string(timeout)};
    return sc.callv(params, BRPOP);
}
Value List::blpop(const std::string &key, double timeout) {
    std::unique_lock l(lock);
    params = {"BLPOP", key, std::to_string(timeout)};
    return sc.callv(params, BLPOP);
}
long List::pop(const std::string &key, long long count) {
    std::unique_lock l(lock);
    params = {"LPOP", key, std::to_string(count)};
    barch::repl::call(params);
    return sc.callv(params, LPOP).i();
}

std::string List::back(const std::string &key) {
    std::unique_lock l(lock);
    params = {"LBACK", key};
    return  sc.callv(params, LBACK).s();
}

std::string List::front(const std::string &key) {
    std::unique_lock l(lock);
    params = {"LFRONT", key};
    return sc.callv(params, LFRONT).s();
}

KeyValue::KeyValue() {

}
long long KeyValue::getShards() const {
    return sc.kspace()->opt_shard_count;
}

void KeyValue::flush() {
}

bool KeyValue::getOrdered() const {
    return sc.kspace()->opt_ordered_keys;
}

KeyValue::KeyValue(std::string keys_space) {
    sc.set_kspace(barch::get_keyspace(keys_space));
}

KeyValue::KeyValue(const std::string& host, int port) {
    sc.host = barch::repl::create(host,port);
}
KeyValue::KeyValue(const std::string& host, int port, const std::string& keys_space) {
    sc.host = barch::repl::create(host,port);
    Caller::use(keys_space);
}
bool KeyValue::put(const std::string &key, const std::string& value) {
    sc.kspace()->buffer_insert(key, value);
    return true;
}
Value KeyValue::set(const std::string &key, const std::string &value) {
    std::unique_lock l(lock);
    params = {"SET", key, value};
    barch::repl::call(params);
    return sc.callv(params, SET);
}

Value KeyValue::seti(long long key, long long value) {
    std::unique_lock l(lock);
    params = {"SET", Value{key}.s(), Value{value}.s()};
    barch::repl::call(params);
    return sc.callv(params, SET) == "OK";
}

Value KeyValue::set(const std::string &key, long long value) {
    std::unique_lock l(lock);
    params = {"SET", key, Variable{value}.s()};
    barch::repl::call(params);
    return sc.callv(params, SET)=="OK";
}
Value KeyValue::set(const std::string &key, double value) {
    std::unique_lock l(lock);
    params = {"SET", key, Variable{value}.s()};
    barch::repl::call(params);
    return sc.callv(params, SET) == "OK";
}
std::string KeyValue::get(const std::string &key) const {
    std::unique_lock l(lock);
    params = {"GET", key};
    return  sc.callv(params, ::GET).s();
}

Value KeyValue::vget(const std::string &key) const {
    std::unique_lock l(lock);
    params = {"GET", key};
    return sc.callv(params, ::GET);
}

Value KeyValue::erase(const std::string &key) {
    std::unique_lock l(lock);
    params = {"REM", key};
    barch::repl::call(params);
    return sc.callv(params, ::REM);
}
bool KeyValue::exists(const std::string &key) {
    std::unique_lock l(lock);
    params = {"EXISTS", key};
    return sc.callv(params, ::EXISTS).b(); // may be too short
}
bool KeyValue::append(const std::string& key, const std::string& value) {
    std::unique_lock l(lock);
    params = {"APPEND", key, value};
    barch::repl::call(params);
    return sc.callv(params, ::APPEND) == "OK";
}

bool KeyValue::clear() {
    std::unique_lock l(lock);
    params = {"CLEAR"};
    barch::repl::call(params);
    return sc.callv(params, ::CLEAR) == "OK";
}

bool KeyValue::expire(const std::string &key, long long sec, const std::string& flag) {
    std::unique_lock l(lock);
    if (flag.empty()) {
        params = {"EXPIRE", key, std::to_string(sec)};
    }else
        params = {"EXPIRE", key, std::to_string(sec), flag};
    barch::repl::call(params);
    int r = sc.call(params, ::EXPIRE);
    if (r == 0) {
        return sc.results.empty() ? false: sc.results[0].i() == 1;
    }
    return false;
}

long long KeyValue::ttl(const std::string &key) {
    std::unique_lock l(lock);
    params = {"TTL", key};

    int r = sc.call(params, ::TTL);
    if (r == 0) {
        return sc.results.empty() ? 0: sc.results[0].i();
    }
    return 0;
}

Value KeyValue::min() const {
    std::unique_lock l(lock);
    params = {"MIN"};

    return sc.callv(params, ::MIN);
}
Value KeyValue::max() const {
    std::unique_lock l(lock);
    params = {"MAX"};
    return sc.callv(params, ::MAX);
}

Value KeyValue::incr(const std::string& key, double by) {
    std::unique_lock l(lock);
    params = {"INCRBY",key, Value((long long)by).s()};
    barch::repl::call(params);
    return sc.callv(params, ::INCRBY);
}

Value KeyValue::decr(const std::string& key, double by) {
    std::unique_lock l(lock);
    params = {"DECRBY", key, Value((long long)by).s()};
    barch::repl::call(params);
    return sc.callv(params, ::DECRBY);
}
Value KeyValue::decr(const std::string& key) {
    std::unique_lock l(lock);
    return decr(key, 1ll);
}

Value KeyValue::incr(const std::string& key, long long by) {
    std::unique_lock l(lock);
    params = {"INCRBY",key, Value(by).s()};
    barch::repl::call(params);
    return sc.callv(params, ::INCRBY);
}
Value KeyValue::incr(const std::string& key) {
    return incr(key, 1ll);
}

Value KeyValue::decr(const std::string& key, long long by) {
    std::unique_lock l(lock);
    params = {"DECRBY", key, Value{by}.s()};
    barch::repl::call(params);
    return sc.callv(params, ::DECRBY);

}
long long KeyValue::count(const std::string &start, const std::string &end) {
    std::unique_lock l(lock);
    result.clear();
    params = {"COUNT", start, end} ;
    return sc.callv(params, ::COUNT).i();
}
std::vector<Value> KeyValue::range(const std::string &start, const std::string &end, long long limit) {
    std::unique_lock l(lock);
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

std::vector<Value> KeyValue::glob(const std::string &glob, long long max_) const {
    std::unique_lock l(lock);
    result.clear();

    if ( max_ > 0) {
        params = {"KEYS", glob, "MAX", Value{max_}.s()} ;
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
    std::unique_lock l(lock);
    params = {"KEYS", glob, "COUNT"};

    return sc.callv(params, ::KEYS).i();

}
Value KeyValue::lowerBound(const std::string& key) const {
    std::unique_lock l(lock);
    params = {"LB", key};
    return  sc.callv(params, ::LB);
}

Value KeyValue::upperBound(const std::string& key) const {
    std::unique_lock l(lock);
    params = {"UB", key};

    return sc.callv(params, ::UB, "");
}

long long KeyValue::size() const {
    std::unique_lock l(lock);
    std::vector<std::string_view> params = {"SIZE"};
    return sc.callv(params, ::SIZE, 0).i();
}

void load() {

    std::vector<std::string_view> params = {"LOAD"};
    rpc_caller sc;
    int r = sc.call(params, ::LOAD);
    if (r != 0) {
        barch::std_err("load failed");
    }
}
repl_statistics repl_stats() {
    auto ar = barch::get_repl_statistics();
    repl_statistics r;
    r.instructions_failed = ar.instructions_failed;
    r.out_queue_size = ar.out_queue_size;
    r.bytes_recv = ar.bytes_recv;
    r.bytes_sent = ar.bytes_sent;
    r.insert_requests = ar.insert_requests;
    r.remove_requests = ar.remove_requests;
    r.routes_succeeded = ar.routes_succeeded;
    r.attempted_routes = ar.attempted_routes;
    r.request_errors = ar.request_errors;
    r.barch_requests = ar.barch_requests;
    return r;
}

ops_statistics ops_stats() {
    auto t =  barch::get_ops_statistics();
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
    auto t = barch::get_statistics();
    statistics_values r;
    r.bytes_allocated = t.bytes_allocated;
    r.bytes_interior = t.bytes_interior;
    r.exceptions_raised = t.exceptions_raised;
    r.heap_bytes_allocated = t.heap_bytes_allocated;
    r.keys_evicted = t.keys_evicted;
    r.last_vacuum_time = t.last_vacuum_time;
    r.leaf_nodes = t.leaf_nodes;
    r.leaf_nodes_replaced = t.leaf_nodes_replaced;
    r.node4_nodes = t.node4_nodes;
    r.node16_nodes = t.node16_nodes;
    r.node48_nodes = t.node48_nodes;
    r.node256_nodes = t.node256_nodes;
    r.node256_occupants = t.node256_occupants;
    r.value_bytes_compressed = t.value_bytes_compressed;
    r.pages_evicted = t.pages_evicted;
    r.pages_defragged = t.pages_defragged;
    r.pages_evicted = t.pages_evicted;
    r.vacuums_performed = t.vacuums_performed;
    r.maintenance_cycles = t.maintenance_cycles;
    r.shards = t.shards;
    r.local_calls = t.local_calls;
    r.max_spin = t.max_spin;
    r.logical_allocated = t.logical_allocated;
    r.oom_avoided_inserts = t.oom_avoided_inserts;
    r.keys_found = t.keys_found;
    r.new_keys_added = t.new_keys_added;
    r.keys_replaced = t.keys_replaced;
    return r;
}
configuration_values config() {
    configuration_values r;
    auto i = barch::get_configuration();
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
    r.maintenance_poll_delay = (long long)i.maintenance_poll_delay;
    r.max_defrag_page_count = (long long)i.max_defrag_page_count;
    r.max_modifications_before_save = (long long)i.max_modifications_before_save;
    r.min_fragmentation_ratio = i.min_fragmentation_ratio;
    r.n_max_memory_bytes = (long long)i.n_max_memory_bytes;
    r.rpc_max_buffer = (long long)i.rpc_max_buffer;
    r.rpc_client_max_wait_ms = (long long)i.rpc_client_max_wait_ms;
    r.save_interval = (long long)i.save_interval;
    r.use_vmm_memory = i.use_vmm_memory;
    return r;
}

Caller::Caller(){}
Caller::Caller(const std::string& host, int port) {
    sc.host = barch::repl::create(host,port);
}
bool Caller::use(const std::string& key_space) {
    std::unique_lock l(lock);
    params = {"USE",key_space};
    return sc.callv(params, ::USE) == "OK";
}
long long Caller::getShardCount() const {
    return sc.kspace()->opt_shard_count;
}
bool Caller::getOrdered() const {
    return sc.kspace()->opt_ordered_keys;
}
bool Caller::setOrdered(bool ordered) {
    std::unique_lock l(lock);
    params = {"SPACES", "OPTIONS", "SET", "ORDERED", ordered?"ON":"OFF"};
    return sc.callv(params, ::SPACES) == "OK";
}

std::vector<Value> Caller::call(const std::string &method, const std::vector<Value> &args) {
    std::unique_lock l(lock);
    std::string cn = std::string{params[0]};
    auto ic = barch_functions->find(cn);
    if (ic == barch_functions->end()) {
        barch::std_err("invalid call", cn);
        return {};
    }
    auto f = ic->second.call;
    ++ic->second.calls;

    params = {method};
    params.insert(params.end(), args.begin(), args.end());
    result.clear();
    if (ic->second.is_write()) {
        barch::repl::call(params);
    }
    int r = sc.call(params, f);
    if (r != 0) {
        result.insert(result.end(), sc.errors.begin(), sc.errors.end());
    }else {
        result.insert(result.end(), sc.results.begin(), sc.results.end());
    }
    return result;
}

HashSet::HashSet(){}

HashSet::HashSet(const std::string &host, int port) {
    sc.host = barch::repl::create(host,port);
}

void HashSet::set(const std::string &k, const std::vector<std::string>& members) {
    std::unique_lock l(lock);
    params = {"HSET", k};
    params.insert(params.end(), members.begin(), members.end());
    barch::repl::call(params);
    int r = sc.call(params, ::HSET);
    if (r != 0) {
        barch::std_err("set failed");
    }

}
Value HashSet::get(const std::string &k, const std::string &member) {
    std::unique_lock l(lock);
    params = {"HGET", k, member};
    return sc.callv(params, ::HGET);
}
std::vector<Value> HashSet::mget(const std::string& k, const std::vector<std::string> &fields) {
    std::unique_lock l(lock);
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
    std::unique_lock l(lock);
    result.clear();
    params = {"HGETALL", k};
    sc.call(params, ::HGETALL);

    result.insert(result.end(), sc.results.begin(), sc.results.end());

    return result;
}
std::vector<Value> HashSet::expiretime(const std::string &k, const std::vector<std::string> &fields) {
    std::unique_lock l(lock);
    result.clear();
    params = {"HEXPIRETIME", k, "FIELDS", Value{(long long)fields.size()}.s()};

    params.insert(params.end(), fields.begin(), fields.end());

    sc.call(params, ::HEXPIRETIME);
    result.insert(result.end(), sc.results.begin(), sc.results.end());

    return result;
}
Value HashSet::exists(const std::string &k, const std::string &member) {
    std::unique_lock l(lock);
    result.clear();
    params = {"HEXISTS", k, member};
    sc.call(params, ::HEXISTS);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    if (result.empty()) return {nullptr};
    return result[0];
}

Value HashSet::remove(const std::string &k, const std::vector<std::string> &member) {
    std::unique_lock l(lock);
    result.clear();
    params = {"HDEL", k};
    params.insert(params.end(), member.begin(), member.end());
    sc.call(params, ::HDEL);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    if (result.empty()) return {nullptr};
    return result[0];
}

Value HashSet::getdel(const std::string &k, const std::vector<std::string> &member) {
    std::unique_lock l(lock);
    result.clear();
    params = {"HGETDEL", k , "FIELDS", std::to_string(member.size())};
    params.insert(params.end(), member.begin(), member.end());
    sc.call(params, ::HGETDEL);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    if (result.empty()) return {nullptr};
    return result[0];
}
std::vector<Value> HashSet::ttl(const std::string &k, const std::vector<std::string> &member) {
    std::unique_lock l(lock);
    result.clear();
    params = {"HTTL", k , "FIELDS", std::to_string(member.size())};
    params.insert(params.end(), member.begin(), member.end());
    sc.call(params, ::HTTL);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}
// k+args+fields
std::vector<Value> HashSet::expire(const std::string &k, const std::vector<std::string> &args, const std::vector<std::string> &fields) {
    std::unique_lock l(lock);
    result.clear();
    params = {"HEXPIRE", k};
    params.insert(params.end(), args.begin(), args.end());
    params.emplace_back("FIELDS");
    params.emplace_back(std::to_string(fields.size()));
    params.insert(params.end(), fields.begin(), fields.end());
    barch::repl::call(params);
    sc.call(params, ::HEXPIRE);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}
std::vector<Value> HashSet::expireat(const std::string &k, long long exp, const std::string &flags, const std::vector<std::string> &fields) {
    std::unique_lock l(lock);
    result.clear();
    params = {"HEXPIREAT", k, std::to_string(exp), flags,"FIELDS", std::to_string(fields.size())};
    params.insert(params.end(), fields.begin(), fields.end());
    sc.call(params, ::HEXPIREAT);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}
Value HashSet::incrby(const std::string &k, const std::string& field, long long by) {
    std::unique_lock l(lock);
    result.clear();
    params = {"HINCRBY", k, field, std::to_string(by)};
    barch::repl::call(params);
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
    std::unique_lock l(lock);
    result.clear();
    params = {"ZADD", k};
    params.insert(params.end(), flags.begin(), flags.end());
    params.insert(params.end(), members.begin(), members.end());
    barch::repl::call(params);
    sc.call(params, ::ZADD);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

std::vector<Value> OrderedSet::range(const std::string &k, double start, double stop, const std::vector<std::string>& flags) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZRANGE", k, std::to_string(start), std::to_string(stop)};
    params.insert(params.end(), flags.begin(), flags.end());
    sc.call(params, ::ZRANGE);
    if (sc.results.empty()) return {nullptr};
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}
std::vector<Value> OrderedSet::revrange(const std::string &k, double start, double stop, const std::vector<std::string>& flags) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZREVRANGE", k, std::to_string(start), std::to_string(stop)};
    params.insert(params.end(), flags.begin(), flags.end());
    sc.call(params, ::ZREVRANGE);
    if (sc.results.empty()) return {nullptr};
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}

Value OrderedSet::card(const std::string &k) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZCARD", k};
    sc.call(params, ::ZCARD);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

Value OrderedSet::popmin(const std::string &k) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZPOPMIN", k};
    sc.call(params, ::ZPOPMIN);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

Value OrderedSet::popmax(const std::string &k) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZPOPMAX", k};
    sc.call(params, ::ZPOPMAX);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

Value OrderedSet::rank(const std::string &k, double lower, double upper) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZFASTRANK", k, std::to_string(lower), std::to_string(upper)};
    sc.call(params, ::ZFASTRANK);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

Value OrderedSet::remove(const std::string &k, const std::vector<std::string>& members) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZREM", k};
    params.insert(params.end(), members.begin(), members.end());
    barch::repl::call(params);
    sc.call(params, ::ZREM);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}
std::vector<Value> OrderedSet::diff(const std::vector<std::string>& keys, const std::vector<std::string>& flags) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZDIFF", std::to_string(keys.size())};
    params.insert(params.end(), keys.begin(), keys.end());
    params.insert(params.end(), flags.begin(), flags.end());
    sc.call(params, ::ZDIFF);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}

Value OrderedSet::diffstore(const std::string &destkey, const std::vector<std::string>& keys) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZDIFFSTORE", destkey, std::to_string(keys.size())};
    params.insert(params.end(), keys.begin(), keys.end());
    barch::repl::call(params);
    sc.call(params, ::ZDIFFSTORE);
    return sc.results[0];
}

Value OrderedSet::incrby(const std::string &key, double val, const std::string &field) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZINCRBY", key, std::to_string(val), field};
    barch::repl::call(params);
    sc.call(params, ::ZINCRBY);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

std::vector<Value> OrderedSet::inter(const std::vector<std::string>& keys, const std::vector<std::string>& flags) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZINTER", std::to_string(keys.size())};
    params.insert(params.end(), keys.begin(), keys.end());
    params.insert(params.end(), flags.begin(), flags.end());
    sc.call(params, ::ZINTER);
    result.insert(result.end(), sc.results.begin(), sc.results.end());
    return result;
}

Value OrderedSet::interstore(const std::string &destkey, const std::vector<std::string>& keys) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZINTERSTORE", destkey, std::to_string(keys.size())};
    params.insert(params.end(), keys.begin(), keys.end());
    barch::repl::call(params);
    sc.call(params, ::ZINTERSTORE);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

Value OrderedSet::intercard(const std::vector<std::string>& keys) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZINTERCARD", std::to_string(keys.size())};
    params.insert(params.end(), keys.begin(), keys.end());
    barch::repl::call(params);
    sc.call(params, ::ZINTERCARD);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}

Value OrderedSet::remrangebylex(const std::string &key, const std::string& lower, const std::string& upper) {
    std::unique_lock l(lock);
    result.clear();
    params = {"ZREMRANGEBYLEX", key, lower, upper};
    barch::repl::call(params);
    sc.call(params, ::ZREMRANGEBYLEX);
    if (sc.results.empty()) return {nullptr};
    return sc.results[0];
}
