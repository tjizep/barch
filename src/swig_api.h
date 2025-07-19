//
// Created by teejip on 5/20/25.
//

#ifndef SWIG_API_H
#define SWIG_API_H
#include "swig_caller.h"
#include <string>
#include <vector>
void setConfiguration(const std::string& name, const std::string& value);
void load(const std::string& host, const std::string& port);
void load(const std::string& host, int port);
void ping(const std::string &host, const std::string& port);
void ping(const std::string &host, int port);
void start(const std::string &host, const std::string& port);
void start(const std::string &host, int port);
void start(const std::string& port);
void stop();


unsigned long long size();
void save();
void load();
void clear();
void publish(const std::string &ip, const std::string &port);
void publish(const std::string &ip, int port);
void pull(const std::string &ip, const std::string &port);
void pull(const std::string &ip, int port);
struct repl_statistics {
    repl_statistics(){}
    ~repl_statistics(){}
    long long key_add_recv{};
    long long key_add_recv_applied{};
    long long key_rem_recv{};
    long long key_rem_recv_applied{};
    long long bytes_recv{};
    long long bytes_sent{};
    long long out_queue_size{};
    long long instructions_failed{};
    long long insert_requests{};
    long long remove_requests{};
    long long find_requests{};
    long long request_errors{};
    long long redis_sessions{};
};
struct ops_statistics {
    ops_statistics(){}
    ~ops_statistics(){}
    long long delete_ops {};
    long long set_ops {};
    long long iter_ops {};
    long long iter_range_ops {};
    long long range_ops {};
    long long get_ops {};
    long long lb_ops {};
    long long size_ops {};
    long long insert_ops {};
    long long min_ops {};
    long long max_ops {};
};
struct statistics_values {
    statistics_values() {}
    ~statistics_values() {}
    long long leaf_nodes {};
    long long node4_nodes {};
    long long node16_nodes {};
    long long node48_nodes {};
    long long node256_nodes {};
    long long node256_occupants {};
    long long bytes_allocated {};
    long long bytes_interior {};
    long long heap_bytes_allocated {};
    long long page_bytes_compressed {};
    long long pages_uncompressed {};
    long long pages_compressed {};
    long long max_page_bytes_uncompressed {};
    long long page_bytes_uncompressed {};
    long long vacuums_performed {};
    long long last_vacuum_time {};
    long long leaf_nodes_replaced {};
    long long pages_evicted {};
    long long keys_evicted {};
    long long pages_defragged {};
    long long exceptions_raised {};
    long long maintenance_cycles {};
    long long shards {};
    long long local_calls {};
    long long logical_allocated {};
    long long oom_avoided_inserts {};
};

struct configuration_values {
    int compression = 0;
    long long n_max_memory_bytes{std::numeric_limits<long long>::max()};
    long long maintenance_poll_delay{10};
    long long max_defrag_page_count{1};
    long long save_interval{120 * 1000};
    long long max_modifications_before_save{1300000};
    long long rpc_max_buffer{32768*4};
    long long rpc_client_max_wait_ms{30000};
    unsigned iteration_worker_count{2};
    float min_fragmentation_ratio = 0.6f;
    bool use_vmm_memory{true};
    bool active_defrag = false;
    bool evict_volatile_lru{false};
    bool evict_allkeys_lru{false};
    bool evict_volatile_lfu{false};
    bool evict_allkeys_lfu{false};
    bool evict_volatile_random{false};
    bool evict_allkeys_random{false};
    bool evict_volatile_ttl{false};
    bool log_page_access_trace{false};
    std::string external_host{"localhost"};
    std::string bind_interface{"127.0.0.1"};
    int listen_port{12145};
};
configuration_values config();
repl_statistics repl_stats();
ops_statistics ops_stats();
statistics_values stats();
/**
 * A value holds one of string, integer, double, bool or null
 */
class Value {
public:
    int tuple = 0;
    Value(){};
    Value(nullptr_t): var(nullptr){};
    Value(const Variable& var) : var(var){};
    Value(const bool var) : var(var){};
    Value(const std::string& var) : var(var){};
    Value(const char* var) : var(var){};
    Value(const double& var) : var(var){};
    Value(const long long& var) : var((int64_t)var){};
    bool isBoolean() const {
        return var.isBoolean();
    }
    bool isInteger() const {
        return var.isInteger();
    }
    bool isDouble() const {
        return var.isDouble();
    }
    bool isString() const {
        return var.isString();
    }
    bool isNull() const {
        return var.isNull();
    }
    void set(const char* v)  {
        var = v;
    };
    void set(const double& v) {
        var = v;
    };
    void set(const long long& v) {
        var = v;
    };

    std::string s() const {
        return var.s();
    }

    double d() const {
        return var.d();
    }

    long long i() const {
        return var.i();
    }

    long long b() const {
        return var.b();
    }

    std::string t() const {
        return var.t();
    }

    void set(const Variable& v) {
        this->var = v;
    }

    bool operator==(const std::string& r)  const {
        return r == s();
    }

    bool operator==(const double& r)  const {
        return r == d();
    }

    bool operator==(const long long& r)  const {
        return r == i();
    }

    bool operator<(const std::string& r)  const {
        return s() < r;
    }

    bool operator<(const double& r)  const {
        return d() < r;
    }

    bool operator<(const long long& r)  const {
        return i() < r;
    }

    bool operator>(const std::string& r)  const {
        return s() > r;
    }

    bool operator>(const double& r)  const {
        return d() > r;
    }

    bool operator>(const long long& r)  const {
        return i() > r;
    }

    bool operator<=(const std::string& r)  const {
        return s() <= r;
    }

    bool operator<=(const double& r)  const {
        return d() <= r;
    }

    bool operator<=(const long long& r)  const {
        return i() <= r;
    }
    bool operator>=(const std::string& r)  const {
        return s() >= r;
    }

    bool operator>=(const double& r)  const {
        return d() >= r;
    }

    bool operator>=(const long long& r) const {
        return i() >= r;
    }
    operator std::string() const {
        return s();
    }
    operator std::string_view() const {
        return s();
    }
private:
    Variable var{};
};
struct Route {
    Route(const Value& h, const Value& p) : host(h), port(p) {}
    Route() {}
    Value host{};
    Value port{};
};
void setRoute(int shard, const std::string& host, int port);
void removeRoute(int shard);
Route getRoute(int shard);
class List {
public:
    List();
    List(const std::string& host, int port);
    long long push(const std::string &key, const std::vector<std::string> &items);
    long long len(const std::string &key);
    std::string back(const std::string &key);
    std::string front(const std::string &key);
    long pop(const std::string &key,long long count);
private:
    mutable std::vector<std::string_view> params {};
    mutable std::vector<Value> result{};
    mutable swig_caller sc{};
};

class KeyValue {
public:
    KeyValue();
    KeyValue(const std::string& host, int port);
    void set(const std::string &key, const std::string &value);
    std::string get(const std::string &key) const;
    void incr(const std::string& key, double by);
    void incr(const std::string& key, long long by);
    void decr(const std::string& key, double by);
    void decr(const std::string& key, long long by);
    void erase(const std::string &key);
    std::vector<Value> glob(const std::string& glob, unsigned long long max_ = 0) const;
    size_t globCount(const std::string& glob) const;
    Value lowerBound(const std::string& key) const ;

    Value min() const ;
    Value max() const ;
private:
    mutable std::vector<std::string_view> params {};
    mutable std::vector<Value> result{};
    mutable swig_caller sc{};
};

/**
 * The has set like interface
 */
class HashSet {
public:

    HashSet();
    HashSet(const std::string& host, int port);
    void set(const std::string &k, const std::vector<std::string>& members);
    Value get(const std::string &k, const std::string &member);
    std::vector<Value> mget(const std::string &k, const std::vector<std::string> &fields);
    std::vector<Value> getall(const std::string &k);
    std::vector<Value> expiretime(const std::string &k, const std::vector<std::string> &fields);
    Value exists(const std::string &k, const std::string &member);
    Value remove(const std::string &k, const std::vector<std::string> &member);
    Value getdel(const std::string &k, const std::vector<std::string> &member);
    std::vector<Value> ttl(const std::string &k, const std::vector<std::string> &member);
    std::vector<Value> expire(const std::string &k, const std::vector<std::string> &args, const std::vector<std::string> &fields);
    std::vector<Value> expireat(const std::string &k, long long exp, const std::string &flags, const std::vector<std::string> &fields);
    Value incrby(const std::string &k, const std::string& field, long long by);

private:
    mutable std::vector<std::string_view> params{};
    mutable std::vector<Value> result{};
    mutable swig_caller sc{};
};

/**
 * the ordered set like interface similar to Z* commands in redis or valkey
 *
 */
class OrderedSet {
public:

    OrderedSet();
    OrderedSet(const std::string& host, int port);
    Value add(const std::string &k, const std::vector<std::string>& flags, const std::vector<std::string>& members);
    Value add(const std::string &k, const std::vector<std::string>& members) {
        return add(k, {},members);
    }

    std::vector<Value> range(const std::string &k, double start, double stop,const std::vector<std::string>& flags);
    std::vector<Value> range(const std::string &k, double start, double stop) {
        return range(k, start, stop, {});
    }
    std::vector<Value> revrange(const std::string &k, double start, double stop,const std::vector<std::string>& flags);
    std::vector<Value> revrange(const std::string &k, double start, double stop) {
        return revrange(k, start, stop, {});
    }
    Value card(const std::string &k);
    Value popmin(const std::string &k);
    Value popmax(const std::string &k);
    Value rank(const std::string &k, double lower, double upper);
    Value rank(const std::string &k, double upper) {
        return rank(k, std::numeric_limits<double>::min(), upper);
    }
    Value remove(const std::string &k, const std::vector<std::string>& members);
    Value count(const std::string &k, double min, double max) {
        return rank(k, min, max);
    }
    std::vector<Value> diff(const std::vector<std::string>& keys, const std::vector<std::string>& flags);
    std::vector<Value> diff(const std::vector<std::string>& keys) {
        return diff(keys, {});
    };
    Value diffstore(const std::string &destkey, const std::vector<std::string>& keys);
    Value incrby(const std::string &key, double val, const std::string &field);
    std::vector<Value> inter(const std::vector<std::string>& keys, const std::vector<std::string>& flags);
    std::vector<Value> inter(const std::vector<std::string>& keys) {
        return inter(keys, {});
    };
    Value interstore(const std::string &destkey, const std::vector<std::string>& keys);
    Value intercard(const std::vector<std::string>& keys);
    Value remrangebylex(const std::string &key, const std::string& lower, const std::string& upper);
private:
    mutable std::vector<std::string_view> params{};
    mutable std::vector<Value> result{};
    mutable swig_caller sc{};
};
#endif //SWIG_API_H
