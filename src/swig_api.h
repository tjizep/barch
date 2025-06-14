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
void ping(const std::string &host, const std::string& port);
void start(const std::string &host, const std::string& port);
void start(const std::string& port);
void stop();
unsigned long long size();
void save();
void load();
void clear();
void publish(const std::string &ip, const std::string &port);
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
};

repl_statistics repl_stats();
art_ops_statistics ops_stats();
art_statistics stats();
/**
 * A value holds one of string, integer, double, bool or null
 */
class Value {
public:
    int tuple = 0;
    Value(){};
    Value(nullptr_t): var(nullptr){};
    Value(const conversion::Variable& var) : var(var){};
    Value(const bool var) : var(var){};
    Value(const std::string& var) : var(var){};
    Value(const char* var) : var(var){};
    Value(const double& var) : var(var){};
    Value(const long long& var) : var((int64_t)var){};
    Value& operator=(const std::string& v) {
        var = v;
        return *this;
    };
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
        return conversion::to_string(var);
    }

    double d() const {
        return conversion::to_double(var);
    }

    long long i() const {
        return conversion::to_int64(var);
    }

    long long b() const {
        return conversion::to_bool(var);
    }

    std::string t() const {
        switch (var.index()) {
            case 0: return "boolean";
            case 1: return "integer";
            case 2: return "double";
            case 3: return "string";
            case 4: return "null";

            default:
                return "<unknown>";
        }
    }

    void set(const conversion::Variable& v) {
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
    conversion::Variable var{};
};

class KeyMap {
public:
    KeyMap();
    void set(const std::string &key, const std::string &value);
    std::string get(const std::string &key) const;
    void incr(const std::string& key, double by);
    void decr(const std::string& key, double by);
    void erase(const std::string &key);
    std::vector<Value> glob(const std::string& glob, int max_ = 0) const;
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
    Value add(const std::string &k, const std::vector<std::string>& flags, const std::vector<std::string>& members);
    std::vector<Value> range(const std::string &k, double start, double stop,const std::vector<std::string>& flags);
    Value card(const std::string &k);
    Value popmin(const std::string &k);
    Value popmax(const std::string &k);
    Value rank(const std::string &k, double lower, double upper);

private:
    mutable std::vector<std::string_view> params{};
    mutable std::vector<Value> result{};
    mutable swig_caller sc{};
};
#endif //SWIG_API_H
