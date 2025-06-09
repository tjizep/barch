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

void load(const std::string &host, const std::string& port) {
    std::vector<std::string_view> params = {"b", host, port};
    swig_caller sc;
    int r = sc.call(params, RETRIEVE);
    if (r == 0) {
        art::std_log("loaded all shards from", host, port);
    }
}

void start(const std::string &host, const std::string& port) {
    std::vector<std::string_view> params = {"b", host, port};
    swig_caller sc;
    int r = sc.call(params, START);
    if (r == 0) {
        art::std_log("started server on", host, port);
    }
}
void start(const std::string& port) {
    start("127.0.0.1", port);
}
void stop() {
    std::vector<std::string_view> params = {"b"};
    swig_caller sc;
    int r = sc.call(params, STOP);
    if (r == 0) {
        art::std_log("stopped server");
    }
}
void ping(const std::string &host, const std::string& port) {
    std::vector<std::string_view> params = {"b", host, port};
    swig_caller sc;
    int r = sc.call(params, PING);
    if (r != 0) {
        art::std_log("ping failed", host, port);
    }
}

unsigned long long size()  {
    std::vector<std::string_view> params = {"b"};
    swig_caller sc;
    int r = sc.call(params, ::SIZE);
    if (r == 0) {
        return sc.results.empty() ? 0: std::atoll(sc.results[0].c_str());
    }
    return 0;
}
void save() {
    std::vector<std::string_view> params = {"b"};
    swig_caller sc;
    int r = sc.call(params, ::SAVE);
    if (r == 0) {}
}
void clear() {
    for (auto shard : art::get_shard_count()) {
        get_art(shard)->clear();
    }
}

KeyMap::KeyMap() {

}

void KeyMap::set(const std::string &key, const std::string &value) {
    params = {"b", key, value};

    int r = sc.call(params, SET);
    if (r != 0) {
        art::std_err("set failed", key, value);
    }

}
std::string KeyMap::get(const std::string &key) const {
    params = {"b", key};

    int r = sc.call(params, ::GET);
    if (r == 0) {
        return sc.results.empty() ? "": sc.results[0];
    }
    return "";
}
void KeyMap::erase(const std::string &key) {
    params = {"b", key};

    int r = sc.call(params, ::REM);
    if (r == 0) {}
}
std::string KeyMap::min() const {
    params = {"b"};

    int r = sc.call(params, ::MIN);
    if (r == 0) {
        return sc.results.empty() ? "": sc.results[0];
    }
    return "";
}
std::string KeyMap::max() const {
    params = {"b"};

    int r = sc.call(params, ::MAX);
    if (r == 0) {
        return sc.results.empty() ? "": sc.results[0];
    }
    return "";
}

void KeyMap::incr(const std::string& key, double by) {
    std::string b = std::to_string(by);
    params = {"b",key, b};

    int r = sc.call(params, ::INCRBY);
    if (r == 0) {}
}

void KeyMap::decr(const std::string& key, double by) {
    std::string b = std::to_string(by);
    params = {"b", key,b};

    int r = sc.call(params, ::DECRBY);
    if (r == 0) {}
}
std::vector<std::string>& KeyMap::glob(const std::string &glob, int max_) const {
    result.clear();

    if ( max_ > 0) {
        params = {"b", glob, "MAX", std::to_string(max_)} ;
    }else {
        params = {"b", glob} ;
    }

    int r = sc.call(params, ::KEYS);
    if (r == 0) {
        return sc.results;
    }

    return result;
}
size_t KeyMap::globCount(const std::string& glob) const {
    params = {"b", glob, "COUNT"};

    int r = sc.call(params, ::KEYS);
    if (r == 0) {
        return sc.results.empty() ? 0: std::atoll(sc.results[0].c_str());
    }
    return 0;

}
std::string KeyMap::lowerBound(const std::string& key) const {

    params = {"b", key};

    int r = sc.call(params, ::LB);
    if (r == 0) {
        return sc.results.empty() ? "": sc.results[0];
    }
    return "";
}

void load() {
    std::vector<std::string_view> params = {"b"};
    swig_caller sc;
    int r = sc.call(params, ::LOAD);
    if (r != 0) {
        art::std_err("load failed");
    }
}


