//
// Created by teejip on 5/20/25.
//

#include "swig_api.h"
#include "barch_apis.h"
#include "keys.h"
#include "caller.h"
#include "module.h"
#include "configuration.h"


struct swig_caller : caller {
    std::vector<std::string> results{};
    std::vector<std::string> errors{};
    [[nodiscard]] int wrong_arity()  override {
        errors.emplace_back("wrong_arity");
        return 0;
    }
    [[nodiscard]] int syntax_error() override {
        errors.emplace_back("syntax_error");
        return 0;
    }
    [[nodiscard]] int error() override {
        errors.emplace_back("error");
        return 0;
    }
    [[nodiscard]] int error(const char * e) override {
        errors.emplace_back(e);
        return 0;
    }
    int key_check_error(art::value_type k) override {
        if (k.empty()) {
            errors.emplace_back("Key should not be empty");
        }else {
            errors.emplace_back("Unspecified key error");
        }
        return 0;
    }
    int null() override {
        results.emplace_back("");
        return 0;
    }
    [[nodiscard]] int ok() override {
        return 0;
    }
    int boolean(bool value) override {
        results.push_back(std::to_string(value));
        return 0;
    }
    int long_long(int64_t l) override {
        results.emplace_back(std::to_string(l));
        return 0;
    }
    int double_(double l) override {
        results.emplace_back(std::to_string(l));
        return 0;
    }
    int vt(art::value_type v) override {
        results.emplace_back(v.chars());
        return 0;
    }
    int simple(const char * v) override {
        results.emplace_back(v);
        return 0;
    }

    int start_array() override {
        results.emplace_back("start_array");
        return 0;
    }
    int end_array(size_t ) override {
        results.emplace_back("end_array");
        return 0;
    }
    int reply_encoded_key(art::value_type key) override {
        results.emplace_back(encoded_key_as_string(key));
        return 0;
    }
    template<typename TC>
    int call(const std::vector<std::string_view>& params, TC&& f) {
        arg_t args;

        for (const auto& s : params) {
            args.push_back({&*s.begin(),s.length()});
        }
        int r = f(*this, args);
        if (!errors.empty()) {
            r = -1;
        }
        return r;
    }
};

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
KeyMap::KeyMap() {

}

void KeyMap::set(const std::string &key, const std::string &value) {
    std::vector<std::string_view> params = {"b", key, value};
    swig_caller sc;
    int r = sc.call(params, SET);
    if (r != 0) {
        art::std_err("set failed", key, value);
    }

}
std::string KeyMap::get(const std::string &key) const {
    std::vector<std::string_view> params = {"b", key};
    swig_caller sc;
    int r = sc.call(params, ::GET);
    if (r == 0) {
        return sc.results.empty() ? "": sc.results[0];
    }
    return "";
}
void KeyMap::erase(const std::string &key) {
    std::vector<std::string_view> params = {"b", key};
    swig_caller sc;
    int r = sc.call(params, ::REM);
    if (r == 0) {}
}
std::string KeyMap::min() const {
    std::vector<std::string_view> params = {"b"};
    swig_caller sc;
    int r = sc.call(params, ::MIN);
    if (r == 0) {
        return sc.results.empty() ? "": sc.results[0];
    }
    return "";
}
std::string KeyMap::max() const {
    std::vector<std::string_view> params = {"b"};
    swig_caller sc;
    int r = sc.call(params, ::MAX);
    if (r == 0) {
        return sc.results.empty() ? "": sc.results[0];
    }
    return "";
}
size_t KeyMap::size() const {
    std::vector<std::string_view> params = {"b"};
    swig_caller sc;
    int r = sc.call(params, ::SIZE);
    if (r == 0) {
        return sc.results.empty() ? 0: std::atoll(sc.results[0].c_str());
    }
    return 0;
}
void KeyMap::save() {
    std::vector<std::string_view> params = {"b"};
    swig_caller sc;
    int r = sc.call(params, ::SAVE);
    if (r == 0) {}
}
void KeyMap::clear() {
    for (auto shard : art::get_shard_count()) {
        get_art(shard)->clear();
    }
}
void KeyMap::incr(const std::string& key, double by) {
    std::string b = std::to_string(by);
    std::vector<std::string_view> params = {"b",key, b};
    swig_caller sc;
    int r = sc.call(params, ::INCRBY);
    if (r == 0) {}
}

void KeyMap::decr(const std::string& key, double by) {
    std::string b = std::to_string(by);
    std::vector<std::string_view> params = {"b", key,b};
    swig_caller sc;
    int r = sc.call(params, ::DECRBY);
    if (r == 0) {}
}
std::vector<std::string> KeyMap::glob(const std::string &glob, int max_) const {
    std::vector<std::string_view> params;
    if ( max_ > 0) {
        params = {"b", glob, "MAX", std::to_string(max_)} ;
    }else {
        params = {"b", glob} ;
    }
    swig_caller sc;
    int r = sc.call(params, ::KEYS);
    if (r == 0) {
        return sc.results;
    }
    return {};
}
size_t KeyMap::globCount(const std::string& glob) const {
    std::vector<std::string_view> params = {"b", glob, "COUNT"};
    swig_caller sc;
    int r = sc.call(params, ::KEYS);
    if (r == 0) {
        return sc.results.empty() ? 0: std::atoll(sc.results[0].c_str());
    }
    return 0;

}
std::string KeyMap::lowerBound(const std::string& key) const {

    std::vector<std::string_view> params = {"b", key};
    swig_caller sc;
    int r = sc.call(params, ::LB);
    if (r == 0) {
        return sc.results.empty() ? "": sc.results[0];
    }
    return "";
}

void KeyMap::load() {
    std::vector<std::string_view> params = {"b"};
    swig_caller sc;
    int r = sc.call(params, ::LOAD);
    if (r != 0) {
        art::std_err("load failed");
    }
}


