//
// Created by teejip on 5/20/25.
//

#include "swig_api.h"
#include "barch_apis.h"
#include "keys.h"
#include "caller.h"
#include "module.h"

struct swig_caller : caller {
    std::vector<std::string> results{};
    [[nodiscard]] int wrong_arity()  override {
        results.push_back("wrong_arity");
        return 0;
    }
    [[nodiscard]] int syntax_error() override {
        results.push_back("syntax_error");
        return 0;
    }
    [[nodiscard]] int error() override {
        results.push_back("error");
        return 0;
    }
    [[nodiscard]] int error(const char * e) override {
        results.push_back(e);
        return 0;
    }
    int key_check_error(art::value_type k) override {
        results.push_back(k.chars());
        return 0;
    }
    int null() override {
        results.push_back("");
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
        results.push_back(std::to_string(l));
        return 0;
    }
    int double_(double l) override {
        results.push_back(std::to_string(l));
        return 0;
    }
    int vt(art::value_type v) override {
        results.push_back(v.chars());
        return 0;
    }
    int simple(const char * v) override {
        results.push_back(v);
        return 0;
    }

    int start_array() override {
        results.push_back("start_array");
        return 0;
    }
    int end_array(size_t ) override {
        results.push_back("end_array");
        return 0;
    }
    int reply_encoded_key(art::value_type key) override {
        results.push_back(encoded_key_as_string(key));
        return 0;
    }
    template<typename TC>
    int call(const std::vector<std::string_view>& params, TC&& f) {
        arg_t args;

        for (const auto& s : params) {
            args.push_back({&*s.begin(),s.length()});
        }
        return f(*this, args);
    }
};

KeyMap::KeyMap() {

}

void KeyMap::set(const std::string &key, const std::string &value) {
    std::vector<std::string_view> params = {"b", key, value};
    swig_caller sc;
    int r = sc.call(params, SET);
    if (r == 0) {

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

