//
// Created by teejip on 6/6/25.
//

#ifndef SWIG_CALLER_H
#define SWIG_CALLER_H
#include "caller.h"
#include <string>
#include <vector>
#include "keys.h"
#include "module.h"
#include "server.h"
#include "barch_apis.h"
#include "sastam.h"
struct swig_caller : caller {

    std::shared_ptr<barch::repl::rpc> host {};
    heap::vector<std::shared_ptr<barch::repl::rpc>> routes {};
    size_t valid_routes{};
    std::string r{};
    heap::vector<Variable> results{};
    heap::vector<std::string> errors{};
    swig_caller() {
        routes.reserve(art::get_shard_count().size());
        for (size_t shard : art::get_shard_count()) {
            auto route = barch::repl::get_route(shard);
            if (route.ip.empty()) {
                routes[shard] = nullptr;
            }else {
                ++valid_routes;
                routes.emplace_back(barch::repl::create(route.ip,route.port));
            }
        }
    }
    [[nodiscard]] int wrong_arity()  override {
        errors.emplace_back("wrong_arity");
        return 0;
    }
    [[nodiscard]] int syntax_error() override {
        errors.emplace_back("syntax_error");
        return 0;
    }
    [[nodiscard]] int error() const override {
        return -1;
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
        results.emplace_back(nullptr);
        return 0;
    }
    [[nodiscard]] int ok() override {
        return 0;
    }
    int boolean(bool value) override {
        results.push_back(value);
        return 0;
    }
    int long_long(int64_t l) override {
        results.emplace_back(l);
        return 0;
    }
    int double_(double l) override {
        results.emplace_back(l);
        return 0;
    }
    int vt(art::value_type v) override {
        r.clear();
        r.push_back('$');
        r.insert(r.end(), v.begin(), v.end());
        results.emplace_back(r); // values are currently always a string
        return 0;
    }
    int simple(const char * v) override {
        results.emplace_back(v);
        return 0;
    }

    int start_array() override {
        //results.emplace_back("start_array");
        return 0;
    }
    int end_array(size_t ) override {
        //results.emplace_back("end_array");
        return 0;
    }
    int reply_encoded_key(art::value_type key) override {
        results.emplace_back(encoded_key_as_variant(key));
        return 0;
    }

    int reply_values(const std::initializer_list<Variable>& keys) override {
        for (auto &k : keys) {
            results.emplace_back(k);
        }
        return 0;
    };
    bool call_route(int &r, const std::vector<std::string_view>& params) {
        if (valid_routes && params.size() > 1) {
            size_t shard = get_shard(params[1]);
            if (shard < routes.size()) {
                if (routes[shard] != nullptr) { // dont do any lookups if there's no route for perf
                    auto &fbn = functions_by_name();
                    std::string n = {params[0].data(),params[0].size()};
                    auto fi = fbn.find(n);
                    if (fi != fbn.end() && fi->second.data) { // only route data calls
                        ++statistics::repl::attempted_routes;
                        if (routes[shard]->call(r, results, params) == 0) {
                            ++statistics::repl::routes_succeeded;
                            return true;
                        } else {
                            // TODO: should we => if the data route network fails we will continue with other functions
                            routes[shard] = nullptr;
                            return false;
                        }
                    }
                }
            }
        }
        return false;
    }

    template<typename TC>
    int call(const std::vector<std::string_view>& params, TC&& f) {

        ++statistics::local_calls;
        arg_t args;
        errors.clear();
        results.clear();
        int r = 0;
        if (call_route(r, params)) {
            return r;
        }
        if (host != nullptr) {
            if (host->call(r, results, params) == 0) {
                return r;
            }else {
                return -1;
            }
        }
        for (const auto& s : params) {
            args.push_back({s.data(),s.size()});
        }
        try {
            r = f(*this, args);
        }catch (const std::exception& e) {
            ++statistics::exceptions_raised;
            errors.emplace_back(e.what());
            r = -1;
        }
        if (r != 0) {
            if (errors.empty())
                errors.emplace_back("call failed");
            return r;
        }
        if (!errors.empty()) {
            r = -1;
        }
        return r;
    }
    template<typename TC>
    int call(const heap::vector<std::string>& params, TC&& f) {
        std::vector<std::string_view> sv;

        for (auto &p: params) {
            sv.emplace_back(p.data(),p.size());
        }
        return call(sv, std::forward<TC>(f));
    }
};

#endif //SWIG_CALLER_H
