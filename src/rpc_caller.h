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
#include "rpc/server.h"
#include "barch_apis.h"
#include "sastam.h"
#include "auth_api.h"
#include "rpc/barch_functions.h"
struct rpc_caller : caller {
    barch::key_space_ptr ks = get_default_ks();
    std::shared_ptr<barch::repl::rpc> host {};
    heap::vector<std::shared_ptr<barch::repl::rpc>> routes {};
    size_t valid_routes{};
    std::string r{};
    heap::vector<Variable> results{};
    heap::vector<std::string> errors{};
    heap::vector<bool> acl{get_all_acl()};
    arg_t args{};
    std::string user = "default";
    void create(const std::string& h, uint_least16_t port) {
        this->host = barch::repl::create(h,port);
    }
    rpc_caller() {
        routes.resize(barch::get_shard_count().size());
        for (size_t shard : barch::get_shard_count()) {
            auto route = barch::repl::get_route(shard);
            if (route.ip.empty()) {
                routes[shard] = nullptr;
            }else {
                ++valid_routes;
                routes[shard] =barch::repl::create(route.ip,route.port);
            }
        }
        std::vector<std::string_view> auth = {"AUTH","default","empty"};
        if (this->call( auth,::AUTH) != 0) {
            barch::std_err("could not authenticate `default`");
        }
    }
    [[nodiscard]] int wrong_arity()  override {
        errors.emplace_back("Wrong Arity");
        return 0;
    }
    [[nodiscard]] int syntax_error() override {
        errors.emplace_back("Syntax Error");
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
        results.emplace_back(value);
        return 0;
    }
    int long_long(int64_t l) override {
        results.emplace_back(l);
        return 0;
    }
    int any_int(long long l) override {
        results.emplace_back(l);
        return 0;
    }
    int any_int(unsigned long long l) override {
        results.emplace_back(l);
        return 0;
    }
    int any_int(int64_t l) override {
        results.emplace_back(l);
        return 0;
    }
    int any_int(uint64_t l) override {
        results.emplace_back(l);
        return 0;
    }
    int any_int(int32_t l) override {
        results.emplace_back((int64_t)l);
        return 0;
    }
    int any_int(uint32_t l) override {
        results.emplace_back((uint64_t)l);
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
    int reply(const std::string& value) override {
        results.emplace_back(value);
        return 0;
    }
    int reply_values(const std::initializer_list<Variable>& keys) override {
        for (auto &k : keys) {
            results.emplace_back(k);
        }
        return 0;
    };
    barch::repl::call_result call_route(const std::vector<std::string_view>& params) {
        if (valid_routes && params.size() > 1) {
            size_t shard = ks->get_shard_index(params[1]);
            if (shard < routes.size()) {
                if (routes[shard] != nullptr) { // dont do any lookups if there's no route for perf
                    auto fbn = barch::barch_functions;
                    std::string n = {params[0].data(),params[0].size()};
                    auto fi = fbn->find(n);
                    if (fi != fbn->end() && fi->second.is_data()) { // only route data calls
                        ++statistics::repl::attempted_routes;
                        auto cr = routes[shard]->call(results, params);
                        if (cr.net_error == 0) {
                            ++statistics::repl::routes_succeeded;
                            return cr;
                        } else {
                            // TODO: should we => if the data route network fails we will continue with other functions
                            routes[shard] = nullptr;
                            return cr;
                        }
                    }
                }
            }
        }
        return {-1,-1};
    }

    template<typename TC, typename VT>
    int call(const VT& params, TC&& f) {

        ++statistics::local_calls;
        args.clear();
        errors.clear();
        results.clear();
        auto cr = call_route(params);
        if (cr.net_error == 0) {
            return cr.call_error;
        }
        if (host != nullptr) {
            if (host->call(results, params).ok()) {
                return 0;
            }else {
                return -1;
            }
        }
        for (const auto& s : params) {
            args.push_back({s.data(),s.size()});
        }
        try {
            cr.call_error = f(*this, args);
        }catch (const std::exception& e) {
            ++statistics::exceptions_raised;
            errors.emplace_back(e.what());
            cr.call_error = -1;
        }
        if (cr.call_error != 0) {
            if (errors.empty())
                errors.emplace_back("call failed");
            return cr.call_error;
        }
        if (!errors.empty()) {
            cr.call_error = -1;
        }
        return cr.call_error;
    }

    [[nodiscard]] const std::string& get_user() const override  {
        return user;
    }
    [[nodiscard]] const heap::vector<bool>& get_acl() const override {
        return acl;
    }
    void set_acl(const std::string& user,const heap::vector<bool>& acl) override {
        this->user = user;
        this->acl = acl;
    };
    barch::key_space_ptr& kspace() override {
        if (!ks ) {throw_exception<std::runtime_error>("key space not set");
        }
        return ks;
    }
    void set_kspace(const barch::key_space_ptr& kspace) override{
        this->ks = kspace;
    }
    void use(const std::string& name) override {
        this->ks = barch::get_keyspace(name);
    }
};

#endif //SWIG_CALLER_H
