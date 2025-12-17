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
    std::function<std::string()> info_fun;
    bool call_buffering = false;
    keys_t blocks{};
    Variable empty{nullptr};
    std::function<void(caller&, const keys_t&)> block_fun;
    uint64_t block_to_ms = 0;
    heap::vector<Variable> buffered_results{};
    heap::vector<std::string> buffered_errors{};

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
    [[nodiscard]] int push_error(const char * e) override {
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
    int push_null() override {
        results.emplace_back(nullptr);
        return 0;
    }
    [[nodiscard]] int ok() const override {
        return 0;
    }
    int push_bool(bool value) override {
        results.emplace_back(value);
        return 0;
    }
    int push_ll(int64_t l) override {
        results.emplace_back(l);
        return 0;
    }
    int push_int(long long l) override {
        results.emplace_back(l);
        return 0;
    }
    int push_int(unsigned long long l) override {
        results.emplace_back(l);
        return 0;
    }
    int push_int(int64_t l) override {
        results.emplace_back(l);
        return 0;
    }
    int push_int(uint64_t l) override {
        results.emplace_back(l);
        return 0;
    }
    int push_int(int32_t l) override {
        results.emplace_back((int64_t)l);
        return 0;
    }
    int push_int(uint32_t l) override {
        results.emplace_back((uint64_t)l);
        return 0;
    }

    int push_double(double l) override {
        results.emplace_back(l);
        return 0;
    }
    int push_vt(art::value_type v) override {
        r.clear();
        r.push_back('$');
        r.insert(r.end(), v.begin(), v.end());
        results.emplace_back(r); // values are currently always a string
        return 0;
    }
    int push_simple(const char * v) override {
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
    int push_encoded_key(art::value_type key) override {
        results.emplace_back(encoded_key_as_variant(key));
        return 0;
    }
    int push_string(const std::string& value) override {
        results.emplace_back(value);
        return 0;
    }
    int push_values(const std::initializer_list<Variable>& keys) override {
        for (auto &k : keys) {
            results.emplace_back(k);
        }
        return 0;
    };
    std::string convert(const std::string& v) {
        return v;
    }
    std::string convert(const std::string_view& v) {
        return {v.data(),v.size()};
    }
    std::string convert(const art::value_type& v) {
        return {v.chars(),v.size};
    }
    size_t pop_back(size_t n) override {
        size_t popped = 0;
        while (!results.empty() && popped < n) {
            results.pop_back();
            ++popped;
        }
        return popped;
    };
    [[nodiscard]] size_t stack() const override {
        return results.size();
    }
    [[nodiscard]] const Variable& back() const override {
        if (!results.empty()) {
            return results.back();
        }
        return empty;
    }
    template<typename ArgT>
    barch::repl::call_result call_route(const ArgT& params) {
        if (valid_routes && params.size() > 1) {
            size_t shard = ks->get_shard_index(params[1]);
            if (shard < routes.size()) {
                if (routes[shard] != nullptr) { // dont do any lookups if there's no route for perf
                    auto fbn = barch::barch_functions;
                    std::string n = convert(params[0]);
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
    call_type fexec = EXEC;
    commands_t commands;
    template<typename TC, typename VT>
    int call(const VT& params, TC&& f) {
        if (params.empty()) {
            barch::std_err("invalid parameters");
            return 0;
        }
        if (is_buffering() && (params[0] != "EXEC" || params[0] == "MULTI")) {
            commands.emplace_back(f, params, ks);
            return 0;
        }
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
            args.push_back(s);
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
    void call_blocks() {
        results.clear();
        errors.clear();
        args.clear();
        block_fun(*this, blocks);
    }
    std::string get_info() const override {
        if (!info_fun) return "";
        return info_fun();
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
        if (ks != kspace)
            this->ks = kspace;
    }
    void use(const std::string& name) override {
        this->ks = barch::get_keyspace(name);
    }
    void add_block(const keys_t &key_names, uint64_t to_ms,  std::function<void(caller&, const keys_t&)> fn) override {
        this->blocks = key_names;
        this->block_to_ms = to_ms;
        this->block_fun = fn;
        for (auto& d:blocks) {
            d.space = ks;
        }
    }
    void transfer_rpc_blocks(const barch::abstract_session_ptr& session) override {
        for (auto &d: blocks) {
            auto shard = d.shard();
            std::unique_lock lck(shard->get_latch());
            d.space->get(d.shard_index)->add_rpc_block(d.key, session);
        }
        //add_rpc_blocks(caller.get_blocks(),session);
    }
    void erase_blocks(const barch::abstract_session_ptr& session) override {
        for (auto &d: blocks) {
            auto shard = d.shard();
            std::unique_lock lck(shard->get_latch());
            shard->erase_rpc_block(d.key, session);
        }
        clear_blocks();
    }
    bool has_blocks() override {
        return !this->blocks.empty();
    }
    void clear_blocks() {
        this->blocks.clear();
    }
    [[nodiscard]] auto& get_blocks() const {
        return this->blocks;
    }

    void start_call_buffer() override {
        if (!call_buffering) {
            commands.clear();
        }
        call_buffering = true;
    }


    void finish_call_buffer() override {
        call_buffering = false;
        buffered_results.clear();
        buffered_errors.clear();
        auto original = ks;
        for (auto& cmd: commands) {
            this->ks = cmd.space;
            int e = this->call(cmd.args, cmd.call);
            // analyze results
            if (e != 0) {
                buffered_results.emplace_back(push_error(errors[0].c_str()));
            } else {
                if (results.empty()) {
                    buffered_results.emplace_back(nullptr);
                }else {
                    for (auto& r: results) {
                        buffered_results.emplace_back(r);
                    }
                }
            }

        }

        results = std::move(buffered_results);
        ks = original;
        commands.clear();
    }
    void sort_pushed_results() override {
        std::sort(results.begin(), results.end());
    }
    bool is_buffering() const {
        return call_buffering;
    }
};

#endif //SWIG_CALLER_H
