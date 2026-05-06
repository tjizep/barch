//
// Created by teejip on 5/18/25.
//

#ifndef CALLER_H
#define CALLER_H
#include "value_type.h"
#include "variable.h"
#include <initializer_list>
#include <utility>
#include "key_space.h"
enum contexts {
    ctx_resp = 1,
    ctx_valkey,
    ctx_rpc
};

struct block_data {
    block_data(std::string key, size_t shard) : key(std::move(key)), shard_index(shard){}
    std::string key{};
    size_t shard_index{};
    barch::key_space_ptr space{};
    auto shard() {
        if (!space) {
            abort_with("key space not set");
        }
        return space->get(shard_index);
    }
};
struct iteration {
    size_t shard{};
    size_t page{};
    size_t pos{};
    size_t bytes{};
    size_t id{};
    bool is_source = false;
    heap::vector<uint8_t> buffer{};

};
struct caller {
    typedef heap::vector<block_data> keys_t;
    typedef std::shared_ptr<iteration> iteration_ptr;
    typedef heap::map<size_t,iteration_ptr> iterations_t;
private:
    iterations_t iterations{};
    size_t iteration_id = 65535;
public:
    iteration_ptr create_iteration() {
        auto iter = std::make_shared<iteration>();
        iter->id = ++iteration_id;
        iterations[iter->id] = iter;
        return iter;
    }
    iteration_ptr get_iteration(size_t id) {
        auto iter = iterations.find(id);
        if (iter == iterations.end()) {
            return nullptr;
        }
        return iter->second;
    }
    void erase_iteration(size_t id) {
        iterations.erase(id);
    }
    virtual ~caller() = default;
    int ctx{ctx_valkey};

    void set_context(int in_ctx) {
        this->ctx = in_ctx;
    }
    [[nodiscard]] int get_context() const {
        return this->ctx;
    }
    [[nodiscard]] virtual size_t results_count() const = 0;
    [[nodiscard]] virtual size_t errors_count() const = 0;
    [[nodiscard]] virtual bool is_remote() const = 0;
    [[nodiscard]] virtual int wrong_arity() = 0;
    [[nodiscard]] virtual int syntax_error() = 0;
    [[nodiscard]] virtual int error() const = 0;
    [[nodiscard]] virtual int push_error(const char * e) = 0;
    virtual int key_check_error(art::value_type k) = 0;
    virtual int push_null() = 0;
    [[nodiscard]] virtual int ok() const = 0;
    virtual int push_bool(bool value) = 0;
    virtual int push_ll(int64_t l) = 0;
    virtual int push_int(long long l) = 0;
    virtual int push_int(unsigned long long l) = 0;
    virtual int push_int(int64_t l) = 0;
    virtual int push_int(uint64_t l) = 0;
    virtual int push_int(int32_t l) = 0;
    virtual int push_int(uint32_t l) = 0;
    virtual int set_int(size_t at, long long l) = 0;
    virtual int set_int(size_t at, unsigned long long l) = 0;
    virtual int set_int(size_t at, int64_t l) = 0;
    virtual int set_int(size_t at, uint64_t l) = 0;
    virtual int set_int(size_t at, int32_t l) = 0;
    virtual int set_int(size_t at, uint32_t l) = 0;
    virtual int to_array(size_t at) = 0;
    virtual int push_double(double l) = 0;
    virtual int push_vt(art::value_type v) = 0;
    virtual int push_simple(art::value_type v) = 0;
    virtual int push_simple(const char * v) = 0;
    virtual int push_simple(const std::string& v) = 0;
    virtual int push(const Variable & v) = 0;
    virtual size_t pop_back(size_t) {
        return 0;
    }
    virtual int start_array() = 0;
    virtual int end_array(size_t length) = 0;
    virtual int push_encoded_key(art::value_type key) = 0;
    virtual int push_string(const std::string& value) = 0;
    virtual int set_string(size_t at, const std::string& value) = 0;
    virtual int push_values(const std::initializer_list<Variable>& keys) = 0;
    [[nodiscard]] virtual std::string get_info() const = 0;
    virtual void start_call_buffer() = 0;
    virtual void finish_call_buffer() = 0;
    [[nodiscard]] virtual const std::string& get_user() const = 0;
    [[nodiscard]] virtual const heap::vector<bool>& get_acl() const = 0;
    virtual void set_acl(const std::string& user, const heap::vector<bool>& acl) = 0;
    virtual barch::key_space_ptr& kspace() = 0;
    virtual barch::key_space_ref ks_ref() = 0;
    virtual void set_kspace(const barch::key_space_ptr& ks) = 0;
    virtual void use(const std::string& name) = 0;
    virtual void transfer_rpc_blocks(const barch::abstract_session_ptr& ) {};
    virtual void erase_blocks(const barch::abstract_session_ptr& ) {};
    virtual void add_block(const keys_t& blocks, uint64_t to_ms, std::function<void(caller&, const keys_t&)>) = 0;
    virtual bool has_blocks() = 0;
    virtual void sort_pushed_results() = 0;
    [[nodiscard]] virtual size_t stack() const {
        return 0;
    }
    Variable empty {nullptr};

    [[nodiscard]] virtual const Variable& back() const {
        return empty;
    }
    int push_variable(const Variable& var) {
        switch (var.index()) {
            case var_bool:
                return push_bool(var.to_bool());
            case var_int64:
                return push_int(var.to_int64());
            case var_uint64:
                return push_int(var.to_uint64());
            case var_double:
                return push_double(var.to_double());
            case var_string:
                return push_string(var.to_string());
            case var_array: {
                const auto &a = std::get< heap::vector<wrapped_variable_t>>(var);
                start_array();
                for (const auto& el: a) {
                    const Variable & v = el;
                    push_variable(v);
                }
                return end_array(1);
            }
            case var_null:
                return push_null();
            case var_error:
                return push_error(var.to_string().c_str());
            default:
                abort_with("invalid type");
        }
    }
};

typedef std::function<int(caller& call,const arg_t& argv)> call_type;
struct command {
    command(const call_type& call, const std::vector<std::string_view>& args_,barch::key_space_ptr space) : call(call),space(space) {
        for (auto& a: args_) {
            args.emplace_back(a);
        }
    }
    command(const call_type& call, const std::vector<std::string>& args_,barch::key_space_ptr space) : call(call),space(space) {
        for (auto& a: args_) {
            args.emplace_back(a);
        }
    }
    command(const call_type& call, const heap::vector<std::string>& args_,barch::key_space_ptr space) : call(call),space(space) {
        for (auto& a: args_) {
            args.emplace_back(a);
        }
    }
    command(const call_type& call, arg_t args_, barch::key_space_ptr space) : call(call), space(space) {
        for (auto a : args_) {
            args.push_back(a.to_string());
        }
    }
    call_type call{};
    heap::vector<std::string> args{};
    barch::key_space_ptr space{};
};
typedef heap::vector<command> commands_t;
#endif //CALLER_H
