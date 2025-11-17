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

struct caller {
    typedef heap::vector<block_data> keys_t;
    virtual ~caller() = default;
    int ctx{ctx_valkey};

    void set_context(int in_ctx) {
        this->ctx = in_ctx;
    }
    [[nodiscard]] int get_context() const {
        return this->ctx;
    }
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
    virtual int push_double(double l) = 0;
    virtual int push_vt(art::value_type v) = 0;
    virtual int push_simple(const char * v) = 0;
    virtual size_t pop_back(size_t) {
        return 0;
    }
    virtual int start_array() = 0;
    virtual int end_array(size_t length) = 0;
    virtual int push_encoded_key(art::value_type key) = 0;
    virtual int push_string(const std::string& value) = 0;
    virtual int push_values(const std::initializer_list<Variable>& keys) = 0;
    [[nodiscard]] virtual std::string get_info() const = 0;
    virtual void start_call_buffer() = 0;
    virtual void finish_call_buffer() = 0;
    [[nodiscard]] virtual const std::string& get_user() const = 0;
    [[nodiscard]] virtual const heap::vector<bool>& get_acl() const = 0;
    virtual void set_acl(const std::string& user, const heap::vector<bool>& acl) = 0;
    virtual barch::key_space_ptr& kspace() = 0;
    virtual void set_kspace(const barch::key_space_ptr& ks) = 0;
    virtual void use(const std::string& name) = 0;
    virtual void transfer_rpc_blocks(const barch::abstract_session_ptr& ) {};
    virtual void erase_blocks(const barch::abstract_session_ptr& ) {};
    virtual void add_block(const keys_t& blocks, uint64_t to_ms, std::function<void(caller&, const keys_t&)>) = 0;
    virtual bool has_blocks() = 0;
    [[nodiscard]] virtual size_t stack() const {
        return 0;
    }
    Variable empty {nullptr};

    [[nodiscard]] virtual const Variable& back() const {
        return empty;
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
    command(const call_type& call, arg_t args, barch::key_space_ptr space) : call(call), args(args), space(space){}
    call_type call{};
    arg_t args{};
    barch::key_space_ptr space{};
};
typedef heap::vector<command> commands_t;
#endif //CALLER_H
