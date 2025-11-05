//
// Created by teejip on 5/18/25.
//

#ifndef CALLER_H
#define CALLER_H
#include "value_type.h"
#include "variable.h"
#include <initializer_list>
#include "key_space.h"
enum contexts {
    ctx_resp = 1,
    ctx_valkey,
    ctx_rpc
};
struct caller {
    virtual ~caller() = default;
    int ctx{ctx_valkey};

    void set_context(int in_ctx) {
        this->ctx = in_ctx;
    }
    int get_context() {
        return this->ctx;
    }
    [[nodiscard]] virtual int wrong_arity() = 0;
    [[nodiscard]] virtual int syntax_error() = 0;
    [[nodiscard]] virtual int error() const = 0;
    [[nodiscard]] virtual int error(const char * e) = 0;
    virtual int key_check_error(art::value_type k) = 0;
    virtual int null() = 0;
    [[nodiscard]] virtual int ok() = 0;
    virtual int boolean(bool value) = 0;
    virtual int long_long(int64_t l) = 0;
    virtual int any_int(long long l) = 0;
    virtual int any_int(unsigned long long l) = 0;
    virtual int any_int(int64_t l) = 0;
    virtual int any_int(uint64_t l) = 0;
    virtual int any_int(int32_t l) = 0;
    virtual int any_int(uint32_t l) = 0;
    virtual int double_(double l) = 0;
    virtual int vt(art::value_type v) = 0;
    virtual int simple(const char * v) = 0;

    virtual int start_array() = 0;
    virtual int end_array(size_t length) = 0;
    virtual int reply_encoded_key(art::value_type key) = 0;
    virtual int reply(const std::string& value) = 0;
    virtual int reply_values(const std::initializer_list<Variable>& keys) = 0;
    virtual std::string get_info() const = 0;
    virtual void start_call_buffer() = 0;
    virtual void finish_call_buffer() = 0;
    [[nodiscard]] virtual const std::string& get_user() const = 0;
    [[nodiscard]] virtual const heap::vector<bool>& get_acl() const = 0;
    virtual void set_acl(const std::string& user, const heap::vector<bool>& acl) = 0;
    virtual barch::key_space_ptr& kspace() = 0;
    virtual void set_kspace(const barch::key_space_ptr& ks) = 0;
    virtual void use(const std::string& name) = 0;
};
typedef std::function<int(caller& call,const arg_t& argv)> call_type;
struct command {
    command(const call_type& call, const std::vector<std::string_view>& args_,barch::key_space_ptr space) : call(call),space(space) {
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
