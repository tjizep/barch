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
    virtual int reply_values(const std::initializer_list<Variable>& keys) = 0;
    [[nodiscard]] virtual const std::string& get_user() const = 0;
    [[nodiscard]] virtual const heap::vector<bool>& get_acl() const = 0;
    virtual void set_acl(const std::string& user, const heap::vector<bool>& acl) = 0;
    virtual barch::key_space_ptr kspace() = 0;
};

#endif //CALLER_H
