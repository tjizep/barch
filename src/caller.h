//
// Created by teejip on 5/18/25.
//

#ifndef CALLER_H
#define CALLER_H
#include "value_type.h"
#include "variable.h"
#include <initializer_list>
struct caller {
    virtual ~caller() = default;
    [[nodiscard]] virtual int wrong_arity() = 0;
    [[nodiscard]] virtual int syntax_error() = 0;
    [[nodiscard]] virtual int error() const = 0;
    [[nodiscard]] virtual int error(const char * e) = 0;
    virtual int key_check_error(art::value_type k) = 0;
    virtual int null() = 0;
    [[nodiscard]] virtual int ok() = 0;
    virtual int boolean(bool value) = 0;
    virtual int long_long(int64_t l) = 0;
    virtual int double_(double l) = 0;
    virtual int vt(art::value_type v) = 0;
    virtual int simple(const char * v) = 0;

    virtual int start_array() = 0;
    virtual int end_array(size_t length) = 0;
    virtual int reply_encoded_key(art::value_type key) = 0;
    virtual int reply_values(const std::initializer_list<Variable>& keys) = 0;
};
typedef heap::small_vector<art::value_type> arg_t;
#endif //CALLER_H
