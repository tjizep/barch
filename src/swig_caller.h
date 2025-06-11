//
// Created by teejip on 6/6/25.
//

#ifndef SWIG_CALLER_H
#define SWIG_CALLER_H
#include "caller.h"
#include <string>
#include <vector>
#include "keys.h"


struct swig_caller : caller {


    std::vector<conversion::Variable> results{};
    std::vector<std::string> errors{};
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
        results.emplace_back(v.chars()); // values are currently always a string
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
    template<typename TC>
    int call(const std::vector<std::string_view>& params, TC&& f) {
        arg_t args;
        errors.clear();
        results.clear();
        for (const auto& s : params) {
            args.push_back({&*s.begin(),s.length()});
        }
        int r = f(*this, args);
        if (r != 0) {
            art::std_err("call failed");
            return r;
        }
        if (!errors.empty()) {
            r = -1;
            for (auto& e: errors) {
                art::std_err(e);
            }
        }
        return r;
    }
};

#endif //SWIG_CALLER_H
