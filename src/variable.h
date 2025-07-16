//
// Created by teejip on 7/14/25.
//

#ifndef VARIABLE_H
#define VARIABLE_H
#include <cstdint>
#include <string>
#include <variant>
#include "ioutil.h"
enum {
    var_bool = 0,
    var_int64 = 1,
    var_double = 2,
    var_string = 3,
    var_null = 4,
    var_error = 5,
    var_max = 6,
};
struct error {
    std::string name;
};
typedef std::variant<bool, int64_t, double, std::string, nullptr_t, error> Variable;

inline void writep(std::ostream& os, const Variable& v) {
    uint32_t i = v.index();
    writep(os, i);
    switch (i) {
        case var_bool:
            writep(os, *std::get_if<bool>(&v));
            break;
        case var_int64:
            writep(os, *std::get_if<int64_t>(&v));
            break;
        case var_double:
            writep(os, *std::get_if<double>(&v));
            break;
        case var_string: {
            auto s = *std::get_if<std::string>(&v);
            writep(os, (uint32_t)s.size());
            writep(os, s.data(), s.size());
        }
            break;
        case var_null:
            break;
        case var_error: {
            auto e = *std::get_if<error>(&v);
            std::string s = e.name;
            writep(os, (uint32_t)s.size());
            writep(os, s.data(), s.size());
        }
            break;
        default:
            break;
    }
}
inline void readp(std::istream& is, Variable& v) {
    uint32_t i = 0;
    readp(is, i);
    uint32_t l = 0;
    bool b;
    int64_t i64;
    double d;
    std::string s;
    switch (i) {
        case var_bool:

            readp(is, b);
            v = b;
            break;
        case var_int64:
            readp(is, i64);
            v = i64;
            break;
        case var_double:
            readp(is, d);
            v = d;
            break;
        case var_string:
            readp(is, l);
            s.resize(l);
            readp(is, (uint8_t*)s.data(), l);
            v = s;
            break;
        case var_null:
            v = nullptr;
            break;
        case var_error:
            readp(is, l);
            s.resize(l);
            readp(is, (uint8_t*)s.data(), l);
            v = error{s};
        default:
            break;
    }
}
#endif //VARIABLE_H
