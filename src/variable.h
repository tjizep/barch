//
// Created by teejip on 7/14/25.
//

#ifndef VARIABLE_H
#define VARIABLE_H
#include <cstdint>
#include <string>
#include <variant>

typedef std::variant<bool, int64_t, double, std::string, nullptr_t> Variable;

inline void writep(std::ostream& os, const Variable& v) {
    uint32_t i = v.index();
    writep(os, i);
    switch (i) {
        case 0:
            writep(os, *std::get_if<bool>(&v));
            break;
        case 1:
            writep(os, *std::get_if<int64_t>(&v));
            break;
        case 2:
            writep(os, *std::get_if<double>(&v));
            break;
        case 3: {
            auto s = *std::get_if<std::string>(&v);
            writep(os, s.data(), s.size());
        }
            break;
        case 4:
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
        case 0:

            readp(is, b);
            v = b;
            break;
        case 1:
            readp(is, i64);
            v = i64;
            break;
        case 2:
            readp(is, d);
            v = d;
            break;
        case 3:
            readp(is, l);
            s.resize(l);
            readp(is, (uint8_t*)s.data(), l);
            v = s;
            break;
        case 4:
            v = nullptr;
            break;
        default:
            break;
    }
}
#endif //VARIABLE_H
