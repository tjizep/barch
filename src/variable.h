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
typedef std::variant<bool, int64_t, double, std::string, nullptr_t, error> variable_t;
class Variable : public variable_t {
public:
    Variable() = default;
    Variable(const Variable&) = default;
    template<typename TM>
    Variable(const TM & m) {
        variable_t::operator=(m);
    }

    Variable& operator=(const Variable&) = default;
     bool isBoolean() const {
        return index() == var_bool;
    }
    bool isInteger() const {
        return index() == var_int64;
    }
    bool isDouble() const {
        return index() == var_double;
    }
    bool isString() const {
        return index() == var_string;
    }
    bool isError() const {
        return index() == var_error;
    }
    bool isNull() const {
        return index() == var_null;
    }
    bool is_bulk(const std::string& item) const {
         if (item.empty()) { return false;}
         return item[0] == '$';
    }

    const char* bulk_str(const std::string& item) const {
         if (is_bulk(item)) return item.data()+1;
         return item.c_str();
     }

    std::string to_string() const {
        switch (index()) {
            case var_bool:
                return std::get<bool>(*this) ? "true" : "false";
            case var_int64:
                return std::to_string(std::get<int64_t>(*this));
            case var_double:
                return std::to_string(std::get<double>(*this));
            case var_string: {
                auto &s = std::get<std::string>(*this);
                if (is_bulk(s)) {
                    return {s.data()+1,s.size()-1};
                }
                return s;
            }

            case var_null:
                return {};
            case var_error:
                return std::get<error>(*this).name;
            default:
                abort_with("invalid type");
        }
    }

    double to_double() const {
        switch (index()) {
            case var_bool:
                return std::get<bool>(*this) ? 1 : 0;
            case var_int64:
                return std::get<int64_t>(*this);
            case var_double:
                return std::get<double>(*this);
            case var_string: {
                auto &s = std::get<std::string>(*this);

                return std::atof(bulk_str(s));
            }
            case var_null:
                return 0.0f;
            case var_error:
                return std::numeric_limits<double>::quiet_NaN();
            default:
                abort_with("invalid type");
        }
    }

    bool to_bool() const {
        switch (index()) {
            case var_bool:
                return std::get<bool>(*this);
            case var_int64:
                return std::get<int64_t>(*this) == 0 ;
            case var_double:
                return std::get<double>(*this) == 0.0f;
            case var_string: {
                auto &s = std::get<std::string>(*this);

                return std::atoi(bulk_str(s)) > 0;
            }
            case var_null:
                return false;
            case var_error:
                return false;
            default:
                abort_with("invalid type");
        }
    }

    int64_t to_int64() const {
        switch (index()) {
            case var_bool:
                return std::get<bool>(*this) ? 1 : 0;
            case var_int64:
                return std::get<int64_t>(*this);
            case var_double:
                return std::get<double>(*this);
            case var_string: {
                auto &s = std::get<std::string>(*this);
                return std::atoll(bulk_str(s));
            }
            case var_null:
            case var_error:
                return 0;
            default:
                abort_with("invalid type");
        }
    }

    std::string s() const {
        return to_string();
    }

    double d() const {
        return to_double();
    }

    long long i() const {
        return to_int64();
    }

    long long b() const {
        return to_bool();
    }

    std::string t() const {
        switch (index()) {
            case var_bool: return "boolean";
            case var_int64: return "integer";
            case var_double: return "double";
            case var_string: return "string";
            case var_null: return "null";
            case var_error: return "error";
            default:
                return "<unknown>";
        }
    }

    bool operator==(const std::string& r)  const {
        return r == s();
    }

    bool operator==(const double& r)  const {
        return r == d();
    }

    bool operator==(const long long& r)  const {
        return r == i();
    }

    bool operator<(const std::string& r)  const {
        return s() < r;
    }

    bool operator<(const double& r)  const {
        return d() < r;
    }

    bool operator<(const long long& r)  const {
        return i() < r;
    }

    bool operator>(const std::string& r)  const {
        return s() > r;
    }

    bool operator>(const double& r)  const {
        return d() > r;
    }

    bool operator>(const long long& r)  const {
        return i() > r;
    }

    bool operator<=(const std::string& r)  const {
        return s() <= r;
    }

    bool operator<=(const double& r)  const {
        return d() <= r;
    }

    bool operator<=(const long long& r)  const {
        return i() <= r;
    }
    bool operator>=(const std::string& r)  const {
        return s() >= r;
    }

    bool operator>=(const double& r)  const {
        return d() >= r;
    }

    bool operator>=(const long long& r) const {
        return i() >= r;
    }
    operator std::string() const {
        return s();
    }
    operator std::string_view() const {
        return s();
    }
};

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
