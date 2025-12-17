//
// Created by teejip on 7/14/25.
//

#ifndef VARIABLE_H
#define VARIABLE_H
#include <cstdint>
#include <string>
#include <variant>
#include "value_type.h"
#include "ioutil.h"

enum {
    var_bool = 0,
    var_int64 = 1,
    var_uint64 = 2,
    var_double = 3,
    var_string = 4,
    var_null = 5,
    var_error = 6,
    var_max = 7,
};
struct error {
    std::string name;
};
typedef std::variant<bool, int64_t, uint64_t, double, std::string, nullptr_t, error> variable_t;
extern bool to_ui64(art::value_type v, uint64_t &i);
class Variable : public variable_t {
public:
    Variable() = default;
    Variable(const Variable&) = default;
    template<typename TM>
    Variable(const TM & m) {
        variable_t::operator=(m);
    }

    Variable& operator=(const Variable&) = default;
    [[nodiscard]] bool isBoolean() const {
        return index() == var_bool;
    }
    [[nodiscard]] bool isInteger() const {
        return index() == var_int64;
    }
    [[nodiscard]] bool isUnsignedInteger() const {
         return index() == var_uint64;
     }
    [[nodiscard]] bool isDouble() const {
        return index() == var_double;
    }
    [[nodiscard]] bool isString() const {
        return index() == var_string;
    }
    [[nodiscard]] bool isError() const {
        return index() == var_error;
    }
    [[nodiscard]] bool isNull() const {
        return index() == var_null;
    }
    [[nodiscard]] bool is_bulk(const std::string& item) const {
         if (item.empty()) { return false;}
         return item[0] == '$';
    }

    [[nodiscard]] const char* bulk_str(const std::string& item) const {
         if (is_bulk(item)) return item.data()+1;
         return item.c_str();
     }
    [[nodiscard]] art::value_type bulk_vt(const std::string& item) const {
         if (is_bulk(item)) return {item.data()+1,item.size()-1};
         return {item.data(),item.size()};
     }

    [[nodiscard]] std::string to_string() const {
        switch (index()) {
            case var_bool:
                return std::get<bool>(*this) ? "true" : "false";
            case var_int64:
                return std::to_string(std::get<int64_t>(*this));
            case var_uint64:
                return std::to_string(std::get<uint64_t>(*this));
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

    [[nodiscard]] double to_double() const {
        switch (index()) {
            case var_bool:
                return std::get<bool>(*this) ? 1 : 0;
            case var_int64:
                return std::get<int64_t>(*this);
            case var_uint64:
                return std::get<uint64_t>(*this);
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
            case var_uint64:
                return std::get<uint64_t>(*this) == 0 ;
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

    [[nodiscard]] int64_t to_int64() const {
        switch (index()) {
            case var_bool:
                return std::get<bool>(*this) ? 1 : 0;
            case var_int64:
                return std::get<int64_t>(*this);
            case var_uint64:
                return std::get<uint64_t>(*this);
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
    [[nodiscard]] uint64_t to_uint64() const {
         uint64_t v = 0;
        switch (index()) {
            case var_bool:
                return std::get<bool>(*this) ? 1 : 0;
            case var_int64:
                return std::get<int64_t>(*this);
            case var_uint64:
                return std::get<uint64_t>(*this);
            case var_double:
                return std::get<double>(*this);
            case var_string: {
                auto &s = std::get<std::string>(*this);
                to_ui64(bulk_vt(s),v);
                return v;
            }
            case var_null:
            case var_error:
                return 0;
            default:
                abort_with("invalid type");
        }
    }
    bool operator<(const Variable& other) {
        if (index() != other.index()) return index() < other.index();
        switch (index()) {
            case var_bool:
                return std::get<bool>(*this) < std::get<bool>(other);
            case var_int64:
                return std::get<int64_t>(*this) < std::get<int64_t>(other);
            case var_uint64:
                return std::get<uint64_t>(*this) < std::get<uint64_t>(other);
            case var_double:
                return std::get<double>(*this) < std::get<double>(other);
            case var_string:
                return std::get<std::string>(*this) < std::get<std::string>(other);
            case var_null:
            case var_error:
                return false;
            default:
                abort_with("invalid type");
        }
        return false;
    }

    [[nodiscard]] std::string s() const {
        return to_string();
    }

    [[nodiscard]] double d() const {
        return to_double();
    }

    [[nodiscard]] long long i() const {
        return to_int64();
    }
    [[nodiscard]] unsigned long long ui() const {
        return to_uint64();
    }

    [[nodiscard]] long long b() const {
        return to_bool();
    }

    [[nodiscard]] std::string t() const {
        switch (index()) {
            case var_bool: return "boolean";
            case var_int64: return "integer";
            case var_uint64: return "unsigned";
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
    bool operator<(const unsigned long long& r)  const {
        return ui() < r;
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
    explicit operator std::string() const {
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
        case var_uint64:
            writep(os, *std::get_if<uint64_t>(&v));
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
    uint64_t ui64;
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
        case var_uint64:
            readp(is, ui64);
            v = ui64;
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
