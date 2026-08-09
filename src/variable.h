//
// Created by teejip on 7/14/25.
//

#ifndef VARIABLE_H
#define VARIABLE_H
#include <charconv>
#include <string>
#include <variant>
#include <fast_float/fast_float.h>
#include <fmt/format.h>
#include "value_type.h"
#include "ioutil.h"

// the first eight are the RESP2 shapes and their indices are relied on elsewhere, so
// anything new is appended. the last three exist because RESP3 gives a map, a set and
// a verbatim string their own wire types; RESP2 writes them as an array or a bulk
// string, which is what they degrade to when the connection never asked for RESP3
enum {
    var_bool = 0,
    var_int64 = 1,
    var_uint64 = 2,
    var_double = 3,
    var_string = 4,
    var_null = 5,
    var_error = 6,
    var_array = 7,
    var_map = 8,
    var_set = 9,
    var_verbatim = 10,
    var_max = 11,
};
struct error {
    error(const std::string n) : name(n){}
    error(const error&)=default;
    error(error&&)=default;
    error& operator=(const error&)=default;
    error& operator=( error&&)=default;
    const char * what() const {
        return name.c_str();
    }
    size_t size() const {
        return name.size();
    }
private:
    std::string name;
};
struct wrapped_variable_t;
// a map and a set carry the same elements an array does - a map as alternating key
// and value - so they wrap the vector instead of repeating it. they exist only so the
// writer can tell the three apart and pick the right RESP3 type
struct map_t {
    heap::vector<wrapped_variable_t> items{};
};
struct set_t {
    heap::vector<wrapped_variable_t> items{};
};
// RESP3 tags a verbatim string with a three letter format, "txt" or "mkd"
struct verbatim_t {
    std::string format{"txt"};
    std::string text{};
};
// 0 - var_bool, 1 - var_int64, 2 - var_uint64, 3 - var_double, 4 - var_string, 5 - var_null, 6 - var_error
typedef std::variant<bool, int64_t, uint64_t, double, std::string, nullptr_t, error, heap::vector<wrapped_variable_t>,
                     map_t, set_t, verbatim_t> variable_t;

struct wrapped_variable_t{
    variable_t var;
    wrapped_variable_t() = default;
    wrapped_variable_t(const variable_t& v) : var(v){}
    wrapped_variable_t& operator=(const variable_t& v) {
        var = v;
        return *this;
    }
    operator variable_t&() {
        return var;
    }
    operator const variable_t&() const {
        return var;
    }
};

namespace conversion {
    extern bool to(art::value_type v, double &d);
    extern bool to(art::value_type v, float &f);
    extern bool to(art::value_type v, uint64_t &i);
    extern bool to(art::value_type v, uint32_t &i);
    extern bool to(art::value_type v, uint16_t &i);

    extern bool to(art::value_type v, int64_t &i);
    extern bool to(art::value_type v, int32_t &i);
    extern bool to(art::value_type v, int16_t &i);
    template<typename T>
    static T to_e(art::value_type v) {
        T t;
        if (!to(v, t)) {
            throw_exception<std::runtime_error>("conversion failed");
        }
        return t;
    }
}
extern variable_t as_variable(art::value_type v);

class Variable : public variable_t {
public:
    Variable() = default;
    Variable(const Variable&) = default;
    template<typename TM>
    Variable(const TM & m) {
        variable_t::operator=(m);
    }
    Variable(const art::value_type & v) : variable_t(::as_variable(v)){}

    /*
     * The unsigned types narrower than 64 bits, named so that they resolve.
     *
     * They convert without narrowing to both int64_t and uint64_t, so the variant's
     * converting constructor cannot choose between them and does not compile at all:
     * `Variable v = some_uint32_t` was an error, in a type meant to hold any value.
     * Naming them picks uint64_t, which is what they are.
     *
     * Only these three. Every other arithmetic type already resolves, and leaving those
     * to the template means nothing that compiles today changes which alternative it
     * lands on. bool in particular is deliberately not named: a Variable(bool) would
     * swallow any pointer through the pointer to bool conversion, and that is a compile
     * error today and should stay one.
     */
    Variable(unsigned char v) : variable_t((uint64_t) v) {}
    Variable(unsigned short v) : variable_t((uint64_t) v) {}
    Variable(unsigned int v) : variable_t((uint64_t) v) {}

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

    /**
     * the elements of an array, a map or a set - the three share one representation and
     * differ only in the wire type the writer gives them
     */
    [[nodiscard]] const heap::vector<wrapped_variable_t>& elements() const {
        switch (index()) {
            case var_map: return std::get<map_t>(*this).items;
            case var_set: return std::get<set_t>(*this).items;
            default: return std::get<heap::vector<wrapped_variable_t>>(*this);
        }
    }
    [[nodiscard]] bool is_aggregate() const {
        auto i = index();
        return i == var_array || i == var_map || i == var_set;
    }

    [[nodiscard]] std::string to_string() const {
        switch (index()) {
            case var_bool:
                return std::get<bool>(*this) ? "true" : "false";
            case var_int64:
                return std::to_string(std::get<int64_t>(*this));
            case var_uint64:
                return std::to_string(std::get<uint64_t>(*this));
            case var_double: {
                int64_t t =0 ;
                auto d = std::get<double>(*this);
                auto s = fmt::format("{}",d);
                if (conversion::to(s,t)) {
                    s += ".0"; // so that the result will be converted back as a double
                }
                return s;
            }
            case var_string: {
                auto &s = std::get<std::string>(*this);
                if (is_bulk(s)) {
                    return {s.data()+1,s.size()-1};
                }
                return s;
            }
            case var_array:
            case var_map:
            case var_set: {
                const auto &a = elements();
                std::string s;
                bool first = true;
                for (const auto& el: a) {
                    const Variable & v = el;
                    if (first) {
                        first = false;
                    }else {
                        s += ",";
                    }
                    s += v.to_string();
                }
                return s;
            }
            case var_verbatim:
                return std::get<verbatim_t>(*this).text;

            case var_null:
                return {};
            case var_error:
                return std::get<error>(*this).what();
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
            case var_string:
                return conversion::to_e<double>(bulk_str(std::get<std::string>(*this)));
            case var_array:
            case var_map:
            case var_set:
                //return 0.0f;// TODO: maybe accumulate IDK
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
            // a number is true when it is not zero. these three read `== 0`, which is
            // the opposite, and nothing noticed while the only commands reaching here
            // answered with a bool. EXISTS started answering with a count and the
            // binding that calls .b() on it began reporting a key that exists as absent
            case var_int64:
                return std::get<int64_t>(*this) != 0 ;
            case var_uint64:
                return std::get<uint64_t>(*this) != 0 ;
            case var_double:
                return std::get<double>(*this) != 0.0;
            case var_string:
                return conversion::to_e<int>(bulk_str(std::get<std::string>(*this))) > 0;
            case var_array:
            case var_map:
            case var_set:
            case var_null:
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
            case var_string: // not this can throw an error
                return conversion::to_e<int64_t>(bulk_str(std::get<std::string>(*this)));
            case var_array:
            case var_map:
            case var_set:
            case var_null:
            case var_error:
                return 0;
            default:
                abort_with("invalid type");
        }
    }
    [[nodiscard]] uint64_t to_uint64() const {
        switch (index()) {
            case var_bool:
                return std::get<bool>(*this) ? 1 : 0;
            case var_int64:
                return std::get<int64_t>(*this);
            case var_uint64:
                return std::get<uint64_t>(*this);
            case var_double:
                return std::get<double>(*this);
            case var_string:
                return conversion::to_e<uint64_t>(bulk_vt(std::get<std::string>(*this)));
            case var_array:
            case var_map:
            case var_set:
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
            case var_array:
            case var_map:
            case var_set:
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
            case var_array: return "array";
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
        case var_array: {
            const auto &a = std::get< heap::vector<wrapped_variable_t>>(v);
            for (const auto& el: a) {
                auto pv = reinterpret_cast<const variable_t *>(&el);
                writep(os, *pv);
            }
        }
            break;
        case var_null:
            break;
        case var_error: {
            auto e = *std::get_if<error>(&v);
            std::string s = e.what();
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
        case var_array:
            throw_exception<std::runtime_error>("cannot deserialize array types");
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
