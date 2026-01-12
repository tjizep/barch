//
// Created by teejip on 4/9/25.
//
#include "conversion.h"

#include "composite.h"
// take a string and convert to a number as bytes or leave it alone
// and return the bytes directly. the bytes will be copied
conversion::comparable_key conversion::convert(art::value_type vt, bool noint) {
    return convert(vt.chars(),vt.size,noint);
}
inline bool is_bulk(const std::string& item) {
    if (item.empty()) { return false;}
    return item[0] == '$';
}
inline const char* bulk_str(const std::string& item) {
    if (is_bulk(item)) return item.data()+1;
    return item.c_str();
}
namespace conversion {
    std::string to_string(const Variable &v) {
        return v.to_string();
    }

    double to_double(const Variable& v) {
        return v.to_double();
    }

    bool to_bool(const Variable& v) {
        return v.to_bool();
    }

    int64_t to_i64(const Variable& v) {
        return v.to_int64();
    }

    uint64_t to_ui64(const Variable& v) {
        return v.to_int64();
    }
}
template<typename T>
bool to_t(art::value_type v, T &i) {

    auto ianswer = fast_float::from_chars(v.chars(), v.chars() + v.size, i); // check if it's an integer first

    return (ianswer.ec == std::errc() && ianswer.ptr == v.chars() + v.size) ;
}
bool conversion::to_ll(art::value_type v, long long &i) {
    return to_t(v, i);
}
bool conversion::to(art::value_type v, double &d) {
    return to_t(v, d);
}
bool conversion::to(art::value_type v, float &f) {
    return to_t(v, f);
}
bool conversion::to(art::value_type v, uint64_t &i) {
    return to_t(v, i);
}
bool conversion::to(art::value_type v, uint32_t &i) {
    return to_t(v, i);
}
bool conversion::to(art::value_type v, uint16_t &i) {
    return to_t(v, i);
}
bool conversion::to(art::value_type v, int64_t &i) {
    return to_t(v, i);
}
bool conversion::to(art::value_type v, int32_t &i) {
    return to_t(v, i);
}
bool conversion::to(art::value_type v, int16_t &i) {
    return to_t(v, i);
}
bool conversion::to_ui64(art::value_type v, uint64_t &i) {
    return to_t(v, i);
}
bool conversion::to_i64(art::value_type v, int64_t &i) {
    return to_t(v, i);
}

bool conversion::to_double(art::value_type v, double &i) {
    return to_t(v, i);
}

Variable conversion::as_variable(art::value_type v, bool noint) {
    return as_variable(v.chars(), v.size, noint);
}
conversion::comparable_key conversion::as_composite(art::value_type v, bool noint, char sep) {

    auto plast = (const char * )memchr(v.begin(),sep, v.size);

    if (plast == nullptr) [[likely]]
        return conversion::convert(v.chars(), v.size, noint);
    {
        thread_local composite tuple;
        tuple.create({});
        char spc[] = {sep,'\0'};
        char * state;
        auto last_tok = (char *)v.begin();
        auto token = strtok_r(last_tok, &spc[0], &state);
        while (token != nullptr) {
            size_t len = state - token;
            while (len && token[len - 1] == 0) {
                --len;
            }
            if (!len) continue;
            tuple.push(convert(token, len, noint));
            token = strtok_r(0, &spc[0], &state);
        }

        return tuple.create();
    }
}
conversion::comparable_key conversion::convert(const char *v, size_t vlen, bool noint) {
    int64_t i;
    double d;

    if (!noint) {
        if (to_i64({v,vlen},i)) {
            return comparable_key(i);
        }

    }
    auto fanswer = fast_float::from_chars(v, v + vlen, d); // TODO: not sure if its strict enough, well see

    if (fanswer.ec == std::errc() && fanswer.ptr == v + vlen) {
        return comparable_key(d);
    }
    return {v, vlen + 1};
}
variable_t as_variable(const char *v, size_t vlen, bool noint) {
    int64_t i;
    double d;

    if (!noint) {
        if (conversion::to({v,vlen},i)) {
            return {i};
        }
    }
    auto fanswer = fast_float::from_chars(v, v + vlen, d); // TODO: not sure if its strict enough, well see

    if (fanswer.ec == std::errc() && fanswer.ptr == v + vlen) {
        return d;
    }
    return std::string(v,vlen);
}
variable_t as_variable(art::value_type v) {
    return as_variable(v.data(), v.size, false);
}
Variable conversion::as_variable(const char *v, size_t vlen, bool noint) {
    return ::as_variable(v, vlen, noint);
}

conversion::comparable_key conversion::convert(const std::string &str, bool noint) {
    return convert(str.c_str(), str.size(), noint);
}

art::value_type conversion::to_value(const std::string &s) {
    return {s.c_str(), (unsigned) s.length()};
}

