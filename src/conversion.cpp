//
// Created by teejip on 4/9/25.
//
#include "conversion.h"
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
        switch (v.index()) {
            case var_bool:
                return std::get<bool>(v) ? "true" : "false";
            case var_int64:
                return std::to_string(std::get<int64_t>(v));
            case var_double:
                return std::to_string(std::get<double>(v));
            case var_string: {
                auto &s = std::get<std::string>(v);
                if (is_bulk(s)) {
                    return {s.data()+1,s.size()-1};
                }
                return s;
            }

            case var_null:
                return {};
            case var_error:
                return std::get<error>(v).name;
            default:
                abort_with("invalid type");
        }
    }

    double to_double(const Variable& v) {
        switch (v.index()) {
            case var_bool:
                return std::get<bool>(v) ? 1 : 0;
            case var_int64:
                return std::get<int64_t>(v);
            case var_double:
                return std::get<double>(v);
            case var_string: {
                auto &s = std::get<std::string>(v);

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

    bool to_bool(const Variable& v) {
        switch (v.index()) {
            case var_bool:
                return std::get<bool>(v);
            case var_int64:
                return std::get<int64_t>(v) == 0 ;
            case var_double:
                return std::get<double>(v) == 0.0f;
            case var_string: {
                auto &s = std::get<std::string>(v);

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

    int64_t to_int64(const Variable& v) {
        switch (v.index()) {
            case var_bool:
                return std::get<bool>(v) ? 1 : 0;
            case var_int64:
                return std::get<int64_t>(v);
            case var_double:
                return std::get<double>(v);
            case var_string: {
                auto &s = std::get<std::string>(v);
                return std::atoll(bulk_str(s));
            }
            case var_null:
            case var_error:
                return 0;
            default:
                abort_with("invalid type");
        }
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
bool conversion::to_i64(art::value_type v, int64_t &i) {
    return to_t(v, i);
}

bool conversion::to_double(art::value_type v, double &i) {
    return to_t(v, i);
}

Variable conversion::as_variable(art::value_type v, bool noint) {
    return as_variable(v.chars(), v.size, noint);
}
conversion::comparable_key conversion::convert(const char *v, size_t vlen, bool noint) {
    int64_t i;
    double d;

    if (!noint) {
#if 1
        if (to_i64({v,vlen},i)) {
            return comparable_key(i);
        }
#else
		char * ep;
		i = strtoll(v,&ep,10 );
		if (ep == v + vlen)
		{
			return comparable_key(i);
		}
#endif
    }
#if 1
    auto fanswer = fast_float::from_chars(v, v + vlen, d); // TODO: not sure if its strict enough, well see

    if (fanswer.ec == std::errc() && fanswer.ptr == v + vlen) {
        return comparable_key(d);
    }
#else
	char * ep;
	d = strtod(v,&ep);
	if (ep == v + vlen)
	{
		return comparable_key(d);
	}

#endif
    return {v, vlen + 1};
}
Variable conversion::as_variable(const char *v, size_t vlen, bool noint) {
    int64_t i;
    double d;

    if (!noint) {
#if 1
        if (to_i64({v,vlen},i)) {
            return {i};
        }
#else
        char * ep;
        i = strtoll(v,&ep,10 );
        if (ep == v + vlen)
        {
            return {i};
        }
#endif
    }
#if 1
    auto fanswer = fast_float::from_chars(v, v + vlen, d); // TODO: not sure if its strict enough, well see

    if (fanswer.ec == std::errc() && fanswer.ptr == v + vlen) {
        return d;
    }
#else
    char * ep;
    d = strtod(v,&ep);
    if (ep == v + vlen)
    {
        return comparable_key(d);
    }

#endif
    return std::string(v,vlen);
}

conversion::comparable_key conversion::convert(const std::string &str, bool noint) {
    return convert(str.c_str(), str.size(), noint);
}

art::value_type conversion::to_value(const std::string &s) {
    return {s.c_str(), (unsigned) s.length()};
}

const char *conversion::eat_space(const char *str, size_t l) {
    const char *s = str;
    for (; s != str + l; ++s) // eat continuous initial spaces
    {
        if (*s == ' ')
            continue;
        break;
    }
    return s;
}

bool conversion::is_integer(const char *str, size_t l) {
    const char *s = eat_space(str, l);
    if (s == str + l) {
        return false;
    }
    if (*s == '-' || *s == '+') {
        ++s;
    }
    s = eat_space(s, l - (s - str));

    for (; s != str + l; ++s) {
        if (!fast_float::is_integer(*s))
            return false;
    }
    return true;
}
