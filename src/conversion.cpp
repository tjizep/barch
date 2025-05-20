//
// Created by teejip on 4/9/25.
//
#include "conversion.h"
// take a string and convert to a number as bytes or leave it alone
// and return the bytes directly. the bytes will be copied
conversion::comparable_key conversion::convert(art::value_type vt, bool noint) {
    return convert(vt.chars(),vt.size,noint);
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
