//
// Created by teejip on 4/9/25.
//
#include "conversion.h"
// take a string and convert to a number as bytes or leave it alone
// and return the bytes directly. the bytes will be copied
conversion::comparable_result conversion::convert(const char* v, size_t vlen, bool noint)
{
	int64_t i;
	double d;

	if (!noint && is_integer(v, vlen))
	{
		auto ianswer = fast_float::from_chars(v, v + vlen, i); // check if it's an integer first

		if (ianswer.ec == std::errc() && ianswer.ptr == v + vlen)
		{
			return comparable_result(i);
		}
	}

	auto fanswer = fast_float::from_chars(v, v + vlen, d); // TODO: not sure if its strict enough, well see

	if (fanswer.ec == std::errc() && fanswer.ptr == v + vlen)
	{
		return comparable_result(d);
	}

	return {v, vlen + 1};
}
conversion::comparable_result conversion::convert(const std::string& str, bool noint)
{
	return convert(str.c_str(), str.size(), noint);
}
art::value_type conversion::to_value(const std::string& s)
{
	return {s.c_str(),(unsigned)s.length()};
}
const char* conversion::eat_space(const char* str, size_t l)
{
	const char* s = str;
	for (; s != str + l; ++s) // eat continuous initial spaces
	{
		if (*s == ' ')
			continue;
		break;
	}
	return s;
}
bool conversion::is_integer(const char* str, size_t l)
{
	const char* s = eat_space(str, l);
	if (s == str + l)
	{
		return false;
	}
	if (*s == '-')
	{
		++s;
	}
	s = eat_space(s, l - (s - str));

	for (; s != str + l; ++s)
	{
		if (!fast_float::is_integer(*s))
			return false;
	}
	return true;
}