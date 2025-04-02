#pragma once
#include <cstdint>
#include "nodes.h"
#include <fast_float/fast_float.h>
#include "sastam.h"

namespace conversion
{
    template <typename I>
    struct byte_comparable
    {
        byte_comparable() = default;

        explicit byte_comparable(const uint8_t* data, size_t len)
        {
            memcpy(&bytes[0], data, std::min(sizeof(bytes) - 1, len));
            bytes[9] = 0;
        }

        // probably cpp will optimize this
        [[nodiscard]] size_t get_size() const
        {
            return sizeof(bytes);
        }

        uint8_t bytes[sizeof(I) + 2]{}; // there's a hidden trailing 0 added
    };

    inline int64_t dec_bytes_to_int(const byte_comparable<int64_t>& i)
    {
        int64_t r = 0;

        r += (i.bytes[1] & 0xFF);
        r <<= 8;
        r += (i.bytes[2] & 0xFF);
        r <<= 8;
        r += (i.bytes[3] & 0xFF);
        r <<= 8;
        r += (i.bytes[4] & 0xFF);
        r <<= 8;
        r += (i.bytes[5] & 0xFF);
        r <<= 8;
        r += (i.bytes[6] & 0xFF);
        r <<= 8;
        r += (i.bytes[7] & 0xFF);
        r <<= 8;
        r += (i.bytes[8] & 0xFF);
        int64_t v = (r - (1ll << 63));
        return v;
    }

    // compute a comparable string of bytes from a number using a type byte to separate
    // floats and integers else theres going to be floats mixed in integers
    // regardless of memory representation
    static inline byte_comparable<int64_t> comparable_bytes(int64_t n, uint8_t type_byte)
    {
        byte_comparable<int64_t> r;
        uint64_t t = n + (1ull << 63); // so that negative numbers compare to less than positive numbers
        r.bytes[0] = type_byte; // most significant is the type (overriding any value bytes)

        r.bytes[8] = (uint8_t)(t & 0xFF);
        t >>= 8;
        r.bytes[7] = (uint8_t)(t & 0xFF);
        t >>= 8;
        r.bytes[6] = (uint8_t)(t & 0xFF);
        t >>= 8;
        r.bytes[5] = (uint8_t)(t & 0xFF);
        t >>= 8;
        r.bytes[4] = (uint8_t)(t & 0xFF);
        t >>= 8;
        r.bytes[3] = (uint8_t)(t & 0xFF);
        t >>= 8;
        r.bytes[2] = (uint8_t)(t & 0xFF);
        t >>= 8;
        r.bytes[1] = (uint8_t)(t & 0xFF);
        return r;
    }

    // TODO: function isnt considering mantissa maybe
    static byte_comparable<int64_t> comparable_bytes(double n, uint8_t)
    {
        int64_t in;
        memcpy(&in, &n, sizeof(in)); // apparently mantissa is most significant - but I'm not so sure
        return comparable_bytes(in, art::tdouble);
    }

    struct comparable_result
    {
    private:
        uint8_t* data = nullptr; // this may point to the integer or another externally allocated variable
        byte_comparable<int64_t> integer{};
        size_t size = 0; // the size as initialized - only changed on construction
        uint8_t* bytes = nullptr;

    public:
        explicit comparable_result(int64_t value)
            : data(&integer.bytes[0])
              , integer(comparable_bytes(value, art::tinteger))
              // numbers are ordered before most ascii strings unless they start with 0x01
              , size(integer.get_size())
        {
        }

        explicit comparable_result(double value)
            : data(&integer.bytes[0])
              , integer(comparable_bytes(value, art::tdouble))
              , size(integer.get_size())
        {
            size = integer.get_size();
        }

        comparable_result(const char* val, size_t size)
            : size(size + 1)
        {
            bytes = heap::allocate<uint8_t>(this->size + 1);
            //TODO: ?hack? a hidden trailing null pointer has to be added
            memcpy(bytes + 1, val, this->size - 1);
            bytes[0] = art::tstring;
            data = bytes;
        }

        comparable_result(const comparable_result& r)
        {
            *this = r;
        }

        ~comparable_result()
        {
            if (bytes != nullptr) heap::free(bytes, this->size + 1);
        }

        comparable_result& operator=(const comparable_result& r)
        {
            if (this == &r) return *this;
            bytes = heap::allocate<uint8_t>(r.size + 1); // hidden 0 byte at end
            bytes[size] = 0;
            size = r.size;
            data = bytes;
            memcpy(bytes, r.bytes, size);
            return *this;
        }

        [[nodiscard]] const uint8_t* get_data() const
        {
            return data;
        }

        [[nodiscard]] unsigned get_size() const
        {
            return size;
        }

        [[nodiscard]] art::value_type get_value() const
        {
            return {get_data(), get_size() + 1}; // include the null terminator for this case
        }
    };

    static const char* eat_space(const char* str, size_t l)
    {
        const char* s = str;
        for (; s != str + l; ++s) // eat continuous initial spaces
        {
            if (*s == ' ')
                continue;
            else
                break;
        }
        return s;
    }

    static bool is_integer(const char* str, size_t l)
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

    // take a string and convert to a number as bytes or leave it alone
    // and return the bytes directly. the bytes will be copied
    static comparable_result convert(const char* v, size_t vlen)
    {
        int64_t i;
        double d;

        if (is_integer(v, vlen))
        {
            auto ianswer = fast_float::from_chars(v, v + vlen, i); // check if it's an integer first

            if (ianswer.ec == std::errc())
            {
                return comparable_result(i);
            }
        }

        auto fanswer = fast_float::from_chars(v, v + vlen, d); // TODO: not sure if its strict enough, well see

        if (fanswer.ec == std::errc() && fanswer.ptr == v + vlen)
        {
            return comparable_result(d);
        }

        return {v, vlen};
    }

    inline int64_t enc_bytes_to_int(const uint8_t* bytes, size_t len)
    {
        int64_t r = 0;
        if (len != 9)
            return r;
        byte_comparable<int64_t> dec(bytes, len);

        return dec_bytes_to_int(dec);
    }
}
