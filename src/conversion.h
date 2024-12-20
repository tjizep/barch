#pragma once
#include <cstdint>
#include <fast_float/fast_float.h>

namespace conversion
{
    template <typename I>
    struct byte_comparable
    {
        byte_comparable() : value(I())
        {
        }
        byte_comparable(I value) : value(value)
        {
        }
        // probably cpp will optimize this
        size_t get_size() const
        {
            return sizeof(bytes);
        }
        union
        {
            uint8_t bytes[sizeof(I) + 1];
            I value;
        };
    };
    // compute a comparable string of bytes from a number using a type byte to separate 
    // floats and integers else theres going to be floats mixed in integers
    // regardless of memory representation
    static inline byte_comparable<uint64_t> comparable_bytes(uint64_t n, uint8_t type_byte)
    {
        byte_comparable<uint64_t> r;
        uint64_t in = n;
        const uint64_t mask = (uint64_t)0xFF;
        r.bytes[0] = type_byte; // most significant is the type (overriding any value bytes)
        size_t i = r.get_size() - 2;
        // writing from least to most significant byte
        for (; i > 1; --i)
        {
            r.bytes[i] = (uint8_t)(in & mask);
            in = in / 256; // drop the written bits (probably should shift 8)
        }
        return r;
    }
    // TODO: function isnt considering mantissa maybe
    static inline byte_comparable<uint64_t> comparable_bytes(double n, uint8_t type_byte)
    {
        uint64_t in;
        memcpy(&in, &n, sizeof(in)); // apparently mantissa is most significant - but I'm not so sure
        //
        // should be copy most significant 10-bits (mantissa) then rest of number
        // [0][     1         ][      2                         ][  3       ][  4  ][  5  ][  6  ][ 7   ][8]
        // [8][8 bits mantissa][2 bits mantissa + 6 bits integer][8 bits int][8 int][8 int][8 int][8 int][8]
        // [8][           10         ][                       54                                           ] 
        //
        return comparable_bytes(in, type_byte);
    }

    struct comparable_result
    {
    private:
        const uint8_t *data; // nthis may point to the integer or another externally allocated variable 
        byte_comparable<uint64_t> integer;
        size_t size; // the size as initialized - only changed on construction

    public:

        comparable_result(uint64_t value) 
        : data(&integer.bytes[0])
        , integer (comparable_bytes(value, 0)) // numbers are ordered before most ascii strings unless they start with 0x01
        , size(integer.get_size())
        {}

        comparable_result(double value) 
        : data(&integer.bytes[0])
        , integer (comparable_bytes(value, 1)) // doubles are bigger than ints for our type ordering
        , size(integer.get_size())
        {
            size = integer.get_size();
        }

        comparable_result(const char *val, size_t size) 
        : data((const uint8_t *)val)
        , integer(0)
        , size(size)
        {
        }

        const uint8_t *get_data() const
        {
            return data;
        }

        size_t get_size() const
        {
            return size;
        }
    };

    // take a null terminated string and convert to a number as bytes or leave it alone
    // and return the bytes directly. the bytes will be copied
    static comparable_result convert(const char *v, size_t vlen)
    {
        uint64_t i;
        double d;


        auto fanswer = fast_float::from_chars(v, v + vlen, d); // TODO: not sure if its strict enough, well see

        if (fanswer.ec == std::errc())
        {
            return comparable_result(d);
        }

        auto ianswer = fast_float::from_chars(v, v + vlen, i);

        if (ianswer.ec == std::errc())
        {
            return comparable_result(i);
        }

        return comparable_result(v, vlen);
    }
    uint64_t enc_bytes_to_int(const uint8_t * bytes, size_t len){
        uint64_t r = 0;
        if (len != 9)
            return r;
        
        for (size_t a = 0; a < 8; ++a) 
        {
            r = (r << 8) + bytes[a+1]; // << 8 equivalent to x256
        }
        
        return r;
    }
}
