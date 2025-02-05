#pragma once
#include <cstdint>
#include <vector>
#include <fast_float/fast_float.h>

namespace conversion
{
    template <typename I>
    struct byte_comparable
    {
        byte_comparable() = default;
        explicit byte_comparable(const uint8_t * data, size_t len ){
            memcpy(&bytes[0], data, std::min(sizeof(bytes), len));
        }
        
        // probably cpp will optimize this
        [[nodiscard]] size_t get_size() const
        {
            return sizeof(bytes);
        }
        uint8_t bytes[sizeof(I) + 1]{};

    };
    inline int64_t dec_bytes_to_int(const byte_comparable<int64_t> &i){
        int64_t r = 0;
        
        r += (i.bytes[1] & 0xFF); r <<= 8;
        r += (i.bytes[2] & 0xFF); r <<= 8;
        r += (i.bytes[3] & 0xFF); r <<= 8;
        r += (i.bytes[4] & 0xFF); r <<= 8;
        r += (i.bytes[5] & 0xFF); r <<= 8;
        r += (i.bytes[6] & 0xFF); r <<= 8;
        r += (i.bytes[7] & 0xFF); r <<= 8;
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
        
        r.bytes[8] = (uint8_t) (t & 0xFF); t >>= 8;
        r.bytes[7] = (uint8_t) (t & 0xFF); t >>= 8;
        r.bytes[6] = (uint8_t) (t & 0xFF); t >>= 8;
        r.bytes[5] = (uint8_t) (t & 0xFF); t >>= 8;
        r.bytes[4] = (uint8_t) (t & 0xFF); t >>= 8;
        r.bytes[3] = (uint8_t) (t & 0xFF); t >>= 8;
        r.bytes[2] = (uint8_t) (t & 0xFF); t >>= 8;
        r.bytes[1] = (uint8_t) (t & 0xFF);
        return r;
    }
    // TODO: function isnt considering mantissa maybe
    static byte_comparable<int64_t> comparable_bytes(double n, uint8_t )
    {
        int64_t in;
        memcpy(&in, &n, sizeof(in)); // apparently mantissa is most significant - but I'm not so sure
        return comparable_bytes(in, 1);
    }

    struct comparable_result
    {
        enum types
        {
            rinteger = 0,
            rdouble = 1,
            rbuffer = 2
        };
    private:
        byte_comparable<int64_t> integer;
        size_t size; // the size as initialized - only changed on construction
        std::vector<uint8_t> bytes{};
        uint8_t type;

    public:

        explicit comparable_result(int64_t value)
        : integer (comparable_bytes(value, rinteger)) // numbers are ordered before byte buffers
        , size(integer.get_size())
        , type(rinteger)
        {}

        explicit comparable_result(double value)
        : integer(comparable_bytes(value, rdouble))
        , size(integer.get_size())
        , type(rdouble)
        {}

        comparable_result(const char *val, size_t size) 
        : integer()
        , size(size)
        , type(rbuffer)
        {
            //bytes.push_back(rbuffer); // push the type
            bytes.insert(bytes.end(),val, val + size);
            this->size = bytes.size();
        }

        [[nodiscard]] const uint8_t *get_data() const
        {
            switch (type)
            {
                case rinteger:
                    return &integer.bytes[0];
                case rdouble:
                    return &integer.bytes[0];
                case rbuffer:
                    return bytes.data();
                default:
                    abort();
            }
        }

        [[nodiscard]] size_t get_size() const
        {
            return size;
        }
    };

    static const char* eat_space(const char * str, size_t l){
        const char * s = str;
        for (;s != str + l; ++s) // eat continuous initial spaces
        { 
            if(*s == ' ')
                continue;
            else
                break;
        }
        return s;
    }
    static bool is_integer(const char * str, size_t l) 
    {
        const char * s = eat_space(str, l);
        if (s == str + l)
        {
            return false;
        }
        if(*s =='-'){
            ++s;
        }
        s = eat_space(s, l - (s- str));

        for (;s != str + l; ++s)
        {
            if(!fast_float::is_integer(*s)) 
                return false;
        }
        return true;
    }
    // take a string and convert to a number as bytes or leave it alone
    // and return the bytes directly. the bytes will be copied
    static comparable_result convert(const char *v, size_t vlen)
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
    
        if (fanswer.ec == std::errc())
        {
            return comparable_result(d);
        }

        return {v, vlen};
    }

    inline int64_t enc_bytes_to_int(const uint8_t * bytes, size_t len){
        int64_t r = 0;
        if (len != 9)
            return r;
        byte_comparable<int64_t> dec(bytes, len);
        
        return dec_bytes_to_int(dec);
    }
}
