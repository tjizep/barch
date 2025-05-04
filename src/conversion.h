#pragma once
#include <cstdint>
#include "nodes.h"
#include <fast_float/fast_float.h>
#include "sastam.h"

namespace conversion {
    template<typename I>
    struct byte_comparable {
        byte_comparable() = default;

        explicit byte_comparable(const uint8_t *data, size_t len) {
            memcpy(&bytes[0], data, std::min(sizeof(bytes) - 1, len));
            bytes[sizeof(I) + 1] = 0;
        }

        // probably cpp will optimize this
        [[nodiscard]] size_t get_size() const {
            return sizeof(bytes);
        }

        uint8_t bytes[sizeof(I) + 2]{}; // there's a hidden trailing 0 added
    };

    inline int64_t dec_bytes_to_int(const byte_comparable<int64_t> &i) {
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

    inline int32_t dec_bytes_to_int32(const byte_comparable<int32_t> &i) {
        int32_t r = 0;
        r += (i.bytes[1] & 0xFF);
        r <<= 8;
        r += (i.bytes[2] & 0xFF);
        r <<= 8;
        r += (i.bytes[3] & 0xFF);
        r <<= 8;
        r += (i.bytes[4] & 0xFF);
        int32_t v = (r - (1 << 31));
        return v;
    }

    // compute a comparable string of bytes from a number using a type byte to separate
    // floats and integers else theres going to be floats mixed in integers
    // regardless of memory representation
    static inline byte_comparable<int64_t> comparable_bytes(int64_t n, uint8_t type_byte) {
        byte_comparable<int64_t> r;
        uint64_t t = n + (1ull << 63); // so that negative numbers compare to less than positive numbers
        r.bytes[0] = type_byte; // most significant is the type (overriding any value bytes)

        r.bytes[8] = (uint8_t) (t & 0xFF);
        t >>= 8;
        r.bytes[7] = (uint8_t) (t & 0xFF);
        t >>= 8;
        r.bytes[6] = (uint8_t) (t & 0xFF);
        t >>= 8;
        r.bytes[5] = (uint8_t) (t & 0xFF);
        t >>= 8;
        r.bytes[4] = (uint8_t) (t & 0xFF);
        t >>= 8;
        r.bytes[3] = (uint8_t) (t & 0xFF);
        t >>= 8;
        r.bytes[2] = (uint8_t) (t & 0xFF);
        t >>= 8;
        r.bytes[1] = (uint8_t) (t & 0xFF);
        return r;
    }

    static inline byte_comparable<int32_t> comparable_bytes32(int32_t n, uint8_t type_byte) {
        byte_comparable<int32_t> r;
        uint32_t t = n + (1 << 31); // so that negative numbers compare to less than positive numbers
        r.bytes[0] = type_byte; // most significant is the type (overriding any value bytes)

        r.bytes[4] = (uint8_t) (t & 0xFF);
        t >>= 8;
        r.bytes[3] = (uint8_t) (t & 0xFF);
        t >>= 8;
        r.bytes[2] = (uint8_t) (t & 0xFF);
        t >>= 8;
        r.bytes[1] = (uint8_t) (t & 0xFF);
        return r;
    }

    // TODO: function isnt considering mantissa maybe
    static byte_comparable<int64_t> comparable_bytes(double n, uint8_t) {
        int64_t in;
        memcpy(&in, &n, sizeof(in)); // apparently mantissa is most significant - but I'm not so sure
        return comparable_bytes(in, art::tdouble);
    }

    static byte_comparable<int32_t> comparable_bytes(float n, uint8_t) {
        int32_t in;
        memcpy(&in, &n, sizeof(in)); // apparently mantissa is most significant - but I'm not so sure
        return comparable_bytes32(in, art::tfloat);
    }

    static byte_comparable<int32_t> comparable_bytes(int32_t n, uint8_t) {
        int32_t in;
        memcpy(&in, &n, sizeof(in)); // apparently mantissa is most significant - but I'm not so sure
        return comparable_bytes32(in, art::tshort);
    }


    struct comparable_key {
    private:
        uint8_t storage[comparable_key_static_size]{};
        uint8_t *data = nullptr; // this may point to the integer or another externally allocated variable
        byte_comparable<int64_t> integer{};
        byte_comparable<int32_t> int32{};
        size_t size = 0; // the size as initialized - only changed on construction
        uint8_t *bytes = nullptr; // NB! this gets freed

    public:
        comparable_key() = default;

        explicit comparable_key(int64_t value)
            : data(&integer.bytes[0])
              , integer(comparable_bytes(value, art::tinteger))
              // numbers are ordered before most ascii strings unless they start with 0x01
              , size(integer.get_size()) {
        }

        explicit comparable_key(int32_t value)
            : data(&int32.bytes[0])
              , int32(comparable_bytes(value, art::tshort))
              // numbers are ordered before most ascii strings unless they start with 0x01
              , size(integer.get_size()) {
        }


        explicit comparable_key(double value)
            : data(&integer.bytes[0])
              , integer(comparable_bytes(value, art::tdouble))
              , size(integer.get_size()) {
            size = integer.get_size();
        }

        explicit comparable_key(float value)
            : data(&int32.bytes[0])
              , int32(comparable_bytes(value, art::tfloat))
              , size(int32.get_size()) {
            size = int32.get_size();
        }

        comparable_key(const art::composite_type &ct)
            : size(2) {
            storage[0] = ct.id;
            storage[1] = 0x00;
            data = storage;
        }

        comparable_key(const char *val) {
            size = strlen(val) + 1; // include type byte
            if (this->size < sizeof(storage) - 1) {
                memset(storage, 0, sizeof(storage));
                data = storage;
            } else {
                bytes = heap::allocate<uint8_t>(this->size + 1);
                data = bytes;
            }
            //TODO: ?hack? a hidden trailing null pointer has to be added
            //data[this->size] = 0x00;
            memcpy(data + 1, val, this->size - 1);
            data[this->size] = 0;
            data[0] = art::tstring;
        }

        comparable_key(const char *val, size_t size)
            : size(size + 1) {
            if (this->size < sizeof(storage) - 1) {
                memset(storage, 0, sizeof(storage));
                data = storage;
            } else {
                bytes = heap::allocate<uint8_t>(this->size + 1);
                data = bytes;
            }
            //TODO: ?hack? a hidden trailing null pointer has to be added
            //data[this->size] = 0x00;
            memcpy(data + 1, val, this->size - 1);
            data[0] = art::tstring;
        }

        comparable_key(art::value_type val)
            : size(val.size) {
            if (!(val.bytes[0] == art::tstring
                  || val.bytes[0] == art::tinteger
                  || val.bytes[0] == art::tdouble
                  || val.bytes[0] == art::tcomposite
                  || val.bytes[0] == art::tfloat
                  || val.bytes[0] == art::tend)) {
                throw_exception<std::invalid_argument>("invalid value_type");
            }
            if (this->size < sizeof(storage) - 1) {
                memset(storage, 0, sizeof(storage));
                data = storage;
            } else {
                bytes = heap::allocate<uint8_t>(this->size);
                data = bytes;
            }
            memcpy(data, val.bytes, this->size);
        }

        comparable_key(const comparable_key &r) {
            *this = r;
        }

        ~comparable_key() {
            if (bytes != nullptr) heap::free(bytes, this->size + 1);
        }

        comparable_key &operator=(const comparable_key &r) {
            if (this == &r) return *this;
            if (bytes != nullptr) heap::free(bytes, this->size + 1);
            bytes = nullptr;
            size = r.size;
            if (r.bytes != nullptr) {
                bytes = heap::allocate<uint8_t>(r.size + 1); // hidden 0 byte at end
                data = bytes;
                data[size] = 0;
            } else if (r.data == &r.integer.bytes[0]) {
                data = &integer.bytes[0];
            } else if (r.data == &r.int32.bytes[0]) {
                data = &int32.bytes[0];
            } else {
                if (size >= sizeof(storage) - 1) {
                    abort();
                }
                data = storage;
                data[size] = 0;
            }
            memcpy(data, r.data, size);

            return *this;
        }

        [[nodiscard]] const uint8_t *get_data() const {
            return data;
        }

        [[nodiscard]] unsigned get_size() const {
            return size;
        }

        [[nodiscard]] art::value_type get_value() const {
            return {get_data(), get_size()}; // include the null terminator for this case
        }

        [[nodiscard]] int ctype() const {
            if (get_size() == 0) return art::tnone;

            return get_data()[0];
        }
    };

    const char *eat_space(const char *str, size_t l);

    bool is_integer(const char *str, size_t l);

    template<typename IntType>
    static bool convert_value(IntType &i, art::value_type v) {
        if (is_integer(v.chars(), v.size)) {
            auto ianswer = fast_float::from_chars(v.chars(), v.chars() + v.size, i); // check if it's an integer first

            if (ianswer.ec == std::errc() && ianswer.ptr == v.chars() + v.size) {
                return true;
            }
        }
        return false;
    }


    art::value_type to_value(const std::string &s);

    // take a string and convert to a number as bytes or leave it alone
    // and return the bytes directly. the bytes will be copied
    comparable_key convert(const char *v, size_t vlen, bool noint = false);

    comparable_key convert(const std::string &str, bool noint = false);

    inline int64_t enc_bytes_to_int(const uint8_t *bytes, size_t len) {
        int64_t r = 0;
        if (len != numeric_key_size)
            return r;
        byte_comparable<int64_t> dec(bytes, len);

        return dec_bytes_to_int(dec);
    }

    inline int32_t enc_bytes_to_int32(const uint8_t *bytes, size_t len) {
        int32_t r = 0;
        if (len != num32_key_size)
            return r;
        byte_comparable<int32_t> dec(bytes, len);

        return dec_bytes_to_int32(dec);
    }

    inline int64_t enc_bytes_to_int(art::value_type value) {
        int64_t r = 0;
        if (value.empty()) return r;

        if (value.size != numeric_key_size)
            return r;

        if (*value.bytes != art::tdouble && *value.bytes != art::tinteger)
            return r;

        byte_comparable<int64_t> dec(value.bytes, value.size);

        r = dec_bytes_to_int(dec);
        return r;
    }

    inline int32_t enc_bytes_to_int32(art::value_type value) {
        int32_t r = 0;
        if (value.size != num32_key_size)
            return r;

        if (*value.bytes != art::tfloat)
            return r;

        byte_comparable<int32_t> dec(value.bytes, value.size);

        r = dec_bytes_to_int32(dec);
        return r;
    }

    inline double enc_bytes_to_dbl(art::value_type value);

    inline float enc_bytes_to_float(art::value_type value) {
        if (value.empty()) {
            return 0.0;
        }
        if (value.bytes[0] == art::tdouble) {
            return enc_bytes_to_dbl(value);
        }
        if (value.bytes[0] == art::tshort) {
            return enc_bytes_to_int32(value);
        }
        if (value.bytes[0] != art::tfloat) {
            return 0.0;
        }
        int32_t r = enc_bytes_to_int32(value);
        float fl = 0;
        memcpy(&fl, &r, sizeof(int32_t));
        return fl;
    }

    inline double enc_bytes_to_dbl(art::value_type value) {
        if (value.empty()) {
            return 0.0;
        }
        if (value.bytes[0] == art::tfloat) {
            return std::round((double) enc_bytes_to_float(value) * 100000.0) / 100000.0;;
        }
        if (value.bytes[0] == art::tshort) {
            return enc_bytes_to_int32(value);
        }
        if (value.bytes[0] == art::tinteger) {
            return enc_bytes_to_int(value);
        }
        if (value.bytes[0] != art::tdouble) {
            return 0.0;
        }
        int64_t r = enc_bytes_to_int(value);
        double dbl = 0;
        memcpy(&dbl, &r, sizeof(int64_t));
        return dbl;
    }
}
