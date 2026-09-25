#pragma once
#include <cstdint>
#include <regex>
#include "art/nodes.h"
#include <fast_float/fast_float.h>
#include "sastam.h"
#include "variable.h"

namespace conversion {

    std::string to_string(const Variable& v);
    double to_double(const Variable& v);
    bool to_bool(const Variable& v);
    int64_t to_i64(const Variable& v);
    uint64_t to_ui64(const Variable& v);

    bool to_ll(art::value_type vt, long long& l);
    bool to_double(art::value_type vt, double& l);
    bool to_i64(art::value_type v, int64_t &i);
    bool to_ui64(art::value_type v, uint64_t &i);
    inline Variable as_variable(const Variable& var) {
        return var;
    };

    Variable as_variable(const char *v, size_t vlen, bool noint = false);
    Variable as_variable(art::value_type v, bool noint = false);

    template<typename I>
    struct byte_comparable {
        byte_comparable() = default;

        explicit byte_comparable(const uint8_t *data, size_t len) {
            memcpy(&bytes[0], data, std::min(sizeof(bytes) - 1, len));
            bytes[sizeof(I) + 3] = 0; // there's a hidden trailing 0
        }

        // probably cpp will optimize this
        [[nodiscard]] size_t get_size() const {
            return sizeof(bytes);
        }
        void clear() {
            memset(bytes, 0, sizeof(bytes));
        }

        uint8_t bytes[sizeof(I) + 4]{0}; // there's a hidden trailing 0 added
    };

    // Recover the ordered unsigned key the emitter was given, with the digit
    // bias removed. This is the common inverse of ordered_bytes/ordered_bytes32;
    // the integer decoders below turn it back into a signed number, and the
    // float ones into an IEEE-754 bit pattern.
    inline uint64_t dec_bytes_to_order_key(const byte_comparable<int64_t> &i) {
        uint64_t r = 0;
        for (int b = 1; b <= 10; ++b) {
            r = r * encoding_width + ((i.bytes[b] & 0xFFu) - 1u);
        }
        return r;
    }

    inline uint32_t dec_bytes_to_order_key32(const byte_comparable<int32_t> &i) {
        uint32_t r = 0;
        for (int b = 1; b <= 4; ++b) {
            r = (r << 8) | (i.bytes[b] & 0xFFu);
        }
        return r;
    }

    /*
     * Decode in unsigned arithmetic, then move the sign back.
     *
     * The decoded key is `n + 2^63`, which is above INT64_MAX for every
     * non-negative n, and the old code accumulated that in an int64 and then
     * subtracted 1<<63. Both steps overflow a signed type, which is undefined
     * behaviour the build does not license: release is -O3 and nothing passes
     * -fwrapv, so the compiler is entitled to assume it never happens. The
     * modular arithmetic below is the same number with nothing undefined about
     * it, so the bytes it accepts are unchanged.
     */
    inline int64_t dec_bytes_to_int(const byte_comparable<int64_t> &i) {
        return (int64_t)(dec_bytes_to_order_key(i) - (1ull << 63));
    }

    inline int32_t dec_bytes_to_int32(const byte_comparable<int32_t> &i) {
        return (int32_t)(dec_bytes_to_order_key32(i) - (1u << 31));
    }

    /*
     * The shared emitters: ten base-128 digits for a 64-bit ordered key, four
     * base-256 bytes for a 32-bit one. Every digit is biased by one so no byte
     * is zero, which keeps the terminator unambiguous. Both take the value in
     * the form that is already order-preserving, and do not touch its sign.
     */
    static inline byte_comparable<int64_t> ordered_bytes(uint64_t t, uint8_t type_byte) {
        byte_comparable<int64_t> r;
        uint8_t delta = 1;
        r.bytes[0] = type_byte;
        for (int b = 10; b >= 2; --b) {
            r.bytes[b] = (uint8_t)(t % encoding_width) + delta;
            t /= encoding_width;
        }
        if (t > 255) {
            abort_with("encoding challenge");
        }
        r.bytes[1] = (uint8_t)(t % encoding_width) + delta;
        return r;
    }

    static inline byte_comparable<int32_t> ordered_bytes32(uint32_t t, uint8_t type_byte) {
        byte_comparable<int32_t> r;
        r.bytes[0] = type_byte;
        for (int b = 4; b >= 1; --b) {
            r.bytes[b] = (uint8_t)(t & 0xFFu);
            t >>= 8;
        }
        return r;
    }

    // compute a comparable string of bytes from a number using a type byte to separate
    // floats and integers else theres going to be floats mixed in integers
    // regardless of memory representation
    static inline byte_comparable<int64_t> comparable_bytes(int64_t n, uint8_t type_byte) {
        // signed order becomes unsigned order by moving the sign bit
        return ordered_bytes((uint64_t)n + (1ull << 63), type_byte);
    }

    static inline byte_comparable<int64_t> make_int64_bytes(int64_t n) {
        return comparable_bytes(n, art::tinteger);
    }

    static inline byte_comparable<int32_t> comparable_bytes32(int32_t n, uint8_t type_byte) {
        // signed order becomes unsigned order by moving the sign bit
        return ordered_bytes32((uint32_t)n + (1u << 31), type_byte);
    }

    /*
     * The order-preserving unsigned form of a float, and back.
     *
     * The sign bit is the top bit of both an IEEE-754 float and a signed
     * integer, but the two orderings differ: for a negative float a larger bit
     * pattern is a smaller number, while for a negative integer it is the other
     * way round. Running the raw bits through the integer encoder therefore
     * stored every negative score backwards - ZRANGEBYSCORE walked the negative
     * half of an ordered set in the wrong direction. This maps the float order
     * onto unsigned order: a non-negative number gets its sign bit set, a
     * negative one has every bit inverted. A non-negative value ends up with
     * exactly the bytes the old code produced, so those still read back; a
     * negative one changes, and there was never a correct store of those to
     * keep. See TODO 448.
     */
    static inline uint64_t float_order_key(double n) {
        uint64_t bits;
        memcpy(&bits, &n, sizeof(bits));
        return (bits & (1ull << 63)) ? ~bits : (bits | (1ull << 63));
    }
    static inline uint32_t float_order_key(float n) {
        uint32_t bits;
        memcpy(&bits, &n, sizeof(bits));
        return (bits & (1u << 31)) ? ~bits : (bits | (1u << 31));
    }
    static inline uint64_t float_order_value(uint64_t key) {
        return (key & (1ull << 63)) ? (key & ~(1ull << 63)) : ~key;
    }
    static inline uint32_t float_order_value(uint32_t key) {
        return (key & (1u << 31)) ? (key & ~(1u << 31)) : ~key;
    }

    static byte_comparable<int64_t> comparable_bytes(double n, uint8_t) {
        return ordered_bytes(float_order_key(n), art::tdouble);
    }

    static byte_comparable<int32_t> comparable_bytes(float n, uint8_t) {
        return ordered_bytes32(float_order_key(n), art::tfloat);
    }

    static byte_comparable<int32_t> comparable_bytes(int32_t n, uint8_t) {
        return ordered_bytes32((uint32_t)n + (1u << 31), art::tshort);
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
              // the 32-bit key length, not the int64 member's - the latter read
              // four bytes past this buffer, which are not part of the value. See
              // TODO 449.
              , size(num32_key_size) {
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
              , size(num32_key_size) {
            size = num32_key_size;
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
                  || art::is_composite_lead(val.bytes[0])
                  || val.bytes[0] == art::tfloat
                  || val.bytes[0] == art::tshort
                  || val.bytes[0] == art::tlast_valid
                  || val.bytes[0] == art::tend)) {
                throw_exception<std::invalid_argument>("invalid value_type");
            }
            if (this->size < sizeof(storage) - 1) {
                memset(storage, 0, sizeof(storage));
                data = storage;
            } else {
                // we need to allocate one more byte for the null terminator (which is not explicitly implied)
                bytes = heap::allocate<uint8_t>(this->size + 1);
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

    //const char *eat_space(const char *str, size_t l);

    //bool is_integer(const char *str, size_t l);

    template<typename IntType>
    static bool convert_value(IntType &i, art::value_type v) {
        auto ianswer = fast_float::from_chars(v.chars(), v.chars() + v.size, i); // check if it's an integer first

        if (ianswer.ec == std::errc() && ianswer.ptr == v.chars() + v.size) {
            return true;
        }
        return false;
    }


    art::value_type to_value(const std::string &s);

    // take a string and convert to a number as bytes or leave it alone
    // and return the bytes directly. the bytes will be copied
    comparable_key convert(const char *v, size_t vlen, bool noint = false);

    comparable_key convert(art::value_type vt, bool noint = false);

    /**
     * The empty component, built the way every other component is.
     *
     * Neither `comparable_key("")` nor `comparable_key("", 0)` produces one: both leave
     * out the separator that follows a component, so whatever comes next merges into it.
     * An ordered set's member index used the bare literal as its marker and the key it
     * built was byte for byte the same as the key of a set whose name began with an 0x03 -
     * two different things with one encoding, which no reader can tell apart. Only
     * convert() lays it out properly. See DONE 62.
     */
    inline comparable_key empty_component() {
        return convert(art::value_type{(const uint8_t *) "", 0u});
    }

    comparable_key as_composite(art::value_type v, bool noint = false, char sep = ' ');
    /** split on `split` instead of a single character. null keeps the space split. */
    comparable_key as_composite(art::value_type v, bool noint, const std::regex* split);
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
        // the stored key is the float-order form, so undo it rather than
        // reading the bits straight back - see float_order_value
        byte_comparable<int32_t> dec(value.bytes, value.size);
        uint32_t raw = float_order_value(dec_bytes_to_order_key32(dec));
        float fl = 0;
        memcpy(&fl, &raw, sizeof(raw));
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
        // the stored key is the float-order form, so undo it rather than
        // reading the bits straight back - see float_order_value
        byte_comparable<int64_t> dec(value.bytes, value.size);
        uint64_t raw = float_order_value(dec_bytes_to_order_key(dec));
        double dbl = 0;
        memcpy(&dbl, &raw, sizeof(raw));
        return dbl;
    }
}

