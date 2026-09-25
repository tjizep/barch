//
// The numeric key encoding, and the NUL terminator a key is assumed to carry.
//
// Three premises the store was built on, pinned here - see TODO 448:
//
//   1. A float or double score keeps its numeric order through the encoder.
//      The old code ran the raw IEEE-754 bits through the integer encoder, and
//      the sign bit orders the two types oppositely, so every negative score
//      was stored backwards.
//   2. The integer decoder does its arithmetic without signed overflow. The
//      old one accumulated into an int64 and subtracted 1<<63, which overflows
//      for every non-negative value - undefined behaviour the build does not
//      allow for. This program is compiled with the signed-overflow sanitizer
//      so the decode cannot fall back on wrapping, as it did.
//   3. A key is NUL terminated, size includes that terminator, and an interior
//      NUL is refused. Built without it, art::insert used to store the key one
//      byte short rather than complain.
//
#include "conversion.h"
#include "art/art.h"
#include "art/nodes.h"

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
    int failures = 0;

    void check(bool ok, const char* what) {
        if (!ok) {
            std::printf("FAIL: %s\n", what);
            ++failures;
        }
    }

    using bytes = std::vector<uint8_t>;

    bytes as_bytes(const uint8_t* p, size_t n) {
        return bytes(p, p + n);
    }

    // type + ten base-128 digits for a 64-bit key, type + four bytes for a 32-bit one
    bytes enc_i64(int64_t v) {
        auto b = conversion::comparable_bytes(v, art::tinteger);
        return as_bytes(b.bytes, 11);
    }
    bytes enc_dbl(double v) {
        auto b = conversion::comparable_bytes(v, art::tdouble);
        return as_bytes(b.bytes, 11);
    }
    bytes enc_i32(int32_t v) {
        auto b = conversion::comparable_bytes32(v, art::tshort);
        return as_bytes(b.bytes, 5);
    }
    bytes enc_flt(float v) {
        auto b = conversion::comparable_bytes(v, art::tfloat);
        return as_bytes(b.bytes, 5);
    }

    // byte zero is the type, the same for every value in a group, so the order
    // has to come out of the data bytes after it
    template <class V, class Enc>
    void ascending(const char* what, const std::vector<V>& values, Enc enc) {
        for (size_t i = 0; i + 1 < values.size(); ++i) {
            bytes a = enc(values[i]);
            bytes b = enc(values[i + 1]);
            int c = std::memcmp(a.data() + 1, b.data() + 1, a.size() - 1);
            check(c < 0, what);
        }
    }

    void ordering() {
        ascending<int64_t>("int64 order", {
            std::numeric_limits<int64_t>::min(), -1000000000000LL, -1000, -1, 0,
            1, 1000, 1000000000000LL, std::numeric_limits<int64_t>::max()
        }, enc_i64);

        ascending<int32_t>("int32 order", {
            std::numeric_limits<int32_t>::min(), -100000, -1, 0, 1, 100000,
            std::numeric_limits<int32_t>::max()
        }, enc_i32);

        ascending<double>("double order", {
            -std::numeric_limits<double>::infinity(), -3.0, -2.5, -2.0, -1.0,
            -0.5, -0.0, 0.0, 0.5, 1.0, 2.0, 2.5, 3.0,
            std::numeric_limits<double>::infinity()
        }, enc_dbl);

        ascending<float>("float order", {
            -std::numeric_limits<float>::infinity(), -3.0f, -2.0f, -1.0f, -0.5f,
            -0.0f, 0.0f, 0.5f, 1.0f, 2.0f, 3.0f,
            std::numeric_limits<float>::infinity()
        }, enc_flt);
    }

    void round_trips() {
        const int64_t iv[] = {
            std::numeric_limits<int64_t>::min(), -1234567890123LL, -1, 0, 1,
            1234567890123LL, std::numeric_limits<int64_t>::max()
        };
        for (int64_t v : iv) {
            auto b = conversion::comparable_bytes(v, art::tinteger);
            check(conversion::enc_bytes_to_int(b.bytes, numeric_key_size) == v,
                  "int64 round trip");
        }

        const int32_t sv[] = {
            std::numeric_limits<int32_t>::min(), -123456, -1, 0, 1, 123456,
            std::numeric_limits<int32_t>::max()
        };
        for (int32_t v : sv) {
            auto b = conversion::comparable_bytes32(v, art::tshort);
            check(conversion::enc_bytes_to_int32(b.bytes, num32_key_size) == v,
                  "int32 round trip");
        }

        const double dv[] = {
            -std::numeric_limits<double>::infinity(), -3.25, -1.0, -0.5, -0.0,
            0.0, 0.5, 1.0, 3.25, std::numeric_limits<double>::infinity()
        };
        for (double v : dv) {
            auto b = conversion::comparable_bytes(v, art::tdouble);
            art::value_type vt{b.bytes, (unsigned) numeric_key_size};
            double got = conversion::enc_bytes_to_dbl(vt);
            check(std::memcmp(&got, &v, sizeof(double)) == 0, "double round trip");
        }

        const float fv[] = {
            -std::numeric_limits<float>::infinity(), -3.25f, -1.0f, -0.0f, 0.0f,
            1.0f, 3.25f, std::numeric_limits<float>::infinity()
        };
        for (float v : fv) {
            auto b = conversion::comparable_bytes(v, art::tfloat);
            art::value_type vt{b.bytes, (unsigned) num32_key_size};
            float got = conversion::enc_bytes_to_float(vt);
            check(std::memcmp(&got, &v, sizeof(float)) == 0, "float round trip");
        }
    }

    // A 32-bit key is num32_key_size bytes, not the int64 member's length, and
    // nothing outside its own buffer leaks into the encoding. See TODO 449.
    void widths() {
        conversion::comparable_key i32((int32_t) 42);
        check(i32.get_size() == num32_key_size, "int32 key length is num32_key_size");
        auto ienc = conversion::comparable_bytes32((int32_t) 42, art::tshort);
        check(std::memcmp(i32.get_value().bytes, ienc.bytes, num32_key_size) == 0,
              "int32 key bytes are only the encoded value");
        check(conversion::enc_bytes_to_int32(i32.get_value().bytes, num32_key_size) == 42,
              "int32 key round trip");

        conversion::comparable_key f32(1.5f);
        check(f32.get_size() == num32_key_size, "float key length is num32_key_size");
        check(conversion::enc_bytes_to_float(f32.get_value()) == 1.5f, "float key round trip");
    }

    bool throws_runtime_error(const std::function<void()>& fn) {
        try {
            fn();
        } catch (const std::runtime_error&) {
            return true;
        } catch (...) {
            return false;
        }
        return false;
    }

    void key_contract() {
        // A value_type straight from a std::string carries no terminator, and
        // length() still assumes one - the silent half of the assumption.
        std::string text = "abc";
        art::value_type raw{text};
        check(raw.size == 3, "std::string value_type size is the bytes");
        check(raw.length() == 2, "length() assumes a hidden terminator");

        // s_filter_key is what puts the terminator back on the command path.
        std::string scratch;
        art::value_type filtered = art::s_filter_key(scratch, raw);
        check(filtered.size == 4, "s_filter_key appends the terminator");
        check(filtered.bytes[filtered.size - 1] == 0, "filtered key ends in NUL");
        check(filtered.length() == 3, "filtered length is the content");

        // A key already ending in NUL is passed through unchanged.
        std::string terminated = "abc";
        terminated.push_back('\0');
        std::string scratch2;
        art::value_type same =
            art::s_filter_key(scratch2, art::value_type{terminated.data(), terminated.size()});
        check(same.size == 4 && same.bytes[3] == 0, "terminated key is kept");

        // An interior NUL is refused, so the terminator means one thing.
        std::string interior = "a";
        interior.push_back('\0');
        interior.push_back('b');
        check(throws_runtime_error([&] {
                  std::string scratch3;
                  art::s_filter_key(scratch3, art::value_type{interior.data(), interior.size()});
              }),
              "interior NUL refused");

        // The insert guard. A key without its terminator is now refused loudly
        // instead of being stored a byte short.
        check(throws_runtime_error([&] { art::require_terminated_key(raw); }),
              "unterminated key refused by insert guard");
        check(!throws_runtime_error([&] { art::require_terminated_key(filtered); }),
              "terminated key accepted by insert guard");
    }
}

int main() {
    ordering();
    round_trips();
    widths();
    key_contract();

    if (failures) {
        std::printf("%d check(s) failed\n", failures);
        return 1;
    }
    std::printf("numeric encoding and key contract: ok\n");
    return 0;
}
