#pragma once
#include <cstdint>
enum OPERATION_BIT {
    eq = 1,
    gt = 2,
    lt = 4
};

/**
 * utility to create N copies of unsigned character C c
 */

namespace simd
{
    template<unsigned N, typename C = unsigned char>
    struct nuchar {
        C data[N];
        nuchar(C c){
            memset(data, c, sizeof(data)); // hopefully this uses simd anyway (if available)
        }
        operator const C* () const {
            return data;
        }
    };
    extern unsigned bits_oper16(const unsigned char * a, const unsigned char * b, unsigned mask, unsigned operbits);
    extern size_t first_byte_gt(const uint8_t* data, unsigned size, uint8_t ch);
    extern size_t first_byte_eq(const uint8_t* data, unsigned size, uint8_t ch);
}