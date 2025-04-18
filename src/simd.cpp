/**
 * hopefully __ARM_NEON__ is mutually exclusive with __i386__ or __amd64__
 */
#ifdef __ARM_NEON__
    #include "sse2neon.h"
#endif

#ifdef __i386__
    #include <emmintrin.h>
#else
#ifdef __amd64__
#include <emmintrin.h>
#endif
#endif
#include "simd.h"

#include <cstdint>
#include <cstring>
/**
 * compare two buffers and put the result in a bitmap
 */

unsigned simd::bits_oper16(const unsigned char* a, const unsigned char* b, unsigned mask, unsigned operbits)
{
    unsigned bitfield = 0;
    // support non-86 architectures via sse2neon
#if defined(__i386__) || defined(__amd64__) || defined(__ARM_NEON__)
    // Compare the key to all 16 stored keys
    __m128i cmp;
#if 0 // this doesnt work yet but hopefully one day
            if (operbits == (eq | gt)) {
                // supposedly a >= b same as !(b < a)
                cmp = _mm_cmplt_epi8(_mm_loadu_si128((__m128i*)b), _mm_loadu_si128((__m128i*)a));
                bitfield |= _mm_movemask_epi8(cmp);
                bitfield &= mask;
                return ~bitfield;
            }
#endif

    if ((operbits & eq) == eq)
    {
        // _mm_set1_epi8(a)
        cmp = _mm_cmpeq_epi8(_mm_loadu_si128((__m128i*)a), _mm_loadu_si128((__m128i*)b));
        bitfield |= _mm_movemask_epi8(cmp);
    }

    if ((operbits & lt) == lt)
    {
        cmp = _mm_cmplt_epi8(_mm_loadu_si128((__m128i*)a), _mm_loadu_si128((__m128i*)b));
        bitfield |= _mm_movemask_epi8(cmp);
    }

    if ((operbits & gt) == gt)
    {
        cmp = _mm_cmpgt_epi8(_mm_loadu_si128((__m128i*)a), _mm_loadu_si128((__m128i*)b));
        bitfield |= (_mm_movemask_epi8(cmp));
    }
#else
        unsigned i = 0;
    // Compare the key to all 16 stored keys
        if (operbits & eq) {
            for (i = 0; i < 16; ++i) {
                if (a[i] == b[i])
                    bitfield |= (1 << i);
            }
        }
        if (operbits & gt) {
            for (i = 0; i < 16; ++i) {
                if (a[i] > b[i])
                    bitfield |= (1 << i);
            }
        }
        if (operbits & lt) {
            for (i = 0; i < 16; ++i) {
                if (a[i] < b[i])
                    bitfield |= (1 << i);
            }
        }
#endif
    bitfield &= mask;
    return bitfield;
}

extern size_t simd::count_chars(const uint8_t* data, unsigned size, uint8_t ch)
{
    size_t total = 0;
#if defined(__i386__) || defined(__amd64__) || defined(__ARM_NEON__)
    __m128i tocmp =  _mm_set1_epi8(ch);
    const uint8_t* ptr = data;
    uint8_t rest[16];
    while (size) {
        size_t diff = 16;
        const uint8_t* cur_ptr = ptr;
        if (size < 16)
        {
            memset(rest, ~ch, sizeof(rest));
            memcpy(rest, ptr, size);
            diff = size;
        }else{
            memcpy(rest, ptr, 16);
        }
        cur_ptr = rest;
        int mask = 0;
        __m128i chunk = _mm_load_si128 ((__m128i const*)cur_ptr);
        __m128i results =  _mm_cmpeq_epi8(chunk, tocmp);
        mask = _mm_movemask_epi8(results);
        total += __builtin_popcount(mask);
        ptr += diff;
        size -= diff;
    }
#endif
    return total;
}
size_t simd::first_byte_gt(const uint8_t* data, unsigned size, uint8_t ch)
{
    size_t first = 0;
#if defined(__i386__) || defined(__amd64__) || defined(__ARM_NEON__)
    __m128i tocmp =  _mm_set1_epi8(ch);
    const uint8_t* ptr = data;
    uint8_t rest[16];
    while (size) {
        size_t diff = 16;
        if (size < 16)
        {
            memset(rest, ~ch, sizeof(rest));
            memcpy(rest, ptr, size);
            diff = size;
        }else{
            memcpy(rest, ptr, 16);
        }
        int mask = 0;
        __m128i chunk = _mm_load_si128 ((__m128i const*)rest);
        __m128i results =  _mm_cmpgt_epi8(chunk, tocmp);
        mask = _mm_movemask_epi8(results);
        if (mask)
        {
            int lz = __builtin_ctz(mask);
            return first + lz;
        }
        first += diff;
        ptr += diff;
        size -= diff;
    }
#else
    for (int i = 0; i < size;++i)
    {
        if (data[i]>ch) return i;
    }
    first = size;;

#endif
    return first;

}