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

#include <chrono>
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

size_t simd::first_byte_gt(const uint8_t* data, unsigned size, uint8_t ch)
{
    const uint8_t* ptr = data;
    const uint8_t* end = data + size;
#if defined(__i386__) || defined(__amd64__) || defined(__ARM_NEON__)
    //size_t first = 0;
    __m128i tocmp =  _mm_set1_epi8(ch);
    while (size >= 16) {
        __builtin_prefetch(ptr+16);
        size_t diff = 16;
        int mask = 0;
        __m128i chunk = _mm_loadu_si128 ((__m128i const*)ptr);
        __m128i results =  _mm_cmpgt_epi8(chunk, tocmp);
        mask = _mm_movemask_epi8(results);
        if (mask)
        {
            int lz = __builtin_ctz(mask);
            return ptr - data + lz;
        }
        //first += diff;
        ptr += diff;
        size -= diff;
    }
#endif

    while (ptr!=end)
    {
        if (*ptr > ch)
        {
            return ptr - data;
        }
        ++ptr;
    }
    return ptr - data;
}
size_t simd::first_byte_eq(const uint8_t* data, unsigned size, uint8_t ch)
{
    const uint8_t* ptr = data;
    const uint8_t* end = data + size;
#if 1
#if defined(__i386__) || defined(__amd64__) || defined(__ARM_NEON__)
    __m128i tocmp =  _mm_set1_epi8(ch);
    while (size >= 16) {
        __builtin_prefetch(ptr+16);
        size_t diff = 16;
        int mask = 0;
        __m128i chunk = _mm_loadu_si128 ((__m128i const*)ptr);
        __m128i results =  _mm_cmpeq_epi8(chunk, tocmp);
        mask = _mm_movemask_epi8(results);
        if (mask)
        {
            int lz = __builtin_ctz(mask);
            return ptr - data + lz;
        }
        ptr += diff;
        size -= diff;
    }
#endif
#endif
    while (ptr!=end)
    {
        if (*ptr == ch)
        {
            return ptr - data;
        }
        ++ptr;
    }
    return ptr - data;
}
#include "logger.h"
int test()
{
    unsigned char data[16] = {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01,0x00,0x00,0x00,0x01,0x00};
    unsigned char data1[16] = {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
    unsigned char data2[35] = {0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01,0x00,0x00};
    if (10 != simd::first_byte_eq(data, 16, 1))
    {
        abort();
    }
    if (16 != simd::first_byte_eq(data1, 16, 1))
    {
        abort();
    }
    if (32 != simd::first_byte_eq(data2, 35, 1))
    {
        abort();
    }
    if (32 != simd::first_byte_gt(data2, 35, 0))
    {
        abort();
    }
#if 1
    int64_t test_total = 0;
    int64_t test_total1 = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i< 3000000;++i)
    {
        int at = 0;
        for (auto ch : data2)
        {
            if (ch == 1)
            {
                if (at == 32)
                    ++test_total;
            }
            ++at;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    int64_t normal_time = duration.count();
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i< 3000000;++i)
    {
        if (simd::first_byte_eq(data2, 35, 1)==32)
        {
            ++test_total1;
        }

    }
    end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    int64_t simd_time = duration.count();
    art::std_log("normal vs sse time",normal_time, simd_time,"ms");
    if (test_total1 != test_total)
    {
        abort();
    }
#endif

    return 0;
}
static int t = test();