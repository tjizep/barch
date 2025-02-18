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
/**
 * compare two buffers and put the result in a bitmap
 */

unsigned bits_oper16(const unsigned char * a, const unsigned char * b, unsigned mask, unsigned operbits){
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

        if ((operbits & eq) == eq) {
            // _mm_set1_epi8(a)
            cmp = _mm_cmpeq_epi8(_mm_loadu_si128((__m128i*)a),_mm_loadu_si128((__m128i*)b)); 
            bitfield |= _mm_movemask_epi8(cmp);
        }
        
        if ((operbits & lt) == lt) {
            cmp = _mm_cmplt_epi8(_mm_loadu_si128((__m128i*)a), _mm_loadu_si128((__m128i*)b));
            bitfield |= _mm_movemask_epi8(cmp);
        }       
        
        if ((operbits & gt) == gt) {
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