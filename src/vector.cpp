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
#include "vector.h"
/**
 * compare two buffers and put the result in a bitmap
 */

unsigned bits_oper16(const unsigned char * a, const unsigned char * b, unsigned mask, unsigned operbits){
    unsigned bitfield = 0;
    // support non-86 architectures
    #if defined(__i386__) || defined(__amd64__) || defined(__ARM_NEON__)
        // Compare the key to all 16 stored keys
        __m128i cmp; 
        #if 0 // this doesnt work yet but hopefully one day
            if (operbits == (OPERATION_BIT::eq | OPERATION_BIT::gt)) {
                // supposedly a >= b same as !(b < a)
                cmp = _mm_cmplt_epi8(_mm_loadu_si128((__m128i*)b), _mm_loadu_si128((__m128i*)a));
                bitfield |= _mm_movemask_epi8(cmp);
                bitfield &= mask;
                return ~bitfield;
            }
        #endif

        if ((operbits & OPERATION_BIT::eq) == OPERATION_BIT::eq) {
            // _mm_set1_epi8(a)
            cmp = _mm_cmpeq_epi8(_mm_loadu_si128((__m128i*)a),_mm_loadu_si128((__m128i*)b)); 
            bitfield |= _mm_movemask_epi8(cmp);
        }
        
        if ((operbits & OPERATION_BIT::lt) == OPERATION_BIT::lt) {
            cmp = _mm_cmplt_epi8(_mm_loadu_si128((__m128i*)a), _mm_loadu_si128((__m128i*)b));
            bitfield |= _mm_movemask_epi8(cmp);
        }       
        
        if ((operbits & OPERATION_BIT::gt) == OPERATION_BIT::gt) {
            cmp = _mm_cmpgt_epi8(_mm_loadu_si128((__m128i*)a), _mm_loadu_si128((__m128i*)b));
            bitfield |= (_mm_movemask_epi8(cmp));
        }
    #else
        // Compare the key to all 16 stored keys
        if (operbits & OPERATION_BIT::eq) {
            for (i = 0; i < 16; ++i) {
                if (a[i] == b[i])
                    bitfield |= (1 << i);
            }
        }
        if (operbits & OPERATION_BIT::gt) {
            for (i = 0; i < 16; ++i) {
                if (a[i] > b[i])
                    bitfield |= (1 << i);
            }
        }
        if (operbits & OPERATION_BIT::lt) {
            for (i = 0; i < 16; ++i) {
                if (a[i] < b[i])
                    bitfield |= (1 << i);
            }
        }
    #endif
    bitfield &= mask;
    return bitfield;
}