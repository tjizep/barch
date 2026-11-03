// The barch.vec kernels for one instruction set. vec_luau.cpp includes this
// three times, with VEC_ISA set to 0 (plain C++), 1 (AVX2) or 2 (AVX-512) and
// VEC_NS naming the namespace, inside a `#pragma GCC target` region for that
// set. The three copies share everything but the block primitives at the top:
// a block is up to B candidates, its pass/fail bits are one uint64_t, and the
// emitters turn those bits into u32 indexes.
//
// Every read of an element is in bounds when a kernel starts: the binding has
// checked the last dense element, or the largest index in sel. The string
// bytes behind a str element are the one thing checked here.

namespace VEC_NS {

// block primitives ------------------------------------------------------------

// The B elements of width W from candidate k, as one contiguous run: a pointer
// straight into b when the candidates are dense and packed, otherwise a copy
// into tmp. A short last block is zero padded, so a primitive can always read
// a whole block; its extra bits are cleared afterwards.
template <size_t W>
static inline const uint8_t* fetch(const col& c, size_t k, size_t cnt, uint8_t* tmp) {
    const uint8_t* base = c.b + c.off;
    if (!c.sel && c.stride == W && cnt == B)
        return base + k * W;
    if (!c.sel) {
        if (c.stride == W) {
            memcpy(tmp, base + k * W, cnt * W);
        } else {
            for (size_t j = 0; j < cnt; ++j)
                memcpy(tmp + j * W, base + (k + j) * c.stride, W);
        }
    } else {
        for (size_t j = 0; j < cnt; ++j)
            memcpy(tmp + j * W, base + size_t(ld<uint32_t>(c.sel + 4 * (k + j))) * c.stride, W);
    }
    if (cnt < B)
        memset(tmp + cnt * W, 0, (B - cnt) * W);
    return tmp;
}

// Writes the candidates of block k whose bit is set in m to dst, in order, and
// answers how many. May store up to B entries past dst whatever m holds, so
// dst needs room for B; when dst is the output at or before candidate k (the
// in-place case), every store lands on candidates this block already read.
static inline size_t emit_to(const col& c, size_t k, size_t cnt, uint64_t m, uint32_t* dst) {
#if VEC_ISA == 2
    size_t got = 0;
    const __m512i iota = _mm512_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    for (size_t q = 0; q < cnt; q += 16) {
        auto bits = __mmask16(m >> q);
        if (!bits)
            continue;
        __m512i idx;
        if (c.sel) {
            size_t left = cnt - q;
            auto lanes = __mmask16(left >= 16 ? 0xFFFF : (1u << left) - 1);
            idx = _mm512_maskz_loadu_epi32(lanes, c.sel + 4 * (k + q));
        } else {
            idx = _mm512_add_epi32(iota, _mm512_set1_epi32(int(k + q)));
        }
        // compress into a register and store it whole: a compress straight
        // to memory is microcoded and slow on Zen 4
        _mm512_storeu_si512(dst + got, _mm512_maskz_compress_epi32(bits, idx));
        got += size_t(__builtin_popcount(bits));
    }
    return got;
#elif VEC_ISA == 1
    size_t got = 0;
    const __m256i iota = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
    for (size_t q = 0; q < cnt; q += 8) {
        auto bits = unsigned(m >> q) & 0xFF;
        if (!bits)
            continue;
        __m256i idx;
        if (c.sel) {
            size_t left = cnt - q;
            if (left >= 8) {
                idx = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(c.sel + 4 * (k + q)));
            } else {
                __m256i lanes = _mm256_cmpgt_epi32(_mm256_set1_epi32(int(left)), iota);
                idx = _mm256_maskload_epi32(reinterpret_cast<const int*>(c.sel + 4 * (k + q)), lanes);
            }
        } else {
            idx = _mm256_add_epi32(iota, _mm256_set1_epi32(int(k + q)));
        }
        __m256i perm = _mm256_load_si256(reinterpret_cast<const __m256i*>(compress8[bits]));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + got), _mm256_permutevar8x32_epi32(idx, perm));
        got += size_t(__builtin_popcount(bits));
    }
    return got;
#else
    size_t got = 0;
    for (size_t j = 0; j < cnt; ++j) {
        dst[got] = sel_at(c, k + j);
        got += (m >> j) & 1;
    }
    return got;
#endif
}

// emit_to into the caller's output, through a stack block when fewer than B
// slots are left. Answers false, having written what fits, when the output
// runs out.
static inline bool emit(const col& c, size_t k, size_t cnt, uint64_t m, sink& s) {
    if (!m)
        return true;
    size_t room = s.cap - s.kept;
    if (room >= B) {
        s.kept += emit_to(c, k, cnt, m, s.out + s.kept);
        return true;
    }
    alignas(64) uint32_t tmp[B];
    size_t got = emit_to(c, k, cnt, m, tmp);
    size_t fit = got < room ? got : room;
    memcpy(s.out + s.kept, tmp, fit * 4);
    s.kept += fit;
    return got <= room;
}

static inline uint64_t low_bits(size_t cnt) {
    return cnt >= 64 ? ~uint64_t(0) : (uint64_t(1) << cnt) - 1;
}

// Pass bits of a block of B integers of type T against r: lo <= x <= hi as one
// unsigned compare of x - lo against hi - lo, then the nil element.
template <class T>
static inline uint64_t range_block(const uint8_t* p, const irange& r) {
    using U = std::make_unsigned_t<T>;
    const auto lo = T(r.lo);
    const auto span = U(U(T(r.hi)) - U(lo));
    const auto none = T(r.none);
    uint64_t in = 0, nil = 0;
#if VEC_ISA == 2
    if constexpr (sizeof(T) == 1) {
        __m512i v = _mm512_loadu_si512(p);
        in = _mm512_cmple_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8(char(lo))), _mm512_set1_epi8(char(span)));
        if (r.has_none)
            nil = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(char(none)));
    } else if constexpr (sizeof(T) == 4) {
        const __m512i vlo = _mm512_set1_epi32(int(lo)), vspan = _mm512_set1_epi32(int(span));
        const __m512i vnone = _mm512_set1_epi32(int(none));
        for (int q = 0; q < 4; ++q) {
            __m512i v = _mm512_loadu_si512(p + 64 * q);
            in |= uint64_t(_mm512_cmple_epu32_mask(_mm512_sub_epi32(v, vlo), vspan)) << (16 * q);
            if (r.has_none)
                nil |= uint64_t(_mm512_cmpeq_epi32_mask(v, vnone)) << (16 * q);
        }
    } else {
        const __m512i vlo = _mm512_set1_epi64(lo), vspan = _mm512_set1_epi64(int64_t(span));
        const __m512i vnone = _mm512_set1_epi64(none);
        for (int q = 0; q < 8; ++q) {
            __m512i v = _mm512_loadu_si512(p + 64 * q);
            in |= uint64_t(_mm512_cmple_epu64_mask(_mm512_sub_epi64(v, vlo), vspan)) << (8 * q);
            if (r.has_none)
                nil |= uint64_t(_mm512_cmpeq_epi64_mask(v, vnone)) << (8 * q);
        }
    }
#elif VEC_ISA == 1
    // AVX2 has no unsigned compare: d <= span as min(d, span) == d for bytes
    // and dwords, and with the sign bits flipped for qwords
    if constexpr (sizeof(T) == 1) {
        const __m256i vlo = _mm256_set1_epi8(char(lo)), vspan = _mm256_set1_epi8(char(span));
        const __m256i vnone = _mm256_set1_epi8(char(none));
        for (int q = 0; q < 2; ++q) {
            __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32 * q));
            __m256i d = _mm256_sub_epi8(v, vlo);
            in |= uint64_t(uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_min_epu8(d, vspan), d)))) << (32 * q);
            if (r.has_none)
                nil |= uint64_t(uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, vnone)))) << (32 * q);
        }
    } else if constexpr (sizeof(T) == 4) {
        const __m256i vlo = _mm256_set1_epi32(int(lo)), vspan = _mm256_set1_epi32(int(span));
        const __m256i vnone = _mm256_set1_epi32(int(none));
        for (int q = 0; q < 8; ++q) {
            __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32 * q));
            __m256i d = _mm256_sub_epi32(v, vlo);
            in |= uint64_t(_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(_mm256_min_epu32(d, vspan), d)))) << (8 * q);
            if (r.has_none)
                nil |= uint64_t(_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(v, vnone)))) << (8 * q);
        }
    } else {
        const __m256i msb = _mm256_set1_epi64x(INT64_MIN);
        const __m256i vlo = _mm256_set1_epi64x(lo), vspan = _mm256_xor_si256(_mm256_set1_epi64x(int64_t(span)), msb);
        const __m256i vnone = _mm256_set1_epi64x(none);
        for (int q = 0; q < 16; ++q) {
            __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32 * q));
            __m256i d = _mm256_xor_si256(_mm256_sub_epi64(v, vlo), msb);
            uint64_t out = uint64_t(_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(d, vspan))));
            in |= (~out & 0xF) << (4 * q);
            if (r.has_none)
                nil |= uint64_t(_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(v, vnone)))) << (4 * q);
        }
    }
#else
    for (size_t j = 0; j < B; ++j) {
        T x = ld<T>(p + j * sizeof(T));
        in |= uint64_t(U(U(x) - U(lo)) <= span) << j;
        if (r.has_none)
            nil |= uint64_t(x == none) << j;
    }
#endif
    return r.ne ? (~in | nil) : (in & ~nil);
}

// Pass bits of a block of B f64s against w, IEEE style: NaN fails every op
// but ~=, which it passes.
template <int OP>
static inline uint64_t f64_block(const uint8_t* p, double w) {
    uint64_t m = 0;
#if VEC_ISA == 2
    constexpr int imm = OP == op_eq ? _CMP_EQ_OQ : OP == op_ne ? _CMP_NEQ_UQ : OP == op_lt ? _CMP_LT_OQ
                      : OP == op_le ? _CMP_LE_OQ : OP == op_gt ? _CMP_GT_OQ : _CMP_GE_OQ;
    const __m512d vw = _mm512_set1_pd(w);
    for (int q = 0; q < 8; ++q)
        m |= uint64_t(_mm512_cmp_pd_mask(_mm512_loadu_pd(reinterpret_cast<const double*>(p + 64 * q)), vw, imm)) << (8 * q);
#elif VEC_ISA == 1
    constexpr int imm = OP == op_eq ? _CMP_EQ_OQ : OP == op_ne ? _CMP_NEQ_UQ : OP == op_lt ? _CMP_LT_OQ
                      : OP == op_le ? _CMP_LE_OQ : OP == op_gt ? _CMP_GT_OQ : _CMP_GE_OQ;
    const __m256d vw = _mm256_set1_pd(w);
    for (int q = 0; q < 16; ++q)
        m |= uint64_t(_mm256_movemask_pd(_mm256_cmp_pd(_mm256_loadu_pd(reinterpret_cast<const double*>(p + 32 * q)), vw, imm))) << (4 * q);
#else
    for (size_t j = 0; j < B; ++j) {
        double x = ld<double>(p + 8 * j);
        bool pass = OP == op_eq ? x == w : OP == op_ne ? x != w : OP == op_lt ? x < w
                  : OP == op_le ? x <= w : OP == op_gt ? x > w : x >= w;
        m |= uint64_t(pass) << j;
    }
#endif
    return m;
}

// Pass bits of a block of B i8 codes against a 128-bit set of the codes 0 to
// 127: a nibble shuffle picks the byte, a second one the bit. A negative code
// fails.
static inline uint64_t in_i8_block(const uint8_t* p, const uint8_t* bits128) {
#if VEC_ISA == 2
    const __m512i table = _mm512_broadcast_i32x4(_mm_loadu_si128(reinterpret_cast<const __m128i*>(bits128)));
    const __m512i pow2 = _mm512_broadcast_i32x4(_mm_setr_epi8(1, 2, 4, 8, 16, 32, 64, -128, 1, 2, 4, 8, 16, 32, 64, -128));
    __m512i v = _mm512_loadu_si512(p);
    __m512i byte = _mm512_and_si512(_mm512_srli_epi16(v, 3), _mm512_set1_epi8(0x0F));
    __m512i got = _mm512_shuffle_epi8(table, byte);
    __m512i bit = _mm512_shuffle_epi8(pow2, _mm512_and_si512(v, _mm512_set1_epi8(7)));
    return _mm512_test_epi8_mask(got, bit) & ~uint64_t(_mm512_movepi8_mask(v));
#elif VEC_ISA == 1
    const __m256i table = _mm256_broadcastsi128_si256(_mm_loadu_si128(reinterpret_cast<const __m128i*>(bits128)));
    const __m256i pow2 = _mm256_setr_epi8(1, 2, 4, 8, 16, 32, 64, -128, 1, 2, 4, 8, 16, 32, 64, -128,
                                          1, 2, 4, 8, 16, 32, 64, -128, 1, 2, 4, 8, 16, 32, 64, -128);
    uint64_t m = 0;
    for (int q = 0; q < 2; ++q) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32 * q));
        __m256i byte = _mm256_and_si256(_mm256_srli_epi16(v, 3), _mm256_set1_epi8(0x0F));
        __m256i got = _mm256_shuffle_epi8(table, byte);
        __m256i bit = _mm256_shuffle_epi8(pow2, _mm256_and_si256(v, _mm256_set1_epi8(7)));
        __m256i miss = _mm256_cmpeq_epi8(_mm256_and_si256(got, bit), _mm256_setzero_si256());
        uint32_t hit = ~uint32_t(_mm256_movemask_epi8(miss)) & ~uint32_t(_mm256_movemask_epi8(v));
        m |= uint64_t(hit) << (32 * q);
    }
    return m;
#else
    uint64_t m = 0;
    for (size_t j = 0; j < B; ++j) {
        uint8_t c = p[j];
        m |= uint64_t(c < 128 && ((bits128[c >> 3] >> (c & 7)) & 1)) << j;
    }
    return m;
#endif
}

// The same for i32 codes, which first have to be below `size` (at most 128)
static inline uint64_t in_i32_block(const uint8_t* p, const uint8_t* bits128, uint32_t size) {
#if VEC_ISA == 2
    const __m128i table = _mm_loadu_si128(reinterpret_cast<const __m128i*>(bits128));
    const __m128i pow2 = _mm_setr_epi8(1, 2, 4, 8, 16, 32, 64, -128, 1, 2, 4, 8, 16, 32, 64, -128);
    const __m512i vsize = _mm512_set1_epi32(int(size));
    uint64_t m = 0;
    for (int q = 0; q < 4; ++q) {
        __m512i v = _mm512_loadu_si512(p + 64 * q);
        __mmask16 valid = _mm512_cmplt_epu32_mask(v, vsize);
        __m128i c = _mm512_cvtepi32_epi8(v);
        __m128i byte = _mm_and_si128(_mm_srli_epi16(c, 3), _mm_set1_epi8(0x0F));
        __m128i got = _mm_shuffle_epi8(table, byte);
        __m128i bit = _mm_shuffle_epi8(pow2, _mm_and_si128(c, _mm_set1_epi8(7)));
        m |= uint64_t(_mm_test_epi8_mask(got, bit) & valid) << (16 * q);
    }
    return m;
#elif VEC_ISA == 1
    const __m256i table = _mm256_broadcastsi128_si256(_mm_loadu_si128(reinterpret_cast<const __m128i*>(bits128)));
    const __m256i pow2 = _mm256_setr_epi8(1, 2, 4, 8, 16, 32, 64, -128, 1, 2, 4, 8, 16, 32, 64, -128,
                                          1, 2, 4, 8, 16, 32, 64, -128, 1, 2, 4, 8, 16, 32, 64, -128);
    const __m256i last = _mm256_set1_epi32(int(size - 1));
    const __m256i order = _mm256_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7);
    uint64_t m = 0;
    for (int q = 0; q < 2; ++q) {
        // 32 codes; the valid ones are 0 to 127, so packing them is exact
        __m256i v[4];
        uint32_t valid = 0;
        for (int r = 0; r < 4; ++r) {
            v[r] = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 128 * q + 32 * r));
            __m256i ok = _mm256_cmpeq_epi32(_mm256_min_epu32(v[r], last), v[r]);
            valid |= uint32_t(_mm256_movemask_ps(_mm256_castsi256_ps(ok))) << (8 * r);
        }
        __m256i w01 = _mm256_packs_epi32(v[0], v[1]);
        __m256i w23 = _mm256_packs_epi32(v[2], v[3]);
        __m256i c = _mm256_permutevar8x32_epi32(_mm256_packs_epi16(w01, w23), order);
        __m256i byte = _mm256_and_si256(_mm256_srli_epi16(c, 3), _mm256_set1_epi8(0x0F));
        __m256i got = _mm256_shuffle_epi8(table, byte);
        __m256i bit = _mm256_shuffle_epi8(pow2, _mm256_and_si256(c, _mm256_set1_epi8(7)));
        __m256i miss = _mm256_cmpeq_epi8(_mm256_and_si256(got, bit), _mm256_setzero_si256());
        m |= uint64_t(~uint32_t(_mm256_movemask_epi8(miss)) & valid) << (32 * q);
    }
    return m;
#else
    uint64_t m = 0;
    for (size_t j = 0; j < B; ++j) {
        uint32_t c = ld<uint32_t>(p + 4 * j);
        m |= uint64_t(c < size && ((bits128[c >> 3] >> (c & 7)) & 1)) << j;
    }
    return m;
#endif
}

// The nonzero bytes of a block of B
static inline uint64_t nonzero_block(const uint8_t* p) {
#if VEC_ISA == 2
    __m512i v = _mm512_loadu_si512(p);
    return _mm512_test_epi8_mask(v, v);
#elif VEC_ISA == 1
    uint64_t m = 0;
    for (int q = 0; q < 2; ++q) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32 * q));
        m |= uint64_t(~uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_setzero_si256())))) << (32 * q);
    }
    return m;
#else
    uint64_t m = 0;
    for (size_t j = 0; j < B; ++j)
        m |= uint64_t(p[j] != 0) << j;
    return m;
#endif
}

// kernels -----------------------------------------------------------------

template <size_t W, class Pred>
static inline status run_filter(const col& c, sink& s, Pred pred) {
    alignas(64) uint8_t tmp[B * W];
    for (size_t k = 0; k < c.n; k += B) {
        size_t cnt = c.n - k < B ? c.n - k : B;
        uint64_t m = pred(fetch<W>(c, k, cnt, tmp)) & low_bits(cnt);
        if (!emit(c, k, cnt, m, s))
            return write_past;
    }
    return ok;
}

template <int OP>
static status filter_f64_op(const col& c, double w, sink& s) {
    return run_filter<8>(c, s, [w](const uint8_t* p) { return f64_block<OP>(p, w); });
}

static status filter_num(int kind, const col& c, const irange& r, double w, int op, sink& s) {
    switch (kind) {
        case k_i8: return run_filter<1>(c, s, [&r](const uint8_t* p) { return range_block<int8_t>(p, r); });
        case k_i32: return run_filter<4>(c, s, [&r](const uint8_t* p) { return range_block<int32_t>(p, r); });
        case k_i64: return run_filter<8>(c, s, [&r](const uint8_t* p) { return range_block<int64_t>(p, r); });
        default: break;
    }
    switch (op) {
        case op_eq: return filter_f64_op<op_eq>(c, w, s);
        case op_ne: return filter_f64_op<op_ne>(c, w, s);
        case op_lt: return filter_f64_op<op_lt>(c, w, s);
        case op_le: return filter_f64_op<op_le>(c, w, s);
        case op_gt: return filter_f64_op<op_gt>(c, w, s);
        default: return filter_f64_op<op_ge>(c, w, s);
    }
}

// str. Each string's first 8 bytes, zero padded and byte swapped, compare as
// one integer in memcmp order. When those tie, the shorter string comes first,
// unless both run past 8 bytes, which is the one case that needs memcmp. A nil
// element is run as an empty string at 0 and then passes only ~=.

// one element's pass bit, or -1 when its string runs past the end of b
template <int Op>
static inline int str_pass(const col& c, uint32_t start, uint32_t size, const uint8_t* w, size_t wl, uint64_t w8) {
    bool some = size != 0;
    uint32_t len = some ? size - 1 : 0;
    start = some ? start : 0;
    if (size_t(start) + len > c.blen)
        return -1;
    uint64_t x = __builtin_bswap64(str_head(c.b, c.blen, start, len));
    if constexpr (Op == op_eq || Op == op_ne) {
        bool same = (len == wl) & (x == w8);
        if (same && len > 8)
            same = memcmp(c.b + start + 8, w + 8, len - 8) == 0;
        return some ? (Op == op_eq) == same : Op == op_ne;
    } else {
        int cmp = (x > w8) - (x < w8);
        if (cmp == 0)
            cmp = (len > 8) & (wl > 8) ? cmp_tail(c.b + start, len, w, wl) : (len > wl) - (len < wl);
        bool hold = Op == op_lt ? cmp < 0 : Op == op_le ? cmp <= 0 : Op == op_gt ? cmp > 0 : cmp >= 0;
        return some & hold;
    }
}

#if VEC_ISA == 2
// Eight elements at q at once. Each string's first 8 bytes come from a plain
// load: gathers are no faster on Zen 4 and much slower on Intel parts with
// the Downfall microcode fix. Answers 1 with the pass bits in `bits`, -1 when
// a string runs past the end of b, or 0 when an 8-byte load could read past
// it (a string within 8 bytes of the end), which leaves these eight to
// str_pass.
template <int Op>
static inline int str_block8(const col& c, const uint8_t* q, const uint8_t* w, size_t wl, uint64_t w8,
                             uint32_t& bits) {
    const __m512i one = _mm512_set1_epi64(1), eight = _mm512_set1_epi64(8);
    const __m512i blen = _mm512_set1_epi64(int64_t(c.blen));
    __m512i e = _mm512_loadu_si512(q);
    __m512i size = _mm512_srli_epi64(e, 32);
    __mmask8 some = _mm512_test_epi64_mask(size, size);
    __m512i start = _mm512_maskz_mov_epi64(some, _mm512_and_si512(e, _mm512_set1_epi64(0xFFFFFFFF)));
    __m512i len = _mm512_maskz_sub_epi64(some, size, one);
    if (_mm512_cmp_epu64_mask(_mm512_add_epi64(start, len), blen, _MM_CMPINT_NLE))
        return -1;
    if (_mm512_cmp_epu64_mask(_mm512_add_epi64(start, eight), blen, _MM_CMPINT_NLE))
        return 0;
    alignas(64) uint32_t at[16];
    _mm512_store_si512(at, start);
    const uint8_t* b = c.b;
    __m512i v = _mm512_set_epi64(ld<int64_t>(b + at[14]), ld<int64_t>(b + at[12]), ld<int64_t>(b + at[10]),
                                 ld<int64_t>(b + at[8]), ld<int64_t>(b + at[6]), ld<int64_t>(b + at[4]),
                                 ld<int64_t>(b + at[2]), ld<int64_t>(b + at[0]));
    // keep min(len, 8) bytes: shifting by 64 or more gives 0
    __m512i drop = _mm512_slli_epi64(_mm512_sub_epi64(eight, _mm512_min_epu64(len, eight)), 3);
    v = _mm512_and_si512(v, _mm512_srlv_epi64(_mm512_set1_epi64(-1), drop));
    const __m512i rev = _mm512_broadcast_i32x4(_mm_set_epi8(8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7));
    __m512i x = _mm512_shuffle_epi8(v, rev);
    __m512i wv = _mm512_set1_epi64(int64_t(w8)), wlv = _mm512_set1_epi64(int64_t(wl));
    __mmask8 lt = _mm512_cmp_epu64_mask(x, wv, _MM_CMPINT_LT);
    __mmask8 gt = _mm512_cmp_epu64_mask(x, wv, _MM_CMPINT_NLE);
    __mmask8 eq = __mmask8(~(lt | gt));
    __mmask8 long8 = _mm512_cmp_epu64_mask(len, eight, _MM_CMPINT_NLE);
    if constexpr (Op == op_eq || Op == op_ne) {
        __mmask8 same = eq & _mm512_cmp_epu64_mask(len, wlv, _MM_CMPINT_EQ) & some;
        for (unsigned t = same & long8; t; t &= t - 1) {
            unsigned j = unsigned(__builtin_ctz(t));
            uint32_t at = ld<uint32_t>(q + 8 * j), n = ld<uint32_t>(q + 8 * j + 4) - 1;
            if (memcmp(c.b + at + 8, w + 8, n - 8) != 0)
                same &= __mmask8(~(1u << j));
        }
        bits = Op == op_eq ? same : uint8_t(~same);
    } else {
        __mmask8 less = lt | (eq & _mm512_cmp_epu64_mask(len, wlv, _MM_CMPINT_LT));
        __mmask8 more = gt | (eq & _mm512_cmp_epu64_mask(len, wlv, _MM_CMPINT_NLE));
        if (wl > 8) {
            unsigned slow = eq & long8;
            less &= __mmask8(~slow), more &= __mmask8(~slow);
            for (; slow; slow &= slow - 1) {
                unsigned j = unsigned(__builtin_ctz(slow));
                uint32_t at = ld<uint32_t>(q + 8 * j), n = ld<uint32_t>(q + 8 * j + 4) - 1;
                int r = cmp_tail(c.b + at, n, w, wl);
                less |= __mmask8(r < 0 ? 1u << j : 0), more |= __mmask8(r > 0 ? 1u << j : 0);
            }
        }
        unsigned hold = Op == op_lt ? less : Op == op_le ? uint8_t(~more) : Op == op_gt ? more : uint8_t(~less);
        bits = hold & some;
    }
    return 1;
}
#endif

#if VEC_ISA == 1
// str_block8 with four lanes. AVX2 has no unsigned 64-bit compare, so both
// sides get their sign bit flipped first and compare signed.
template <int Op>
static inline int str_block4(const col& c, const uint8_t* q, const uint8_t* w, size_t wl, uint64_t w8,
                             uint32_t& bits) {
    const __m256i one = _mm256_set1_epi64x(1), eight = _mm256_set1_epi64x(8);
    const __m256i sign = _mm256_set1_epi64x(INT64_MIN);
    auto gtu = [&](__m256i a, __m256i b) {
        return _mm256_cmpgt_epi64(_mm256_xor_si256(a, sign), _mm256_xor_si256(b, sign));
    };
    auto bits4 = [](__m256i m) { return unsigned(_mm256_movemask_pd(_mm256_castsi256_pd(m))); };
    const __m256i lim = _mm256_set1_epi64x(int64_t(c.blen));
    __m256i e = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(q));
    __m256i size = _mm256_srli_epi64(e, 32);
    __m256i somev = _mm256_xor_si256(_mm256_cmpeq_epi64(size, _mm256_setzero_si256()), _mm256_set1_epi64x(-1));
    unsigned some = bits4(somev);
    __m256i start = _mm256_and_si256(_mm256_and_si256(e, _mm256_set1_epi64x(0xFFFFFFFF)), somev);
    __m256i len = _mm256_and_si256(_mm256_sub_epi64(size, one), somev);
    if (bits4(gtu(_mm256_add_epi64(start, len), lim)))
        return -1;
    if (bits4(gtu(_mm256_add_epi64(start, eight), lim)))
        return 0;
    alignas(32) uint32_t at[8];
    _mm256_store_si256(reinterpret_cast<__m256i*>(at), start);
    __m256i v = _mm256_set_epi64x(ld<int64_t>(c.b + at[6]), ld<int64_t>(c.b + at[4]), ld<int64_t>(c.b + at[2]),
                                  ld<int64_t>(c.b + at[0]));
    // keep min(len, 8) bytes: lengths are below 2^32, so a signed compare does
    __m256i l8 = _mm256_blendv_epi8(len, eight, _mm256_cmpgt_epi64(len, eight));
    __m256i drop = _mm256_slli_epi64(_mm256_sub_epi64(eight, l8), 3);
    v = _mm256_and_si256(v, _mm256_srlv_epi64(_mm256_set1_epi64x(-1), drop));
    const __m256i rev = _mm256_broadcastsi128_si256(_mm_set_epi8(8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7));
    __m256i x = _mm256_shuffle_epi8(v, rev);
    __m256i wv = _mm256_set1_epi64x(int64_t(w8)), wlv = _mm256_set1_epi64x(int64_t(wl));
    unsigned gt = bits4(gtu(x, wv)), lt = bits4(gtu(wv, x));
    unsigned eq = ~(lt | gt) & 15;
    unsigned long8 = bits4(_mm256_cmpgt_epi64(len, eight));
    if constexpr (Op == op_eq || Op == op_ne) {
        unsigned same = eq & bits4(_mm256_cmpeq_epi64(len, wlv)) & some;
        for (unsigned t = same & long8; t; t &= t - 1) {
            unsigned j = unsigned(__builtin_ctz(t));
            uint32_t at = ld<uint32_t>(q + 8 * j), n = ld<uint32_t>(q + 8 * j + 4) - 1;
            if (memcmp(c.b + at + 8, w + 8, n - 8) != 0)
                same &= ~(1u << j);
        }
        bits = Op == op_eq ? same : ~same & 15;
    } else {
        unsigned less = lt | (eq & bits4(gtu(wlv, len)));
        unsigned more = gt | (eq & bits4(gtu(len, wlv)));
        if (wl > 8) {
            unsigned slow = eq & long8;
            less &= ~slow, more &= ~slow;
            for (; slow; slow &= slow - 1) {
                unsigned j = unsigned(__builtin_ctz(slow));
                uint32_t at = ld<uint32_t>(q + 8 * j), n = ld<uint32_t>(q + 8 * j + 4) - 1;
                int r = cmp_tail(c.b + at, n, w, wl);
                less |= r < 0 ? 1u << j : 0, more |= r > 0 ? 1u << j : 0;
            }
        }
        unsigned hold = Op == op_lt ? less : Op == op_le ? ~more : Op == op_gt ? more : ~less;
        bits = hold & some & 15;
    }
    return 1;
}
#endif

template <int Op>
static status filter_str_op(const col& c, const uint8_t* w, size_t wl, sink& s) {
    const uint64_t w8 = __builtin_bswap64(load_short(w, wl < 8 ? wl : 8));
    alignas(64) uint8_t tmp[B * 8];
    for (size_t k = 0; k < c.n; k += B) {
        size_t cnt = c.n - k < B ? c.n - k : B;
        const uint8_t* p = fetch<8>(c, k, cnt, tmp);
        uint64_t m = 0;
        size_t j = 0;
#if VEC_ISA == 2
        for (; j + 8 <= cnt; j += 8) {
            uint32_t bits;
            int r = str_block8<Op>(c, p + 8 * j, w, wl, w8, bits);
            if (r < 0)
                return read_past;
            if (r == 0)
                break;
            m |= uint64_t(bits) << j;
        }
#elif VEC_ISA == 1
        for (; j + 4 <= cnt; j += 4) {
            uint32_t bits;
            int r = str_block4<Op>(c, p + 8 * j, w, wl, w8, bits);
            if (r < 0)
                return read_past;
            if (r == 0)
                break;
            m |= uint64_t(bits) << j;
        }
#endif
        for (; j < cnt; ++j) {
            int r = str_pass<Op>(c, ld<uint32_t>(p + 8 * j), ld<uint32_t>(p + 8 * j + 4), w, wl, w8);
            if (r < 0)
                return read_past;
            m |= uint64_t(r) << j;
        }
        if (!emit(c, k, cnt, m, s))
            return write_past;
    }
    return ok;
}

static status filter_str(const col& c, int op, const uint8_t* w, size_t wl, sink& s) {
    switch (op) {
        case op_eq: return filter_str_op<op_eq>(c, w, wl, s);
        case op_ne: return filter_str_op<op_ne>(c, w, wl, s);
        case op_lt: return filter_str_op<op_lt>(c, w, wl, s);
        case op_le: return filter_str_op<op_le>(c, w, wl, s);
        case op_gt: return filter_str_op<op_gt>(c, w, wl, s);
        default: return filter_str_op<op_ge>(c, w, wl, s);
    }
}

// filterIn with a set of at most 128 codes; larger sets take the plain loop
// in vec_luau.cpp
static status filter_in(int kind, const col& c, const uint8_t* bits128, uint32_t size, sink& s) {
    if (kind == k_i8)
        return run_filter<1>(c, s, [bits128](const uint8_t* p) { return in_i8_block(p, bits128); });
    return run_filter<4>(c, s, [bits128, size](const uint8_t* p) { return in_i32_block(p, bits128, size); });
}

static status mask(const uint8_t* m, size_t n, sink& s) {
    col c{m, n, 0, 1, nullptr, n};
    return run_filter<1>(c, s, [](const uint8_t* p) { return nonzero_block(p); });
}

// fold into one group. Integer sums wrap in uint64_t: the result is exact
// whenever the true total fits in int64, whatever order the lanes add in.
template <class T, bool HasNone>
static inline void fold_ints(const uint8_t* p, size_t cnt, T none, int_acc<T>& a) {
    uint64_t count = 0, sum = 0;
    T lo = a.lo, hi = a.hi;
    for (size_t j = 0; j < cnt; ++j) {
        T x = ld<T>(p + j * sizeof(T));
        bool ok = !HasNone || x != none;
        count += ok;
        sum += ok ? uint64_t(int64_t(x)) : 0;
        T xl = ok ? x : std::numeric_limits<T>::max();
        T xh = ok ? x : std::numeric_limits<T>::min();
        lo = xl < lo ? xl : lo;
        hi = xh > hi ? xh : hi;
    }
    a.count += count;
    a.sum += sum;
    a.lo = lo;
    a.hi = hi;
}

// eight independent lanes, so the adds and compares vectorize without
// reassociating anything; NaN is nil. min(x, lo) and max(x, hi) answer their
// second operand when x is NaN or ties, which is `x < lo ? x : lo` exactly.
static inline void fold_f64_lanes(const uint8_t* p, size_t cnt, f64_acc& a) {
    for (size_t j = 0; j < cnt; ++j) {
        size_t l = j & 7;
        double x = ld<double>(p + 8 * j);
        bool ok = x == x;
        a.count[l] += ok;
        a.sum[l] += ok ? x : 0.0;
        a.lo[l] = x < a.lo[l] ? x : a.lo[l];
        a.hi[l] = x > a.hi[l] ? x : a.hi[l];
    }
}

static inline void fold_f64s(const uint8_t* p, size_t cnt, f64_acc& a) {
#if VEC_ISA == 2
    // two sets of eight lanes, so each add waits on the one two loads back
    __m512d sum0 = _mm512_loadu_pd(a.sum), sum1 = _mm512_setzero_pd();
    __m512d lo0 = _mm512_loadu_pd(a.lo), lo1 = _mm512_set1_pd(HUGE_VAL);
    __m512d hi0 = _mm512_loadu_pd(a.hi), hi1 = _mm512_set1_pd(-HUGE_VAL);
    uint64_t count = 0;
    size_t j = 0;
    for (; j + 16 <= cnt; j += 16) {
        __m512d x0 = _mm512_loadu_pd(reinterpret_cast<const double*>(p + 8 * j));
        __m512d x1 = _mm512_loadu_pd(reinterpret_cast<const double*>(p + 8 * j + 64));
        __mmask8 ok0 = _mm512_cmp_pd_mask(x0, x0, _CMP_ORD_Q), ok1 = _mm512_cmp_pd_mask(x1, x1, _CMP_ORD_Q);
        count += unsigned(__builtin_popcount(ok0)) + unsigned(__builtin_popcount(ok1));
        sum0 = _mm512_mask_add_pd(sum0, ok0, sum0, x0);
        sum1 = _mm512_mask_add_pd(sum1, ok1, sum1, x1);
        lo0 = _mm512_min_pd(x0, lo0);
        lo1 = _mm512_min_pd(x1, lo1);
        hi0 = _mm512_max_pd(x0, hi0);
        hi1 = _mm512_max_pd(x1, hi1);
    }
    _mm512_storeu_pd(a.sum, _mm512_add_pd(sum0, sum1));
    _mm512_storeu_pd(a.lo, _mm512_min_pd(lo1, lo0));
    _mm512_storeu_pd(a.hi, _mm512_max_pd(hi1, hi0));
    a.count[0] += count;
    fold_f64_lanes(p + 8 * j, cnt - j, a);
#elif VEC_ISA == 1
    __m256d sum0 = _mm256_loadu_pd(a.sum), sum1 = _mm256_loadu_pd(a.sum + 4);
    __m256d lo0 = _mm256_loadu_pd(a.lo), lo1 = _mm256_loadu_pd(a.lo + 4);
    __m256d hi0 = _mm256_loadu_pd(a.hi), hi1 = _mm256_loadu_pd(a.hi + 4);
    uint64_t count = 0;
    size_t j = 0;
    for (; j + 8 <= cnt; j += 8) {
        __m256d x0 = _mm256_loadu_pd(reinterpret_cast<const double*>(p + 8 * j));
        __m256d x1 = _mm256_loadu_pd(reinterpret_cast<const double*>(p + 8 * j + 32));
        __m256d ok0 = _mm256_cmp_pd(x0, x0, _CMP_ORD_Q), ok1 = _mm256_cmp_pd(x1, x1, _CMP_ORD_Q);
        count += unsigned(__builtin_popcount(_mm256_movemask_pd(ok0) | (_mm256_movemask_pd(ok1) << 4)));
        sum0 = _mm256_add_pd(sum0, _mm256_and_pd(x0, ok0));
        sum1 = _mm256_add_pd(sum1, _mm256_and_pd(x1, ok1));
        lo0 = _mm256_min_pd(x0, lo0);
        lo1 = _mm256_min_pd(x1, lo1);
        hi0 = _mm256_max_pd(x0, hi0);
        hi1 = _mm256_max_pd(x1, hi1);
    }
    _mm256_storeu_pd(a.sum, sum0);
    _mm256_storeu_pd(a.sum + 4, sum1);
    _mm256_storeu_pd(a.lo, lo0);
    _mm256_storeu_pd(a.lo + 4, lo1);
    _mm256_storeu_pd(a.hi, hi0);
    _mm256_storeu_pd(a.hi + 4, hi1);
    a.count[0] += count;
    fold_f64_lanes(p + 8 * j, cnt - j, a);
#else
    fold_f64_lanes(p, cnt, a);
#endif
}

template <class T, bool HasNone>
static void fold_int_col(const col& c, T none, int_acc<T>& a) {
    if (!c.sel && c.stride == sizeof(T)) {
        fold_ints<T, HasNone>(c.b + c.off, c.n, none, a);
        return;
    }
    alignas(64) uint8_t tmp[B * sizeof(T)];
    for (size_t k = 0; k < c.n; k += B) {
        size_t cnt = c.n - k < B ? c.n - k : B;
        fold_ints<T, HasNone>(fetch<sizeof(T)>(c, k, cnt, tmp), cnt, none, a);
    }
}

static void fold_f64_col(const col& c, f64_acc& a) {
    if (!c.sel && c.stride == 8) {
        fold_f64s(c.b + c.off, c.n, a);
        return;
    }
    alignas(64) uint8_t tmp[B * 8];
    for (size_t k = 0; k < c.n; k += B) {
        size_t cnt = c.n - k < B ? c.n - k : B;
        fold_f64s(fetch<8>(c, k, cnt, tmp), cnt, a);
    }
}

static uint64_t count_str_col(const col& c) {
    uint64_t count = 0;
    if (!c.sel) {
        const uint8_t* base = c.b + c.off + 4;
        for (size_t k = 0; k < c.n; ++k)
            count += ld<uint32_t>(base + k * c.stride) != 0;
    } else {
        for (size_t k = 0; k < c.n; ++k)
            count += ld<uint32_t>(c.b + c.off + size_t(ld<uint32_t>(c.sel + 4 * k)) * c.stride + 4) != 0;
    }
    return count;
}

// Adds the candidates to the one record at rec: count, sum, least, greatest
static void fold1(int kind, const col& c, const nil_of& none, uint8_t* rec) {
    switch (kind) {
        case k_i8: {
            int_acc<int8_t> a;
            if (none.has) fold_int_col<int8_t, true>(c, int8_t(none.v), a);
            else fold_int_col<int8_t, false>(c, 0, a);
            a.into(rec);
            return;
        }
        case k_i32: {
            int_acc<int32_t> a;
            if (none.has) fold_int_col<int32_t, true>(c, int32_t(none.v), a);
            else fold_int_col<int32_t, false>(c, 0, a);
            a.into(rec);
            return;
        }
        case k_i64: {
            int_acc<int64_t> a;
            if (none.has) fold_int_col<int64_t, true>(c, none.v, a);
            else fold_int_col<int64_t, false>(c, 0, a);
            a.into(rec);
            return;
        }
        case k_f64: {
            f64_acc a;
            fold_f64_col(c, a);
            a.into(rec);
            return;
        }
        default:
            add_f64(rec, 0, double(count_str_col(c)));
            return;
    }
}

static const kernels table = {filter_num, filter_str, filter_in, mask, fold1};

} // namespace VEC_NS
