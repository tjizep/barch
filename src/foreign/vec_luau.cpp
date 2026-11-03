// barch.vec: native kernels that filter, group and fold typed values held in
// Luau buffers, for lent's column scans. lent's lent/vec.luau is the
// reference for every result and error, and barchex's docs/barch-vec.md the
// request. Every function is pure: it reads the buffers it's given and
// writes only the output buffers, so it's safe in any space and inside or
// outside a transaction.
//
// The kernels live in vec_kernels.inc, compiled once per instruction set
// (plain C++, AVX2, AVX-512). The first call picks the best one the CPU has;
// BARCH_VEC_ISA=scalar|avx2|avx512 asks for a lower one, which is how the
// tests cover every copy on one machine. `barch.vec.isa` says which is in use.

#include "vec_luau.h"

#include "lua.h"
#include "lualib.h"

#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <type_traits>

#if (defined(__x86_64__) || defined(_M_X64)) && defined(__GNUC__)
#define VEC_X86 1
#include <immintrin.h>
#else
#define VEC_X86 0
#endif

namespace barch_vec {

// candidates per block: one uint64_t of pass bits
constexpr size_t B = 64;

enum kind_t : int { k_i8 = 1, k_i32, k_i64, k_f64, k_str };
enum op_t : int { op_eq = 1, op_ne, op_lt, op_le, op_gt, op_ge };
enum status : int { ok = 0, read_past, write_past };

const char* const kind_name[] = {"", "i8", "i32", "i64", "f64", "str"};
const size_t kind_width[] = {0, 1, 4, 8, 8, 8};

// a column: element i at b + off + i * stride, candidates sel[0..n) or 0..n-1
struct col {
    const uint8_t* b;
    size_t blen;
    size_t off;
    size_t stride;
    const uint8_t* sel; // u32 indexes, or null for dense
    size_t n;
};

// where kept indexes go; cap counts u32s
struct sink {
    uint32_t* out;
    size_t cap;
    size_t kept;
};

// An integer filter turned into bounds: lo <= x <= hi passes, a nil element
// (x == none, when has_none) fails, and `ne` inverts both for ~=.
struct irange {
    int64_t lo, hi, none;
    bool has_none, ne;
};

// the nil of an integer column
struct nil_of {
    bool has;
    int64_t v;
};

template <class T>
__attribute__((always_inline)) inline T ld(const uint8_t* p) {
    T v;
    memcpy(&v, p, sizeof v);
    return v;
}

template <class T>
__attribute__((always_inline)) inline void st(uint8_t* p, T v) {
    memcpy(p, &v, sizeof v);
}

inline void add_f64(uint8_t* rec, size_t at, double v) {
    st(rec + at, ld<double>(rec + at) + v);
}

// the len (at most 8) bytes at p as a little-endian number, without reading
// past them or calling memcpy: two overlapping 4-byte loads, or three bytes
inline uint64_t load_short(const uint8_t* p, size_t len) {
    if (len >= 4)
        return uint64_t(ld<uint32_t>(p)) | (uint64_t(ld<uint32_t>(p + len - 4)) << (8 * (len - 4)));
    if (len == 0)
        return 0;
    return uint64_t(p[0]) | (uint64_t(p[len >> 1]) << (8 * (len >> 1))) | (uint64_t(p[len - 1]) << (8 * (len - 1)));
}

// The first min(len, 8) bytes of the string at b[start], with zeros above
// them. That's a single 8-byte load unless the string ends within 8 bytes of
// the end of the buffer.
inline uint64_t str_head(const uint8_t* b, size_t blen, uint32_t start, uint32_t len) {
    if (size_t(start) + 8 <= blen) {
        uint64_t v = ld<uint64_t>(b + start);
        return len >= 8 ? v : v & ((uint64_t(1) << (8 * len)) - 1);
    }
    return load_short(b + start, len < 8 ? len : 8);
}

// memcmp order for two strings longer than 8 bytes whose first 8 match:
// -1, 0 or 1, with the shorter first when one is a prefix of the other
__attribute__((noinline, cold)) static int cmp_tail(const uint8_t* a, size_t al, const uint8_t* b, size_t bl) {
    int r = memcmp(a + 8, b + 8, (al < bl ? al : bl) - 8);
    if (r)
        return r < 0 ? -1 : 1;
    return (al > bl) - (al < bl);
}

inline uint32_t sel_at(const col& c, size_t k) {
    return c.sel ? ld<uint32_t>(c.sel + 4 * k) : uint32_t(k);
}

// a record's least and greatest move only on a strict compare, as in the
// reference, so -0 and 0 keep whichever came first
inline void into_record(uint8_t* rec, double count, double sum, double lo, double hi) {
    add_f64(rec, 0, count);
    add_f64(rec, 8, sum);
    if (lo < ld<double>(rec + 16))
        st(rec + 16, lo);
    if (hi > ld<double>(rec + 24))
        st(rec + 24, hi);
}

template <class T>
struct int_acc {
    uint64_t count = 0, sum = 0;
    T lo = std::numeric_limits<T>::max(), hi = std::numeric_limits<T>::min();
    void into(uint8_t* rec) const {
        if (count)
            into_record(rec, double(count), double(int64_t(sum)), double(lo), double(hi));
    }
};

struct f64_acc {
    uint64_t count[8] = {};
    double sum[8] = {};
    double lo[8], hi[8];
    f64_acc() {
        for (int l = 0; l < 8; ++l) {
            lo[l] = HUGE_VAL;
            hi[l] = -HUGE_VAL;
        }
    }
    void into(uint8_t* rec) const {
        uint64_t c = 0;
        double s = 0, l = HUGE_VAL, h = -HUGE_VAL;
        for (int i = 0; i < 8; ++i) {
            c += count[i];
            s += sum[i];
            l = lo[i] < l ? lo[i] : l;
            h = hi[i] > h ? hi[i] : h;
        }
        if (c)
            into_record(rec, double(c), s, l, h);
    }
};

// what each instruction set's copy of vec_kernels.inc provides
struct kernels {
    status (*filter_num)(int kind, const col& c, const irange& r, double w, int op, sink& s);
    status (*filter_str)(const col& c, int op, const uint8_t* w, size_t wl, sink& s);
    status (*filter_in)(int kind, const col& c, const uint8_t* bits128, uint32_t size, sink& s);
    status (*mask)(const uint8_t* m, size_t n, sink& s);
    void (*fold1)(int kind, const col& c, const nil_of& none, uint8_t* rec);
};

// AVX2 compress: for each 8-bit mask, the lanes to gather to the front
struct compress_table {
    alignas(32) uint32_t lanes[256][8];
    constexpr compress_table() : lanes{} {
        for (unsigned m = 0; m < 256; ++m) {
            unsigned k = 0;
            for (unsigned j = 0; j < 8; ++j)
                if (m & (1u << j))
                    lanes[m][k++] = j;
            while (k < 8)
                lanes[m][k++] = 0;
        }
    }
};
constexpr compress_table compress_lanes{};
#define compress8 compress_lanes.lanes

} // namespace barch_vec

// one copy of the kernels per instruction set --------------------------------

namespace barch_vec {
#define VEC_ISA 0
#define VEC_NS isa_plain
#include "vec_kernels.inc"
#undef VEC_NS
#undef VEC_ISA
} // namespace barch_vec

#if VEC_X86
#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("avx2,bmi,bmi2,popcnt"))), apply_to = function)
#else
#pragma GCC push_options
#pragma GCC target("avx2,bmi,bmi2,popcnt")
#endif
namespace barch_vec {
#define VEC_ISA 1
#define VEC_NS isa_avx2
#include "vec_kernels.inc"
#undef VEC_NS
#undef VEC_ISA
} // namespace barch_vec
#if defined(__clang__)
#pragma clang attribute pop
#pragma clang attribute push(__attribute__((target("avx512f,avx512bw,avx512vl,avx512dq,avx2,bmi,bmi2,popcnt"))), apply_to = function)
#else
#pragma GCC pop_options
#pragma GCC push_options
#pragma GCC target("avx512f,avx512bw,avx512vl,avx512dq,avx2,bmi,bmi2,popcnt")
#endif
namespace barch_vec {
#define VEC_ISA 2
#define VEC_NS isa_avx512
#include "vec_kernels.inc"
#undef VEC_NS
#undef VEC_ISA
} // namespace barch_vec
#if defined(__clang__)
#pragma clang attribute pop
#else
#pragma GCC pop_options
#endif
#endif

namespace barch_vec {
namespace {

struct chosen {
    const kernels* k;
    const char* name;
};

chosen pick() {
    int best = 0;
#if VEC_X86
    __builtin_cpu_init();
    if (__builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512bw") &&
        __builtin_cpu_supports("avx512vl") && __builtin_cpu_supports("avx512dq") &&
        __builtin_cpu_supports("bmi2") && __builtin_cpu_supports("popcnt"))
        best = 2;
    else if (__builtin_cpu_supports("avx2") && __builtin_cpu_supports("bmi2") &&
             __builtin_cpu_supports("popcnt"))
        best = 1;
#endif
    int use = best;
    if (const char* want = std::getenv("BARCH_VEC_ISA")) {
        if (strcmp(want, "scalar") == 0)
            use = 0;
        else if (strcmp(want, "avx2") == 0 && best >= 1)
            use = 1;
    }
#if VEC_X86
    if (use == 2)
        return {&isa_avx512::table, "avx512"};
    if (use == 1)
        return {&isa_avx2::table, "avx2"};
#endif
    return {&isa_plain::table, "scalar"};
}

const chosen& isa() {
    static const chosen c = pick();
    return c;
}

// argument checks --------------------------------------------------------

using i128 = __int128;

[[noreturn]] void fail(lua_State* L, const char* fn, const char* msg) {
    lua_pushfstring(L, "vec.%s: %s", fn, msg);
    lua_error(L);
}

// tostring(v), as the reference puts it into a message
const char* shown(lua_State* L, int idx) {
    return luaL_tolstring(L, idx, nullptr);
}

uint8_t* buf(lua_State* L, int idx, size_t* len) {
    if (lua_type(L, idx) != LUA_TBUFFER)
        return nullptr;
    return static_cast<uint8_t*>(lua_tobuffer(L, idx, len));
}

// a whole number from `least` to 2^53
size_t whole(lua_State* L, const char* fn, int idx, const char* name, int least) {
    if (lua_type(L, idx) == LUA_TNUMBER) {
        double v = lua_tonumber(L, idx);
        if (v == std::floor(v) && v >= least && v <= 9007199254740992.0)
            return size_t(v);
    }
    lua_pushfstring(L, "vec.%s: %s must be a whole number from %d", fn, name, least);
    lua_error(L);
}

int kind_of(lua_State* L, const char* fn, int idx) {
    if (lua_type(L, idx) == LUA_TSTRING) {
        const char* s = lua_tostring(L, idx);
        for (int k = k_i8; k <= k_str; ++k)
            if (strcmp(s, kind_name[k]) == 0)
                return k;
    }
    lua_pushfstring(L, "vec.%s: unknown kind %s", fn, shown(L, idx));
    lua_error(L);
}

uint32_t max_u32(const uint8_t* p, size_t n) {
    uint32_t m = 0;
    for (size_t k = 0; k < n; ++k) {
        uint32_t v = ld<uint32_t>(p + 4 * k);
        m = v > m ? v : m;
    }
    return m;
}

// The reference's column(): the buffer, off, stride, n and sel, and that every
// candidate's element lies in b. For a sel this takes its largest index first.
col column(lua_State* L, const char* fn, int kind, int ib, int ioff, int istride, int isel, int in) {
    col c{};
    c.b = buf(L, ib, &c.blen);
    if (!c.b)
        fail(L, fn, "takes a buffer of elements");
    c.off = whole(L, fn, ioff, "off", 0);
    c.stride = whole(L, fn, istride, "stride", 1);
    c.n = whole(L, fn, in, "n", 0);
    size_t sellen = 0;
    if (!lua_isnil(L, isel)) {
        c.sel = buf(L, isel, &sellen);
        if (!c.sel)
            fail(L, fn, "takes a buffer of u32 indexes or nil for sel");
        if (i128(c.n) * 4 > i128(sellen))
            fail(L, fn, "reads past the end of a buffer");
    }
    if (c.n > 0) {
        size_t last = c.sel ? max_u32(c.sel, c.n) : c.n - 1;
        if (i128(c.off) + i128(last) * i128(c.stride) + i128(kind_width[kind]) > i128(c.blen))
            fail(L, fn, "reads past the end of a buffer");
    }
    return c;
}

uint32_t* output(lua_State* L, const char* fn, int idx, size_t* len) {
    auto* out = buf(L, idx, len);
    if (!out)
        fail(L, fn, "takes a buffer for its results");
    return reinterpret_cast<uint32_t*>(out);
}

// An integer kind's nil: `none` when it's a number one of the kind's values
// can equal, otherwise there's none.
nil_of nil_arg(lua_State* L, int idx, int kind) {
    if (kind == k_f64 || kind == k_str || lua_type(L, idx) != LUA_TNUMBER)
        return {false, 0};
    double v = lua_tonumber(L, idx);
    double lo = kind == k_i8 ? -128.0 : kind == k_i32 ? -2147483648.0 : -9223372036854775808.0;
    double hi = kind == k_i8 ? 127.0 : kind == k_i32 ? 2147483647.0 : 9223372036854775807.0;
    if (v != std::floor(v) || v < lo || (kind == k_i64 ? v >= hi : v > hi))
        return {false, 0};
    return {true, int64_t(v)};
}

void kind_range(int kind, int64_t& lo, int64_t& hi) {
    if (kind == k_i8) {
        lo = INT8_MIN;
        hi = INT8_MAX;
    } else if (kind == k_i32) {
        lo = INT32_MIN;
        hi = INT32_MAX;
    } else {
        lo = INT64_MIN;
        hi = INT64_MAX;
    }
}

// `x op w` as integer bounds over the kind's range: false when no value of the
// kind passes. w is never NaN; it may be fractional or infinite.
bool int_bounds(int kind, int op, double w, int64_t& lo, int64_t& hi) {
    auto whole_part = [](double d) -> i128 {
        if (d > 1e30)
            return i128(1) << 100;
        if (d < -1e30)
            return -(i128(1) << 100);
        return i128(d);
    };
    int64_t kmin, kmax;
    kind_range(kind, kmin, kmax);
    i128 l = kmin, h = kmax;
    double fl = std::floor(w), ce = std::ceil(w);
    switch (op) {
        case op_eq:
        case op_ne:
            if (fl != w)
                return false;
            l = h = whole_part(w);
            break;
        case op_lt: h = whole_part(ce) - 1; break;
        case op_le: h = whole_part(fl); break;
        case op_gt: l = whole_part(fl) + 1; break;
        default: l = whole_part(ce); break;
    }
    if (l < kmin)
        l = kmin;
    if (h > kmax)
        h = kmax;
    if (l > h)
        return false;
    lo = int64_t(l);
    hi = int64_t(h);
    return true;
}

int op_of(lua_State* L, int idx) {
    if (lua_type(L, idx) == LUA_TSTRING) {
        size_t len;
        const char* s = lua_tolstring(L, idx, &len);
        if (len == 1) {
            if (s[0] == '=') return op_eq;
            if (s[0] == '<') return op_lt;
            if (s[0] == '>') return op_gt;
        } else if (len == 2 && s[1] == '=') {
            if (s[0] == '~') return op_ne;
            if (s[0] == '<') return op_le;
            if (s[0] == '>') return op_ge;
        }
    }
    lua_pushfstring(L, "vec.filter: unknown op %s", shown(L, idx));
    lua_error(L);
}

void check(lua_State* L, const char* fn, status st) {
    if (st == read_past)
        fail(L, fn, "reads past the end of a buffer");
    if (st == write_past)
        fail(L, fn, "writes past the end of a buffer");
}

// the functions -----------------------------------------------------------

// vec.mask(m, off, n, out) -> count
int vec_mask(lua_State* L) {
    size_t mlen, outlen;
    auto* m = buf(L, 1, &mlen);
    if (!m)
        fail(L, "mask", "takes a buffer of bytes");
    size_t off = whole(L, "mask", 2, "off", 0);
    size_t n = whole(L, "mask", 3, "n", 0);
    auto* out = output(L, "mask", 4, &outlen);
    if (i128(off) + i128(n) > i128(mlen))
        fail(L, "mask", "reads past the end of a buffer");
    sink s{out, outlen / 4, 0};
    check(L, "mask", isa().k->mask(m + off, n, s));
    lua_pushnumber(L, double(s.kept));
    return 1;
}

// vec.filter(kind, b, off, stride, op, value, none, sel, n, out) -> count
int vec_filter(lua_State* L) {
    int kind = kind_of(L, "filter", 1);
    col c = column(L, "filter", kind, 2, 3, 4, 8, 9);
    int op = op_of(L, 5);
    if (kind == k_str) {
        if (lua_type(L, 6) != LUA_TSTRING)
            fail(L, "filter", "kind str compares with a string");
    } else if (lua_type(L, 6) != LUA_TNUMBER || std::isnan(lua_tonumber(L, 6))) {
        lua_pushfstring(L, "vec.filter: kind %s compares with a number", kind_name[kind]);
        lua_error(L);
    }
    size_t outlen;
    auto* out = output(L, "filter", 10, &outlen);
    sink s{out, outlen / 4, 0};
    const kernels& k = *isa().k;
    if (kind == k_str) {
        size_t wl;
        const char* w = lua_tolstring(L, 6, &wl);
        check(L, "filter", k.filter_str(c, op, reinterpret_cast<const uint8_t*>(w), wl, s));
    } else if (kind == k_f64) {
        check(L, "filter", k.filter_num(kind, c, irange{}, lua_tonumber(L, 6), op, s));
    } else {
        nil_of none = nil_arg(L, 7, kind);
        irange r{0, 0, none.v, none.has, op == op_ne};
        if (!int_bounds(kind, op, lua_tonumber(L, 6), r.lo, r.hi)) {
            if (op != op_ne) {
                lua_pushnumber(L, 0);
                return 1;
            }
            // ~= a value the kind can't hold: every element passes, nil too
            kind_range(kind, r.lo, r.hi);
            r.has_none = r.ne = false;
        }
        check(L, "filter", k.filter_num(kind, c, r, 0, op, s));
    }
    lua_pushnumber(L, double(s.kept));
    return 1;
}

// vec.filterIn(kind, b, off, stride, set, sel, n, out) -> count
int vec_filter_in(lua_State* L) {
    int kind = kind_of(L, "filterIn", 1);
    col c = column(L, "filterIn", kind, 2, 3, 4, 6, 7);
    if (kind != k_i8 && kind != k_i32)
        fail(L, "filterIn", "takes kind i8 or i32");
    size_t setlen, outlen;
    auto* set = buf(L, 5, &setlen);
    if (!set)
        fail(L, "filterIn", "takes a buffer of bytes for set");
    auto* out = output(L, "filterIn", 8, &outlen);
    sink s{out, outlen / 4, 0};
    if (setlen > 0 && (kind == k_i8 || setlen <= 128)) {
        // codes 0 to 127 as bits: an i8 code can't go higher
        alignas(16) uint8_t bits[16] = {};
        size_t size = setlen < 128 ? setlen : 128;
        for (size_t i = 0; i < size; ++i)
            bits[i >> 3] |= uint8_t((set[i] != 0) << (i & 7));
        check(L, "filterIn", isa().k->filter_in(kind, c, bits, uint32_t(size), s));
    } else if (setlen > 0) {
        // a larger i32 set: a byte lookup per candidate
        for (size_t k = 0; k < c.n; ++k) {
            uint32_t i = sel_at(c, k);
            uint32_t code = ld<uint32_t>(c.b + c.off + size_t(i) * c.stride);
            if (code < setlen && set[code]) {
                if (s.kept == s.cap)
                    fail(L, "filterIn", "writes past the end of a buffer");
                s.out[s.kept++] = i;
            }
        }
    }
    lua_pushnumber(L, double(s.kept));
    return 1;
}

#define VEC_INLINE __attribute__((always_inline))
#define VEC_COLD __attribute__((noinline, cold))

// fold with gids: per group accumulators on the stack for up to `local_groups`
// groups, added to the records at the end; past that, straight into the
// records, where integer sums stay exact while they're within 2^53
constexpr size_t local_groups = 256;

// one group's count, sum, least and greatest, each in an array of its own: as
// a struct per group, GCC pairs the fields into vector stores that cost more
// than they save in a loop that scatters to a different group every element
template <class T, class Sum>
struct group_accs {
    uint64_t count[local_groups];
    Sum sum[local_groups];
    T lo[local_groups], hi[local_groups];
    explicit group_accs(size_t groups) {
        for (size_t g = 0; g < groups; ++g) {
            count[g] = 0;
            sum[g] = 0;
            lo[g] = std::is_floating_point_v<T> ? T(HUGE_VAL) : std::numeric_limits<T>::max();
            hi[g] = std::is_floating_point_v<T> ? T(-HUGE_VAL) : std::numeric_limits<T>::min();
        }
    }
    VEC_INLINE void add(uint32_t g, T x) {
        ++count[g];
        if constexpr (std::is_floating_point_v<T>)
            sum[g] += x;
        else
            sum[g] += uint64_t(int64_t(x));
        lo[g] = x < lo[g] ? x : lo[g];
        hi[g] = x > hi[g] ? x : hi[g];
    }
    void into(size_t groups, uint8_t* out) const {
        for (size_t g = 0; g < groups; ++g) {
            if (!count[g])
                continue;
            double total;
            if constexpr (std::is_floating_point_v<T>)
                total = sum[g];
            else
                total = double(int64_t(sum[g]));
            into_record(out + 32 * g, double(count[g]), total, double(lo[g]), double(hi[g]));
        }
    }
};

template <class T>
T element(const col& c, size_t k) {
    return ld<T>(c.b + c.off + size_t(sel_at(c, k)) * c.stride);
}

// f(k, x) for each candidate k and its element x, with the column's shape
// (packed, strided, or through sel) picked once rather than per element
template <class T, class F>
inline void each(const col& c, F&& f) {
    const uint8_t* base = c.b + c.off;
    if (c.sel) {
        for (size_t k = 0; k < c.n; ++k)
            f(k, ld<T>(base + size_t(ld<uint32_t>(c.sel + 4 * k)) * c.stride));
    } else if (c.stride == sizeof(T)) {
        for (size_t k = 0; k < c.n; ++k)
            f(k, ld<T>(base + k * sizeof(T)));
    } else {
        for (size_t k = 0; k < c.n; ++k)
            f(k, ld<T>(base + k * c.stride));
    }
}

template <class T, bool HasNone>
void fold_groups_int(const col& c, T none, const uint8_t* gids, size_t groups, uint8_t* out) {
    if (groups <= local_groups) {
        group_accs<T, uint64_t> acc(groups);
        each<T>(c, [&](size_t k, T x) VEC_INLINE {
            if (!(HasNone && x == none))
                acc.add(ld<uint32_t>(gids + 4 * k), x);
        });
        acc.into(groups, out);
        return;
    }
    each<T>(c, [&](size_t k, T x) VEC_INLINE {
        if (!(HasNone && x == none))
            into_record(out + 32 * size_t(ld<uint32_t>(gids + 4 * k)), 1, double(x), double(x), double(x));
    });
}

template <class T>
void fold_groups_int(const col& c, const nil_of& none, const uint8_t* gids, size_t groups, uint8_t* out) {
    if (none.has)
        fold_groups_int<T, true>(c, T(none.v), gids, groups, out);
    else
        fold_groups_int<T, false>(c, T(0), gids, groups, out);
}

void fold_groups_f64(const col& c, const uint8_t* gids, size_t groups, uint8_t* out) {
    if (groups <= local_groups) {
        group_accs<double, double> acc(groups);
        each<double>(c, [&](size_t k, double x) VEC_INLINE {
            if (x == x)
                acc.add(ld<uint32_t>(gids + 4 * k), x);
        });
        acc.into(groups, out);
        return;
    }
    each<double>(c, [&](size_t k, double x) VEC_INLINE {
        if (x == x)
            into_record(out + 32 * size_t(ld<uint32_t>(gids + 4 * k)), 1, x, x, x);
    });
}

void fold_groups_str(const col& c, const uint8_t* gids, size_t groups, uint8_t* out) {
    if (groups <= local_groups) {
        // counted locally: adding straight into the records makes each repeat
        // of a group wait for the store before it
        uint32_t count[local_groups] = {};
        each<uint64_t>(c, [&](size_t k, uint64_t e) VEC_INLINE {
            count[ld<uint32_t>(gids + 4 * k)] += (e >> 32) != 0;
        });
        for (size_t g = 0; g < groups; ++g)
            if (count[g])
                add_f64(out + 32 * g, 0, count[g]);
        return;
    }
    each<uint64_t>(c, [&](size_t k, uint64_t e) VEC_INLINE {
        if ((e >> 32) != 0)
            add_f64(out + 32 * size_t(ld<uint32_t>(gids + 4 * k)), 0, 1);
    });
}

// vec.fold(kind, b, off, stride, none, sel, n, gids, out)
int vec_fold(lua_State* L) {
    int kind = kind_of(L, "fold", 1);
    col c = column(L, "fold", kind, 2, 3, 4, 6, 7);
    size_t gidslen = 0;
    const uint8_t* gids = nullptr;
    if (!lua_isnil(L, 8)) {
        gids = buf(L, 8, &gidslen);
        if (!gids || i128(c.n) * 4 > i128(gidslen))
            fail(L, "fold", "takes a buffer of n u32 group ids or nil for gids");
    }
    size_t outlen;
    auto* out = reinterpret_cast<uint8_t*>(output(L, "fold", 9, &outlen));
    nil_of none = nil_arg(L, 5, kind);
    if (!gids) {
        if (outlen < 32)
            fail(L, "fold", "writes past the end of a buffer");
        if (c.n)
            isa().k->fold1(kind, c, none, out);
        return 0;
    }
    if (c.n == 0)
        return 0;
    size_t groups = size_t(max_u32(gids, c.n)) + 1;
    // str records only ever get their count written, so the last one needs 8 bytes
    if (i128(groups) * 32 - (kind == k_str ? 24 : 0) > i128(outlen))
        fail(L, "fold", "writes past the end of a buffer");
    switch (kind) {
        case k_i8: fold_groups_int<int8_t>(c, none, gids, groups, out); break;
        case k_i32: fold_groups_int<int32_t>(c, none, gids, groups, out); break;
        case k_i64: fold_groups_int<int64_t>(c, none, gids, groups, out); break;
        case k_f64: fold_groups_f64(c, gids, groups, out); break;
        default: fold_groups_str(c, gids, groups, out); break;
    }
    return 0;
}

// vec.offsets(b, off, n, start, out) -> end
int vec_offsets(lua_State* L) {
    size_t blen, outlen;
    auto* b = buf(L, 1, &blen);
    if (!b)
        fail(L, "offsets", "takes a buffer of lengths");
    size_t off = whole(L, "offsets", 2, "off", 0);
    size_t n = whole(L, "offsets", 3, "n", 0);
    uint64_t start = whole(L, "offsets", 4, "start", 0);
    auto* out = reinterpret_cast<uint8_t*>(output(L, "offsets", 5, &outlen));
    if (i128(off) + i128(n) * 4 > i128(blen))
        fail(L, "offsets", "reads past the end of a buffer");
    if (i128(n) * 8 > i128(outlen))
        fail(L, "offsets", "writes past the end of a buffer");
    for (size_t i = 0; i < n; ++i) {
        uint32_t size = ld<uint32_t>(b + off + 4 * i);
        st(out + 8 * i, uint32_t(start));
        st(out + 8 * i + 4, size);
        start += size ? size - 1 : 0;
    }
    lua_pushnumber(L, double(start));
    return 1;
}

// groupers ------------------------------------------------------------------

const char* const grouper_meta = "barch.vec.grouper";
constexpr uint32_t no_id = UINT32_MAX;

// One slot of the lookup table: the key (for str, the string's hash) next to
// its id + 1, so a probe that hits reads one cache line. 0 is an empty slot.
struct slot {
    uint64_t key;
    uint32_t id1;
    uint32_t len; // str: the string's length, so a lookup can skip the arena
};

// Numbers the distinct values of one kind from 0 in the order it first meets
// them. i8 values go through a 256-entry table; the rest through an open
// addressing table keyed by the int64 value, the f64 bits (-0 made 0), or the
// string's hash. The nil group never goes in the table. Memory comes from the
// Luau state's allocator, so it's counted against the function's memory like
// the rest of its state.
struct grouper {
    int kind;
    lua_Alloc fa;
    void* ud;
    uint32_t size = 0;
    uint32_t nil_id = no_id;
    uint32_t direct[256];
    uint64_t* keys = nullptr; // per id: the key, or for str its arena offset
    size_t kcap = 0;
    uint32_t* lens = nullptr; // str: per id
    size_t lcap = 0;
    slot* slots = nullptr;
    size_t scap = 0;
    unsigned shift = 64;
    uint8_t* arena = nullptr;
    size_t alen = 0, acap = 0;

    grouper(int k, lua_Alloc f, void* u) : kind(k), fa(f), ud(u) {
        for (auto& d : direct)
            d = no_id;
    }
    ~grouper() {
        release(keys, kcap * sizeof *keys);
        release(lens, lcap * sizeof *lens);
        release(slots, scap * sizeof *slots);
        release(arena, acap);
    }
    void release(void* p, size_t bytes) {
        if (p)
            fa(ud, p, bytes, 0);
    }
    // grows p from cap to want entries, leaving both alone when it can't
    template <class T>
    bool grow(T*& p, size_t& cap, size_t want) {
        void* q = fa(ud, p, cap * sizeof(T), want * sizeof(T));
        if (!q)
            return false;
        p = static_cast<T*>(q);
        cap = want;
        return true;
    }
};

[[noreturn]] void out_of_memory(lua_State* L) {
    fail(L, "ids", "not enough memory");
}

// Fibonacci hashing: the top bits of the product, which pick the slot, depend
// on every bit of the key. (Folding the high half into the low half first, as
// a pre-mix, cancels keys like r * 2^32 + r and piles them into a few slots.)
inline uint64_t mix(uint64_t h) {
    return h * 0x9E3779B97F4A7C15ull;
}

VEC_INLINE inline uint64_t hash_bytes(const uint8_t* p, size_t len) {
    uint64_t h = 0x243F6A8885A308D3ull ^ len;
    size_t i = 0;
    for (; i + 8 <= len; i += 8)
        h = (h ^ ld<uint64_t>(p + i)) * 0xFF51AFD7ED558CCDull, h ^= h >> 29;
    h = (h ^ load_short(p + i, len - i)) * 0xC4CEB9FE1A85EC53ull;
    return h ^ (h >> 31);
}

inline size_t home(const grouper& g, uint64_t key) {
    return size_t(mix(key) >> g.shift);
}

// room for one more id: the per-id arrays, and the table kept at most half full
VEC_COLD void make_room(lua_State* L, grouper& g) {
    size_t want = size_t(g.size) + 1;
    if (want > g.kcap && !g.grow(g.keys, g.kcap, g.kcap ? g.kcap * 2 : 16))
        out_of_memory(L);
    if (g.kind == k_str && want > g.lcap && !g.grow(g.lens, g.lcap, g.lcap ? g.lcap * 2 : 16))
        out_of_memory(L);
    if (g.kind == k_i8 || want * 2 <= g.scap)
        return;
    size_t cap = g.scap ? g.scap * 2 : 64;
    slot* fresh = nullptr;
    size_t got = 0;
    if (!g.grow(fresh, got, cap))
        out_of_memory(L);
    memset(fresh, 0, cap * sizeof *fresh);
    unsigned shift = 64 - unsigned(__builtin_ctzll(cap));
    for (size_t j = 0; j < g.scap; ++j) {
        const slot& s = g.slots[j];
        if (!s.id1)
            continue;
        size_t i = size_t(mix(s.key) >> shift);
        while (fresh[i].id1)
            i = (i + 1) & (cap - 1);
        fresh[i] = s;
    }
    g.release(g.slots, g.scap * sizeof *g.slots);
    g.slots = fresh;
    g.scap = cap;
    g.shift = shift;
}

VEC_COLD uint32_t nil_group(lua_State* L, grouper& g) {
    if (g.nil_id == no_id) {
        make_room(L, g);
        g.keys[g.size] = 0;
        g.nil_id = g.size++;
    }
    return g.nil_id;
}

// a new id for key, taking slot i
VEC_COLD uint32_t add_number(lua_State* L, grouper& g, uint64_t key) {
    make_room(L, g); // may rehash, so the slot is found again after
    size_t i = home(g, key);
    while (g.slots[i].id1)
        i = (i + 1) & (g.scap - 1);
    g.keys[g.size] = key;
    g.slots[i] = {key, g.size + 1};
    return g.size++;
}

// The lookup table's fields as locals, so the loop over a column keeps them in
// registers: it writes ids through a byte pointer, which as far as the
// compiler knows could change anything the grouper holds. Refreshed whenever
// a new key may have grown the table.
struct table_view {
    const slot* slots;
    size_t mask;
    unsigned shift;
    explicit table_view(lua_State* L, grouper& g) {
        if (!g.scap)
            make_room(L, g);
        slots = g.slots;
        mask = g.scap - 1;
        shift = g.shift;
    }
};

VEC_INLINE inline uint32_t number_group(lua_State* L, grouper& g, table_view& t, uint64_t key) {
    for (size_t i = size_t(mix(key) >> t.shift);; i = (i + 1) & t.mask) {
        const slot& s = t.slots[i];
        if (!s.id1)
            break;
        if (s.key == key)
            return s.id1 - 1;
    }
    uint32_t id = add_number(L, g, key);
    t = table_view(L, g);
    return id;
}

// A str key. A string of 8 bytes or less keys as its bytes, with its length in
// the top byte when it's shorter than 8, so slot key and length together match
// only that string and a lookup never reads the arena. A longer string keys as
// its hash and is confirmed against the arena.
VEC_INLINE inline uint64_t str_key(const uint8_t* p, uint32_t len, uint64_t head) {
    if (len < 8)
        return head | uint64_t(len) << 56;
    if (len == 8)
        return head;
    return hash_bytes(p, len);
}

VEC_COLD uint32_t add_string(lua_State* L, grouper& g, uint64_t key, const uint8_t* p, uint32_t len) {
    if (g.alen + len > g.acap) {
        size_t cap = g.acap ? g.acap * 2 : 256;
        while (cap < g.alen + len)
            cap *= 2;
        if (!g.grow(g.arena, g.acap, cap))
            out_of_memory(L);
    }
    make_room(L, g);
    size_t i = home(g, key);
    while (g.slots[i].id1)
        i = (i + 1) & (g.scap - 1);
    if (len)
        memcpy(g.arena + g.alen, p, len);
    g.keys[g.size] = g.alen;
    g.lens[g.size] = len;
    g.alen += len;
    g.slots[i] = {key, g.size + 1, len};
    return g.size++;
}

VEC_INLINE inline uint32_t string_group(lua_State* L, grouper& g, table_view& t, const uint8_t* p, uint32_t len,
                                        uint64_t head) {
    uint64_t key = str_key(p, len, head);
    for (size_t i = size_t(mix(key) >> t.shift);; i = (i + 1) & t.mask) {
        const slot& s = t.slots[i];
        if (!s.id1)
            break;
        if (s.key == key && s.len == len && (len <= 8 || memcmp(g.arena + g.keys[s.id1 - 1], p, len) == 0))
            return s.id1 - 1;
    }
    uint32_t id = add_string(L, g, key, p, len);
    t = table_view(L, g);
    return id;
}

grouper& self(lua_State* L) {
    return *static_cast<grouper*>(luaL_checkudata(L, 1, grouper_meta));
}

// vec.grouper(kind) -> g
int vec_grouper(lua_State* L) {
    int kind = kind_of(L, "grouper", 1);
    void* ud;
    lua_Alloc fa = lua_getallocf(L, &ud);
    void* mem = lua_newuserdatadtor(L, sizeof(grouper), [](void* p) { static_cast<grouper*>(p)->~grouper(); });
    new (mem) grouper(kind, fa, ud);
    luaL_getmetatable(L, grouper_meta);
    lua_setmetatable(L, -2);
    return 1;
}

VEC_COLD uint32_t add_byte(lua_State* L, grouper& g, int8_t x) {
    make_room(L, g);
    g.keys[g.size] = uint64_t(int64_t(x));
    return g.direct[uint8_t(x)] = g.size++;
}

template <class T, bool HasNone>
void ids_ints(lua_State* L, grouper& g, const col& c, T none, uint8_t* gids) {
    [[maybe_unused]] table_view t(L, g);
    each<T>(c, [&](size_t k, T x) VEC_INLINE {
        uint32_t id;
        if (HasNone && x == none) {
            id = nil_group(L, g);
            t = table_view(L, g); // making the nil group may have grown the table
        } else if constexpr (sizeof(T) == 1) {
            id = g.direct[uint8_t(x)];
            if (id == no_id)
                id = add_byte(L, g, x);
        } else {
            id = number_group(L, g, t, uint64_t(int64_t(x)));
        }
        st(gids + 4 * k, id);
    });
}

template <class T>
void ids_ints(lua_State* L, grouper& g, const col& c, const nil_of& none, uint8_t* gids) {
    if (none.has)
        ids_ints<T, true>(L, g, c, T(none.v), gids);
    else
        ids_ints<T, false>(L, g, c, T(0), gids);
}

void ids_f64(lua_State* L, grouper& g, const col& c, uint8_t* gids) {
    table_view t(L, g);
    // on the bits: NaN is nil, and -0 and 0 are one value
    each<uint64_t>(c, [&](size_t k, uint64_t bits) VEC_INLINE {
        uint32_t id;
        if ((bits << 1) > (uint64_t(0x7FF0000000000000) << 1)) {
            id = nil_group(L, g);
            t = table_view(L, g); // making the nil group may have grown the table
        } else {
            id = number_group(L, g, t, (bits << 1) == 0 ? 0 : bits);
        }
        st(gids + 4 * k, id);
    });
}

// Everything but a short string the grouper already holds: nil, a string past
// 8 bytes, a new string, and the read past the end check's failure.
VEC_COLD uint32_t str_group_slow(lua_State* L, grouper& g, const col& c, uint32_t start, uint32_t size) {
    if (size == 0)
        return nil_group(L, g);
    uint32_t len = size - 1;
    if (size_t(start) + len > c.blen)
        fail(L, "ids", "reads past the end of a buffer");
    table_view t(L, g);
    return string_group(L, g, t, c.b + start, len, str_head(c.b, c.blen, start, len));
}

// The loop keeps the table's fields in locals and looks up strings of 8 bytes
// or less inline; anything else goes to str_group_slow, after which the
// locals are loaded again since the table may have grown.
template <bool Sel>
void ids_str_loop(lua_State* L, grouper& g, const col& c, uint8_t* gids) {
    const uint8_t* b = c.b;
    const size_t blen = c.blen, stride = c.stride, n = c.n;
    const uint8_t* base = c.b + c.off;
    const uint8_t* sel = c.sel;
    table_view t(L, g);
    const slot* slots = t.slots;
    size_t mask = t.mask;
    unsigned shift = t.shift;
    for (size_t k = 0; k < n; ++k) {
        size_t row = Sel ? size_t(ld<uint32_t>(sel + 4 * k)) : k;
        uint64_t e = ld<uint64_t>(base + row * stride);
        uint32_t start = uint32_t(e), size = uint32_t(e >> 32), len = size - 1;
        uint32_t id = no_id;
        // size - 1 <= 7 also rules out nil, whose size - 1 wraps
        if (len < 8 && size_t(start) + 8 <= blen) {
            uint64_t key = (ld<uint64_t>(b + start) & ((uint64_t(1) << (8 * len)) - 1)) | uint64_t(len) << 56;
            for (size_t i = size_t(mix(key) >> shift);; i = (i + 1) & mask) {
                const slot& s = slots[i];
                if (s.key == key && s.len == len && s.id1) {
                    id = s.id1 - 1;
                    break;
                }
                if (!s.id1)
                    break;
            }
        }
        if (id == no_id) {
            id = str_group_slow(L, g, c, start, size);
            slots = g.slots, mask = g.scap - 1, shift = g.shift;
        }
        st(gids + 4 * k, id);
    }
}

void ids_str(lua_State* L, grouper& g, const col& c, uint8_t* gids) {
    if (c.sel)
        ids_str_loop<true>(L, g, c, gids);
    else
        ids_str_loop<false>(L, g, c, gids);
}

// g:ids(b, off, stride, none, sel, n, gids) -> size
int grouper_ids(lua_State* L) {
    grouper& g = self(L);
    col c = column(L, "ids", g.kind, 2, 3, 4, 6, 7);
    size_t gidslen;
    auto* gids = reinterpret_cast<uint8_t*>(output(L, "ids", 8, &gidslen));
    if (i128(c.n) * 4 > i128(gidslen))
        fail(L, "ids", "writes past the end of a buffer");
    nil_of none = nil_arg(L, 5, g.kind);
    switch (g.kind) {
        case k_i8: ids_ints<int8_t>(L, g, c, none, gids); break;
        case k_i32: ids_ints<int32_t>(L, g, c, none, gids); break;
        case k_i64: ids_ints<int64_t>(L, g, c, none, gids); break;
        case k_f64: ids_f64(L, g, c, gids); break;
        default: ids_str(L, g, c, gids); break;
    }
    lua_pushnumber(L, g.size);
    return 1;
}

// g:key(id) -> the value of group id, or nil for the nil group
int grouper_key(lua_State* L) {
    grouper& g = self(L);
    double v = lua_type(L, 2) == LUA_TNUMBER ? lua_tonumber(L, 2) : -1;
    if (!(v >= 0 && v < g.size && v == std::floor(v))) {
        lua_pushfstring(L, "vec.key: has no group %s", shown(L, 2));
        lua_error(L);
    }
    auto id = uint32_t(v);
    if (id == g.nil_id) {
        lua_pushnil(L);
    } else if (g.kind == k_str) {
        // the arena stays null while every string seen is empty
        lua_pushlstring(L, g.arena ? reinterpret_cast<const char*>(g.arena + g.keys[id]) : "", g.lens[id]);
    } else if (g.kind == k_f64) {
        lua_pushnumber(L, ld<double>(reinterpret_cast<const uint8_t*>(&g.keys[id])));
    } else {
        lua_pushnumber(L, double(int64_t(g.keys[id])));
    }
    return 1;
}

// g:size() -> how many groups
int grouper_size(lua_State* L) {
    lua_pushnumber(L, self(L).size);
    return 1;
}

} // namespace
} // namespace barch_vec

void push_vec_table(lua_State* L) {
    using namespace barch_vec;
    if (luaL_newmetatable(L, grouper_meta)) {
        lua_newtable(L);
        lua_pushcfunction(L, grouper_ids, "ids");
        lua_setfield(L, -2, "ids");
        lua_pushcfunction(L, grouper_key, "key");
        lua_setfield(L, -2, "key");
        lua_pushcfunction(L, grouper_size, "size");
        lua_setfield(L, -2, "size");
        lua_setreadonly(L, -1, true);
        lua_setfield(L, -2, "__index");
        lua_pushstring(L, "locked");
        lua_setfield(L, -2, "__metatable");
        lua_setreadonly(L, -1, true);
    }
    lua_pop(L, 1);

    lua_newtable(L);
    lua_pushcfunction(L, vec_mask, "mask");
    lua_setfield(L, -2, "mask");
    lua_pushcfunction(L, vec_filter, "filter");
    lua_setfield(L, -2, "filter");
    lua_pushcfunction(L, vec_filter_in, "filterIn");
    lua_setfield(L, -2, "filterIn");
    lua_pushcfunction(L, vec_fold, "fold");
    lua_setfield(L, -2, "fold");
    lua_pushcfunction(L, vec_grouper, "grouper");
    lua_setfield(L, -2, "grouper");
    lua_pushcfunction(L, vec_offsets, "offsets");
    lua_setfield(L, -2, "offsets");
    lua_pushstring(L, isa().name);
    lua_setfield(L, -2, "isa");
    lua_setreadonly(L, -1, true);
}

