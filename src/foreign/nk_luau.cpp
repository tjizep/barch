// NumKong scalars, vectors and matrices for stored Luau. Scalars and vectors
// are f64/f32/f16/bf16: construct from Lua numbers, + - * / compare, and
// 1-based indexing. Matrices are the same four dtypes row-major with
// matmul through the unpacked C reference kernel. Kernels, GPU, and extra
// dtypes stay for a later cut.

#include "nk_luau.h"

#ifdef BARCH_HAS_LUAU

#include "lua.h"
#include "lualib.h"

// Runtime dispatch: the linked numkong_dispatch library probes the CPU at run
// time and picks the fastest kernel (serial fallback included), rather than
// the most advanced one the build host could compile. Needs NK_DYNAMIC_DISPATCH
// on both sides of the header - the C library and this translation unit.
#ifndef NK_DYNAMIC_DISPATCH
#define NK_DYNAMIC_DISPATCH 1
#endif
#if !NK_DYNAMIC_DISPATCH
#undef NK_DYNAMIC_DISPATCH
#define NK_DYNAMIC_DISPATCH 1
#endif
#include <numkong/types.hpp>
#include <numkong/vector.hpp>
#include <numkong/dot.hpp>
#include <numkong/spatial.hpp>
#include <numkong/reduce.hpp>
#include <numkong/dots.hpp>
#include <numkong/matrix.hpp>

#include <cstdio>
#include <cstring>
#include <new>
#include <string>
#include <utility>

namespace nk = ashvardanian::numkong;

namespace {

enum class nk_kind : int { f64, f32, f16, bf16 };

const char* scalar_meta(nk_kind k) {
    switch (k) {
        case nk_kind::f64: return "nk.f64";
        case nk_kind::f32: return "nk.f32";
        case nk_kind::f16: return "nk.f16";
        case nk_kind::bf16: return "nk.bf16";
    }
    return "nk.f32";
}

const char* vector_meta(nk_kind k) {
    switch (k) {
        case nk_kind::f64: return "nk.vector.f64";
        case nk_kind::f32: return "nk.vector.f32";
        case nk_kind::f16: return "nk.vector.f16";
        case nk_kind::bf16: return "nk.vector.bf16";
    }
    return "nk.vector.f32";
}

const char* matrix_meta(nk_kind k) {
    switch (k) {
        case nk_kind::f64: return "nk.matrix.f64";
        case nk_kind::f32: return "nk.matrix.f32";
        case nk_kind::f16: return "nk.matrix.f16";
        case nk_kind::bf16: return "nk.matrix.bf16";
    }
    return "nk.matrix.f32";
}

template<typename T>
struct scalar_ud {
    T v;
};

template<typename T>
struct vector_ud {
    nk::vector<T> v;
};

// Row-major rows x cols. Kept as a flat nk::vector rather than nk::matrix:
// the GEMM entry point takes raw pointers plus extents and strides, and the
// vector already owns contiguous storage with try_zeros for the failure path.
template<typename T>
struct matrix_ud {
    nk::vector<T> v;
    std::size_t rows = 0;
    std::size_t cols = 0;
};

template<typename T>
T from_double(double x);

template<>
nk::f64_t from_double<nk::f64_t>(double x) {
    return nk::f64_t{x};
}
template<>
nk::f32_t from_double<nk::f32_t>(double x) {
    return nk::f32_t{static_cast<float>(x)};
}
template<>
nk::f16_t from_double<nk::f16_t>(double x) {
    return nk::f16_t{static_cast<float>(x)};
}
template<>
nk::bf16_t from_double<nk::bf16_t>(double x) {
    return nk::bf16_t{static_cast<float>(x)};
}

template<typename T>
double to_double(T v) {
    return static_cast<double>(v);
}

void* test_udata(lua_State* L, int idx, const char* tname) {
    if (lua_type(L, idx) != LUA_TUSERDATA)
        return nullptr;
    lua_getmetatable(L, idx);
    luaL_getmetatable(L, tname);
    int ok = lua_rawequal(L, -1, -2);
    lua_pop(L, 2);
    return ok ? lua_touserdata(L, idx) : nullptr;
}

template<typename T>
T* check_scalar(lua_State* L, int idx, nk_kind k) {
    auto* p = static_cast<scalar_ud<T>*>(luaL_checkudata(L, idx, scalar_meta(k)));
    return &p->v;
}

template<typename T>
vector_ud<T>* check_vector(lua_State* L, int idx, nk_kind k) {
    return static_cast<vector_ud<T>*>(luaL_checkudata(L, idx, vector_meta(k)));
}

template<typename T>
matrix_ud<T>* check_matrix(lua_State* L, int idx, nk_kind k) {
    return static_cast<matrix_ud<T>*>(luaL_checkudata(L, idx, matrix_meta(k)));
}

template<typename T>
void push_scalar(lua_State* L, T v, nk_kind k) {
    auto* p = static_cast<scalar_ud<T>*>(lua_newuserdatadtor(L, sizeof(scalar_ud<T>),
        [](void* u) { static_cast<scalar_ud<T>*>(u)->~scalar_ud<T>(); }));
    new (p) scalar_ud<T>{v};
    luaL_getmetatable(L, scalar_meta(k));
    lua_setmetatable(L, -2);
}

template<typename T>
void push_vector(lua_State* L, nk::vector<T>&& v, nk_kind k) {
    auto* p = static_cast<vector_ud<T>*>(lua_newuserdatadtor(L, sizeof(vector_ud<T>),
        [](void* u) { static_cast<vector_ud<T>*>(u)->~vector_ud<T>(); }));
    new (p) vector_ud<T>{std::move(v)};
    luaL_getmetatable(L, vector_meta(k));
    lua_setmetatable(L, -2);
}

template<typename T>
void push_matrix(lua_State* L, nk::vector<T>&& v, std::size_t rows, std::size_t cols, nk_kind k) {
    auto* p = static_cast<matrix_ud<T>*>(lua_newuserdatadtor(L, sizeof(matrix_ud<T>),
        [](void* u) { static_cast<matrix_ud<T>*>(u)->~matrix_ud<T>(); }));
    new (p) matrix_ud<T>{std::move(v), rows, cols};
    luaL_getmetatable(L, matrix_meta(k));
    lua_setmetatable(L, -2);
}

bool try_scalar_double(lua_State* L, int idx, double* out) {
    if (lua_isnumber(L, idx)) {
        *out = lua_tonumber(L, idx);
        return true;
    }
    if (auto* p = test_udata(L, idx, scalar_meta(nk_kind::f64))) {
        *out = to_double(static_cast<scalar_ud<nk::f64_t>*>(p)->v);
        return true;
    }
    if (auto* p = test_udata(L, idx, scalar_meta(nk_kind::f32))) {
        *out = to_double(static_cast<scalar_ud<nk::f32_t>*>(p)->v);
        return true;
    }
    if (auto* p = test_udata(L, idx, scalar_meta(nk_kind::f16))) {
        *out = to_double(static_cast<scalar_ud<nk::f16_t>*>(p)->v);
        return true;
    }
    if (auto* p = test_udata(L, idx, scalar_meta(nk_kind::bf16))) {
        *out = to_double(static_cast<scalar_ud<nk::bf16_t>*>(p)->v);
        return true;
    }
    return false;
}

template<typename T>
T as_scalar(lua_State* L, int idx, nk_kind k) {
    double d;
    if (try_scalar_double(L, idx, &d))
        return from_double<T>(d);
    return *check_scalar<T>(L, idx, k);
}

template<typename Dst>
nk::vector<Dst> vector_from_src(lua_State* L, auto const& src) {
    auto n = src.size();
    auto out = nk::vector<Dst>::try_zeros(n);
    if (n && out.empty())
        luaL_error(L, "nk vector: out of memory");
    for (std::size_t i = 0; i < n; ++i)
        out[i] = from_double<Dst>(to_double(src[i]));
    return out;
}

template<typename Dst>
nk::vector<Dst> vector_from_any(lua_State* L, int idx) {
    if (auto* p = test_udata(L, idx, vector_meta(nk_kind::f64)))
        return vector_from_src<Dst>(L, static_cast<vector_ud<nk::f64_t>*>(p)->v);
    if (auto* p = test_udata(L, idx, vector_meta(nk_kind::f32)))
        return vector_from_src<Dst>(L, static_cast<vector_ud<nk::f32_t>*>(p)->v);
    if (auto* p = test_udata(L, idx, vector_meta(nk_kind::f16)))
        return vector_from_src<Dst>(L, static_cast<vector_ud<nk::f16_t>*>(p)->v);
    if (auto* p = test_udata(L, idx, vector_meta(nk_kind::bf16)))
        return vector_from_src<Dst>(L, static_cast<vector_ud<nk::bf16_t>*>(p)->v);
    luaL_error(L, "expected nk vector");
    return {};
}

bool is_nk_vector(lua_State* L, int idx) {
    return test_udata(L, idx, vector_meta(nk_kind::f64))
        || test_udata(L, idx, vector_meta(nk_kind::f32))
        || test_udata(L, idx, vector_meta(nk_kind::f16))
        || test_udata(L, idx, vector_meta(nk_kind::bf16));
}

template<typename T>
int scalar_new(lua_State* L, nk_kind k) {
    // nk.f32(x) is a table __call, so arg 1 is the type table.
    // nkf32(x) is a C function, so arg 1 is the value.
    int i = 1;
    if (lua_istable(L, 1))
        i = 2;
    T v{};
    if (lua_gettop(L) >= i && !lua_isnoneornil(L, i))
        v = as_scalar<T>(L, i, k);
    push_scalar<T>(L, v, k);
    return 1;
}

template<typename T>
int scalar_add(lua_State* L, nk_kind k) {
    push_scalar<T>(L, as_scalar<T>(L, 1, k) + as_scalar<T>(L, 2, k), k);
    return 1;
}
template<typename T>
int scalar_sub(lua_State* L, nk_kind k) {
    push_scalar<T>(L, as_scalar<T>(L, 1, k) - as_scalar<T>(L, 2, k), k);
    return 1;
}
template<typename T>
int scalar_mul(lua_State* L, nk_kind k) {
    push_scalar<T>(L, as_scalar<T>(L, 1, k) * as_scalar<T>(L, 2, k), k);
    return 1;
}
template<typename T>
int scalar_div(lua_State* L, nk_kind k) {
    push_scalar<T>(L, as_scalar<T>(L, 1, k) / as_scalar<T>(L, 2, k), k);
    return 1;
}
template<typename T>
int scalar_unm(lua_State* L, nk_kind k) {
    push_scalar<T>(L, -(*check_scalar<T>(L, 1, k)), k);
    return 1;
}
template<typename T>
int scalar_eq(lua_State* L, nk_kind k) {
    lua_pushboolean(L, as_scalar<T>(L, 1, k) == as_scalar<T>(L, 2, k));
    return 1;
}
template<typename T>
int scalar_lt(lua_State* L, nk_kind k) {
    lua_pushboolean(L, as_scalar<T>(L, 1, k) < as_scalar<T>(L, 2, k));
    return 1;
}
template<typename T>
int scalar_le(lua_State* L, nk_kind k) {
    lua_pushboolean(L, as_scalar<T>(L, 1, k) <= as_scalar<T>(L, 2, k));
    return 1;
}
template<typename T>
int scalar_tostring(lua_State* L, nk_kind k) {
    char buf[64];
    std::snprintf(buf, sizeof buf, "%.9g", to_double(*check_scalar<T>(L, 1, k)));
    lua_pushstring(L, buf);
    return 1;
}
template<typename T>
int scalar_tonumber(lua_State* L, nk_kind k) {
    lua_pushnumber(L, to_double(*check_scalar<T>(L, 1, k)));
    return 1;
}
template<typename T>
int scalar_index(lua_State* L, nk_kind k) {
    const char* m = luaL_checkstring(L, 2);
    if (std::strcmp(m, "tonumber") == 0) {
        lua_pushinteger(L, (int) k);
        lua_pushcclosure(L, [](lua_State* L2) {
            auto kk = static_cast<nk_kind>(lua_tointeger(L2, lua_upvalueindex(1)));
            return scalar_tonumber<T>(L2, kk);
        }, "tonumber", 1);
        return 1;
    }
    lua_pushnil(L);
    return 1;
}

template<typename T>
nk::vector<T> vector_from_table(lua_State* L, int idx, nk_kind k) {
    int n = (int) lua_objlen(L, idx);
    auto v = nk::vector<T>::try_zeros((std::size_t) n);
    if (n > 0 && v.empty())
        luaL_error(L, "nk vector: out of memory");
    for (int i = 1; i <= n; ++i) {
        lua_rawgeti(L, idx, i);
        v[(std::size_t) (i - 1)] = as_scalar<T>(L, -1, k);
        lua_pop(L, 1);
    }
    return v;
}

template<typename T>
nk::vector<T> vector_from_bytes(lua_State* L, void const* bytes, std::size_t nbytes) {
    constexpr std::size_t es = sizeof(typename T::raw_t);
    if (nbytes % es != 0)
        luaL_error(L, "nk vector: buffer size is not a multiple of the element size");
    std::size_t n = nbytes / es;
    auto v = nk::vector<T>::try_zeros(n);
    if (n && v.empty())
        luaL_error(L, "nk vector: out of memory");
    auto const* raw = static_cast<typename T::raw_t const*>(bytes);
    for (std::size_t i = 0; i < n; ++i)
        v[i] = T::from_raw(raw[i]);
    return v;
}

template<typename T>
int vector_to_buffer(lua_State* L, nk_kind k) {
    auto* u = check_vector<T>(L, 1, k);
    std::size_t n = u->v.size();
    std::size_t nbytes = n * sizeof(typename T::raw_t);
    auto* out = static_cast<typename T::raw_t*>(lua_newbuffer(L, nbytes));
    for (std::size_t i = 0; i < n; ++i)
        out[i] = u->v[i].raw_;
    return 1;
}

template<typename T>
int vector_new(lua_State* L, nk_kind k) {
    int top = lua_gettop(L);
    if (top == 0) {
        push_vector<T>(L, nk::vector<T>::try_zeros(0), k);
        return 1;
    }
    if (top == 1 && lua_istable(L, 1)) {
        push_vector<T>(L, vector_from_table<T>(L, 1, k), k);
        return 1;
    }
    if (top == 1 && is_nk_vector(L, 1)) {
        push_vector<T>(L, vector_from_any<T>(L, 1), k);
        return 1;
    }
    size_t nbytes = 0;
    if (top == 1) {
        if (void* b = lua_tobuffer(L, 1, &nbytes)) {
            push_vector<T>(L, vector_from_bytes<T>(L, b, nbytes), k);
            return 1;
        }
        // A string is its bytes, whatever those bytes happen to look like.
        // The test is on the type, not `lua_isstring && !lua_isnumber`: Luau
        // calls a string that converts a number, and the conversion stops at
        // an embedded NUL - so an f32 buffer starting `33 00` ('3', NUL) read
        // as the number 3 and came back as a three element zero vector
        // instead of the 64 values it was handed. About one random f32 buffer
        // in a hundred starts that way, which is enough to quietly wreck a
        // loaded index. See TODO 395.
        if (lua_type(L, 1) == LUA_TSTRING) {
            size_t n = 0;
            const char* s = lua_tolstring(L, 1, &n);
            push_vector<T>(L, vector_from_bytes<T>(L, s, n), k);
            return 1;
        }
    }
    if (top == 1 && lua_type(L, 1) == LUA_TNUMBER) {
        double n = lua_tonumber(L, 1);
        if (n < 0 || n != (double) (int) n)
            luaL_error(L, "nk vector length must be a non-negative integer");
        auto v = nk::vector<T>::try_zeros((std::size_t) n);
        if (n > 0 && v.empty())
            luaL_error(L, "nk vector: out of memory");
        push_vector<T>(L, std::move(v), k);
        return 1;
    }
    auto v = nk::vector<T>::try_zeros((std::size_t) top);
    if (top > 0 && v.empty())
        luaL_error(L, "nk vector: out of memory");
    for (int i = 1; i <= top; ++i)
        v[(std::size_t) (i - 1)] = as_scalar<T>(L, i, k);
    push_vector<T>(L, std::move(v), k);
    return 1;
}

template<typename T>
int vector_len(lua_State* L, nk_kind k) {
    lua_pushnumber(L, (double) check_vector<T>(L, 1, k)->v.size());
    return 1;
}

// 1-based inclusive, like Luau / string.sub. Out of range is clamped, not
// an error. `v:slice(1, 5)` on a length-3 vector is the whole thing.
template<typename T>
int vector_slice(lua_State* L, nk_kind k) {
    auto* u = check_vector<T>(L, 1, k);
    int n = (int) u->v.size();
    int start = luaL_optinteger(L, 2, 1);
    int stop = luaL_optinteger(L, 3, -1);
    if (start < 0)
        start = n + start + 1;
    if (stop < 0)
        stop = n + stop + 1;
    if (start < 1)
        start = 1;
    if (stop > n)
        stop = n;
    int len = (start <= stop) ? (stop - start + 1) : 0;
    auto out = nk::vector<T>::try_zeros((std::size_t) len);
    if (len > 0 && out.empty())
        luaL_error(L, "nk vector: out of memory");
    for (int i = 0; i < len; ++i)
        out[(std::size_t) i] = u->v[(std::size_t) (start - 1 + i)];
    push_vector<T>(L, std::move(out), k);
    return 1;
}

template<typename T>
int vector_metric(lua_State* L, nk_kind k, auto&& fn) {
    auto* a = check_vector<T>(L, 1, k);
    auto* b = check_vector<T>(L, 2, k);
    if (a->v.size() != b->v.size())
        luaL_error(L, "nk vector size mismatch");
    lua_pushnumber(L, fn(a->v.values_data(), b->v.values_data(), a->v.size()));
    return 1;
}

template<typename T>
int vector_dot(lua_State* L, nk_kind k) {
    return vector_metric<T>(L, k, [](T const* a, T const* b, std::size_t n) {
        typename T::dot_result_t r{};
        nk::dot(a, b, n, &r);
        return to_double(r);
    });
}

template<typename T>
int vector_euclidean(lua_State* L, nk_kind k) {
    return vector_metric<T>(L, k, [](T const* a, T const* b, std::size_t n) {
        typename T::euclidean_result_t r{};
        nk::euclidean(a, b, n, &r);
        return to_double(r);
    });
}

// NumKong angular: cosine *distance* 1 − ⟨a,b⟩ / (‖a‖‖b‖), not similarity.
template<typename T>
int vector_cosine(lua_State* L, nk_kind k) {
    return vector_metric<T>(L, k, [](T const* a, T const* b, std::size_t n) {
        typename T::angular_result_t r{};
        nk::angular(a, b, n, &r);
        return to_double(r);
    });
}

template<typename T>
double vector_sum_value(T const* a, std::size_t n) {
    typename T::reduce_moments_sum_t sum{};
    typename T::reduce_moments_sumsq_t sumsq{};
    nk::reduce_moments(a, n, sizeof(T), &sum, &sumsq);
    return to_double(sum);
}

template<typename T>
int vector_sum(lua_State* L, nk_kind k) {
    auto* u = check_vector<T>(L, 1, k);
    lua_pushnumber(L, vector_sum_value(u->v.values_data(), u->v.size()));
    return 1;
}

template<typename T>
int vector_average(lua_State* L, nk_kind k) {
    auto* u = check_vector<T>(L, 1, k);
    auto n = u->v.size();
    if (n == 0) {
        lua_pushnumber(L, 0);
        return 1;
    }
    lua_pushnumber(L, vector_sum_value(u->v.values_data(), n) / (double) n);
    return 1;
}

template<typename T>
int vector_index(lua_State* L, nk_kind k) {
    auto* u = check_vector<T>(L, 1, k);
    if (lua_isnumber(L, 2)) {
        int i = lua_tointeger(L, 2);
        if (i < 1 || (std::size_t) i > u->v.size())
            luaL_error(L, "nk vector index out of range");
        push_scalar<T>(L, u->v[(std::size_t) (i - 1)], k);
        return 1;
    }
    const char* m = luaL_checkstring(L, 2);
    if (std::strcmp(m, "size") == 0 || std::strcmp(m, "len") == 0) {
        lua_pushnumber(L, (double) u->v.size());
        return 1;
    }
    if (std::strcmp(m, "slice") == 0) {
        lua_pushinteger(L, (int) k);
        lua_pushcclosure(L, [](lua_State* L2) {
            auto kk = static_cast<nk_kind>(lua_tointeger(L2, lua_upvalueindex(1)));
            return vector_slice<T>(L2, kk);
        }, "slice", 1);
        return 1;
    }
    if (std::strcmp(m, "buffer") == 0) {
        lua_pushinteger(L, (int) k);
        lua_pushcclosure(L, [](lua_State* L2) {
            auto kk = static_cast<nk_kind>(lua_tointeger(L2, lua_upvalueindex(1)));
            return vector_to_buffer<T>(L2, kk);
        }, "buffer", 1);
        return 1;
    }
    if (std::strcmp(m, "dot") == 0) {
        lua_pushinteger(L, (int) k);
        lua_pushcclosure(L, [](lua_State* L2) {
            auto kk = static_cast<nk_kind>(lua_tointeger(L2, lua_upvalueindex(1)));
            return vector_dot<T>(L2, kk);
        }, "dot", 1);
        return 1;
    }
    if (std::strcmp(m, "euclidean") == 0 || std::strcmp(m, "l2") == 0) {
        lua_pushinteger(L, (int) k);
        lua_pushcclosure(L, [](lua_State* L2) {
            auto kk = static_cast<nk_kind>(lua_tointeger(L2, lua_upvalueindex(1)));
            return vector_euclidean<T>(L2, kk);
        }, "euclidean", 1);
        return 1;
    }
    if (std::strcmp(m, "cosine") == 0 || std::strcmp(m, "angular") == 0) {
        lua_pushinteger(L, (int) k);
        lua_pushcclosure(L, [](lua_State* L2) {
            auto kk = static_cast<nk_kind>(lua_tointeger(L2, lua_upvalueindex(1)));
            return vector_cosine<T>(L2, kk);
        }, "cosine", 1);
        return 1;
    }
    if (std::strcmp(m, "sum") == 0) {
        lua_pushinteger(L, (int) k);
        lua_pushcclosure(L, [](lua_State* L2) {
            auto kk = static_cast<nk_kind>(lua_tointeger(L2, lua_upvalueindex(1)));
            return vector_sum<T>(L2, kk);
        }, "sum", 1);
        return 1;
    }
    if (std::strcmp(m, "average") == 0 || std::strcmp(m, "mean") == 0) {
        lua_pushinteger(L, (int) k);
        lua_pushcclosure(L, [](lua_State* L2) {
            auto kk = static_cast<nk_kind>(lua_tointeger(L2, lua_upvalueindex(1)));
            return vector_average<T>(L2, kk);
        }, "average", 1);
        return 1;
    }
    lua_pushnil(L);
    return 1;
}

template<typename T>
int vector_newindex(lua_State* L, nk_kind k) {
    auto* u = check_vector<T>(L, 1, k);
    int i = luaL_checkinteger(L, 2);
    if (i < 1 || (std::size_t) i > u->v.size())
        luaL_error(L, "nk vector index out of range");
    u->v[(std::size_t) (i - 1)] = as_scalar<T>(L, 3, k);
    return 0;
}

template<typename T>
int vector_add(lua_State* L, nk_kind k) {
    auto* a = check_vector<T>(L, 1, k);
    auto* b = check_vector<T>(L, 2, k);
    if (a->v.size() != b->v.size())
        luaL_error(L, "nk vector size mismatch");
    auto out = nk::vector<T>::try_zeros(a->v.size());
    if (a->v.size() && out.empty())
        luaL_error(L, "nk vector: out of memory");
    for (std::size_t i = 0; i < a->v.size(); ++i)
        out[i] = a->v[i] + b->v[i];
    push_vector<T>(L, std::move(out), k);
    return 1;
}
template<typename T>
int vector_sub(lua_State* L, nk_kind k) {
    auto* a = check_vector<T>(L, 1, k);
    auto* b = check_vector<T>(L, 2, k);
    if (a->v.size() != b->v.size())
        luaL_error(L, "nk vector size mismatch");
    auto out = nk::vector<T>::try_zeros(a->v.size());
    if (a->v.size() && out.empty())
        luaL_error(L, "nk vector: out of memory");
    for (std::size_t i = 0; i < a->v.size(); ++i)
        out[i] = a->v[i] - b->v[i];
    push_vector<T>(L, std::move(out), k);
    return 1;
}
template<typename T>
int vector_mul(lua_State* L, nk_kind k) {
    // vec * vec, vec * scalar, scalar * vec
    bool a_vec = test_udata(L, 1, vector_meta(k)) != nullptr;
    bool b_vec = test_udata(L, 2, vector_meta(k)) != nullptr;
    if (a_vec && b_vec) {
        auto* a = check_vector<T>(L, 1, k);
        auto* b = check_vector<T>(L, 2, k);
        if (a->v.size() != b->v.size())
            luaL_error(L, "nk vector size mismatch");
        auto out = nk::vector<T>::try_zeros(a->v.size());
        if (a->v.size() && out.empty())
            luaL_error(L, "nk vector: out of memory");
        for (std::size_t i = 0; i < a->v.size(); ++i)
            out[i] = a->v[i] * b->v[i];
        push_vector<T>(L, std::move(out), k);
        return 1;
    }
    auto* v = a_vec ? check_vector<T>(L, 1, k) : check_vector<T>(L, 2, k);
    T s = as_scalar<T>(L, a_vec ? 2 : 1, k);
    auto out = nk::vector<T>::try_zeros(v->v.size());
    if (v->v.size() && out.empty())
        luaL_error(L, "nk vector: out of memory");
    for (std::size_t i = 0; i < v->v.size(); ++i)
        out[i] = v->v[i] * s;
    push_vector<T>(L, std::move(out), k);
    return 1;
}
template<typename T>
int vector_unm(lua_State* L, nk_kind k) {
    auto* a = check_vector<T>(L, 1, k);
    auto out = nk::vector<T>::try_zeros(a->v.size());
    if (a->v.size() && out.empty())
        luaL_error(L, "nk vector: out of memory");
    for (std::size_t i = 0; i < a->v.size(); ++i)
        out[i] = -a->v[i];
    push_vector<T>(L, std::move(out), k);
    return 1;
}
template<typename T>
int vector_eq(lua_State* L, nk_kind k) {
    auto* a = check_vector<T>(L, 1, k);
    auto* b = check_vector<T>(L, 2, k);
    if (a->v.size() != b->v.size()) {
        lua_pushboolean(L, 0);
        return 1;
    }
    for (std::size_t i = 0; i < a->v.size(); ++i) {
        if (!(a->v[i] == b->v[i])) {
            lua_pushboolean(L, 0);
            return 1;
        }
    }
    lua_pushboolean(L, 1);
    return 1;
}
template<typename T>
int vector_tostring(lua_State* L, nk_kind k) {
    auto* u = check_vector<T>(L, 1, k);
    std::string s = "nk.vector.";
    s += (k == nk_kind::f64 ? "f64" : k == nk_kind::f32 ? "f32" : k == nk_kind::f16 ? "f16" : "bf16");
    s += "{";
    for (std::size_t i = 0; i < u->v.size(); ++i) {
        if (i)
            s += ", ";
        char buf[32];
        std::snprintf(buf, sizeof buf, "%.6g", to_double(u->v[i]));
        s += buf;
    }
    s += "}";
    lua_pushlstring(L, s.data(), s.size());
    return 1;
}

// NK_BIND takes the kind as a template argument because a lua_CFunction is a
// plain function pointer: the lambdas cannot capture, so the kind has to be
// baked into the type. Matrix method dispatch follows the vector shape below
// - __index matches the name and pushes a fresh kind-bound closure.
#define NK_BIND(fn, T, K) \
    [](lua_State* L) { return fn<T>(L, K); }
#define NK_MBIND(fn, T, K) \
    [](lua_State* L) { return fn<T, K>(L); }

// --- matrices ---------------------------------------------------------------
// C = A x B-transpose, row-major throughout: c[i][j] is the dot of A row i
// with B row j. That transpose on B is the kernel's contract, not a quirk of
// the binding - it matches BLAS NoTrans x Trans and the reference role
// upstream gives dots_unpacked. Callers wanting C = A x B pass B already
// transposed. Same dtype both sides; the accumulator is the dtype's own
// dot_result_t (f64 stays f64, the rest widen to f64/f32 per types.hpp).
//
// The multiply below packs B once and runs dots_packed, the SIMD GEMM: the
// packed layout is what makes the kernel cache friendly across rows of A.
// Packing can still fail (out of memory), and then it falls back to the
// serial dots_unpacked reference - same numbers, no SIMD.
template<typename T>
int matrix_new(lua_State* L, nk_kind k) {
    int top = lua_gettop(L);
    if (top == 1 && lua_istable(L, 1)) {
        // nk.f32.matrix({{1, 2}, {3, 4}}) - nested tables, one per row.
        int rows = (int) lua_objlen(L, 1);
        if (rows == 0)
            luaL_error(L, "nk matrix: need at least one row");
        lua_rawgeti(L, 1, 1);
        if (!lua_istable(L, -1))
            luaL_error(L, "nk matrix: rows must be tables");
        int cols = (int) lua_objlen(L, -1);
        lua_pop(L, 1);
        if (cols == 0)
            luaL_error(L, "nk matrix: rows must not be empty");
        auto v = nk::vector<T>::try_zeros((std::size_t) rows * (std::size_t) cols);
        if (v.empty())
            luaL_error(L, "nk matrix: out of memory");
        for (int i = 1; i <= rows; ++i) {
            lua_rawgeti(L, 1, i);
            if (!lua_istable(L, -1))
                luaL_error(L, "nk matrix: rows must be tables");
            if ((int) lua_objlen(L, -1) != cols)
                luaL_error(L, "nk matrix: ragged rows");
            for (int j = 1; j <= cols; ++j) {
                lua_rawgeti(L, -1, j);
                v[(std::size_t) ((i - 1) * cols + (j - 1))] = as_scalar<T>(L, -1, k);
                lua_pop(L, 1);
            }
            lua_pop(L, 1);
        }
        push_matrix<T>(L, std::move(v), (std::size_t) rows, (std::size_t) cols, k);
        return 1;
    }
    if (top == 2 && lua_isnumber(L, 1) && lua_isnumber(L, 2)) {
        // nk.f32.matrix(2, 3) - rows, cols, zero filled.
        double r = lua_tonumber(L, 1), c = lua_tonumber(L, 2);
        if (r < 1 || c < 1 || r != (double) (int) r || c != (double) (int) c)
            luaL_error(L, "nk matrix dimensions must be positive integers");
        auto v = nk::vector<T>::try_zeros((std::size_t) r * (std::size_t) c);
        if (v.empty())
            luaL_error(L, "nk matrix: out of memory");
        push_matrix<T>(L, std::move(v), (std::size_t) r, (std::size_t) c, k);
        return 1;
    }
    luaL_error(L, "nk matrix: expected a table of row tables or rows, cols");
    return 0;
}

template<typename T, nk_kind K>
int matrix_rows(lua_State* L) {
    lua_pushnumber(L, (double) check_matrix<T>(L, 1, K)->rows);
    return 1;
}

template<typename T, nk_kind K>
int matrix_cols(lua_State* L) {
    lua_pushnumber(L, (double) check_matrix<T>(L, 1, K)->cols);
    return 1;
}

template<typename T, nk_kind K>
int matrix_get(lua_State* L) {
    auto* u = check_matrix<T>(L, 1, K);
    int i = luaL_checkinteger(L, 2);
    int j = luaL_checkinteger(L, 3);
    if (i < 1 || j < 1 || (std::size_t) i > u->rows || (std::size_t) j > u->cols)
        luaL_error(L, "nk matrix index out of range");
    // a plain Lua number, not an nk scalar: scalars answer tostring() but stay
    // opaque to the RESP reply encoder, so get() would hand back nil.
    lua_pushnumber(L, to_double(u->v[(std::size_t) ((i - 1) * (int) u->cols + (j - 1))]));
    return 1;
}

template<typename T, nk_kind K>
int matrix_set(lua_State* L) {
    auto* u = check_matrix<T>(L, 1, K);
    int i = luaL_checkinteger(L, 2);
    int j = luaL_checkinteger(L, 3);
    if (i < 1 || j < 1 || (std::size_t) i > u->rows || (std::size_t) j > u->cols)
        luaL_error(L, "nk matrix index out of range");
    u->v[(std::size_t) ((i - 1) * (int) u->cols + (j - 1))] = as_scalar<T>(L, 4, K);
    return 0;
}

template<typename T, nk_kind K>
int matrix_matmul(lua_State* L) {
    auto* a = check_matrix<T>(L, 1, K);
    auto* b = check_matrix<T>(L, 2, K);
    if (a->cols != b->cols)
        luaL_error(L, "nk matrix size mismatch: A is %d wide, B is %d wide",
            (int) a->cols, (int) b->cols);
    using R = typename T::dot_result_t;
    auto out = nk::vector<R>::try_zeros(a->rows * b->rows);
    if (a->rows * b->rows && out.empty())
        luaL_error(L, "nk matrix: out of memory");
    std::size_t a_stride = sizeof(T) * a->cols; // rows are contiguous
    std::size_t c_stride = sizeof(R) * b->rows;
    // A views the live storage; B is copied so the packer sees one
    // contiguous row-major matrix even though matrix_ud already is one -
    // try_pack takes a view, and the view over the copy outlives the call.
    auto bc = nk::matrix<T>::try_zeros({b->rows, b->cols});
    if (b->rows * b->cols && bc.empty())
        luaL_error(L, "nk matrix: out of memory");
    std::memcpy(bc.data(), b->v.values_data(), b->rows * b->cols * sizeof(T));
    auto packed = nk::packed_matrix<T>::try_pack(bc.as_matrix_view());
    if (!packed.empty()) {
        nk::dots_packed(a->v.values_data(), packed.data(), out.values_data(),
            a->rows, b->rows, a->cols, a_stride, c_stride);
    }
    else {
        std::size_t b_stride = sizeof(T) * b->cols;
        nk::dots_unpacked(a->v.values_data(), b->v.values_data(), out.values_data(),
            a->rows, b->rows, a->cols, a_stride, b_stride, c_stride);
    }
    auto flat = nk::vector<T>::try_zeros(a->rows * b->rows);
    if (a->rows * b->rows && flat.empty())
        luaL_error(L, "nk matrix: out of memory");
    for (std::size_t i = 0; i < a->rows * b->rows; ++i)
        flat[i] = from_double<T>(to_double(out[i]));
    push_matrix<T>(L, std::move(flat), a->rows, b->rows, K);
    return 1;
}

template<typename T, nk_kind K>
int matrix_index(lua_State* L) {
    const char* m = luaL_checkstring(L, 2);
    if (std::strcmp(m, "rows") == 0) {
        lua_pushcfunction(L, NK_MBIND(matrix_rows, T, K), "rows");
        return 1;
    }
    if (std::strcmp(m, "cols") == 0) {
        lua_pushcfunction(L, NK_MBIND(matrix_cols, T, K), "cols");
        return 1;
    }
    if (std::strcmp(m, "get") == 0) {
        lua_pushcfunction(L, NK_MBIND(matrix_get, T, K), "get");
        return 1;
    }
    if (std::strcmp(m, "set") == 0) {
        lua_pushcfunction(L, NK_MBIND(matrix_set, T, K), "set");
        return 1;
    }
    if (std::strcmp(m, "matmul") == 0) {
        lua_pushcfunction(L, NK_MBIND(matrix_matmul, T, K), "matmul");
        return 1;
    }
    lua_pushnil(L);
    return 1;
}

template<typename T, nk_kind K>
int matrix_tostring(lua_State* L) {
    auto* u = check_matrix<T>(L, 1, K);
    std::string s = "nk.matrix.";
    s += (K == nk_kind::f64 ? "f64" : K == nk_kind::f32 ? "f32" : K == nk_kind::f16 ? "f16" : "bf16");
    s += "{";
    for (std::size_t i = 0; i < u->rows; ++i) {
        if (i)
            s += "; ";
        s += "{";
        for (std::size_t j = 0; j < u->cols; ++j) {
            if (j)
                s += ", ";
            char buf[32];
            std::snprintf(buf, sizeof buf, "%.6g", to_double(u->v[i * u->cols + j]));
            s += buf;
        }
        s += "}";
    }
    s += "}";
    lua_pushlstring(L, s.data(), s.size());
    return 1;
}

template<typename T, nk_kind K>
int matrix_len(lua_State* L) {
    lua_pushnumber(L, (double) check_matrix<T>(L, 1, K)->rows);
    return 1;
}

template<typename T, nk_kind K>
void make_scalar_meta(lua_State* L) {
    luaL_newmetatable(L, scalar_meta(K));
    lua_pushcfunction(L, NK_BIND(scalar_add, T, K), "__add");
    lua_setfield(L, -2, "__add");
    lua_pushcfunction(L, NK_BIND(scalar_sub, T, K), "__sub");
    lua_setfield(L, -2, "__sub");
    lua_pushcfunction(L, NK_BIND(scalar_mul, T, K), "__mul");
    lua_setfield(L, -2, "__mul");
    lua_pushcfunction(L, NK_BIND(scalar_div, T, K), "__div");
    lua_setfield(L, -2, "__div");
    lua_pushcfunction(L, NK_BIND(scalar_unm, T, K), "__unm");
    lua_setfield(L, -2, "__unm");
    lua_pushcfunction(L, NK_BIND(scalar_eq, T, K), "__eq");
    lua_setfield(L, -2, "__eq");
    lua_pushcfunction(L, NK_BIND(scalar_lt, T, K), "__lt");
    lua_setfield(L, -2, "__lt");
    lua_pushcfunction(L, NK_BIND(scalar_le, T, K), "__le");
    lua_setfield(L, -2, "__le");
    lua_pushcfunction(L, NK_BIND(scalar_tostring, T, K), "__tostring");
    lua_setfield(L, -2, "__tostring");
    lua_pushcfunction(L, NK_BIND(scalar_index, T, K), "__index");
    lua_setfield(L, -2, "__index");
    lua_setreadonly(L, -1, true);
    lua_pop(L, 1);
}

template<typename T, nk_kind K>
void make_vector_meta(lua_State* L) {
    luaL_newmetatable(L, vector_meta(K));
    lua_pushcfunction(L, NK_BIND(vector_add, T, K), "__add");
    lua_setfield(L, -2, "__add");
    lua_pushcfunction(L, NK_BIND(vector_sub, T, K), "__sub");
    lua_setfield(L, -2, "__sub");
    lua_pushcfunction(L, NK_BIND(vector_mul, T, K), "__mul");
    lua_setfield(L, -2, "__mul");
    lua_pushcfunction(L, NK_BIND(vector_unm, T, K), "__unm");
    lua_setfield(L, -2, "__unm");
    lua_pushcfunction(L, NK_BIND(vector_eq, T, K), "__eq");
    lua_setfield(L, -2, "__eq");
    lua_pushcfunction(L, NK_BIND(vector_len, T, K), "__len");
    lua_setfield(L, -2, "__len");
    lua_pushcfunction(L, NK_BIND(vector_index, T, K), "__index");
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, NK_BIND(vector_newindex, T, K), "__newindex");
    lua_setfield(L, -2, "__newindex");
    lua_pushcfunction(L, NK_BIND(vector_tostring, T, K), "__tostring");
    lua_setfield(L, -2, "__tostring");
    lua_setreadonly(L, -1, true);
    lua_pop(L, 1);
}

template<typename T, nk_kind K>
void make_matrix_meta(lua_State* L) {
    luaL_newmetatable(L, matrix_meta(K));
    lua_pushcfunction(L, NK_MBIND(matrix_len, T, K), "__len");
    lua_setfield(L, -2, "__len");
    lua_pushcfunction(L, NK_MBIND(matrix_index, T, K), "__index");
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, NK_MBIND(matrix_tostring, T, K), "__tostring");
    lua_setfield(L, -2, "__tostring");
    lua_setreadonly(L, -1, true);
    lua_pop(L, 1);
}

void push_type_table(lua_State* L, lua_CFunction scalar_ctor, lua_CFunction vector_ctor,
        lua_CFunction matrix_ctor) {
    lua_newtable(L);
    lua_pushcfunction(L, vector_ctor, "vector");
    lua_setfield(L, -2, "vector");
    lua_pushcfunction(L, matrix_ctor, "matrix");
    lua_setfield(L, -2, "matrix");
    lua_newtable(L);
    lua_pushcfunction(L, scalar_ctor, "__call");
    lua_setfield(L, -2, "__call");
    lua_setreadonly(L, -1, true);
    lua_setmetatable(L, -2);
}

} // namespace

void luaopen_nk(lua_State* L) {
    make_scalar_meta<nk::f64_t, nk_kind::f64>(L);
    make_scalar_meta<nk::f32_t, nk_kind::f32>(L);
    make_scalar_meta<nk::f16_t, nk_kind::f16>(L);
    make_scalar_meta<nk::bf16_t, nk_kind::bf16>(L);
    make_vector_meta<nk::f64_t, nk_kind::f64>(L);
    make_vector_meta<nk::f32_t, nk_kind::f32>(L);
    make_vector_meta<nk::f16_t, nk_kind::f16>(L);
    make_vector_meta<nk::bf16_t, nk_kind::bf16>(L);
    make_matrix_meta<nk::f64_t, nk_kind::f64>(L);
    make_matrix_meta<nk::f32_t, nk_kind::f32>(L);
    make_matrix_meta<nk::f16_t, nk_kind::f16>(L);
    make_matrix_meta<nk::bf16_t, nk_kind::bf16>(L);

    auto cf64 = NK_BIND(scalar_new, nk::f64_t, nk_kind::f64);
    auto cf32 = NK_BIND(scalar_new, nk::f32_t, nk_kind::f32);
    auto cf16 = NK_BIND(scalar_new, nk::f16_t, nk_kind::f16);
    auto cbf16 = NK_BIND(scalar_new, nk::bf16_t, nk_kind::bf16);
    auto vf64 = NK_BIND(vector_new, nk::f64_t, nk_kind::f64);
    auto vf32 = NK_BIND(vector_new, nk::f32_t, nk_kind::f32);
    auto vf16 = NK_BIND(vector_new, nk::f16_t, nk_kind::f16);
    auto vbf16 = NK_BIND(vector_new, nk::bf16_t, nk_kind::bf16);
    auto mf64 = NK_BIND(matrix_new, nk::f64_t, nk_kind::f64);
    auto mf32 = NK_BIND(matrix_new, nk::f32_t, nk_kind::f32);
    auto mf16 = NK_BIND(matrix_new, nk::f16_t, nk_kind::f16);
    auto mbf16 = NK_BIND(matrix_new, nk::bf16_t, nk_kind::bf16);

    lua_newtable(L); // nk
    push_type_table(L, cf64, vf64, mf64);
    lua_setfield(L, -2, "f64");
    push_type_table(L, cf32, vf32, mf32);
    lua_setfield(L, -2, "f32");
    push_type_table(L, cf16, vf16, mf16);
    lua_setfield(L, -2, "f16");
    push_type_table(L, cbf16, vbf16, mbf16);
    lua_setfield(L, -2, "bf16");

    lua_newtable(L); // nk.vector
    lua_pushcfunction(L, vf64, "f64");
    lua_setfield(L, -2, "f64");
    lua_pushcfunction(L, vf32, "f32");
    lua_setfield(L, -2, "f32");
    lua_pushcfunction(L, vf16, "f16");
    lua_setfield(L, -2, "f16");
    lua_pushcfunction(L, vbf16, "bf16");
    lua_setfield(L, -2, "bf16");
    lua_setfield(L, -2, "vector");

    lua_newtable(L); // nk.matrix
    lua_pushcfunction(L, mf64, "f64");
    lua_setfield(L, -2, "f64");
    lua_pushcfunction(L, mf32, "f32");
    lua_setfield(L, -2, "f32");
    lua_pushcfunction(L, mf16, "f16");
    lua_setfield(L, -2, "f16");
    lua_pushcfunction(L, mbf16, "bf16");
    lua_setfield(L, -2, "bf16");
    lua_setfield(L, -2, "matrix");
    lua_setglobal(L, "nk");

    lua_pushcfunction(L, cf64, "nkf64");
    lua_setglobal(L, "nkf64");
    lua_pushcfunction(L, cf32, "nkf32");
    lua_setglobal(L, "nkf32");
    lua_pushcfunction(L, cf16, "nkf16");
    lua_setglobal(L, "nkf16");
    lua_pushcfunction(L, cbf16, "nkbf16");
    lua_setglobal(L, "nkbf16");
    lua_pushcfunction(L, vf64, "nkf64vector");
    lua_setglobal(L, "nkf64vector");
    lua_pushcfunction(L, vf32, "nkf32vector");
    lua_setglobal(L, "nkf32vector");
    lua_pushcfunction(L, vf16, "nkf16vector");
    lua_setglobal(L, "nkf16vector");
    lua_pushcfunction(L, vbf16, "nkbf16vector");
    lua_setglobal(L, "nkbf16vector");
    lua_pushcfunction(L, mf64, "nkf64matrix");
    lua_setglobal(L, "nkf64matrix");
    lua_pushcfunction(L, mf32, "nkf32matrix");
    lua_setglobal(L, "nkf32matrix");
    lua_pushcfunction(L, mf16, "nkf16matrix");
    lua_setglobal(L, "nkf16matrix");
    lua_pushcfunction(L, mbf16, "nkbf16matrix");
    lua_setglobal(L, "nkbf16matrix");
}

#else

void luaopen_nk(lua_State*) {}

#endif
