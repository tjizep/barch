// NumKong scalars and vectors for stored Luau. First cut is f64/f32/f16/bf16
// and nk::vector of those: construct from Lua numbers, + - * / compare, and
// 1-based indexing. Kernels, GPU, and extra dtypes stay for a later cut.

#include "nk_luau.h"

#ifdef BARCH_HAS_LUAU

#include "lua.h"
#include "lualib.h"

// Header-only serial path. Do not enable their shared-library dispatch here.
#ifndef NK_DYNAMIC_DISPATCH
#define NK_DYNAMIC_DISPATCH 0
#endif
#include <numkong/types.hpp>
#include <numkong/vector.hpp>
#include <numkong/dot.hpp>
#include <numkong/spatial.hpp>
#include <numkong/reduce.hpp>

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

template<typename T>
struct scalar_ud {
    T v;
};

template<typename T>
struct vector_ud {
    nk::vector<T> v;
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
        if (lua_isstring(L, 1) && !lua_isnumber(L, 1)) {
            size_t n = 0;
            const char* s = lua_tolstring(L, 1, &n);
            push_vector<T>(L, vector_from_bytes<T>(L, s, n), k);
            return 1;
        }
    }
    if (top == 1 && lua_isnumber(L, 1)) {
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

// lua_CFunction is a function pointer, so these lambdas cannot capture.
// Kind is a template argument so the empty-capture lambda is valid.
#define NK_BIND(fn, T, K) \
    [](lua_State* L) { return fn<T>(L, K); }

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

void push_type_table(lua_State* L, lua_CFunction scalar_ctor, lua_CFunction vector_ctor) {
    lua_newtable(L);
    lua_pushcfunction(L, vector_ctor, "vector");
    lua_setfield(L, -2, "vector");
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

    auto cf64 = NK_BIND(scalar_new, nk::f64_t, nk_kind::f64);
    auto cf32 = NK_BIND(scalar_new, nk::f32_t, nk_kind::f32);
    auto cf16 = NK_BIND(scalar_new, nk::f16_t, nk_kind::f16);
    auto cbf16 = NK_BIND(scalar_new, nk::bf16_t, nk_kind::bf16);
    auto vf64 = NK_BIND(vector_new, nk::f64_t, nk_kind::f64);
    auto vf32 = NK_BIND(vector_new, nk::f32_t, nk_kind::f32);
    auto vf16 = NK_BIND(vector_new, nk::f16_t, nk_kind::f16);
    auto vbf16 = NK_BIND(vector_new, nk::bf16_t, nk_kind::bf16);

    lua_newtable(L); // nk
    push_type_table(L, cf64, vf64);
    lua_setfield(L, -2, "f64");
    push_type_table(L, cf32, vf32);
    lua_setfield(L, -2, "f32");
    push_type_table(L, cf16, vf16);
    lua_setfield(L, -2, "f16");
    push_type_table(L, cbf16, vbf16);
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
}

#else

void luaopen_nk(lua_State*) {}

#endif
