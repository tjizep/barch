//
// Stored functions written in WebAssembly, on WAMR's fast interpreter - TODO 820.
// See wasm_driver.h and WASM-PLAN.md.
//
// A call runs on a fiber of its own, so it can stop when a slice runs out and carry
// on later on whichever pool thread picks it up, the way a Luau call yields from its
// coroutine. WAMR has no way to pause a call, so the interpreter is patched to call
// back into barch every few thousand instructions (cmake/wamr_budget_hook.cmake), and
// that callback checks the deadline and switches fibers. TODO 821.
//
#ifdef BARCH_HAS_WASM

#include "wasm_driver.h"
#include "pool.h"
#include "key_space.h"
#include "configuration.h"
#include "statistics.h"
#include "fetch_luau.h"
#include "ids.h"
#include "../function_api.h"

#include "wasm_export.h"

#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#ifdef _WIN32
#  include <windows.h>
#else
#  include <sys/mman.h>
#  include <ucontext.h>
#  include <unistd.h>
#endif

#if defined(__SANITIZE_ADDRESS__)
#  define BARCH_WASM_ASAN 1
#elif defined(__has_feature)
#  if __has_feature(address_sanitizer)
#    define BARCH_WASM_ASAN 1
#  endif
#endif
#if defined(__SANITIZE_THREAD__)
#  define BARCH_WASM_TSAN 1
#elif defined(__has_feature)
#  if __has_feature(thread_sanitizer)
#    define BARCH_WASM_TSAN 1
#  endif
#endif
#ifdef BARCH_WASM_ASAN
#  include <sanitizer/common_interface_defs.h>
#endif
#ifdef BARCH_WASM_TSAN
#  include <sanitizer/tsan_interface.h>
#endif

// set in WAMR's patched interpreter, see cmake/wamr_budget_hook.cmake
extern "C" int (*barch_wasm_budget_hook)(wasm_exec_env_t exec_env);
// the module's own function in table 0 at `elem`, or null - the same patch, TODO 823
extern "C" wasm_function_inst_t barch_wasm_table_func(wasm_module_inst_t module_inst, uint32_t elem);

namespace barch::foreign {

namespace wasm_impl {
struct wasm_call;
}

namespace {
/** what the patched interpreter calls when a call's count runs out - see below */
int budget_hook(wasm_exec_env_t env);
/** leave the call's fiber until `start`'s wake is called - see below, TODO 822 */
void park_here(wasm_impl::wasm_call& c, const std::function<void(std::function<void()>)>& start);
/** hand out the next part of the budget - see below */
int32_t grant(wasm_impl::wasm_call& c);
}

// named rather than anonymous, since wasm_sessions (declared in the header) holds
// instances from here
namespace wasm_impl {

/** the operand and frame stack each instance's exec env gets */
constexpr uint32_t exec_stack_bytes = 256 * 1024;
/** how many functions one session keeps instances for before it starts again */
constexpr size_t max_session_instances = 256;
/** handles one call may hold at once; past this the guest is leaking them */
constexpr size_t max_live_handles = 1u << 20;
/** what a native sets as the exception when the real message is in the call */
constexpr const char* host_raised = "barch host error";

/*
 * WAMR's loader and instantiation touch shared runtime state, and nothing in its docs
 * says they may run on two threads at once, so they don't - TODO 820. Calls into an
 * instance don't take this; each instance is only ever run by one thread at a time.
 */
std::mutex& runtime_lock() {
    static std::mutex m;
    return m;
}

/** a module loaded once per process, shared by every session's instances */
struct loaded_module {
    /** WAMR keeps pointers into the bytes it loaded from, and may write to them */
    std::vector<uint8_t> bytes{};
    wasm_module_t module{nullptr};
    function_meta meta{};
    /** the transport in its barch section - TODO 823 */
    resp_spec resp{};
    cron_spec cron{};
    queue_spec queue{};

    ~loaded_module() {
        if (module) {
            std::lock_guard<std::mutex> g(runtime_lock());
            wasm_runtime_unload(module);
        }
    }
};
typedef std::shared_ptr<loaded_module> loaded_ptr;

/** one per name, kept only while some instance holds it */
struct module_cache {
    std::mutex m;
    std::unordered_map<std::string, std::weak_ptr<loaded_module>> by_key;
};
module_cache& modules() {
    static module_cache c;
    return c;
}

/** a value the host holds for the guest, behind a handle */
struct held {
    Variable v{nullptr};
    bool tomb{false};
    bool live{false};
};

/** a reply_array still waiting for its elements */
struct open_array {
    heap::vector<wrapped_variable_t> items{};
    size_t want{0};
};

/*
 * How often the interpreter comes back to barch, in instructions. Often enough that a
 * deadline is noticed within a fraction of a millisecond, rarely enough that the clock
 * read costs nothing next to the work in between. Also what a slice is rounded to.
 */
constexpr uint64_t insns_per_check = 10000;
/** what a call runs on the thread that asked before it goes to the pool - the same
 *  as a Luau call's inline_insns */
constexpr uint64_t first_slice_insns = 20000;

/*
 * A call's deadline, counted the way a Luau call's is (run_ctx in luau_driver.cpp,
 * TODO 445): the CPU time its slices used, with `deadline` as a cheap trigger on the
 * real clock that only ever fires early, and `wall` a ceiling that waiting doesn't move.
 */
struct wasm_clock {
    int64_t deadline{0};        // 0: none
    int64_t wall{0};            // 0: none
    int64_t budget_ns{0};
    int64_t used_ns{0};
    int64_t slice_wall_ns{0};   // when this slice started; 0: none open
    int64_t slice_cpu_ns{0};    // the thread's CPU clock then; 0: not measured
    bool measured{false};       // what the open slice was begun with

    int64_t ends() const {
        if (!deadline) return wall;
        if (!wall) return deadline;
        return deadline < wall ? deadline : wall;
    }
    void begin_slice(bool measure) {
        if (!budget_ns) {
            slice_wall_ns = slice_cpu_ns = 0;
            return;
        }
        measured = measure;
        slice_wall_ns = call_steady_ns();
        slice_cpu_ns = measure ? call_thread_cpu_ns() : 0;
    }
    int64_t open_slice_ns() const {
        if (!slice_wall_ns)
            return 0;
        if (slice_cpu_ns)
            return call_thread_cpu_ns() - slice_cpu_ns;
        const int64_t d = call_steady_ns() - slice_wall_ns;
        return d > 0 ? d : 0;
    }
    void end_slice() {
        if (budget_ns)
            used_ns += open_slice_ns();
        slice_wall_ns = slice_cpu_ns = 0;
    }
    /** the trigger fired: move it to where the budget really runs out, if it hasn't */
    void recheck() {
        if (!budget_ns || !deadline)
            return;
        const int64_t left = budget_ns - used_ns - open_slice_ns();
        if (left >= 1000000)
            deadline = art::now() + left / 1000000;
    }
    bool past_end() {
        if (!ends() || art::now() <= ends())
            return false;
        recheck();
        return art::now() > ends();
    }
};

/*
 * A stack a call runs on, so it can stop in the middle and carry on later, maybe on
 * another thread. WAMR keeps nothing about the running thread that matters here: the
 * thread handle it records is only used by its thread manager, which isn't built, and
 * the stack it checks against is the one each call sets (see run_on).
 */
struct fiber {
#ifdef _WIN32
    void* handle{nullptr};      // the Windows fiber, made once and reused
    void* back{nullptr};        // the fiber that resumed it
    void* job{nullptr};         // the wasm_job it runs next
    char* lo{nullptr};          // the lowest address of its stack, known once it runs
    char* stack_lo() const { return lo; }
#else
    char* base{nullptr};        // the mapping; its lowest page is the guard
    size_t size{0};
    ucontext_t ctx{};           // the call, where it stopped
    ucontext_t back{};          // the worker that resumed it
#ifdef BARCH_WASM_ASAN
    void* asan_fake{nullptr};
    const void* back_bottom{nullptr};
    size_t back_size{0};
#endif
#ifdef BARCH_WASM_TSAN
    void* tsan{nullptr};
    void* tsan_back{nullptr};
#endif
    char* stack_lo() const { return base + page(); }
    size_t stack_size() const { return size - page(); }
    static size_t page() {
        static const size_t p = (size_t) sysconf(_SC_PAGESIZE);
        return p;
    }
#endif
};

/** everything one call has, found from the exec env by the natives */
struct wasm_call {
    std::string name;
    call_interface_ptr iface;
    heap::vector<std::string> args;
    bool read_only{false};
    bool from_log{false};

    std::vector<held> handles;
    std::vector<uint32_t> free_handles;
    size_t live{0};

    std::vector<open_array> open;
    Variable reply{nullptr};
    bool replied{false};
    bool reply_error{false};
    std::string error_text;
    /** set when a native refused something: the exact message, not a trap's */
    std::string host_error;

    // the budget the interpreter's callback works through - TODO 821
    /** instructions left under the cap, function_wasm_insns or a log's log_insns */
    uint64_t cap_left{0};
    /** a slice on the pool, and what's left of the one running */
    uint64_t slice{0};
    uint64_t slice_left{0};
    wasm_clock clock{};
    bool timed_out{false};
    /** set when the caller stops waiting - TODO 874, see call_limits::stop */
    std::shared_ptr<std::atomic<bool>> stop{};
    /**
     * Function overtime - TODO 836, as for a Luau call: whether this call may go on
     * past its deadline, its place in the overtime pool once it has one, and what
     * sets the ceiling it gets there.
     */
    bool overtime_ok{false};
    overtime_ptr overtime{};
    uint64_t wall_factor{0};
    /** the fiber it runs on; null when it runs to the end where it started */
    fiber* fb{nullptr};

    // parking, on I/O or a held key lock - TODO 822
    /** set by park_here as it leaves the fiber, for pump to hand on */
    std::shared_ptr<struct park_box> parked;
    /** the lock wait's end, kept across tries as a Luau call's lock_until is */
    std::chrono::steady_clock::time_point lock_until{};
    /** the lock that last woke it, which passes the turn on once the call goes on */
    lock_parking_ptr lock_woken;
    /** spaces space_open opened, by handle - 1; the running space is handle 0 */
    std::vector<std::pair<std::string, const store_access*>> spaces;

    /**
     * Inside store_locked's callback a shard lock is held, so it can't leave its
     * fiber: not at the end of a slice and not to park. barch.call and http are
     * refused, as they are for Luau. TODO 823
     */
    bool locked{false};
    /** what's left of the region's own cap, since it can't end in a slice */
    uint64_t locked_left{0};
};

/*
 * Where a parked call's wake meets pump - TODO 822. The wake can come before pump has
 * seen the call leave its fiber, on any thread, so whichever is second puts the call
 * back on the pool.
 */
struct park_box {
    std::mutex m;
    bool woken{false};
    std::function<void()> resume;
};

/** an instance of one function for one session, with the exec env it runs on */
struct wasm_instance {
    loaded_ptr mod;
    wasm_module_inst_t inst{nullptr};
    wasm_exec_env_t env{nullptr};
    /** null for a module that's only a cron or queue declaration */
    wasm_function_inst_t call_fn{nullptr};
    int32_t arity{0};
    /** a resp method's exposed name, and the export it runs - TODO 823 */
    std::unordered_map<std::string, wasm_function_inst_t> methods{};
    uint64_t epoch{0};
    /** a call is running on it; a nested call to the same function builds its own */
    bool busy{false};

    ~wasm_instance() {
        std::lock_guard<std::mutex> g(runtime_lock());
        if (env)
            wasm_runtime_destroy_exec_env(env);
        if (inst)
            wasm_runtime_deinstantiate(inst);
    }
};
typedef std::shared_ptr<wasm_instance> instance_ptr;

wasm_call* call_of(wasm_exec_env_t env) {
    return static_cast<wasm_call*>(wasm_runtime_get_user_data(env));
}

/** stop the guest with barch's own message, which is what the caller sees */
void raise(wasm_exec_env_t env, const std::string& why) {
    if (auto* c = call_of(env))
        c->host_error = why;
    wasm_runtime_set_exception(wasm_runtime_get_module_inst(env), host_raised);
}

/** the guest's bytes at [off, off+len), or null with an out of bounds trap set */
char* guest(wasm_exec_env_t env, uint32_t off, uint32_t len) {
    auto inst = wasm_runtime_get_module_inst(env);
    if (!wasm_runtime_validate_app_addr(inst, off, len))
        return nullptr;
    return static_cast<char*>(wasm_runtime_addr_app_to_native(inst, off));
}

bool guest_string(wasm_exec_env_t env, uint32_t off, uint32_t len, std::string& out) {
    const char* p = guest(env, off, len);
    if (!p)
        return false;
    out.assign(p, len);
    return true;
}

int32_t new_handle(wasm_exec_env_t env, Variable v, bool tomb = false) {
    auto* c = call_of(env);
    if (c->live >= max_live_handles) {
        raise(env, "FUNCTION " + c->name + " holds too many handles - val_free the ones it's done with");
        return 0;
    }
    uint32_t h;
    if (!c->free_handles.empty()) {
        h = c->free_handles.back();
        c->free_handles.pop_back();
    } else {
        c->handles.emplace_back();
        h = (uint32_t) c->handles.size();
    }
    auto& e = c->handles[h - 1];
    e.v = std::move(v);
    e.tomb = tomb;
    e.live = true;
    ++c->live;
    return (int32_t) h;
}

held* handle_of(wasm_exec_env_t env, int32_t h) {
    auto* c = call_of(env);
    if (h <= 0 || (size_t) h > c->handles.size() || !c->handles[h - 1].live) {
        raise(env, "FUNCTION " + c->name + " used a handle it doesn't have: " + std::to_string(h));
        return nullptr;
    }
    return &c->handles[h - 1];
}

/** the bytes of a string-like value, or null when it isn't one */
const std::string* text_of(const Variable& v) {
    switch (v.index()) {
        case var_string: return &std::get<std::string>(v);
        case var_verbatim: return &std::get<verbatim_t>(v).text;
        default: return nullptr;
    }
}

/*
 * Kinds as val_kind answers them. Fixed once, like the Luau shapes: a guest compiled
 * against these numbers must keep working.
 */
enum kind : int32_t { k_nil = 0, k_string = 1, k_int = 2, k_double = 3, k_array = 4,
                      k_error = 5, k_tomb = 6, k_bool = 7 };

int32_t kind_of(const held& e) {
    if (e.tomb)
        return k_tomb;
    switch (e.v.index()) {
        case var_bool: return k_bool;
        case var_int64:
        case var_uint64: return k_int;
        case var_double: return k_double;
        case var_string:
        case var_verbatim: return k_string;
        case var_error: return k_error;
        case var_array:
        case var_map:
        case var_set: return k_array;
        default: return k_nil;
    }
}

/* --- the natives: module "barch" ------------------------------------------------ */

int32_t n_arg_count(wasm_exec_env_t env) {
    return (int32_t) call_of(env)->args.size();
}

int32_t n_arg_len(wasm_exec_env_t env, int32_t i) {
    auto* c = call_of(env);
    if (i < 0 || (size_t) i >= c->args.size()) {
        raise(env, "FUNCTION " + c->name + " asked for argument " + std::to_string(i) +
                   " of " + std::to_string(c->args.size()));
        return 0;
    }
    return (int32_t) c->args[i].size();
}

int32_t n_arg_read(wasm_exec_env_t env, int32_t i, uint32_t dst) {
    const int32_t n = n_arg_len(env, i);
    if (wasm_runtime_get_exception(wasm_runtime_get_module_inst(env)))
        return 0;
    char* p = guest(env, dst, (uint32_t) n);
    if (!p)
        return 0;
    memcpy(p, call_of(env)->args[i].data(), n);
    return n;
}

int32_t n_val_kind(wasm_exec_env_t env, int32_t h) {
    auto* e = handle_of(env, h);
    return e ? kind_of(*e) : 0;
}

int32_t n_val_len(wasm_exec_env_t env, int32_t h) {
    auto* e = handle_of(env, h);
    if (!e)
        return 0;
    if (e->v.is_aggregate())
        return (int32_t) e->v.elements().size();
    if (e->v.index() == var_error)
        return (int32_t) std::get<error>(e->v).size();
    if (auto* s = text_of(e->v))
        return (int32_t) std::min<size_t>(s->size(), INT32_MAX);
    return 0;
}

int32_t n_val_read(wasm_exec_env_t env, int32_t h, uint32_t dst) {
    auto* e = handle_of(env, h);
    if (!e)
        return 0;
    const char* from = nullptr;
    size_t n = 0;
    if (e->v.index() == var_error) {
        from = std::get<error>(e->v).what();
        n = std::get<error>(e->v).size();
    } else if (auto* s = text_of(e->v)) {
        from = s->data();
        n = s->size();
    }
    if (n > INT32_MAX)
        n = INT32_MAX;
    char* p = guest(env, dst, (uint32_t) n);
    if (!p)
        return 0;
    if (n)
        memcpy(p, from, n);
    return (int32_t) n;
}

int64_t n_val_int(wasm_exec_env_t env, int32_t h) {
    auto* e = handle_of(env, h);
    if (!e)
        return 0;
    switch (e->v.index()) {
        case var_bool: return std::get<bool>(e->v) ? 1 : 0;
        case var_int64: return std::get<int64_t>(e->v);
        case var_uint64: return (int64_t) std::get<uint64_t>(e->v);
        case var_double: return (int64_t) std::get<double>(e->v);
        default: return 0;
    }
}

double n_val_double(wasm_exec_env_t env, int32_t h) {
    auto* e = handle_of(env, h);
    if (!e)
        return 0;
    switch (e->v.index()) {
        case var_int64: return (double) std::get<int64_t>(e->v);
        case var_uint64: return (double) std::get<uint64_t>(e->v);
        case var_double: return std::get<double>(e->v);
        default: return 0;
    }
}

int32_t n_val_at(wasm_exec_env_t env, int32_t h, int32_t i) {
    auto* e = handle_of(env, h);
    if (!e)
        return 0;
    if (!e->v.is_aggregate() || i < 0 || (size_t) i >= e->v.elements().size()) {
        raise(env, "FUNCTION " + call_of(env)->name + " asked for element " + std::to_string(i) +
                   " of a value that hasn't one");
        return 0;
    }
    Variable el = (const variable_t&) e->v.elements()[i];
    return new_handle(env, std::move(el));
}

void n_val_free(wasm_exec_env_t env, int32_t h) {
    auto* e = handle_of(env, h);
    if (!e)
        return;
    auto* c = call_of(env);
    e->v = Variable(nullptr);
    e->live = false;
    --c->live;
    c->free_handles.push_back((uint32_t) h);
}

/** one finished value into the reply: the top open array, or the reply itself */
void add_reply(wasm_exec_env_t env, Variable v) {
    auto* c = call_of(env);
    for (;;) {
        if (c->open.empty()) {
            if (c->replied) {
                raise(env, "FUNCTION " + c->name + " replied twice");
                return;
            }
            c->reply = std::move(v);
            c->replied = true;
            return;
        }
        auto& top = c->open.back();
        top.items.emplace_back((const variable_t&) v);
        if (top.items.size() < top.want)
            return;
        v = Variable(std::move(top.items));
        c->open.pop_back();
    }
}

void n_reply_nil(wasm_exec_env_t env) {
    add_reply(env, Variable(nullptr));
}

void n_reply_int(wasm_exec_env_t env, int64_t v) {
    add_reply(env, Variable(v));
}

void n_reply_double(wasm_exec_env_t env, double v) {
    add_reply(env, Variable(v));
}

void n_reply_string(wasm_exec_env_t env, uint32_t src, uint32_t len) {
    std::string s;
    if (guest_string(env, src, len, s))
        add_reply(env, Variable(std::move(s)));
}

void n_reply_array(wasm_exec_env_t env, int32_t n) {
    auto* c = call_of(env);
    if (n < 0) {
        raise(env, "FUNCTION " + c->name + " asked for an array of " + std::to_string(n));
        return;
    }
    if (n == 0) {
        add_reply(env, Variable(heap::vector<wrapped_variable_t>{}));
        return;
    }
    if (c->open.size() >= 64) {
        raise(env, "FUNCTION return nests too deeply");
        return;
    }
    if (c->open.empty() && c->replied) {
        raise(env, "FUNCTION " + c->name + " replied twice");
        return;
    }
    c->open.emplace_back();
    c->open.back().want = (size_t) n;
}

void n_reply_error(wasm_exec_env_t env, uint32_t src, uint32_t len) {
    auto* c = call_of(env);
    std::string s;
    if (!guest_string(env, src, len, s))
        return;
    c->error_text = std::move(s);
    c->reply_error = true;
}

/** the space this call runs in, for the checks that need its key_space */
barch::key_space_ptr running_space(const wasm_call& c) {
    const std::string& in = c.iface ? c.iface->running_in : std::string();
    return !in.empty() && barch::keyspace_exists(in) ? barch::get_keyspace(in) : nullptr;
}

/*
 * One try of a native's work, with the lock wait set the way lock_parking_call sets it
 * for Luau - TODO 731, 822. Only a call on a fiber of its own can park, so any other
 * (a nested one, or one from the log) clears the wait, or it would find the outer
 * call's and unwind through WAMR. Returns the lock it found held, with nothing done;
 * `why` is an exception's message, which mustn't unwind through WAMR's C frames either.
 *
 * noinline, and no thread local's address kept across it: the native carries on after
 * a park on whichever thread resumed it.
 */
__attribute__((noinline)) lock_parking_ptr try_once(wasm_call& c, const std::function<void()>& work,
                                                    std::string& why) {
    lock_wait wait{nullptr, c.lock_until, true};
    lock_wait* was = script_lock_wait();
    script_lock_wait() = c.fb && !c.locked ? &wait : nullptr;
    lock_parking_ptr busy;
    try {
        work();
    } catch (lock_busy_unwind& b) {
        busy = std::move(b.parking);
        c.lock_until = wait.until;
    } catch (const std::exception& e) {
        why = e.what();
        if (why.empty())
            why = "FUNCTION host error";
    } catch (...) {
        why = "FUNCTION host error";
    }
    script_lock_wait() = was;
    return busy;
}

/*
 * A native's work, parked on a held key lock and tried again from the top when it's
 * woken, as a Luau call's is. Nothing of it ran before the lock was found held, and the
 * guest is stopped meanwhile, so its memory reads the same the second time.
 */
void with_locks(wasm_exec_env_t env, const std::function<void()>& work) {
    auto* c = call_of(env);
    for (;;) {
        std::string why;
        auto busy = try_once(*c, work, why);
        if (!busy) {
            if (c->lock_woken) {
                c->lock_woken->went_on();
                c->lock_woken.reset();
            }
            c->lock_until = {};
            if (!why.empty())
                raise(env, why);
            return;
        }
        // moved on to another lock: the one that woke it isn't waited on any more. Or
        // in the way of a parked writer on the same lock, which may go on - TODO 869
        if (c->lock_woken && (c->lock_woken->key() != busy->key() || busy->behind_parked()))
            c->lock_woken->went_on();
        c->lock_woken = busy;
        park_here(*c, [busy](std::function<void()> wake) { busy->park(std::move(wake)); });
    }
}

bool raised(wasm_exec_env_t env) {
    return wasm_runtime_get_exception(wasm_runtime_get_module_inst(env)) != nullptr;
}

/** a command's argv out of the guest's (ptr, len) pairs, or false with a trap set */
bool guest_argv(wasm_exec_env_t env, int32_t argc, uint32_t argv_at, heap::vector<std::string>& argv) {
    if (argc <= 0 || argc > (1 << 20)) {
        raise(env, "FUNCTION barch.call needs a command");
        return false;
    }
    const char* pairs = guest(env, argv_at, (uint32_t) argc * 8);
    if (!pairs)
        return false;
    argv.reserve(argc);
    for (int32_t i = 0; i < argc; ++i) {
        uint32_t ptr, len;
        memcpy(&ptr, pairs + i * 8, 4);
        memcpy(&len, pairs + i * 8 + 4, 4);
        std::string a;
        if (!guest_string(env, ptr, len, a))
            return false;
        argv.push_back(std::move(a));
    }
    return true;
}

/** the key space a space handle names, for the checks; 0 is the running one */
barch::key_space_ptr space_of(const wasm_call& c, int32_t sp) {
    if (sp == 0)
        return running_space(c);
    const auto& name = c.spaces[sp - 1].first;
    return barch::keyspace_exists(name) ? barch::get_keyspace(name) : nullptr;
}

/** run a command in a space, as barch.call does, answered as a handle */
int32_t run_argv(wasm_exec_env_t env, int32_t sp, const heap::vector<std::string>& argv) {
    auto* c = call_of(env);
    if (!c->iface || !c->iface->run_command) {
        raise(env, "FUNCTION barch.call is not available here");
        return 0;
    }
    if (c->locked) {
        raise(env, "FUNCTION barch.call is not allowed inside a locked region");
        return 0;
    }
    // the same two refusals barch.call makes from Luau - TODO 673, 672
    if (c->from_log) {
        const auto why = barch::functions::refused_from_log(space_of(*c, sp),
                                                            std::vector<std::string>(argv.begin(), argv.end()));
        if (!why.empty()) {
            raise(env, "FUNCTION a function run from the log can't run this: " + why +
                       " - give it {\"replicate\": \"effects\"} in its barch section");
            return 0;
        }
    }
    if (c->read_only &&
        barch::functions::command_writes(space_of(*c, sp), std::vector<std::string>(argv.begin(), argv.end()))) {
        raise(env, "FUNCTION declared \"write\": false in its barch section, so it can't run " + argv[0]);
        return 0;
    }
    const std::string in = sp == 0 ? std::string() : c->spaces[sp - 1].first;
    Variable out;
    std::string err;
    bool ran = false;
    with_locks(env, [&] {
        out = Variable(nullptr);
        err.clear();
        ran = c->iface->run_command(in, argv, out, err);
    });
    if (raised(env))
        return 0;
    if (!ran) {
        // a command's own failure is the guest's to handle, as an error value
        return new_handle(env, Variable(error(err.empty() ? "FUNCTION call failed" : err)));
    }
    return new_handle(env, std::move(out));
}

/** the space a handle names, or null with a trap set */
const store_access* store_of(wasm_exec_env_t env, int32_t sp) {
    auto* c = call_of(env);
    if (sp == 0) {
        if (!c->iface || !c->iface->store.get) {
            raise(env, "FUNCTION barch.store is not available here");
            return nullptr;
        }
        return &c->iface->store;
    }
    if (sp < 0 || (size_t) sp > c->spaces.size()) {
        raise(env, "FUNCTION " + c->name + " used a space it didn't open: " + std::to_string(sp));
        return nullptr;
    }
    return c->spaces[sp - 1].second;
}

const store_access* store_for_read(wasm_exec_env_t env, int32_t sp) {
    auto* s = store_of(env, sp);
    if (!s)
        return nullptr;
    if (!s->may_read) {
        raise(env, sp ? "FUNCTION not authorized to read there" : "FUNCTION not authorized to read here");
        return nullptr;
    }
    return s;
}

const store_access* store_for_write(wasm_exec_env_t env, int32_t sp) {
    auto* s = store_of(env, sp);
    if (!s)
        return nullptr;
    if (!s->set) {
        raise(env, "FUNCTION barch.store is not available here");
        return nullptr;
    }
    if (call_of(env)->read_only) {
        raise(env, "FUNCTION declared \"write\": false in its barch section, so it can't write");
        return nullptr;
    }
    if (!s->may_write) {
        raise(env, "FUNCTION not authorized to write there");
        return nullptr;
    }
    return s;
}

int32_t n_call(wasm_exec_env_t env, int32_t argc, uint32_t argv_at) {
    heap::vector<std::string> argv;
    if (!guest_argv(env, argc, argv_at, argv))
        return 0;
    return run_argv(env, 0, argv);
}

int32_t space_get(wasm_exec_env_t env, int32_t sp, uint32_t k, uint32_t klen) {
    std::string key;
    if (!guest_string(env, k, klen, key))
        return 0;
    auto* s = store_for_read(env, sp);
    if (!s)
        return 0;
    std::string value;
    auto got = store_access::read_state::absent;
    with_locks(env, [&] { got = s->get(key, value); });
    if (raised(env))
        return 0;
    switch (got) {
        case store_access::read_state::present: return new_handle(env, Variable(std::move(value)));
        case store_access::read_state::tombed: return new_handle(env, Variable(nullptr), true);
        default: return new_handle(env, Variable(nullptr));
    }
}

int32_t space_set(wasm_exec_env_t env, int32_t sp, uint32_t k, uint32_t klen, uint32_t v, uint32_t vlen) {
    std::string key, value;
    if (!guest_string(env, k, klen, key) || !guest_string(env, v, vlen, value))
        return 0;
    auto* s = store_for_write(env, sp);
    if (!s)
        return 0;
    std::string err;
    bool ok = false;
    with_locks(env, [&] {
        err.clear();
        ok = s->set(key, value, err);
    });
    if (raised(env))
        return 0;
    if (!ok) {
        raise(env, err.empty() ? "FUNCTION barch.store.set failed" : err);
        return 0;
    }
    return 1;
}

int32_t space_remove(wasm_exec_env_t env, int32_t sp, uint32_t k, uint32_t klen) {
    std::string key;
    if (!guest_string(env, k, klen, key))
        return 0;
    auto* s = store_for_write(env, sp);
    if (!s)
        return 0;
    if (!s->remove) {
        raise(env, "FUNCTION barch.store is not available here");
        return 0;
    }
    bool gone = false;
    with_locks(env, [&] { gone = s->remove(key); });
    return gone ? 1 : 0;
}

int32_t space_exists(wasm_exec_env_t env, int32_t sp, uint32_t k, uint32_t klen) {
    std::string key;
    if (!guest_string(env, k, klen, key))
        return 0;
    auto* s = store_for_read(env, sp);
    if (!s)
        return 0;
    bool there = false;
    with_locks(env, [&] {
        if (s->exists) {
            there = s->exists(key);
            return;
        }
        std::string ignored;
        there = s->get(key, ignored) == store_access::read_state::present;
    });
    return there ? 1 : 0;
}

int64_t space_count(wasm_exec_env_t env, int32_t sp, uint32_t lo, uint32_t lolen, uint32_t hi, uint32_t hilen) {
    std::string l, h;
    if (!guest_string(env, lo, lolen, l) || !guest_string(env, hi, hilen, h))
        return 0;
    auto* s = store_for_read(env, sp);
    if (!s)
        return 0;
    if (!s->count) {
        raise(env, "FUNCTION count is not available here");
        return 0;
    }
    int64_t n = 0;
    with_locks(env, [&] { n = s->count(l, h); });
    return n;
}

/** as many keys as Luau's barch.store.range hands back at once */
constexpr int64_t max_range_batch = 10000;

int32_t space_range(wasm_exec_env_t env, int32_t sp, uint32_t lo, uint32_t lolen, uint32_t hi, uint32_t hilen,
                    int64_t limit, int64_t offset) {
    std::string l, h;
    if (!guest_string(env, lo, lolen, l) || !guest_string(env, hi, hilen, h))
        return 0;
    auto* s = store_for_read(env, sp);
    if (!s)
        return 0;
    // the same limits as Luau's range
    if (limit <= 0 || limit > max_range_batch)
        limit = max_range_batch;
    if (offset < 0) {
        raise(env, "FUNCTION range offset must not be negative");
        return 0;
    }
    if (!s->range || (offset > 0 && !s->range_from)) {
        raise(env, offset > 0 ? "FUNCTION range offset is not available here" : "FUNCTION range is not available here");
        return 0;
    }
    heap::vector<std::string> keys;
    with_locks(env, [&] {
        keys.clear();
        if (offset > 0)
            s->range_from(l, h, limit, offset, keys);
        else
            s->range(l, h, limit, keys);
    });
    if (raised(env))
        return 0;
    heap::vector<wrapped_variable_t> items;
    items.reserve(keys.size());
    for (auto& k : keys)
        items.emplace_back((const variable_t&) Variable(std::move(k)));
    return new_handle(env, Variable(std::move(items)));
}

int32_t space_call(wasm_exec_env_t env, int32_t sp, int32_t argc, uint32_t argv_at) {
    if (!store_of(env, sp))
        return 0;
    heap::vector<std::string> argv;
    if (!guest_argv(env, argc, argv_at, argv))
        return 0;
    return run_argv(env, sp, argv);
}

/*
 * A handle on another key space, as barch.space.NAME is in Luau - TODO 822. Opened
 * through the interface, so it's the one the call's rights were worked out for, and
 * kept there for the next call. A run from the log uses only its own space (TODO 673).
 */
int32_t n_space_open(wasm_exec_env_t env, uint32_t n, uint32_t nlen) {
    auto* c = call_of(env);
    std::string name;
    if (!guest_string(env, n, nlen, name))
        return 0;
    if (c->from_log) {
        std::string canonical = name;
        if (barch::keyspace_exists(name))
            canonical = barch::get_keyspace(name)->canonical();
        if (!c->iface || canonical != c->iface->running_in) {
            raise(env, "FUNCTION a function run from the log uses only the space it runs in, not " + name +
                       " - give it {\"replicate\": \"effects\"} in its barch section to reach other spaces");
            return 0;
        }
    }
    for (size_t i = 0; i < c->spaces.size(); ++i)
        if (c->spaces[i].first == name)
            return (int32_t) i + 1;
    if (!c->iface || !c->iface->open_space) {
        raise(env, "FUNCTION that key space is not open");
        return 0;
    }
    const store_access* s = nullptr;
    with_locks(env, [&] {
        auto have = c->iface->opened.find(name);
        if (have == c->iface->opened.end()) {
            auto opened = std::make_unique<store_access>();
            if (!c->iface->open_space(name, *opened))
                throw std::runtime_error("FUNCTION no key space called " + name);
            have = c->iface->opened.emplace(name, std::move(opened)).first;
        }
        s = have->second.get();
    });
    if (!s)
        return 0;
    if (c->spaces.size() >= 4096) {
        raise(env, "FUNCTION " + c->name + " opened too many spaces");
        return 0;
    }
    c->spaces.emplace_back(name, s);
    return (int32_t) c->spaces.size();
}

int32_t n_store_get(wasm_exec_env_t env, uint32_t k, uint32_t klen) {
    return space_get(env, 0, k, klen);
}

int32_t n_store_set(wasm_exec_env_t env, uint32_t k, uint32_t klen, uint32_t v, uint32_t vlen) {
    return space_set(env, 0, k, klen, v, vlen);
}

int32_t n_store_remove(wasm_exec_env_t env, uint32_t k, uint32_t klen) {
    return space_remove(env, 0, k, klen);
}

int32_t n_store_exists(wasm_exec_env_t env, uint32_t k, uint32_t klen) {
    return space_exists(env, 0, k, klen);
}

int64_t n_store_count(wasm_exec_env_t env, uint32_t lo, uint32_t lolen, uint32_t hi, uint32_t hilen) {
    return space_count(env, 0, lo, lolen, hi, hilen);
}

int32_t n_store_range(wasm_exec_env_t env, uint32_t lo, uint32_t lolen, uint32_t hi, uint32_t hilen,
                      int64_t limit, int64_t offset) {
    return space_range(env, 0, lo, lolen, hi, hilen, limit, offset);
}

/** fn is (i32) -> i32, the one shape store_locked calls */
bool takes_ctx(wasm_exec_env_t env, wasm_function_inst_t fn) {
    auto inst = wasm_runtime_get_module_inst(env);
    if (wasm_func_get_param_count(fn, inst) != 1 || wasm_func_get_result_count(fn, inst) != 1)
        return false;
    wasm_valkind_t p, r;
    wasm_func_get_param_types(fn, inst, &p);
    wasm_func_get_result_types(fn, inst, &r);
    return p == WASM_I32 && r == WASM_I32;
}

/*
 * store_locked(key, klen, fn, ctx) - TODO 823. Luau's barch.store.locked: runs fn with
 * the shard that owns `key` write locked, or the whole space when klen is 0, and
 * answers what fn answers. fn is an index into the module's function table, which is
 * what a C function pointer is in wasm32, and it gets ctx as its one argument.
 *
 * Scoped like Luau's, so the lock goes back however fn ends. fn runs nested right
 * here, on the same exec env and thread, which is what keeps the shard hold (a thread
 * local) good. So it can't leave the fiber, and barch.call and http are refused in it.
 *
 * The interpreter keeps its count in a local and starts a nested call from the exec
 * env's, so fn gets a grant of its own here, or it would run the first one again for
 * free. What it doesn't use of the last grant is lost, which is the same on every
 * member, so a run from the log still stops at the same instruction.
 */
int32_t n_store_locked(wasm_exec_env_t env, uint32_t k, uint32_t klen, uint32_t fn_index, int32_t ctx) {
    auto* c = call_of(env);
    std::string key;
    if (!guest_string(env, k, klen, key))
        return 0;
    if (c->locked) {
        raise(env, "FUNCTION already inside a locked region");
        return 0;
    }
    auto fn = barch_wasm_table_func(wasm_runtime_get_module_inst(env), fn_index);
    if (!fn || !takes_ctx(env, fn)) {
        raise(env, "FUNCTION store_locked needs one of the module's own functions, taking an i32 and "
                   "answering one");
        return 0;
    }
    auto* s = store_of(env, 0);
    if (!s)
        return 0;
    if (!s->locked) {
        raise(env, "FUNCTION store_locked is not available here");
        return 0;
    }
    bool entered = false, ran = false;
    int32_t answer = 0;
    std::string err, why;
    try {
        s->locked(key, [&]() -> bool {
            entered = true;
            c->locked = true;
            // the slice is the region's cap, as it is for Luau, since it can't end in one
            c->locked_left = c->slice ? c->slice : get_function_slice_insns();
            wasm_runtime_set_instruction_count_limit(env, grant(*c));
            uint32_t argv[1] = {(uint32_t) ctx};
            ran = wasm_runtime_call_wasm(env, fn, 1, argv);
            answer = (int32_t) argv[0];
            c->locked = false;
            return ran;
        }, err);
    } catch (const std::exception& e) {
        why = e.what();
    } catch (...) {
        why = "FUNCTION host error";
    }
    c->locked = false;
    if (raised(env))
        return 0;   // what fn raised, now that the lock is back
    if (!why.empty() || !entered) {
        raise(env, !why.empty() ? why : !err.empty() ? err : "FUNCTION locked region refused");
        return 0;
    }
    return answer;
}

/** the shard a key is on, so a guest can tell whether two keys can share a region */
int64_t n_store_shard_number(wasm_exec_env_t env, uint32_t k, uint32_t klen) {
    std::string key;
    if (!guest_string(env, k, klen, key))
        return 0;
    auto* s = store_of(env, 0);
    if (!s)
        return 0;
    if (!s->shard_number) {
        raise(env, "FUNCTION store_shard_number is not available here");
        return 0;
    }
    return s->shard_number(key);
}

/** 1 when a locked region already holds the key's shard */
int32_t n_store_has_lock(wasm_exec_env_t env, uint32_t k, uint32_t klen) {
    std::string key;
    if (!guest_string(env, k, klen, key))
        return 0;
    auto* s = store_of(env, 0);
    if (!s)
        return 0;
    return s->has_lock && s->has_lock(key) ? 1 : 0;
}

/*
 * Transactions, as Luau's barch.tx - TODO 732, 822: BEGINTX, COMMITTX and ABORTTX
 * through barch.call, answered as call answers, so a TXCONFLICT is an error value the
 * guest can retry on. A transaction stays open across slices and parks: it belongs to
 * the session, not the thread.
 */
int32_t n_tx_begin(wasm_exec_env_t env, int32_t pessimistic) {
    heap::vector<std::string> argv{"BEGINTX"};
    if (pessimistic)
        argv.emplace_back("PESSIMISTIC");
    return run_argv(env, 0, argv);
}

int32_t n_tx_commit(wasm_exec_env_t env) {
    return run_argv(env, 0, heap::vector<std::string>{"COMMITTX"});
}

int32_t n_tx_abort(wasm_exec_env_t env) {
    return run_argv(env, 0, heap::vector<std::string>{"ABORTTX"});
}

int32_t n_tx_open(wasm_exec_env_t env) {
    auto* c = call_of(env);
    if (!c->iface || !c->iface->store.in_transaction)
        return 0;
    bool open = false;
    with_locks(env, [&] { open = c->iface->store.in_transaction(); });
    return open ? 1 : 0;
}

/*
 * The time in ms and a UUIDv7, as barch.time and barch.uuid - TODO 761, 763. In a run
 * from a Raft log they come from the entry, so every member answers the same.
 */
int64_t n_time(wasm_exec_env_t env) {
    auto* c = call_of(env);
    if (c->from_log) {
        const auto* logged = barch::functions::running_from_log();
        if (!logged || logged->ms < 0) {
            raise(env, "FUNCTION time has no time to answer in a run from a log entry written before "
                       "entries carried one");
            return 0;
        }
        return logged->ms;
    }
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

int32_t n_uuid(wasm_exec_env_t env, uint32_t dst) {
    auto* c = call_of(env);
    char* p = guest(env, dst, 36);
    if (!p)
        return 0;
    std::string u;
    if (c->from_log) {
        const auto* logged = barch::functions::running_from_log();
        if (logged)
            u = logged->uuid();
        if (u.empty()) {
            raise(env, "FUNCTION uuid has no time to answer in a run from a log entry written before "
                       "entries carried one");
            return 0;
        }
    } else {
        u = barch::ordered_uuid();
    }
    if (u.size() != 36) {
        raise(env, "FUNCTION uuid made a malformed id");
        return 0;
    }
    memcpy(p, u.data(), 36);
    return 36;
}

/** what a request answered, waited for where the call can't park */
struct http_wait {
    std::mutex m;
    std::condition_variable cv;
    bool done{false};
    http_answer got;
};

/*
 * An outbound HTTP request, as Luau's http.request - TODO 822. `method` is 0 GET,
 * 1 POST, 2 PUT, 3 PATCH, 4 DELETE; `headers` are "Name: value" lines, one per line.
 * Answers [status, body, [name, value, ...]], or an error value when the request
 * never got an answer (refused, DNS, TLS, timeout).
 *
 * A call on its own fiber parks while it waits, so a slow server costs a socket and no
 * thread. Any other waits right there, off the deadline and cut short at its ceiling,
 * as a Luau call that can't park does (TODO 435).
 */
int32_t n_http_request(wasm_exec_env_t env, int32_t method, uint32_t url, uint32_t urllen, uint32_t hdrs,
                       uint32_t hlen, uint32_t body, uint32_t blen, int32_t timeout_ms) {
    auto* c = call_of(env);
    http_ask ask;
    std::string lines;
    if (!guest_string(env, url, urllen, ask.url) || !guest_string(env, hdrs, hlen, lines) ||
        !guest_string(env, body, blen, ask.body))
        return 0;
    if (c->locked) {
        raise(env, "FUNCTION http is not allowed inside a locked region");
        return 0;
    }
    if (c->from_log) {
        raise(env, "FUNCTION a function run from the log can't use http - give it {\"replicate\": \"effects\"} "
                   "in its barch section");
        return 0;
    }
    // no interface means nobody to ask, as current_access answers for Luau - TODO 304
    if (c->iface && !c->iface->store.may_reach_out) {
        raise(env, "FUNCTION http needs the outbound category");
        return 0;
    }
    if (!http_available()) {
        raise(env, "FUNCTION this build has no HTTP client");
        return 0;
    }
    if (method < 0 || method > 4) {
        raise(env, "FUNCTION http method " + std::to_string(method) + " isn't one of 0 to 4");
        return 0;
    }
    ask.method = method;
    for (size_t at = 0; at < lines.size();) {
        size_t nl = lines.find('\n', at);
        if (nl == std::string::npos)
            nl = lines.size();
        std::string line = lines.substr(at, nl - at);
        if (!line.empty() && line.back() == '\r')
            line.pop_back();
        if (!line.empty())
            ask.headers.push_back(std::move(line));
        at = nl + 1;
    }
    ask.timeout_ms = timeout_ms > 0 ? timeout_ms : 30000;
    // nothing can stop the wait itself, so it ends by the wall ceiling - TODO 435
    if (c->clock.wall) {
        const int64_t left = c->clock.wall - art::now();
        if (left < ask.timeout_ms)
            ask.timeout_ms = left > 1 ? (long) left : 1;
    }

    auto box = std::make_shared<http_wait>();
    auto start = [&ask, box](std::function<void()> wake) {
        http_start(std::move(ask), [box, wake = std::move(wake)](http_answer a) {
            {
                std::lock_guard<std::mutex> g(box->m);
                box->got = std::move(a);
                box->done = true;
            }
            box->cv.notify_one();
            if (wake)
                wake();
        });
    };
    if (c->fb) {
        park_here(*c, start);
    } else {
        // not running, so off the deadline
        const bool measured = c->clock.measured;
        const int64_t began = art::now();
        c->clock.end_slice();
        start(nullptr);
        {
            std::unique_lock<std::mutex> l(box->m);
            box->cv.wait(l, [&] { return box->done; });
        }
        if (c->clock.deadline)
            c->clock.deadline += art::now() - began;
        c->clock.begin_slice(measured);
    }
    http_answer a;
    {
        std::lock_guard<std::mutex> g(box->m);
        a = std::move(box->got);
    }
    if (!a.ok && a.status == 0)
        return new_handle(env, Variable(error(a.error.empty() ? "FUNCTION http request failed" : a.error)));
    heap::vector<wrapped_variable_t> headers;
    for (auto& [hn, hv] : a.headers) {
        headers.emplace_back((const variable_t&) Variable(std::move(hn)));
        headers.emplace_back((const variable_t&) Variable(std::move(hv)));
    }
    heap::vector<wrapped_variable_t> out;
    out.emplace_back((const variable_t&) Variable((int64_t) a.status));
    out.emplace_back((const variable_t&) Variable(std::move(a.body)));
    out.emplace_back((const variable_t&) Variable(std::move(headers)));
    return new_handle(env, Variable(std::move(out)));
}

/*
 * WAMR checks each import's type against these signatures when it loads a module, and
 * leaves one that doesn't match unlinked, which check_wasm then refuses. `i` is i32,
 * `I` i64, `F` f64. Pointers are plain `i` and checked by hand, so a bad one answers
 * barch's message rather than WAMR's.
 */
NativeSymbol natives[] = {
    {"arg_count", (void*) n_arg_count, "()i", nullptr},
    {"arg_len", (void*) n_arg_len, "(i)i", nullptr},
    {"arg_read", (void*) n_arg_read, "(ii)i", nullptr},
    {"val_kind", (void*) n_val_kind, "(i)i", nullptr},
    {"val_len", (void*) n_val_len, "(i)i", nullptr},
    {"val_read", (void*) n_val_read, "(ii)i", nullptr},
    {"val_int", (void*) n_val_int, "(i)I", nullptr},
    {"val_double", (void*) n_val_double, "(i)F", nullptr},
    {"val_at", (void*) n_val_at, "(ii)i", nullptr},
    {"val_free", (void*) n_val_free, "(i)", nullptr},
    {"reply_nil", (void*) n_reply_nil, "()", nullptr},
    {"reply_int", (void*) n_reply_int, "(I)", nullptr},
    {"reply_double", (void*) n_reply_double, "(F)", nullptr},
    {"reply_string", (void*) n_reply_string, "(ii)", nullptr},
    {"reply_array", (void*) n_reply_array, "(i)", nullptr},
    {"reply_error", (void*) n_reply_error, "(ii)", nullptr},
    {"call", (void*) n_call, "(ii)i", nullptr},
    {"store_get", (void*) n_store_get, "(ii)i", nullptr},
    {"store_set", (void*) n_store_set, "(iiii)i", nullptr},
    {"store_remove", (void*) n_store_remove, "(ii)i", nullptr},
    {"store_exists", (void*) n_store_exists, "(ii)i", nullptr},
    {"store_count", (void*) n_store_count, "(iiii)I", nullptr},
    {"store_range", (void*) n_store_range, "(iiiiII)i", nullptr},
    {"store_locked", (void*) n_store_locked, "(iiii)i", nullptr},
    {"store_shard_number", (void*) n_store_shard_number, "(ii)I", nullptr},
    {"store_has_lock", (void*) n_store_has_lock, "(ii)i", nullptr},
    {"space_open", (void*) n_space_open, "(ii)i", nullptr},
    {"space_get", (void*) space_get, "(iii)i", nullptr},
    {"space_set", (void*) space_set, "(iiiii)i", nullptr},
    {"space_remove", (void*) space_remove, "(iii)i", nullptr},
    {"space_exists", (void*) space_exists, "(iii)i", nullptr},
    {"space_count", (void*) space_count, "(iiiii)I", nullptr},
    {"space_range", (void*) space_range, "(iiiiiII)i", nullptr},
    {"space_call", (void*) space_call, "(iii)i", nullptr},
    {"tx_begin", (void*) n_tx_begin, "(i)i", nullptr},
    {"tx_commit", (void*) n_tx_commit, "()i", nullptr},
    {"tx_abort", (void*) n_tx_abort, "()i", nullptr},
    {"tx_open", (void*) n_tx_open, "()i", nullptr},
    {"time", (void*) n_time, "()I", nullptr},
    {"uuid", (void*) n_uuid, "(i)i", nullptr},
    {"http_request", (void*) n_http_request, "(iiiiiiii)i", nullptr},
};

/** the runtime, once per process, with the natives registered before any load */
bool runtime_ready() {
    static const bool ready = [] {
        RuntimeInitArgs init;
        memset(&init, 0, sizeof init);
        init.mem_alloc_type = Alloc_With_System_Allocator;
        init.native_module_name = "barch";
        init.native_symbols = natives;
        init.n_native_symbols = sizeof natives / sizeof natives[0];
        if (!wasm_runtime_full_init(&init))
            return false;
        barch_wasm_budget_hook = budget_hook;
        return true;
    }();
    return ready;
}

/*
 * Load a module and check it is one barch will run. The caller holds runtime_lock.
 * `mod->bytes` must already be the module.
 */
bool load_checked(loaded_module& mod, std::string& err) {
    if (!runtime_ready()) {
        err = "FUNCTION the WASM runtime didn't start";
        return false;
    }
    wasm_sections sections;
    if (!read_wasm_sections(std::string((const char*) mod.bytes.data(), mod.bytes.size()), sections, err))
        return false;
    if (sections.has_start) {
        err = "a WASM function can't have a start section: it would run with no instruction cap";
        return false;
    }
    char why[128] = {0};
    mod.module = wasm_runtime_load(mod.bytes.data(), (uint32_t) mod.bytes.size(), why, sizeof why);
    if (!mod.module) {
        err = std::string("the WASM module doesn't load: ") + why;
        return false;
    }
    const int32_t imports = wasm_runtime_get_import_count(mod.module);
    for (int32_t i = 0; i < imports; ++i) {
        wasm_import_t im;
        wasm_runtime_get_import_type(mod.module, i, &im);
        const std::string from = im.module_name ? im.module_name : "";
        const std::string what = im.name ? im.name : "";
        if (from != "barch" || im.kind != WASM_IMPORT_EXPORT_KIND_FUNC) {
            err = "a WASM function can only import barch's functions, not " + from + "." + what;
            return false;
        }
        if (!im.linked) {
            err = "barch has no function " + what + " of that type to import";
            return false;
        }
    }
    bool has_call = false;
    // the exports shaped like call, for the resp methods below
    std::unordered_set<std::string> runnable;
    const int32_t exports = wasm_runtime_get_export_count(mod.module);
    for (int32_t i = 0; i < exports; ++i) {
        wasm_export_t ex;
        wasm_runtime_get_export_type(mod.module, i, &ex);
        const std::string name = ex.name ? ex.name : "";
        if (name == "__post_instantiate") {
            err = "a WASM function can't export __post_instantiate: it would run with no instruction cap";
            return false;
        }
        const bool shaped = ex.kind == WASM_IMPORT_EXPORT_KIND_FUNC &&
                            wasm_func_type_get_param_count(ex.u.func_type) == 0 &&
                            wasm_func_type_get_result_count(ex.u.func_type) == 1 &&
                            wasm_func_type_get_result_valkind(ex.u.func_type, 0) == WASM_I32;
        if (shaped)
            runnable.insert(name);
        if (name == "call") {
            if (!shaped) {
                err = "a WASM function's call has to be a function () -> i32";
                return false;
            }
            has_call = true;
        } else if (name == "arity") {
            if (ex.kind != WASM_IMPORT_EXPORT_KIND_GLOBAL ||
                wasm_global_type_get_valkind(ex.u.global_type) != WASM_I32) {
                err = "a WASM function's arity has to be an i32 global";
                return false;
            }
        }
    }
    const std::string source((const char*) mod.bytes.data(), mod.bytes.size());
    if (!read_function_meta(source, mod.meta, err))
        return false;
    if (!read_wasm_transport(source, &mod.resp, &mod.cron, &mod.queue, err))
        return false;
    // a cron entry or a queue declaration names a function elsewhere to run, so it
    // needs no call of its own; a resp one still does, as in Luau - TODO 823
    if (!has_call && !mod.cron.is_cron && !mod.queue.is_queue) {
        err = "a WASM function has to export call";
        return false;
    }
    for (const auto& m : mod.resp.methods) {
        if (!runnable.count(m.export_name)) {
            err = "resp method '" + m.name + "' runs " + m.export_name +
                  ", which the module doesn't export as a function () -> i32";
            return false;
        }
    }
    return true;
}

/** the process-wide module for `key`, loading it when nobody holds a current one */
loaded_ptr module_for(const std::string& key, const std::string& source, std::string& err) {
    auto& cache = modules();
    {
        std::lock_guard<std::mutex> g(cache.m);
        auto it = cache.by_key.find(key);
        if (it != cache.by_key.end()) {
            if (auto have = it->second.lock()) {
                // the bytes, not an epoch: whoever holds the source in hand is the
                // authority, and comparing is cheap next to a load
                if (have->bytes.size() == source.size() &&
                    memcmp(have->bytes.data(), source.data(), source.size()) == 0)
                    return have;
            }
        }
    }
    auto mod = std::make_shared<loaded_module>();
    mod->bytes.assign(source.begin(), source.end());
    {
        std::lock_guard<std::mutex> g(runtime_lock());
        if (!load_checked(*mod, err))
            return nullptr;
    }
    std::lock_guard<std::mutex> g(cache.m);
    // forget the names nobody holds any more while the lock is here anyway
    if (cache.by_key.size() > 64) {
        for (auto it = cache.by_key.begin(); it != cache.by_key.end();)
            it = it->second.expired() ? cache.by_key.erase(it) : std::next(it);
    }
    cache.by_key[key] = mod;
    return mod;
}

instance_ptr instantiate(const loaded_ptr& mod, std::string& err) {
    auto wi = std::make_shared<wasm_instance>();
    wi->mod = mod;
    wi->epoch = barch::functions::compile_epoch();
    std::lock_guard<std::mutex> g(runtime_lock());
    InstantiationArgs2* args = nullptr;
    if (!wasm_runtime_instantiation_args_create(&args)) {
        err = "FUNCTION WASM instantiation";
        return nullptr;
    }
    uint64_t pages = barch::get_function_wasm_max_mb() * 16;
    if (pages > 65536)
        pages = 65536;
    wasm_runtime_instantiation_args_set_max_memory_pages(args, (uint32_t) pages);
    wasm_runtime_instantiation_args_set_default_stack_size(args, exec_stack_bytes);
    wasm_runtime_instantiation_args_set_host_managed_heap_size(args, 0);
    char why[128] = {0};
    wi->inst = wasm_runtime_instantiate_ex2(mod->module, args, why, sizeof why);
    wasm_runtime_instantiation_args_destroy(args);
    if (!wi->inst) {
        err = std::string("FUNCTION WASM instantiation: ") + why;
        return nullptr;
    }
    wi->env = wasm_runtime_create_exec_env(wi->inst, exec_stack_bytes);
    if (!wi->env) {
        err = "FUNCTION WASM exec env";
        return nullptr;
    }
    // a cron or queue declaration may have no call; launch says so if it's called
    wi->call_fn = wasm_runtime_lookup_function(wi->inst, "call");
    for (const auto& m : mod->resp.methods) {
        auto* fn = wasm_runtime_lookup_function(wi->inst, m.export_name.c_str());
        if (!fn) {
            err = "resp method '" + m.name + "' has no export " + m.export_name;
            return nullptr;
        }
        wi->methods[m.name] = fn;
    }
    wasm_global_inst_t g_arity;
    if (wasm_runtime_get_export_global_inst(wi->inst, "arity", &g_arity) &&
        g_arity.kind == WASM_I32 && g_arity.global_data)
        memcpy(&wi->arity, g_arity.global_data, sizeof wi->arity);
    return wi;
}

/** has the function been published since this instance was built - see TODO 245 */
bool stale(const std::string& key, wasm_instance& wi) {
    const auto now = barch::functions::compile_epoch();
    if (wi.epoch == now)
        return false;
    if (barch::functions::published_at(key) > wi.epoch)
        return true;
    wi.epoch = now;
    return false;
}

}  // namespace wasm_impl

using namespace wasm_impl;

struct wasm_sessions {
    std::mutex m;
    std::unordered_map<std::string, instance_ptr> by_key;
};

wasm_sessions_ptr make_wasm_sessions() {
    return std::make_shared<wasm_sessions>();
}

bool check_wasm(const std::string& name, const std::string& source, std::string& err,
                resp_spec* resp, cron_spec* cron, queue_spec* queue) {
    loaded_module mod;
    mod.bytes.assign(source.begin(), source.end());
    std::lock_guard<std::mutex> g(runtime_lock());
    if (!load_checked(mod, err)) {
        if (err.rfind("FUNCTION", 0) != 0)
            err = "FUNCTION " + name + ": " + err;
        return false;
    }
    if (resp)
        *resp = mod.resp;
    if (cron)
        *cron = mod.cron;
    if (queue)
        *queue = mod.queue;
    return true;
}

namespace {

/** hand the outcome back, counted the way finish_job counts a Luau call */
void finish(bool ok, Variable out, std::string err,
            const function_done& done) {
    if (!ok) {
        if (err.find("FUNCTION timeout") != std::string::npos)
            ++statistics::function_timeouts;
        else
            ++statistics::function_errors;
    }
    done(ok, std::move(out), std::move(err));
}

/*
 * Fibers.
 *
 * ucontext rather than hand written assembly: a switch costs a signal mask syscall
 * each way, which is noise next to a pool hop, and it works the same on every target
 * glibc supports. A stack is mapped with MAP_NORESERVE, so only the pages a call
 * touches cost memory, and the lowest page is left unmapped as a guard.
 *
 * The switch functions are noinline, and nothing on a fiber keeps a thread local's
 * address across one: the compiler may compute that once per function, and the
 * function may be on another thread when it carries on.
 *
 * Windows has no ucontext. It has fibers of its own, which do the same job: a
 * thread becomes a fiber before it switches to one, and a fiber can be switched to
 * from any thread. A Windows fiber runs one function for its whole life, so that
 * function loops: run the job it was given, go back, wait for the next. Windows
 * reserves its stack, commits pages as they're touched and keeps its own guard
 * page - TODO 832.
 */
constexpr size_t fiber_stack_bytes = 4u << 20;
/** room above the guard page that WAMR's own stack check keeps clear */
constexpr size_t fiber_stack_reserve = 64 * 1024;
constexpr size_t max_free_fibers = 64;

struct fiber_pool {
    std::mutex m;
    std::vector<fiber*> free;
};
fiber_pool& fibers() {
    static auto* p = new fiber_pool();   // never destroyed: a pool thread may outlive statics
    return *p;
}

#ifdef _WIN32
void WINAPI fiber_loop(void* arg);
#endif

/** the fiber running on this thread, for a call nested inside it - see run_on */
thread_local fiber* running_fiber = nullptr;

fiber* take_fiber() {
    {
        auto& p = fibers();
        std::lock_guard<std::mutex> g(p.m);
        if (!p.free.empty()) {
            auto* f = p.free.back();
            p.free.pop_back();
            return f;
        }
    }
#ifdef _WIN32
    auto* f = new fiber();
    f->handle = CreateFiberEx(0, fiber_stack_bytes, FIBER_FLAG_FLOAT_SWITCH, fiber_loop, f);
    if (!f->handle) {
        delete f;
        return nullptr;
    }
#else
    void* at = mmap(nullptr, fiber_stack_bytes, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | MAP_STACK, -1, 0);
    if (at == MAP_FAILED)
        return nullptr;
    auto* f = new fiber();
    f->base = static_cast<char*>(at);
    f->size = fiber_stack_bytes;
    if (mprotect(f->base, fiber::page(), PROT_NONE) != 0) {
        munmap(at, fiber_stack_bytes);
        delete f;
        return nullptr;
    }
#ifdef BARCH_WASM_TSAN
    f->tsan = __tsan_create_fiber(0);
#endif
#endif
    return f;
}

void give_fiber(fiber* f) {
    {
        auto& p = fibers();
        std::lock_guard<std::mutex> g(p.m);
        if (p.free.size() < max_free_fibers) {
            p.free.push_back(f);
            return;
        }
    }
#ifdef _WIN32
    DeleteFiber(f->handle);
#else
#ifdef BARCH_WASM_TSAN
    __tsan_destroy_fiber(f->tsan);
#endif
    munmap(f->base, f->size);
#endif
    delete f;
}

/** a worker runs the fiber until it stops or ends */
__attribute__((noinline)) void resume_fiber(fiber& f) {
    fiber* outer = running_fiber;
    running_fiber = &f;
#ifdef _WIN32
    // a pool thread is made a fiber the first time it runs one, and stays one
    if (!IsThreadAFiber())
        ConvertThreadToFiberEx(nullptr, FIBER_FLAG_FLOAT_SWITCH);
    f.back = GetCurrentFiber();
    SwitchToFiber(f.handle);
#else
#ifdef BARCH_WASM_TSAN
    f.tsan_back = __tsan_get_current_fiber();
    __tsan_switch_to_fiber(f.tsan, 0);
#endif
#ifdef BARCH_WASM_ASAN
    void* fake = nullptr;
    __sanitizer_start_switch_fiber(&fake, f.stack_lo(), f.stack_size());
#endif
    swapcontext(&f.back, &f.ctx);
#ifdef BARCH_WASM_ASAN
    __sanitizer_finish_switch_fiber(fake, nullptr, nullptr);
#endif
#endif
    running_fiber = outer;
}

/** the fiber is running now, just started or just resumed */
inline void on_fiber(fiber& f) {
#ifdef BARCH_WASM_ASAN
    __sanitizer_finish_switch_fiber(f.asan_fake, &f.back_bottom, &f.back_size);
#else
    (void) f;
#endif
}

/** back to the worker; `ending` when it won't be resumed */
__attribute__((noinline)) void leave_fiber(fiber& f, bool ending) {
#ifdef BARCH_WASM_TSAN
    __tsan_switch_to_fiber(f.tsan_back, 0);
#endif
#ifdef BARCH_WASM_ASAN
    __sanitizer_start_switch_fiber(ending ? nullptr : &f.asan_fake, f.back_bottom, f.back_size);
#else
    (void) ending;
#endif
#ifdef _WIN32
    SwitchToFiber(f.back);
#else
    swapcontext(&f.ctx, &f.back);
#endif
    on_fiber(f);
}

/*
 * Park the call: `start` gets a wake to call once, on any thread, when what it waits
 * for is ready, and the call leaves its fiber. pump hands it to the box, and whichever
 * of the two is second puts it back on the pool - TODO 822. Back here when it's
 * resumed, maybe on another thread.
 */
void park_here(wasm_call& c, const std::function<void(std::function<void()>)>& start) {
    auto box = std::make_shared<park_box>();
    c.parked = box;
    start([box] {
        std::function<void()> go;
        {
            std::lock_guard<std::mutex> g(box->m);
            if (box->resume)
                go = std::move(box->resume);
            else
                box->woken = true;
        }
        if (go)
            go();
    });
    leave_fiber(*c.fb, false);
}

/*
 * The budget.
 *
 * The interpreter counts down what it was given and calls budget_hook at zero. A
 * grant is at most insns_per_check, and never more than is left of the cap or the
 * slice, so the cap is exact - a call from a log stops at the same instruction on
 * every member - and a slice ends where it should.
 */
int32_t grant(wasm_call& c) {
    uint64_t n = insns_per_check;
    if (n > c.cap_left)
        n = c.cap_left;
    if (n > c.slice_left)
        n = c.slice_left;
    if (c.locked && n > c.locked_left)
        n = c.locked_left;
    c.cap_left -= n;
    c.slice_left -= n;
    if (c.locked)
        c.locked_left -= n;
    return (int32_t) n;
}

/*
 * Past its deadline, a call on a fiber goes into overtime if it may and there's room:
 * function_overtime_max_ms more CPU time, and a new ceiling. One that runs where it
 * started has its caller waiting right there, and a locked region can't leave its
 * fiber, so neither goes. TODO 836
 */
bool try_overtime(wasm_call& c) {
    if (!c.overtime_ok || c.overtime || c.locked || !c.fb || !c.clock.budget_ns)
        return false;
    c.overtime = enter_overtime();
    if (!c.overtime)
        return false;
    const int64_t ms = (int64_t) barch::get_function_overtime_max_ms();
    const int64_t now = art::now();
    c.clock.budget_ns = c.clock.used_ns + c.clock.open_slice_ns() + ms * 1000000;
    c.clock.deadline = now + ms;
    if (c.clock.wall)
        c.clock.wall = now + ms * (int64_t) (c.wall_factor ? c.wall_factor : 1);
    return true;
}

int budget_hook(wasm_exec_env_t env) {
    auto* c = call_of(env);
    if (!c)
        return 0;
    if (c->stop && c->stop->load(std::memory_order_relaxed)) {
        c->host_error = call_stopped_error;
        return 0;
    }
    if (c->clock.past_end() && !try_overtime(*c)) {
        c->timed_out = true;
        return 0;
    }
    if (!c->cap_left)
        return 0;
    if (c->locked && !c->locked_left) {
        c->host_error = "FUNCTION locked region ran too long";
        return 0;
    }
    if (!c->slice_left && c->locked) {
        // a held shard can't wait on the pool, so the slice just starts over here;
        // the region's own cap is what ends it
        c->slice_left = c->slice ? c->slice : insns_per_check;
    } else if (!c->slice_left) {
        // the slice is spent: a call on a fiber goes back to the worker, which puts
        // it on the pool; one that runs where it started just carries on
        if (c->fb) {
            leave_fiber(*c->fb, false);
            if (c->clock.past_end() && !try_overtime(*c)) {
                c->timed_out = true;
                return 0;
            }
        }
        c->slice_left = c->slice ? c->slice : insns_per_check;
    }
    return grant(*c);
}

/** one call: the instance it runs on, the caller, and how it ended */
struct wasm_job {
    std::shared_ptr<wasm_sessions> sessions;
    std::string key;
    instance_ptr wi;
    /** the instance is the one the session keeps; a temporary goes with the call */
    bool kept{false};
    std::shared_ptr<wasm_call> c;
    function_done done;
    /** call, or the export a resp method runs */
    wasm_function_inst_t fn{nullptr};

    bool ended{false};
    bool ran{false};
    int32_t ret{0};
    /** it has been on the pool, so its slices are measured - as a Luau call's */
    bool pooled{false};
    /** when it went back on the pool, so waiting for a worker isn't charged to it */
    int64_t queued_since{0};
};
typedef std::shared_ptr<wasm_job> job_ptr;

/*
 * Run the call to the end, or to wherever it stops. `boundary` is the lowest address
 * of the stack it runs on that WAMR may use, or null for the thread's own stack: WAMR
 * reads the thread's stack bounds by default, which on a fiber are some other stack.
 */
void run_on(wasm_job& job, uint8_t* boundary) {
    auto& wi = *job.wi;
    auto& c = *job.c;
    wasm_runtime_set_user_data(wi.env, &c);
    wasm_runtime_set_native_stack_boundary(wi.env, boundary);
    wasm_runtime_set_instruction_count_limit(wi.env, grant(c));
    uint32_t argv[2] = {0, 0};
    job.ran = wasm_runtime_call_wasm(wi.env, job.fn, 0, argv);
    job.ret = (int32_t) argv[0];
    wasm_runtime_set_user_data(wi.env, nullptr);
    job.ended = true;
}

#ifdef _WIN32
void WINAPI fiber_loop(void* arg) {
    auto* f = static_cast<fiber*>(arg);
    ULONG_PTR lo = 0, hi = 0;
    GetCurrentThreadStackLimits(&lo, &hi);   // the running fiber's stack, from its TEB
    f->lo = reinterpret_cast<char*>(lo);
    for (;;) {
        run_on(*static_cast<wasm_job*>(f->job), reinterpret_cast<uint8_t*>(f->stack_lo() + fiber_stack_reserve));
        f->job = nullptr;
        // resumed here when start_fiber has given it the next job
        leave_fiber(*f, true);
    }
}

void start_fiber(wasm_job& job) {
    job.c->fb->job = &job;
}
#else
void fiber_main(unsigned lo, unsigned hi) {
    auto* job = reinterpret_cast<wasm_job*>((uintptr_t) lo | ((uintptr_t) hi << 32));
    fiber& f = *job->c->fb;
    on_fiber(f);
    run_on(*job, reinterpret_cast<uint8_t*>(f.stack_lo() + fiber_stack_reserve));
    leave_fiber(f, true);
    // never resumed after that; it gets a new context when it's used again
    abort();
}

void start_fiber(wasm_job& job) {
    fiber& f = *job.c->fb;
    getcontext(&f.ctx);
    f.ctx.uc_stack.ss_sp = f.stack_lo();
    f.ctx.uc_stack.ss_size = f.stack_size();
    f.ctx.uc_link = nullptr;
#ifdef BARCH_WASM_ASAN
    f.asan_fake = nullptr;
#endif
    const auto at = reinterpret_cast<uintptr_t>(&job);
    makecontext(&f.ctx, reinterpret_cast<void (*)()>(fiber_main), 2,
                (unsigned) (at & 0xffffffffu), (unsigned) (at >> 32));
}
#endif

/** the call has ended: say how, and let the session have its instance back */
void complete(wasm_job& job) {
    auto& wi = *job.wi;
    auto& c = *job.c;
    bool ok = true;
    std::string err;
    if (!job.ran) {
        ok = false;
        const char* ex = wasm_runtime_get_exception(wi.inst);
        std::string what = ex ? ex : "";
        if (what.rfind("Exception: ", 0) == 0)
            what.erase(0, 11);
        if (!c.host_error.empty())
            err = c.host_error;
        else if (c.from_log && what == "instruction limit exceeded")
            // the same words as Luau's, so a log run says which cap stopped it - TODO 823
            err = "FUNCTION ran past function_log_insns, the cap on a function run from the log";
        else if (c.timed_out || what == "instruction limit exceeded")
            err = "FUNCTION timeout";
        else
            err = "FUNCTION " + c.name + " trap: " + what;
        wasm_runtime_clear_exception(wi.inst);
    } else if (job.ret != 0 || c.reply_error) {
        ok = false;
        err = !c.error_text.empty() ? c.error_text
                                    : "FUNCTION " + c.name + " failed with " + std::to_string(job.ret);
    } else if (!c.open.empty()) {
        ok = false;
        err = "FUNCTION " + c.name + " returned before its reply_array was filled";
    }

    if (job.kept) {
        std::lock_guard<std::mutex> g(job.sessions->m);
        wi.busy = false;
        // a trap can leave the instance's memory half updated, so the next call gets
        // a new one
        if (!job.ran) {
            auto it = job.sessions->by_key.find(job.key);
            if (it != job.sessions->by_key.end() && it->second == job.wi)
                job.sessions->by_key.erase(it);
        }
    }
    // the overtime place goes back before the answer, so the next call can have it
    c.overtime.reset();
    auto done = std::move(job.done);
    job.done = nullptr;
    if (ok)
        finish(true, std::move(c.reply), {}, done);
    else
        finish(false, Variable(nullptr), std::move(err), done);
}

void pump(const job_ptr& job);

/** back on the pool for its next slice; past its deadline, the overtime pool - TODO 836 */
void requeue(const job_ptr& job, bool overtime) {
    if (overtime)
        enqueue_overtime([job] { pump(job); });
    else
        enqueue([job] { pump(job); });
}

/*
 * Run one slice of a call on its fiber, the way pump_call runs one of a Luau call's.
 * A call whose slice ran out goes back on the pool; one that ended is answered.
 */
void pump(const job_ptr& job) {
    auto& c = *job->c;
    // waiting for a worker isn't running - TODO 434
    if (job->queued_since) {
        if (c.clock.deadline)
            c.clock.deadline += art::now() - job->queued_since;
        job->queued_since = 0;
    }
    c.clock.begin_slice(job->pooled);
    resume_fiber(*c.fb);
    c.clock.end_slice();
    if (c.parked) {
        // waiting, not running: the wait goes on the deadline as a queue does
        auto box = std::move(c.parked);
        ++statistics::wasm_parks;
        job->queued_since = art::now();
        job->pooled = true;
        // in overtime it comes back on the overtime pool - TODO 836
        const bool overtime = (bool) c.overtime;
        std::unique_lock<std::mutex> g(box->m);
        if (!box->woken) {
            box->resume = [job, overtime] { requeue(job, overtime); };
            return;
        }
        g.unlock();
        requeue(job, overtime);
        return;
    }
    if (!job->ended) {
        ++statistics::wasm_slices;
        job->queued_since = art::now();
        job->pooled = true;
        requeue(job, (bool) c.overtime);
        return;
    }
    give_fiber(c.fb);
    c.fb = nullptr;
    complete(*job);
}

void launch(const std::shared_ptr<wasm_sessions>& sessions, const std::string& key,
            const std::string& name, const instance_ptr& wi, bool kept,
            const call_interface_ptr& iface, const heap::vector<std::string>& args,
            const call_limits& limits, const function_done& done, const std::string& entry) {
    auto release = [&] {
        if (kept) {
            std::lock_guard<std::mutex> g(sessions->m);
            wi->busy = false;
        }
    };
    /*
     * With no entry it's call. An entry is a command a resp transport exposed, and
     * runs the export the method names, with the arity the method declared - TODO 823.
     */
    wasm_function_inst_t fn = wi->call_fn;
    int want_arity = wi->arity;
    if (!entry.empty()) {
        auto m = wi->methods.find(entry);
        if (m == wi->methods.end()) {
            release();
            done(false, Variable(nullptr), "no such method '" + entry + "'");
            return;
        }
        fn = m->second;
        want_arity = 0;
        for (const auto& rm : wi->mod->resp.methods)
            if (rm.name == entry)
                want_arity = rm.arity;
    }
    if (!fn) {
        release();
        done(false, Variable(nullptr), "FUNCTION " + name + " has no call: it only declares a transport");
        return;
    }
    const int given = (int) args.size();
    if ((want_arity > 0 && given != want_arity) || (want_arity < 0 && given < -want_arity)) {
        release();
        done(false, Variable(nullptr),
             "wrong number of arguments for '" + (entry.empty() ? name : entry) + "'");
        return;
    }
    auto job = std::make_shared<wasm_job>();
    job->sessions = sessions;
    job->key = key;
    job->wi = wi;
    job->kept = kept;
    job->done = done;
    job->fn = fn;
    auto c = std::make_shared<wasm_call>();
    job->c = c;
    c->name = name;
    c->iface = iface;
    c->args = args;
    c->read_only = wi->mod->meta.read_only;
    c->from_log = limits.from_log;
    c->stop = limits.stop;  // TODO 874
    c->cap_left = limits.from_log ? limits.log_insns : barch::get_function_wasm_insns();
    if (c->cap_left == 0)
        c->cap_left = 1;

    // the function's own settings may lower the space's slice and deadline, and
    // raise them up to the caps, as a Luau function's header does - TODO 434
    const auto& meta = wi->mod->meta;
    c->slice = function_limit(meta.slice_insns, limits.slice_insns, limits.slice_max_insns);
    const uint64_t deadline_ms = function_limit(meta.deadline_ms, limits.deadline_ms,
                                                limits.deadline_max_ms);
    if (deadline_ms && !limits.from_log) {
        const int64_t now = art::now();
        c->clock.deadline = now + (int64_t) deadline_ms;
        c->clock.budget_ns = (int64_t) deadline_ms * 1000000;
        if (limits.wall_factor)
            c->clock.wall = now + (int64_t) (deadline_ms * limits.wall_factor);
        // TODO 836; a call on no fiber can't go, which try_overtime checks
        c->wall_factor = limits.wall_factor;
        c->overtime_ok = meta.overtime;
    }

    /*
     * A call its caller waits for right there (inline_only, from_log) runs to the end
     * on this thread, as a Luau one does: its slices just follow each other. If this
     * thread is itself on a fiber - a WASM call that called this one through
     * barch.call - WAMR is told that fiber's stack.
     */
    fiber* outer = running_fiber;
    if (limits.inline_only || limits.from_log || outer) {
        c->slice_left = c->slice ? c->slice : insns_per_check;
        c->clock.begin_slice(false);
        run_on(*job, outer ? reinterpret_cast<uint8_t*>(outer->stack_lo() + fiber_stack_reserve)
                           : nullptr);
        c->clock.end_slice();
        complete(*job);
        return;
    }
    c->fb = take_fiber();
    if (!c->fb) {
        release();
        finish(false, Variable(nullptr), "FUNCTION no memory for a WASM call's stack", done);
        return;
    }
    start_fiber(*job);
    // the first slice runs here, on the thread that asked, and is short - see
    // inline_insns in luau_driver.cpp for why
    c->slice_left = first_slice_insns;
    pump(job);
}

}  // namespace

bool start_wasm_warm(const wasm_sessions_ptr& sessions, const std::string& key, const std::string& name,
                     const call_interface_ptr& iface, const heap::vector<std::string>& args,
                     const call_limits& limits, const function_done& done,
                     const std::string& entry) {
    instance_ptr wi;
    instance_ptr dropped;   // destroyed outside the session lock
    {
        std::lock_guard<std::mutex> g(sessions->m);
        auto it = sessions->by_key.find(key);
        if (it == sessions->by_key.end())
            return false;
        if (it->second->busy)
            return false;   // a nested call: the cold path builds a temporary one
        if (stale(key, *it->second)) {
            dropped = std::move(it->second);
            sessions->by_key.erase(it);
            return false;
        }
        wi = it->second;
        wi->busy = true;
    }
    launch(sessions, key, name, wi, true, iface, args, limits, done, entry);
    return true;
}

void start_wasm(const wasm_sessions_ptr& sessions, const std::string& key, const std::string& name,
                const std::string& source, const call_interface_ptr& iface,
                const heap::vector<std::string>& args, const call_limits& limits,
                const function_done& done, const std::string& entry) {
    std::string err;
    auto mod = module_for(key, source, err);
    if (!mod) {
        if (err.rfind("FUNCTION", 0) != 0)
            err = "FUNCTION " + name + ": " + err;
        ++statistics::function_errors;
        done(false, Variable(nullptr), err);
        return;
    }
    auto wi = instantiate(mod, err);
    if (!wi) {
        ++statistics::function_errors;
        done(false, Variable(nullptr), err);
        return;
    }
    bool kept = false;
    instance_ptr dropped;
    {
        std::lock_guard<std::mutex> g(sessions->m);
        auto it = sessions->by_key.find(key);
        if (it == sessions->by_key.end() || !it->second->busy) {
            if (it != sessions->by_key.end())
                dropped = std::move(it->second);
            else if (sessions->by_key.size() >= max_session_instances) {
                for (auto i = sessions->by_key.begin(); i != sessions->by_key.end();)
                    i = i->second->busy ? std::next(i) : sessions->by_key.erase(i);
            }
            wi->busy = true;
            sessions->by_key[key] = wi;
            kept = true;
        }
    }
    launch(sessions, key, name, wi, kept, iface, args, limits, done, entry);
}

}

#endif
