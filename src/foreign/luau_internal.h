#pragma once

#include "driver.h"
#include "pool.h"
#include "wasm_driver.h"

#ifdef BARCH_HAS_LUAU

#include "../repo_library.h"
#include "../function_api.h"
#include "statistics.h"
#include "lua.h"
#include "lualib.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace barch {
namespace foreign {

namespace detail {
void close_counted_state(lua_State* L);
}

struct run_ctx {
    uint64_t left{0};
    uint64_t slice{0};
    int64_t deadline{0};
    /**
     * inside a locked region a shard lock is held, so the three things that would
     * deadlock or stall behind it are refused: yielding (a parked call holding a shard
     * lock never comes back), barch.call (a command takes shard locks of its own), and
     * sql.query (network I/O with a shard stopped). See TODO 98 F6.
     */
    bool locked{false};
    /**
     * the function's header said `"write": false` - TODO 672. Every write it tries,
     * through the store, a space handle, or a write command through barch.call, is
     * refused.
     */
    bool read_only{false};
    /** running from a Raft log, and how many interrupts it has left - TODO 673 */
    bool from_log{false};
    uint64_t log_left{0};
    /** what is left of the region's own cap, since it cannot end in a yield */
    uint64_t locked_left{0};
    /**
     * raised by the deadline watch when a native frame has held its thread
     * for a slice's worth of time. A hookless frame has no interrupt firing
     * to count instructions with, so the watch counts time instead and the
     * interrupt turns that into the yield the slice would have made. Shared
     * with the watch entry rather than pointing into it, so neither side can
     * outlive the flag - it used to be a raw pointer into the entry that every
     * disarm had to null by hand (TODO 430). Null when nothing is armed. See
     * TODO 398.
     */
    std::shared_ptr<std::atomic<bool>> slice_due{};
    /** this resume runs with the hook down (a native frame), so an ask the
     *  interrupt cannot act on now is better answered by putting the hook
     *  back down than by leaving it up - see function_interrupt */
    bool hookless{false};
    /**
     * park_yield made the yield the pump sees. A raise between park_call and
     * park_yield leaves the call marked parked, and a script's pcall can catch it,
     * so being parked does not make a yield a park's - TODO 942
     */
    bool park_yielded{false};
    /**
     * this call runs on a coroutine, so there is somewhere to yield to - and a
     * slice that runs out at a moment when a yield is not allowed can simply
     * wait for the next one. A call with no coroutine (an HTTP route, a
     * compile check) has nowhere to yield to ever, so for those the slice
     * stays a hard cap. See TODO 397.
     */
    bool on_coroutine{false};
    /**
     * The wall clock ceiling, fixed when the call starts - TODO 434. The deadline
     * counts running time and moves on past parks and queueing; this doesn't, so a
     * call that keeps waiting still ends. 0 is none.
     */
    int64_t wall{0};
    /** interrupts left before the next clock read - see function_interrupt */
    uint32_t clock_wait{0};
    /**
     * Function overtime - TODO 836. A call that may go on past its deadline: it's on
     * a coroutine of its own, its caller isn't waiting for it right there, it isn't
     * run from a log, and its header didn't say "overtime": false. `overtime` is its
     * place in the overtime pool once it has one, and `wall_factor` sets the ceiling
     * it gets there.
     */
    bool overtime_ok{false};
    overtime_ptr overtime{};
    uint64_t wall_factor{0};
    /** set when the caller stops waiting - TODO 874, see call_limits::stop */
    std::shared_ptr<std::atomic<bool>> stop{};
    /**
     * The call's own coroutine, the one the pump resumes - TODO 875. A coroutine the
     * script made is any other thread. A slice that ends in one of those breaks out
     * of it, and `broken` holds the threads that break left suspended below `top`,
     * outermost first. See end_slice_here.
     */
    lua_State* top{nullptr};
    std::vector<lua_State*> broken{};
    /** the earlier of the two, what a check against the clock compares with; 0 is none */
    int64_t ends() const {
        if (!deadline) return wall;
        if (!wall) return deadline;
        return deadline < wall ? deadline : wall;
    }
    /*
     * The deadline counts the call's CPU time - TODO 445.
     *
     * "Running" used to be real time while the call held a worker, less the waits
     * credited back one by one (the queue, a park, sql.query). That left gaps: a
     * worker descheduled between taking the job and starting the slice, or after
     * it and before the requeue, charged its call the whole wait. On a busy runner
     * 50ms of work came to 300ms and the call timed out, and with only the slices
     * measured it still leaked about 3ms a gap.
     *
     * So what's counted is the call's CPU time, not what's left after credits. Each
     * slice adds its CPU time to `used_ns`. `deadline` stays a cheap real-clock
     * trigger that the interrupt can afford to check all the time; the credits only
     * ever move it to somewhere before the real point of running out, never past
     * it. When it fires, `recheck` adds up what the call has really used and, with
     * budget left, moves the trigger to now plus what's left.
     *
     * The CPU clock is a syscall, about 300ns, so the inline first slice, run by the
     * thread that asked, isn't measured: that would be a third of a one line call.
     * It adds its real time instead, less any blocking wait inside it (credit_wait),
     * and it's short by construction. Slices on the pool and HTTP handlers are
     * measured. The wall ceiling doesn't move for any of this.
     */
    int64_t budget_ns{0};       // 0: no deadline
    int64_t used_ns{0};         // what the slices that ended used
    int64_t slice_wall_ns{0};   // when this slice started, steady clock; 0: none open
    int64_t slice_cpu_ns{0};    // the thread's CPU clock then; 0: this slice isn't measured
    void begin_slice(bool measure);
    void end_slice();
    /** the trigger fired: add up what's really been used; true if there's budget left */
    bool recheck();
    /** past the deadline: go into overtime if the call may and there's room */
    bool try_overtime();
    /** a blocking wait of `ms` inside the slice, which isn't running */
    void credit_wait(int64_t ms) {
        // after a wait the next interrupt reads the clock - TODO 809
        clock_wait = 0;
        if (!deadline)
            return;
        deadline += ms;
        // a measured slice's CPU clock stopped while it waited; an unmeasured one
        // counts real time, so the wait comes off it
        if (!slice_cpu_ns && slice_wall_ns)
            slice_wall_ns += ms * 1000000;
    }
};

/**
 * what a running call carries on its coroutine - TODO 150.
 *
 * Not in the function's environment, which is where it first looks like it belongs:
 * `luaL_sandboxthread` gives each function its own globals table and a script can
 * write to it, so a script could overwrite the space and point `sql.query` at another
 * space's database. Thread data is C side only, with no Lua-visible key to shadow.
 *
 * On the coroutine rather than the VM, because one state now serves every space a
 * session uses: a nested CALLF is a second job on the same VM, and anything kept per
 * VM would need saving and restoring around it.
 */
struct call_job;

struct running_call {
    /** the space the function was loaded from, which is where require looks */
    std::string space;
    /** the space the call is running against - barch.store / barch.current */
    std::string running;
    /**
     * the call this coroutine belongs to, when there is one: a stored function
     * or an HTTP handler - TODO 911. Null for the foreign fill states, which
     * can't be parked - see `park_call` in driver.h.
     */
    call_job* owner{nullptr};
    /*
     * A lock wait in progress - TODO 731: when it gives up, the lock that woke the
     * call (whose turn passes on once the call goes on), and the method a parked
     * `sp:get(...)` was called as, since a continuation can't ask for it again.
     */
    std::chrono::steady_clock::time_point lock_until{};
    lock_parking_ptr lock_woken{};
    const char* method{nullptr};
};

/*
 * The compiled functions a session holds.
 *
 * One Luau state per key space, because `require` resolves inside a space and this
 * puts the module cache where the resolution already is - and because a generation
 * bump or an UNLOAD then closes one state rather than picking entries out of a map.
 * Nothing shares a state between sessions, so there is no lock here and none needed:
 * a session runs one call at a time. See TODO 98 C.
 */
struct compiled {
    /** what barch::functions::compile_epoch() said when this was built - TODO 243 */
    uint64_t epoch{0};
    /** for a module out of the file store, the version it was compiled from. 0 when
     *  the file carries none, which means a forced require always rebuilds - TODO 248 */
    uint64_t fs_version{0};
    int fn{LUA_NOREF};      // the function itself, pinned in the registry
    int env{LUA_NOREF};     // its thread, which owns the globals table it closed over
    int envt{LUA_NOREF};    // that globals table, which is what require hands back
    int arity{0};           // 0 is "any", n exactly n, -n at least n. See TODO 98 I.3
    /** exposed command name -> the function transport() named for it - TODO 188 */
    heap::string_map<int> methods{};
    /** and the arity each declared, same convention as the script-level one */
    heap::string_map<int> method_arity{};
    /** SETF ... AOT asked for native code - TODO 392. Recorded so the call path
     *  compiles the same function the check path accepted, per state. */
    bool aot{false};
    /** the function's own deadline and slice from its --@barch header, 0 where it
     *  gave none - TODO 434 */
    uint64_t meta_deadline_ms{0};
    bool meta_read_only{false};                 // "write": false - TODO 672
    bool meta_overtime{true};                   // "overtime": false - TODO 836
    uint64_t meta_slice_insns{0};
};

/*
 * What a script reaches for the length of one call, in one place - TODO 430.
 *
 * It used to be six loose pointers on space_state - load, run_command, store,
 * open_space, opened and the interface id - each set by hand at every place a call
 * starts and cleared at every place one ends. There were six of those places, and a
 * field added to one and missed at another was a pointer left behind into an
 * interface that had gone (TODO 427 was the shape of it). Now the interface is held
 * here by shared_ptr, the pointers are taken from it here, and nothing outside this
 * struct and call_scope sets them. They stay good for as long as the call_ctx does,
 * because it keeps the interface alive.
 */
struct call_ctx {
    call_interface_ptr iface{};
    const source_loader* load{nullptr};
    const command_runner* run_command{nullptr};
    const store_access* store{nullptr};
    /** an HTTP request's own store, when it runs as an identity of its own */
    std::shared_ptr<const store_access> own_store{};
    const space_opener* open_space{nullptr};
    /**
     * the library pin set this call's first require("@lib") resolved, 0 before then -
     * TODO 595. Kept for the rest of the call, so its requires can't straddle a re-pin
     * and get one library before it and another after.
     */
    uint64_t pinset{0};
    /** and the claim that keeps it from being released while this call lives - 596 */
    barch::library::pinset_hold hold{};

    call_ctx() = default;
    explicit call_ctx(call_interface_ptr i) { set(std::move(i)); }
    void set(call_interface_ptr i) {
        iface = std::move(i);
        load = iface ? &iface->load : nullptr;
        run_command = iface && iface->run_command ? &iface->run_command : nullptr;
        store = iface ? &iface->store : nullptr;
        open_space = iface && iface->open_space ? &iface->open_space : nullptr;
        own_store.reset();
        pinset = 0;
        hold.release();
    }
};

struct space_state {
    lua_State* L{nullptr};
    /** stack of space names gathered during require */
    heap::vector<std::string> space_stack;
    /** keyed by `qualified(space, name)` - one map for every space the session uses */
    heap::string_map<compiled> functions{};
    /**
     * scratch for building that key on the call path, so a warm call does not
     * allocate one per call. Measured: building it fresh cost about 8% of the fixed
     * cost of a call, which is the whole of what a function that does nothing does.
     */
    std::string keybuf{};
    /**
     * The call running now, set only by call_scope - TODO 430. A session runs one
     * call at a time, so there is one of these and no lock around it. Everything a
     * script reaches for its call - require's loader, barch.call, barch.store and
     * barch.space - comes through here, and is null when no call is running.
     */
    call_ctx* call{nullptr};
    const source_loader* load() const { return call ? call->load : nullptr; }
    const command_runner* run_command() const { return call ? call->run_command : nullptr; }
    const store_access* store() const { return call ? call->store : nullptr; }
    const space_opener* open_space() const { return call ? call->open_space : nullptr; }
    /** where the spaces `barch.space.NAME` opened live - the interface owns them */
    heap::string_map<std::unique_ptr<store_access>>* opened() const {
        return call && call->iface ? &call->iface->opened : nullptr;
    }
    /** the running interface, which a kept handle compares itself against */
    call_interface* iface() const { return call ? call->iface.get() : nullptr; }
    /**
     * what is being compiled right now, innermost last. A require for something on
     * this stack is a cycle, and the stack is the path to put in the message.
     */
    heap::vector<std::string> loading{};
    /**
     * coroutines that have finished and can be used again.
     *
     * A call runs its script on a coroutine of its own, and making a fresh one every
     * time was 11% of the server between `stack_init` and the collector marking the
     * garbage it left. They are interchangeable - `lua_resetthread` puts one back to
     * empty - so a small pool of them removes both at once. See TODO 98 F5.
     */
    heap::vector<std::pair<lua_State*, int>> free_threads{};

    ~space_state() {
        // the functions go with it, so what they were counted as goes too
        if (statistics::luau_functions >= functions.size())
            statistics::luau_functions -= functions.size();
        detail::close_counted_state(L);
    }
};

/**
 * Puts a call in front of a state for as long as it lives, then puts back whatever
 * was there - TODO 430. The one way st->call is set, so a call can't be left
 * behind: a require compiled inside a running call sees that call, and the call
 * sees itself again when the require is done.
 */
class call_scope {
public:
    call_scope(space_state* st, call_ctx* ctx) : st_(st), prev_(st ? st->call : nullptr) {
        if (st_)
            st_->call = ctx;
    }
    ~call_scope() {
        if (st_)
            st_->call = prev_;
    }
    call_scope(const call_scope&) = delete;
    call_scope& operator=(const call_scope&) = delete;
private:
    space_state* st_;
    call_ctx* prev_;
};

struct function_states {
    /** where this cache's Luau bytes are counted as well, if anyone asked - TODO 181 */
    std::shared_ptr<std::atomic<uint64_t>> bytes{};
    /*
     * One state for the session, not one per space it touches - TODO 150.
     *
     * A state is about 50kB (ten libraries, the barch and sql tables, the VM's own
     * globals) and a compiled function about 0.5kB, so duplicating the state per space
     * was paying the expensive part per space and capping the cheap one. Almost
     * nothing in it was ever space specific: the functions map is, and is keyed by
     * space now, and the space a call runs in travels on the coroutine.
     */
    std::unique_ptr<space_state> only{};
    /**
     * false for a cache whose state exists only to check that a chunk compiles, so
     * it is built with no codegen context at all - TODO 400.
     */
    bool native{true};
    /** the session's WASM instances, made the first time it calls a module - TODO 820 */
    wasm_sessions_ptr wasm{};
};

/**
 * Every write of the interrupt hook goes through here - TODO 877.
 * Kept directly in barch::foreign so ci/tsan.supp matches `barch::foreign::set_interrupt`.
 */
void set_interrupt(lua_State* L, void (*fn)(lua_State*, int));

namespace detail {

/*
 * A backstop rather than a working limit - TODO 150.
 *
 * A compiled function is about 0.5kB, so this is a couple of megabytes for a session
 * that has called four thousand distinct functions, and it is bounded anyway by how
 * many are stored.
 */
enum { max_cached_functions = 4096 };
/* what a call may run on the calling thread before it has to park. See start_function */
enum { inline_insns = 20000 };
/* how many spent coroutines a space keeps to hand out again */
enum { max_free_threads = 32 };

// ---- Scheduler & state lifecycle (luau_scheduler.cpp) ----
lua_State* new_counted_state(std::atomic<uint64_t>* local = nullptr, bool native = true);
void close_counted_state(lua_State* L);
running_call* call_of(lua_State* L);
std::string current_space(lua_State* L);
void function_interrupt(lua_State* L, int gc);
void take_thread(space_state& st, lua_State*& T, int& ref);
void give_thread(space_state& st, lua_State* T, int ref);
void start_resolve(std::string_view space, std::string_view key, uint64_t deadline_ms,
                   bool requeue, std::function<void(result)> done);

// ---- Compiler, VM setup & require (luau_compiler.cpp) ----
void open_safe(lua_State* L);
bool load_source(const std::string& spec, std::string& source, std::string& err);
bool compile_source(const std::string& source, std::string& bytecode, std::string& err);
space_state* state_for(function_states& cache);
space_state*& state_of(lua_State* L);
bool published_since(const std::string& key, compiled& c);
void drop_compiled(lua_State* L, compiled& c);
std::string qualified(const std::string& space, const std::string& name);
bool compile_into(space_state& st, const std::string& name, const std::string& source,
                  compiled& out, std::string& err, bool needs_call = true, bool aot = false,
                  uint64_t library_version = 0);
bool push_service_fn(lua_State* T);
bool read_resp_transport(lua_State* L, lua_State* T, resp_spec& spec,
                         heap::string_map<int>* refs, std::string& err);
bool read_cron_transport(lua_State* L, lua_State* T, cron_spec& spec, std::string& err);
bool read_queue_transport(lua_State* L, lua_State* T, queue_spec& spec, std::string& err);

// ---- Storage, space, fs, graph, tx & barch bindings (luau_bindings.cpp) ----
void install_barch_bindings(lua_State* L);
int sql_query(lua_State* L);
void push_variable(lua_State* L, const Variable& v);
bool to_variable(lua_State* L, int idx, Variable& out, std::string& err, int depth = 0);
const store_access* store_of(lua_State* L, const char* what, bool writing = false);
void check_may_write(lua_State* L, const store_access* s);
void refuse_other_space(lua_State* L, const std::string& name);
void refuse_in_transaction(lua_State* L, const char* what);

} // namespace detail
} // namespace foreign
} // namespace barch

#endif // BARCH_HAS_LUAU
