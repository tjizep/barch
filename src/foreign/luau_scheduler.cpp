#include "luau_internal.h"

#ifdef BARCH_HAS_LUAU

#include "../test_jitter.h"
#include "../test_fault.h"
#include "../luau_park_core.h"
#include "key_space.h"
#include "configuration.h"
#include "lzr_log.h"
#include "art/nodes.h"
#include "abstract_shard.h"
#include "sastam.h"
#include "Luau/CodeGen.h"
#include "crow_luau.h"
#include "luau_protect.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <time.h>
#include <vector>

namespace barch {
namespace foreign {

static int64_t steady_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

/*
 * Test knob, TODO 805: BARCH_TEST_CPU_CLOCK_PERCENT=N makes the thread CPU clock run
 * at N percent of its real speed. At 10 a call gets a tenth of the CPU time it uses,
 * which is what a crowded machine looks like from inside: the wall clock gets to the
 * deadline long before the CPU budget runs out. It's read once, so it has to be set
 * before barch starts.
 */
static int64_t cpu_clock_percent() {
    static const int64_t pct = [] {
        const char* v = std::getenv("BARCH_TEST_CPU_CLOCK_PERCENT");
        const long n = v ? std::strtol(v, nullptr, 10) : 0;
        return n > 0 && n < 100 ? int64_t(n) : int64_t(100);
    }();
    return pct;
}

static int64_t thread_cpu_ns() {
    timespec ts{};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    int64_t v = static_cast<int64_t>(ts.tv_sec) * 1000000000ll + ts.tv_nsec;
    if (cpu_clock_percent() != 100)
        v = v / 100 * cpu_clock_percent();
    return v ? v : 1;
}

int64_t call_steady_ns() {
    return steady_ns();
}

int64_t call_thread_cpu_ns() {
    return thread_cpu_ns();
}

void run_ctx::begin_slice(bool measure) {
    if (!budget_ns) {
        slice_wall_ns = slice_cpu_ns = 0;
        return;
    }
    slice_wall_ns = steady_ns();
    slice_cpu_ns = measure ? thread_cpu_ns() : 0;
}

/** what the open slice has used so far */
static int64_t open_slice_ns(const run_ctx& c) {
    if (!c.slice_wall_ns)
        return 0;
    if (c.slice_cpu_ns)
        return thread_cpu_ns() - c.slice_cpu_ns;
    const int64_t d = steady_ns() - c.slice_wall_ns;
    return d > 0 ? d : 0;
}

void run_ctx::end_slice() {
    if (budget_ns)
        used_ns += open_slice_ns(*this);
    slice_wall_ns = slice_cpu_ns = 0;
}

bool run_ctx::recheck() {
    if (!budget_ns || !deadline)
        return false;
    const int64_t left = budget_ns - used_ns - open_slice_ns(*this);
    if (left < 1000000)
        return false;
    deadline = art::now() + left / 1000000;
    return true;
}

/*
 * A call in overtime gets function_overtime_max_ms of CPU time from here, counted
 * the way its deadline was, and a new wall ceiling that many times wall_factor from
 * now. A locked region can't leave its thread, so it doesn't go. Once a call has
 * been in overtime it fails at the end of it.
 */
bool run_ctx::try_overtime() {
    if (!overtime_ok || overtime || locked || from_log || !budget_ns)
        return false;
    overtime = enter_overtime();
    if (!overtime)
        return false;
    const int64_t ms = static_cast<int64_t>(barch::get_function_overtime_max_ms());
    const int64_t now = art::now();
    budget_ns = used_ns + open_slice_ns(*this) + ms * 1000000;
    deadline = now + ms;
    if (wall)
        wall = now + ms * static_cast<int64_t>(wall_factor ? wall_factor : 1);
    clock_wait = 0;
    return true;
}


/*
 * Every write of the interrupt hook goes through here - TODO 877.
 *
 * The deadline watch writes it from its own thread while the pool thread runs
 * the state, and the pool thread writes it too. A relaxed atomic store makes our
 * side of that a real atomic access. It costs nothing: on x86-64 and AArch64 it
 * is the same single aligned store as a plain write.
 *
 * Luau's reads stay plain. The interpreter loads the field at each interrupt
 * point and native code does one aligned load at each loop edge. Making those
 * atomic in C++ terms needs a patch to Luau, which is what the TSan suppression
 * on this function stands in for.
 */
void set_interrupt(lua_State* L, void (*fn)(lua_State*, int)) {
    using hook = void (*)(lua_State*, int);
    static_assert(std::atomic_ref<hook>::is_always_lock_free);
    std::atomic_ref<hook>(lua_callbacks(L)->interrupt).store(fn, std::memory_order_relaxed);
}



namespace detail {

/*
 * Every Luau state barch builds is built with this, so the bytes they hold show up in
 * INFO MEMORY - see TODO 151.
 *
 * Counted in the allocator rather than asked for with `lua_gc(LUA_GCCOUNT)`, because
 * a state belongs to the session using it: reading its collector from the thread that
 * happens to be serving INFO would be a race. An allocator sees every state - the
 * per-session function states, the foreign fill states, and the scratch one a SETF
 * compile check uses - and it sees them live.
 */
static void* luau_alloc(void* ud, void* ptr, size_t osize, size_t nsize) {
    // a state built for a pool also reports into that pool's own counter, so HTTP
    // STATUS can say what its VMs hold without reading a live collector - TODO 181
    auto* local = static_cast<std::atomic<uint64_t>*>(ud);
    if (nsize > osize) {
        statistics::luau_bytes += nsize - osize;
        if (local)
            *local += nsize - osize;
    } else {
        statistics::luau_bytes -= osize - nsize;
        if (local)
            *local -= osize - nsize;
    }
    // through the heap namespace, so the states are inside used_memory and go to
    // valkey's allocator when barch is a module - TODO 342
    return heap::luau_reallocate(ptr, osize, nsize);
}

/**
 * One code allocator and one registered gate for the whole process - TODO 400.
 *
 * `Luau::CodeGen::create(L)` on its own builds a StandaloneCodeGenContext per VM,
 * and that is not the cheap thing it looks like: the constructor runs
 * initHeaderFunctions(), which maps a code block for the native gate and hands
 * every FDE in it to `__register_frame`. So a per-VM context means a mapping and
 * a walk of libgcc's global frame registry every time a state is born, and the
 * matching `__deregister_frame` every time one dies.
 *
 * A shared context is what Luau offers for exactly this. It is built once, on
 * first use, and every state attaches to it - the gate is mapped and registered
 * once for the process instead of once per state, and closing a VM leaves it
 * alone (SharedCodeGenContext::onCloseState does nothing, where the standalone
 * one deletes itself).
 *
 * Deliberately never destroyed. Luau requires every VM using a shared context to
 * be gone before it is torn down, and _barch.so is dlopened by python with states
 * owned by sessions that end in no fixed order, so there is no point in the
 * process where that is known to hold. It is one allocator for the lifetime of
 * the library, so leaking it costs nothing that unloading would recover anyway.
 */
static Luau::CodeGen::SharedCodeGenContext* shared_codegen() {
    static Luau::CodeGen::SharedCodeGenContext* ctx = [] {
        auto owned = Luau::CodeGen::createSharedCodeGenContext();
        return owned.release(); // never destroyed, see above
    }();
    return ctx;
}

/**
 * a state built the way barch builds them, counted and countable.
 *
 * `native` off leaves the state with no codegen context at all, which is right
 * for one that only ever compiles to check a chunk parses - it cannot run native
 * code, so attaching an allocator to it buys nothing. See TODO 400.
 */
lua_State* new_counted_state(std::atomic<uint64_t>* local, bool native) {
    lua_State* L = lua_newstate(luau_alloc, local);
    if (L) {
        ++statistics::luau_states;
        // native codegen needs a context on the VM before any compile() call, and
        // every state built here goes through this one function. See TODO 392.
        if (native) {
            if (auto* ctx = shared_codegen())
                Luau::CodeGen::create(L, ctx);
        }
    }
    return L;
}

void close_counted_state(lua_State* L) {
    if (!L)
        return;
    lua_close(L);
    if (statistics::luau_states > 0)
        --statistics::luau_states;
}


/*
 * A slice that ends inside a coroutine the script made - TODO 875.
 *
 * A slice that ends on the call's own coroutine yields it, back to the pump. A
 * yield on any other thread goes to the script's own coroutine.resume or wrap,
 * which hands the script a yield it never made: no values, and the coroutine
 * still suspended. So there the slice breaks instead. The coroutine library
 * passes a break up through each resume, to the call's coroutine, whose
 * lua_resume answers LUA_BREAK. The pump then resumes the innermost broken
 * thread first and the ones above it after: a resume reads what its coroutine
 * did once that coroutine has done it, and breaks again while it hasn't.
 *
 * Each thread from the one the pump resumes down to the one running must be able
 * to break, or the library raises in the first that can't. So the path is
 * checked before the break, and kept for the pump.
 */
static constexpr int max_break_depth = 256;

// the coroutine `at` is resuming now, or null. A coroutine.wrap function holds it
// as its upvalue; coroutine.resume has it as its first argument
static lua_State* resuming(lua_State* at) {
    lua_Debug ar;
    // the upvalue read is only sound on a C frame
    if (!lua_getinfo(at, 0, "s", &ar) || std::strcmp(ar.what, "C") != 0)
        return nullptr;
    for (int idx : {lua_upvalueindex(1), 1}) {
        lua_State* co = lua_tothread(at, idx);
        if (co && co != at && lua_costatus(at, co) == LUA_CONOR)
            return co;
    }
    return nullptr;
}

// whether a break from L reaches the thread the pump resumes. With `keep`, the
// threads it leaves broken go on ctx.broken
static bool break_path(lua_State* L, run_ctx& ctx, bool keep) {
    lua_State* at = ctx.broken.empty() ? ctx.top : ctx.broken.back();
    if (!at || !lua_isyieldable(L))
        return false;
    const size_t had = ctx.broken.size();
    for (int depth = 0; at != L; ++depth) {
        if (depth == max_break_depth || !lua_isyieldable(at) || !(at = resuming(at))) {
            ctx.broken.resize(had);
            return false;
        }
        if (keep)
            ctx.broken.push_back(at);
    }
    return true;
}

// ends the slice here if it can: a yield on the call's own coroutine, a break
// inside one the script made. False when neither can be done now
static bool end_slice_here(lua_State* L, run_ctx& ctx) {
    if (!lua_isyieldable(L))
        return false;
    if (L == ctx.top) {
        lua_yield(L, 0);
        return true;
    }
    if (!break_path(L, ctx, true))
        return false;
    lua_break(L);
    return true;
}

// resumes a call whose slice may have ended in a coroutine of the script's
static int resume_call(run_ctx& ctx, lua_State* T, int narg) {
    while (!ctx.broken.empty()) {
        int status = lua_resume(ctx.broken.back(), nullptr, narg);
        narg = 0;
        if (status == LUA_BREAK)
            return status;
        // it has done what its parent resumed it for. The parent reads that off it
        ctx.broken.pop_back();
    }
    return lua_resume(T, nullptr, narg);
}

// the thread a resume's values go on: the one that broke or yielded
static lua_State* resumed_thread(const run_ctx& ctx, lua_State* T) {
    return ctx.broken.empty() ? T : ctx.broken.back();
}

/*
 * The call a thread belongs to. A coroutine the script made has no thread data of
 * its own - Luau doesn't copy it - so it uses its call's. Not copied at creation:
 * a coroutine kept in a global outlives its call.
 */
running_call* call_of(lua_State* L) {
    if (auto* rc = static_cast<running_call*>(lua_getthreaddata(L)))
        return rc;
    auto* ctx = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    if (!ctx || !ctx->top || ctx->top == L)
        return nullptr;
    return static_cast<running_call*>(lua_getthreaddata(ctx->top));
}


static void interrupt(lua_State* L, int gc) {
    if (gc >= 0)
        return;
    auto* ctx = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    if (!ctx)
        return;
    if (ctx->deadline && art::now() > ctx->deadline)
        luaL_error(L, "FOREIGN timeout");
    if (ctx->left == 0) {
        // slice is done. yield so the foreign worker can take other jobs.
        // cannot yield from sql.query (C call); the next Lua safepoint will.
        // In a coroutine of the script's it breaks instead - TODO 875
        end_slice_here(L, *ctx);
        return;
    }
    --ctx->left;
}


static std::string script_key(std::string_view k) {
    if (k.empty())
        return {};
    size_t start = 0;
    auto lead = static_cast<unsigned char>(k[0]);
    if (lead == art::tstring || lead == art::tinteger || lead == art::tdouble
        || lead == art::tshort || lead == art::tfloat
        || art::is_composite_lead(lead))
        start = 1;
    size_t end = k.size();
    while (end > start && k[end - 1] == 0)
        --end;
    return {k.data() + start, end - start};
}


struct luau_job {
    lua_State* L{nullptr};
    lua_State* T{nullptr};
    run_ctx ctx{};
    bool requeue{true};
    std::function<void(result)> done;

    ~luau_job() {
        if (L)
            close_counted_state(L);
    }
};

static result take_result(lua_State* T, int status) {
    if (status != LUA_OK) {
        std::string err = lua_tostring(T, -1) ? lua_tostring(T, -1) : "FOREIGN luau";
        if (err.find("timeout") != std::string::npos)
            return {result::status::error, "FOREIGN timeout"};
        if (err.find("FOREIGN ") == 0)
            return {result::status::error, err};
        return {result::status::error, "FOREIGN " + err};
    }
    if (lua_isnil(T, -1))
        return {result::status::missing, {}};
    if (lua_isstring(T, -1)) {
        size_t n = 0;
        const char* s = lua_tolstring(T, -1, &n);
        return {result::status::value, std::string(s, n)};
    }
    return {result::status::error, "FOREIGN call must return a string or nil"};
}

static void pump(std::shared_ptr<luau_job> job, int narg) {
    for (;;) {
        int status = resume_call(job->ctx, job->T, narg);
        narg = 0;
        if (status == LUA_YIELD || status == LUA_BREAK) {
            job->ctx.left = job->ctx.slice ? job->ctx.slice : 1;
            if (job->requeue) {
                enqueue([job] { pump(job, 0); });
                return;
            }
            continue;
        }
        result r = take_result(job->T, status);
        if (job->L) {
            close_counted_state(job->L);
            job->L = nullptr;
            job->T = nullptr;
        }
        auto done = std::move(job->done);
        done(std::move(r));
        return;
    }
}

/**
 * compile the space's fill script, once, on the first fill that needs it.
 *
 * The script is a stored function: looked up in this space and then in the default
 * one, the same order a call resolves in. A file path still works and is tried after
 * the name, since a path is a reference rather than code in the settings store.
 */
static bool fill_bytecode(const key_space_ptr& ks, std::string& err) {
    static std::mutex compiling;
    std::lock_guard lock(compiling);
    if (!ks->luau_bytecode.empty())
        return true;              // another fill got here first

    std::string source;
    if (!barch::functions::source_of(ks, ks->foreign_script, source)
        && !load_source(ks->foreign_script, source, err)) {
        if (err.empty())
            err = "no function or file named " + ks->foreign_script;
        return false;
    }
    if (!compile_source(source, ks->luau_bytecode, err)) {
        ks->luau_bytecode.clear();
        return false;
    }
    return true;
}

void start_resolve(std::string_view space, std::string_view key, uint64_t deadline_ms,
                          bool requeue, std::function<void(result)> done) {
    auto ks = get_keyspace(std::string(space));
    if (!ks) {
        done({result::status::error, "FOREIGN no space"});
        return;
    }
    if (ks->luau_bytecode.empty()) {
        // first fill for this space: find the function the configuration names and
        // compile it. Deferred to here because prepare_luau runs before the space has
        // shards to read a key out of - see TODO 139
        std::string err;
        if (!fill_bytecode(ks, err)) {
            done({result::status::error, "FOREIGN " + err});
            return;
        }
    }
    auto job = std::make_shared<luau_job>();
    job->requeue = requeue;
    job->done = std::move(done);
    job->L = new_counted_state();
    if (!job->L) {
        job->done({result::status::error, "FOREIGN luau state"});
        return;
    }
    lua_State* L = job->L;
    lua_pushlstring(L, space.data(), space.size());
    lua_setfield(L, LUA_REGISTRYINDEX, "barch.foreign.space");
    open_safe(L);
    job->ctx.slice = ks->script_insns();
    job->ctx.left = job->ctx.slice ? job->ctx.slice : 1;
    if (deadline_ms)
        job->ctx.deadline = art::now() + static_cast<int64_t>(deadline_ms);
    lua_callbacks(L)->userdata = &job->ctx;
    set_interrupt(L, interrupt);
    int rc = luau_load(L, "=foreign", ks->luau_bytecode.data(), ks->luau_bytecode.size(), 0);
    if (rc != 0) {
        std::string err = lua_tostring(L, -1) ? lua_tostring(L, -1) : "FOREIGN luau load";
        job->done({result::status::error, err});
        return;
    }
    if (lua_pcall(L, 0, 0, 0) != 0) {
        std::string err = lua_tostring(L, -1) ? lua_tostring(L, -1) : "FOREIGN luau script";
        if (err.find("timeout") != std::string::npos)
            job->done({result::status::error, "FOREIGN timeout"});
        else if (err.find("FOREIGN ") == 0)
            job->done({result::status::error, err});
        else
            job->done({result::status::error, "FOREIGN " + err});
        return;
    }
    job->T = lua_newthread(L);
    job->ctx.top = job->T;
    // a raw get: lua_getglobal honours __index, and a raise here would leave this
    // function as `job` closes L, so what() would read a closed state - TODO 871
    lua_rawgetfield(job->T, LUA_GLOBALSINDEX, "call");
    if (lua_type(job->T, -1) != LUA_TFUNCTION) {
        job->done({result::status::error, "FOREIGN call missing"});
        return;
    }
    auto uk = script_key(key);
    lua_pushlstring(job->T, uk.data(), uk.size());
    lua_pushlstring(job->T, space.data(), space.size());
    pump(std::move(job), 2);
}


/*
 * A stored function's interrupt. The foreign one yields so a slice can go back on the
 * pool; this one has nowhere to yield to yet, so the budget and the deadline are both
 * hard stops. Slicing is what turns this into a park - TODO 98 H.
 */
/*
 * The deadline check reads the clock at one interrupt in this many - TODO 809.
 *
 * Reading it at every one was about 7% of an insert done from Luau, since a loop
 * fires an interrupt every turn. An interrupt comes at least once per loop turn
 * and per call, so a call can only run on past its deadline by this many turns.
 * The first interrupt of a call still reads it.
 */
static constexpr uint32_t interrupts_per_clock_read = 64;

void function_interrupt(lua_State* L, int gc) {
    if (gc >= 0)
        return;
    auto* ctx = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    if (!ctx)
        return;
    // a run from the log stops at the same interrupt on every member - TODO 673
    if (ctx->from_log) {
        if (ctx->log_left == 0)
            luaL_error(L, "FUNCTION ran past function_log_insns, the cap on a function run from the log");
        --ctx->log_left;
    }
    // the caller stopped waiting, and waits for the call to end - TODO 874. The flag
    // stays set, so a pcall that caught this raise gets it again at the next interrupt
    if (ctx->stop && ctx->stop->load(std::memory_order_relaxed))
        luaL_error(L, "%s", call_stopped_error);
    if (ctx->ends() && ctx->clock_wait-- == 0) {
        ctx->clock_wait = interrupts_per_clock_read - 1;
        ++statistics::luau_deadline_checks;
        if (art::now() > ctx->ends()) {
            // the trigger, not the verdict: what the call has actually used decides - TODO 445
            ctx->recheck();
            if (art::now() > ctx->ends() && !ctx->try_overtime()) {
                // and again at every interrupt from here: a pcall that caught this one
                // must not let the call run on to its end - TODO 809
                ctx->clock_wait = 0;
                luaL_error(L, "FUNCTION timeout");
            }
        }
    }
    if (ctx->slice_due && ctx->slice_due->load(std::memory_order_relaxed)) {
        // the watch put the hook back because this native frame has held its
        // thread for a slice's worth of time - the yield an interpreted frame
        // would have made when its instruction count ran out. TODO 398.
        if (!ctx->locked && end_slice_here(L, *ctx)) {
            ctx->slice_due->store(false, std::memory_order_relaxed);
            return;
        }
        if (ctx->hookless) {
            /*
             * Not here: a locked region must not yield at all, and Luau will
             * not yield through a C frame. Put the hook back down rather than
             * leave it up, and let the watch ask again in a few milliseconds.
             *
             * Leaving it up means the frame takes the full interrupt at every
             * loop edge for the rest of the call, which is most of what going
             * hookless buys. An HNSW insert spends nearly all of itself inside
             * barch.store.locked, and paying the hook for that remainder cost
             * 15% of the call - more than the slice was ever going to save.
             */
            set_interrupt(L, nullptr);
            return;
        }
    }
    if (ctx->locked) {
        // a hard cap inside the region, because the usual budget ends in a yield and a
        // yield is exactly what must not happen while a shard lock is held
        if (ctx->locked_left == 0)
            luaL_error(L, "FUNCTION locked region ran too long");
        --ctx->locked_left;
        return;
    }
    if (ctx->left == 0) {
        // the slice is spent. Yield so the pool thread can take other work and this
        // script comes back on the next one - the deadline is what ends a runaway,
        // not the slice. A call that is not on a coroutine has nowhere to yield to,
        // which is the synchronous path below, so there the slice is a cap. Inside a
        // coroutine the script made, it breaks instead - TODO 875
        if (end_slice_here(L, *ctx))
            return;
        if (ctx->on_coroutine) {
            /*
             * There is a coroutine, but a C frame is on the stack - the
             * iterator of a `for tok in string.gmatch(...)`, a metamethod,
             * pcall - and Luau will not yield through one. The slice is over,
             * but this is the wrong moment for it, so come back at the next
             * firing: by then the frame has normally returned and the yield
             * goes through.
             *
             * Raising instead is what the code did, and it cost a third of
             * the queries in an accurate-tuned HNSW search over 7,344 points
             * (167 of 500), because that search spends much of its time
             * inside string.gmatch. The error even named the wrong thing -
             * the slice was not what ran out, the chance to yield was. A call
             * that never leaves such a frame is the deadline's business,
             * which is what ends a runaway anyway. TODO 397.
             */
            return;
        }
        luaL_error(L, "FUNCTION instruction budget exceeded");
    }
    --ctx->left;
}


/**
 * Preemption for hookless native frames - the deadline watch.
 *
 * With the interrupt hook down a runaway native frame never returns, so no
 * post-resume check can catch it (probed 21-09-2026: wedged a worker until
 * killed; cross-thread lua_break does not preempt either - L->status is
 * only consulted at safepoints, and with no hook there are none). So
 * something outside the call has to put the hook pointer back on the state
 * once the deadline has passed. A hookless frame pays one cmp per edge
 * while the pointer is null; with it non-null the next edge takes the full
 * interrupt, which raises the timeout. Preemption latency is one loop edge
 * past the deadline.
 *
 * One thread does that for the whole server, waiting on a queue ordered by
 * deadline. It used to be a detached thread per hookless resume that slept
 * the whole deadline whether the call ended or not, which meant a live
 * thread per call made in the last function_deadline_ms: 2000 AOT calls
 * left 2000 threads, AOT came out slower than the interpreter for a short
 * function, and once the thread limit was reached std::thread threw and
 * took the process down with nothing in the log - TODO 393, 394.
 *
 * An entry holds the session's function_states_ptr, so the state it is
 * going to write to cannot be closed under it, and a `done` flag the call
 * sets when it comes back out of lua_resume. A late firing - the flag set
 * just after the watch looked at it - writes the same hook the resume just
 * restored, so the worst it can do is cost the next call on that session
 * its hookless run.
 */
/**
 * How soon the watch looks again when a slice firing did not produce a yield.
 * The frame was somewhere Luau will not yield from - a C call, a metamethod -
 * so the hook is on and the ask is standing; this is only how often to check
 * that it is still standing. A frame that yielded has disarmed its entry by
 * then and the retry finds nothing to do.
 */
enum { slice_retry_ms = 5 };

struct watch_entry {
    /** the session's state cache: held, so `L` stays open while this exists */
    function_states_ptr cache{};
    lua_State* L{nullptr};
    /** when this call has to stop, or 0 for no deadline - a firing before it
     *  is a slice, a firing at it is the timeout */
    int64_t deadline{0};
    /** the watch has put the hook back so the frame can yield its slice: shared
     *  with the job's run_ctx, which reads it - TODO 430 */
    std::shared_ptr<std::atomic<bool>> slice_due{};
    /** the call came back and the hook is its own business again */
    std::atomic<bool> done{false};
};
using watch_ptr = std::shared_ptr<watch_entry>;

class deadline_watch {
public:
    static deadline_watch& get() {
        // leaked on purpose: the thread outlives main's static teardown, and
        // nothing is gained by tearing the queue down at exit
        static auto* one = new deadline_watch();
        return *one;
    }

    /**
     * Put the hook back on `cache`'s state at `at`, unless disarmed first.
     * A firing before `deadline` is a slice - the frame is asked to yield and
     * the entry comes back for the next one; from the deadline on it puts the
     * hook back every slice_retry_ms until the call is disarmed, since the
     * interrupt may find the call has budget left. `deadline` 0 means there is
     * no timeout and only slices.
     */
    watch_ptr arm(const function_states_ptr& cache, lua_State* L, int64_t deadline,
                  int64_t at, std::shared_ptr<std::atomic<bool>> slice_due) {
        auto w = std::make_shared<watch_entry>();
        w->cache = cache;
        w->L = L;
        w->deadline = deadline;
        w->slice_due = std::move(slice_due);
        // the swept entries, released once mu is let go: the last reference to a
        // session closes its Luau state, and its userdata go with it - TODO 877
        std::vector<watch_ptr> swept;
        std::unique_lock<std::mutex> lk(mu);
        start();
        // A disarmed entry otherwise sits here until its deadline, which with
        // a long function_deadline_ms and a busy server is a lot of dead
        // weight. Sweep once the queue has grown past a few thousand: the
        // scan is O(n) but happens once per n inserts, and a sweep that frees
        // nothing just moves the mark up.
        if (queue.size() >= sweep_at) {
            for (auto i = queue.begin(); i != queue.end();) {
                if (i->second->done.load(std::memory_order_acquire)) {
                    swept.push_back(std::move(i->second));
                    i = queue.erase(i);
                } else {
                    i = std::next(i);
                }
            }
            sweep_at = queue.size() * 2 + 4096;
        }
        bool first = queue.empty() || at < queue.begin()->first;
        queue.emplace(at, w);
        lk.unlock();
        if (first)
            cv.notify_one();
        return w;
    }

    /** the call is out of the resume: whatever happens now is not the watch's */
    static void disarm(watch_ptr& w) {
        if (w) {
            w->done.store(true, std::memory_order_release);
            w.reset();
        }
    }

private:
    deadline_watch() = default;

    void start() {
        if (running)
            return;
        running = true;
        std::thread([this] { run(); }).detach();
    }

    void run() {
        std::unique_lock<std::mutex> lk(mu);
        for (;;) {
            if (queue.empty()) {
                cv.wait(lk);
                continue;
            }
            auto it = queue.begin();
            int64_t wait = it->first - art::now();
            if (wait > 0) {
                cv.wait_for(lk, std::chrono::milliseconds(wait));
                continue;
            }
            watch_ptr w = it->second;
            int64_t at = it->first;
            queue.erase(it);
            lk.unlock();
            barch::test::jitter("deadline_watch"); // TODO 805
            int64_t again = 0;
            if (!w->done.load(std::memory_order_acquire)) {
                // before the deadline this is a slice: ask for the yield, and
                // come back for the next one in case the frame is somewhere it
                // cannot yield from. The hook goes back either way - that is
                // what makes the next loop edge take the interrupt at all.
                bool timeout = w->deadline != 0 && at >= w->deadline;
                if (!timeout && w->slice_due)
                    w->slice_due->store(true, std::memory_order_relaxed);
                again = at + slice_retry_ms;
                if (!timeout && w->deadline != 0 && again > w->deadline)
                    again = w->deadline;
                // Past the deadline too, until the call comes back. The interrupt
                // decides whether it's a timeout, and it may not be: the deadline
                // counts CPU time, so on a crowded CPU recheck() finds budget left
                // and moves it on, and a frame that can't yield then puts the hook
                // back down. Dropped here, that call spun for ever - TODO 802.
                set_interrupt(w->L, function_interrupt);
            }
            // The session reference goes before the lock. It can be the last one:
            // the client went while this entry was queued. Then this closes the
            // session's Luau state, and its userdata go with it - a space handle
            // can hold the last reference to a dropped space. None of that may
            // run under mu, which every hookless resume takes to arm - TODO 877.
            if (!again)
                w.reset();
            lk.lock();
            if (again)
                queue.emplace(again, std::move(w));
        }
    }

    std::mutex mu;
    std::condition_variable cv;
    /** by absolute deadline, in the art::now() scale */
    std::multimap<int64_t, watch_ptr> queue;
    /** how big the queue has to get before the next sweep of disarmed entries */
    size_t sweep_at{4096};
    bool running{false};
};


/*
 * How long a native frame may hold its thread before it has to yield.
 *
 * An interpreted frame yields when it has run its slice of instructions. A
 * hookless one has no interrupt firing to count with, so the deadline watch
 * counts time instead, and the scale comes from the interpreter: a million
 * instructions measured 22-23 ms on the box this was written on, so the
 * default million-instruction slice is about 20 ms, and a slice set to
 * something else scales with it. The inline first slice (inline_insns) lands
 * on the floor of a millisecond, which suits what it is for - that one runs
 * on the session's own thread and is meant to be short. An instruction has no
 * fixed cost, so this is a rough equivalent and not a promise. TODO 398.
 */
static int64_t native_slice_ms(uint64_t insns) {
    int64_t ms = (int64_t) (insns / 50000);   // 1,000,000 -> 20 ms
    if (ms < 1)
        ms = 1;
    if (ms > 60000)
        ms = 60000;
    return ms;
}

/** a coroutine to run a call on, reused where there is one going spare */
void take_thread(space_state& st, lua_State*& T, int& ref) {
    if (!st.free_threads.empty()) {
        auto got = st.free_threads.back();
        st.free_threads.pop_back();
        T = got.first;
        ref = got.second;
        lua_resetthread(T);          // back to empty, whatever the last call left
        return;
    }
    T = lua_newthread(st.L);
    ref = lua_ref(st.L, -1);
    lua_pop(st.L, 1);
}

/** done with it: keep it for the next call, or let it go if there are enough */
void give_thread(space_state& st, lua_State* T, int ref) {
    if (!T || ref == LUA_NOREF)
        return;
    if (st.free_threads.size() >= max_free_threads) {
        lua_unref(st.L, ref);
        return;
    }
    // reset on the way out as well as in: a thread that errored is holding the error
    // object, and nothing should keep that alive until it is next used
    lua_resetthread(T);
    st.free_threads.push_back({T, ref});
}


/*
 * A call that is running in slices.
 *
 * It owns everything the script can reach, because the command that started it has
 * already answered "parked" and gone: the loader, the command runner and the store
 * access were built on that stack and would be dangling by the first yield. The cache
 * is held too, so the session's state for this space cannot be closed underneath a
 * suspended coroutine.
 */
std::string current_space(lua_State* L) {
    auto* rc = call_of(L);
    return rc ? rc->space : std::string{};
}



} // namespace detail

/**
 * A call stopped mid-flight while something outside Lua finishes - TODO 186.
 *
 * `resume` holds the job alive for as long as it is parked; nothing else does,
 * because the pool thread that was running it has gone back to the queue. The
 * reference is dropped the moment the call is resumed, so the cycle between the
 * job and its parking slot lasts exactly as long as the wait.
 *
 * `push` is filled by whichever thread the work completed on, and is only ever
 * *called* on the pool thread that resumes the coroutine. That is what keeps a
 * completion off the lua_State.
 */
struct parked_call {
    std::mutex mu;
    push_results push{};
    std::function<void()> resume{};
    /** the work has finished and `push` is set */
    bool done{false};
    /** the pump has seen the yield, so a completion has to wake it */
    bool waiting{false};
    /** the call parked in overtime, so it comes back on the overtime pool - TODO 836 */
    bool overtime{false};
};

struct call_job {
    function_states_ptr cache{};
    space_state* st{nullptr};
    /** points at this job's coroutine for as long as the call runs */
    running_call scope{};
    /*
     * held, not copied. Copying the four interface objects into every job meant a
     * dozen std::function constructions and as many destructions per call, which the
     * profile showed as most of the reference counting and a good share of the
     * malloc traffic. The connection owns these and outlives the job, so one refcount
     * does the whole job. See TODO 98 F5.
     */
    call_interface_ptr iface{};
    /** the call as its script sees it, built from iface - TODO 430 */
    call_ctx cctx{};
    lua_State* T{nullptr};
    int tref{LUA_NOREF};
    run_ctx ctx{};
    function_done done{};
    /** so a C function holding only the lua_State can take a strong reference */
    std::weak_ptr<call_job> self{};
    /** set while this call is waiting on something that is not Lua - TODO 186 */
    parked_call_ptr parked{};
    /** when that wait started, so the deadline can be moved past it */
    int64_t parked_since{0};
    /** when a budget yield put it back on the pool, so waiting for a worker isn't
     *  charged to its deadline either - TODO 434 */
    int64_t queued_since{0};
    /** it has been on the pool, so its slices are measured - TODO 445. The inline
     *  first slice, run by the thread that asked, isn't */
    bool pooled{false};
    /** every slice on the thread that asked, and no parking - TODO 552, see
     *  call_limits::inline_only */
    bool inline_only{false};
    /** the entry point compiled native (SETF ... AOT) - the interrupt hook
     *  stays down across lua_resume for these, since native loop edges check
     *  the hook themselves and a null hook skips even the check */
    bool aot_native{false};
    /** what will put the interrupt hook back if this resume runs past its
     *  deadline - see deadline_watch. Armed per hookless resume, disarmed
     *  the moment lua_resume returns. */
    detail::watch_ptr watch{};
    /** finish_job has run, so a throw that comes after is not the call's - TODO 873 */
    bool finished{false};
    /** an HTTP handler, run by http_vm_start - TODO 911. It holds a VM slot only
     *  while a segment runs, and `resume_via` takes the slot back for the next */
    bool http{false};
    /** the req/res box number its boxes carry, so a resume can take it up */
    uint64_t http_gen{0};
    std::function<void(std::function<void(bool live)>)> resume_via{};

    void release() {
        if (st && st->L && tref != LUA_NOREF) {
            // cleared before the coroutine goes back in the pool, so the next call
            // cannot read this job's space out of a thread it inherited
            if (T)
                lua_setthreaddata(T, nullptr);
            detail::give_thread(*st, T, tref);
            tref = LUA_NOREF;
        }
        T = nullptr;
    }
};


namespace detail {

static void finish_job(const std::shared_ptr<call_job>& job, bool ok, Variable out,
                       std::string err) {
    job->finished = true;
    // a handler's outcome is the HTTP status, not a function's
    if (!ok && !job->http) {
        // script outcomes for monitoring - see statistics.h. The interrupt
        // raises "FUNCTION timeout" (function_interrupt), so a timeout is
        // matched on that substring; anything else failed is an error.
        if (err.find("FUNCTION timeout") != std::string::npos)
            ++statistics::function_timeouts;
        else
            ++statistics::function_errors;
    }
    // nothing is running on the coroutine any more, so nothing may reach in
    // and set a hook for it - the pump has already done this for its own
    // resume, and this covers any other way a job can end
    deadline_watch::disarm(job->watch);
    job->ctx.slice_due = nullptr;
    // nothing of the call to clear from the state: call_scope put it there for each
    // resume and took it away again after - TODO 430
    lua_callbacks(job->st->L)->userdata = nullptr;
    job->release();
    // an error or a return while parked would otherwise leave the job holding the
    // slot and the slot holding the job
    if (job->parked) {
        job->parked->resume = nullptr;
        job->parked.reset();
    }
    // the overtime place goes back before the answer, so the next call can have it
    job->ctx.overtime.reset();
    auto done = job->done;
    job->done = nullptr;
    if (done)
        done(ok, std::move(out), std::move(err));
}

/*
 * A call that raised past Luau's own catch - TODO 873. Luau raises as a C++
 * exception wherever a push or a read runs outside a pcall. One that left the
 * pump skipped finish_job. The job was freed while the state's interrupt
 * userdata and the coroutine's thread data still pointed into it, so the
 * session's next compile ran the interrupt on freed memory. The coroutine and
 * its ref were never given back. On the pool, done was never called: the worker
 * logged the raise and the client waited out its own timeout.
 *
 * A catch, not a destructor. done can throw, and a throw from a destructor
 * during unwinding is std::terminate. A destructor has no message to give.
 */
static void finish_raised(const std::shared_ptr<call_job>& job, const char* what) {
    // a raise inside the hookless window left the hook down
    if (job->ctx.hookless) {
        job->ctx.hookless = false;
        set_interrupt(job->st->L, function_interrupt);
    }
    // copied before the coroutine goes: what() reads the message off its stack -
    // TODO 575
    std::string err = what && *what ? what : "FUNCTION luau call";
    // Dropped, not pooled. A raise with no pcall to catch it unwinds past Luau's
    // own bookkeeping: the coroutine keeps its open frame and stays marked active,
    // and lua_resetthread asserts on both. Unreferenced, the collector frees it
    if (job->T)
        lua_setthreaddata(job->T, nullptr);
    if (job->tref != LUA_NOREF)
        lua_unref(job->st->L, job->tref);
    job->tref = LUA_NOREF;
    job->T = nullptr;
    finish_job(job, false, Variable(nullptr), std::move(err));
}

static void pump_slices(const std::shared_ptr<call_job>& job, int narg);

/** pump_slices, with every way out finishing the job - TODO 873 */
static void pump_call(std::shared_ptr<call_job> job, int narg) {
    try {
        pump_slices(job, narg);
    } catch (const std::exception& e) {
        // out of done itself: the call is answered, and the throw is the caller's
        if (job->finished)
            throw;
        finish_raised(job, e.what());
    }
}

static void pump_slices(const std::shared_ptr<call_job>& job, int narg) {
    // waiting for a worker is not running, the same as a park is not - TODO 434
    if (job->queued_since) {
        if (job->ctx.deadline)
            job->ctx.deadline += art::now() - job->queued_since;
        job->queued_since = 0;
    }
    for (;;) {
        // a call coming back from a park pushes what it was waiting for first, and
        // those values become the return values of the C function that yielded
        if (job->parked) {
            auto p = job->parked;
            push_results push;
            {
                std::lock_guard<std::mutex> lk(p->mu);
                if (!p->done)
                    return; // not our turn: the completion will bring it back
                push = std::move(p->push);
            }
            // drop the mutual hold before running anything else
            job->parked.reset();
            p->resume = nullptr;
            // a handler's waits were cut at the wall ceiling, and one that comes
            // back at it ends there, as a blocking wait does - TODO 911
            if (job->http && job->ctx.wall && art::now() >= job->ctx.wall) {
                finish_job(job, false, Variable(nullptr), "FUNCTION timeout");
                return;
            }
            /*
             * Waiting is not running. The deadline is there to stop a script
             * computing forever, and while parked this one was not computing at
             * all - it was a suspended coroutine and a socket. Charging it for
             * the wait would cap every request at the deadline, which for the
             * default 1000ms means no useful HTTP call at all. The request has
             * its own timeout for bounding the wait.
             */
            if (job->ctx.deadline && job->parked_since)
                job->ctx.deadline += art::now() - job->parked_since;
            job->parked_since = 0;
            // on the thread that parked, which may be a coroutine of the script's
            narg = push ? push(resumed_thread(job->ctx, job->T)) : 0;
        }
        // told to stop while queued or parked: end here, without another slice - TODO 874
        if (job->ctx.stop && job->ctx.stop->load(std::memory_order_relaxed)) {
            finish_job(job, false, Variable(nullptr), call_stopped_error);
            return;
        }
        // the interrupt reads its budget through the state's callback userdata, and
        // the state is shared with anything else this session might run, so it is
        // pointed at this job every time the job is resumed rather than once.
        //
        // Hookless native frames: the hook pointer goes null across the
        // resume - native loop edges check cb.interrupt themselves and a
        // null hook skips even the check (~530ms on the 30M bench). No
        // can_park scan and no per-frame decision: a compiled hookless frame
        // CAN call lua_yield (a C function returning -1 exits native via
        // CALL_FALLBACK_YIELD), and the pump already tells the two yields
        // apart by job->parked - a budget yield has none, a parking yield
        // has one. So every native frame runs hookless, and parking still
        // works: the C++ park path (park_call) yields through the native
        // frame the same way it yields through an interpreted one.
        //
        // Preemption is the deadline watch: it puts the hook back on the
        // live state once the deadline has passed, and the next native edge
        // raises FUNCTION timeout. No deadline means nothing armed and no
        // preemption gap: without a deadline there is nothing to enforce,
        // and a hookless frame that never returns is then a hung worker
        // with no way home (probed 21-09-2026, wedged until killed;
        // cross-thread lua_break does not preempt - L->status is only read
        // at safepoints, and hookless has none).
        auto* cbs = lua_callbacks(job->st->L);
        // a run from the log keeps the hook, so every frame is counted - TODO 673
        const bool hookless = job->aot_native && !job->ctx.from_log;
        cbs->userdata = &job->ctx;
        // Hookless with a watch armed: the hook pointer goes null across
        // the resume, and the watch thread puts it back if the deadline
        // passes while the frame is still running. The next native loop
        // edge then takes the full interrupt and raises FUNCTION timeout.
        //
        // Armed per resume, not per job. A park comes back out of
        // lua_resume like any other return, the hook goes back on (nothing
        // native is running while a call waits on a socket), and the resume
        // after the park arms a fresh watch on the deadline the wait moved.
        if (hookless) {
            // whichever comes first: the slice this frame may hold its thread
            // for, or the deadline. Every hookless resume gets a watch now -
            // with no deadline there are only slices, and a native call that
            // would otherwise hold a pool thread to the end gives it back on
            // the same terms an interpreted one does. TODO 398.
            int64_t at = art::now() + native_slice_ms(job->ctx.left);
            if (job->ctx.ends() && job->ctx.ends() < at)
                at = job->ctx.ends();
            // down before the watch is armed: a firing that's already due could
            // otherwise put the hook back before this line took it away - TODO 802
            job->ctx.hookless = true;
            set_interrupt(job->st->L, nullptr);
            // a fresh flag per resume, shared with the entry it arms - TODO 430
            job->ctx.slice_due = std::make_shared<std::atomic<bool>>(false);
            job->watch = deadline_watch::get().arm(job->cache, job->st->L,
                                                   job->ctx.ends(), at, job->ctx.slice_due);
        }
        int status;
        {
            // what the script reaches, for this resume and no longer - TODO 430
            call_scope in_call(job->st, &job->cctx);
            job->ctx.begin_slice(job->pooled);
            // inside the slice: the wall clock moves and the thread's CPU clock
            // doesn't, as when a crowded CPU takes the thread away - TODO 805
            barch::test::jitter("function");
            status = resume_call(job->ctx, job->T, narg);
            job->ctx.end_slice();
        }
        narg = 0;
        // Whatever came back - a return, an error, a budget yield, a park -
        // nothing native is running now, so the hook goes back and the watch
        // is disarmed. It is named here rather than read off the state at
        // the top of the resume: a parked call left it null there, so saving
        // it and putting it back cost the session its hook for good.
        //
        // A hookless frame that returns past its deadline with nothing armed
        // (no deadline configured) is reported here at whole-call
        // granularity. With a watch the timeout already raised inside the
        // frame and this check is a no-op. The error string matches the
        // interrupt's so the counters (TODO 391) see a timeout either way.
        if (hookless) {
            deadline_watch::disarm(job->watch);
            job->ctx.slice_due = nullptr;
            job->ctx.hookless = false;
            set_interrupt(job->st->L, function_interrupt);
            // with room in overtime, a call that finished late keeps its answer - TODO 836
            if (status == LUA_OK && job->ctx.ends() && art::now() > job->ctx.ends()
                && (job->ctx.recheck(), art::now() > job->ctx.ends())
                && !job->ctx.try_overtime()) {
                finish_job(job, false, Variable(nullptr), "FUNCTION timeout");
                return;
            }
        }
        // a break is a yield from inside a coroutine of the script's - TODO 875
        if (status == LUA_YIELD || status == LUA_BREAK) {
            /*
             * Two yields reach here and they are not the same thing. The budget
             * yield means "this slice is used up", and the job goes straight back
             * on the queue. A parked yield means "waiting on something that is not
             * Lua", and it must NOT be requeued - the completion does that, or the
             * coroutine gets resumed twice.
             */
            const bool park_yielded = job->ctx.park_yielded;
            job->ctx.park_yielded = false;
            if (job->parked) {
                auto p = job->parked;
                std::unique_lock<std::mutex> lk(p->mu);
                switch (luau_park_core::yield(true, park_yielded, p->done)) {
                case luau_park_core::on_yield::wait:
                    p->waiting = true;
                    ++statistics::luau_parks; // TODO 805
                    return;
                case luau_park_core::on_yield::resume_now:
                    // it finished before the yield was even seen; no lost wake-up
                    continue;
                case luau_park_core::on_yield::budget:
                case luau_park_core::on_yield::stale_budget:
                    break;
                }
                /*
                 * A park that never yielded: park_call raised, or what came after it
                 * did, and the script caught it. Dropped, and this is a budget
                 * yield. Its work may still finish, and finds `waiting` false, so it
                 * resumes nothing - TODO 942.
                 */
                lk.unlock();
                p->resume = nullptr;
                job->parked.reset();
                job->parked_since = 0;
            }
            ++statistics::luau_slices; // TODO 805
            // past the inline slice now, so back to the configured one
            job->ctx.left = job->ctx.slice ? job->ctx.slice : 1;
            /*
             * Called from another script, which is waiting for it right here and
             * can't be parked: the next slice runs on this thread too - TODO 552.
             * On the pool, the caller gave up on a call still running, and the job
             * went on using the caller's interface after the caller had gone.
             */
            // a handler holds its VM slot for the slice, so it goes on here - TODO 911
            const auto at = luau_park_core::next_slice(job->inline_only, job->http,
                                                       static_cast<bool>(job->ctx.overtime));
            if (at == luau_park_core::slice_at::here)
                continue;
            job->queued_since = art::now();
            job->pooled = true;
            // past its deadline it goes on in the overtime pool - TODO 836
            if (at == luau_park_core::slice_at::overtime_pool)
                enqueue_overtime([job] { pump_call(job, 0); });
            else
                enqueue([job] { pump_call(job, 0); });
            return;
        }
        if (status != LUA_OK) {
            std::string err = lua_tostring(job->T, -1) ? lua_tostring(job->T, -1)
                                                       : "FUNCTION luau call";
            finish_job(job, false, Variable(nullptr), err);
            return;
        }
        // a handler answers through its response, so what it returned is not read
        if (job->http) {
            finish_job(job, true, Variable(nullptr), {});
            return;
        }
        Variable out;
        std::string err;
        /*
         * A script that returns nothing leaves an empty stack, and 0 is not a valid
         * index - `lua_type(L, 0)` walks off the frame rather than answering LUA_TNONE,
         * which took the server down for a `function call(k) end` with nothing else
         * wrong with it. Nothing to return is a null reply, the same answer redis gives
         * a script with no return statement.
         */
        bool ok = true;
        if (lua_gettop(job->T) == 0) {
            out = Variable(nullptr);
        } else {
            /*
             * Under a pcall - TODO 872. to_variable reads {err=} and {ok=} with
             * lua_getfield, which runs the table's __index. A raise there used to
             * leave pump_call as a C++ exception, past finish_job: the error was
             * not counted, the freed job stayed in the state's userdata, and a
             * pooled call never answered - TODO 873. pump_call catches that now;
             * this keeps the coroutine fit to reuse. The value goes in as the
             * frame's first argument.
             */
            lua_pushvalue(job->T, -1);
            ok = protected_read(job->T, 1, 0, err, [&](lua_State* T) {
                return to_variable(T, 1, out, err, 0);
            });
        }
        finish_job(job, ok, std::move(out), std::move(err));
        return;
    }
}

// what park_call needs, asked without parking - TODO 731

} // namespace detail

function_states_ptr make_function_states() {
    return std::make_shared<function_states>();
}

function_states_ptr make_function_states(std::shared_ptr<std::atomic<uint64_t>> into) {
    auto fs = std::make_shared<function_states>();
    fs->bytes = std::move(into);
    return fs;
}


bool lock_wait::can_park() const {
    using namespace detail;
    if (on_fiber)
        return true;
    if (!L || !lua_isyieldable(L))
        return false;
    auto* ctx = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    if (ctx && ctx->locked)
        return false;           // a locked region must never yield
    auto* rc = call_of(L);
    if (!rc || !rc->owner)
        return false;
    auto job = rc->owner->self.lock();
    return job && !job->inline_only && (L == job->ctx.top || break_path(L, job->ctx, false));
}

parked_call_ptr park_call(lua_State* L) {
    using namespace detail;
    if (!lua_isyieldable(L))
        return nullptr;
    auto* rc = call_of(L);
    if (!rc || !rc->owner)
        return nullptr;
    auto job = rc->owner->self.lock();
    if (!job)
        return nullptr;
    // its caller waits for it on this thread, so it waits the blocking way too -
    // TODO 552
    if (job->inline_only)
        return nullptr;
    // in a coroutine of the script's the park is a break, which has to reach the
    // call's own coroutine - TODO 875
    if (L != job->ctx.top && !break_path(L, job->ctx, false))
        return nullptr;
    auto p = std::make_shared<parked_call>();
    if (job->http) {
        /*
         * A handler's state belongs to a VM slot, and other requests run on it
         * while this one waits. So the resume goes through the slot: it runs once
         * the slot is free, and with the request's boxes good on that thread only
         * while the response is still there - TODO 911.
         */
        auto via = job->resume_via;
        p->resume = [job, via] {
            via([job](bool live) {
                crow_http_resume_call(live ? job->http_gen : 0);
                struct end_call {
                    ~end_call() { crow_http_end_call(); }
                } ended;
                pump_call(job, 0);
            });
        };
    } else {
        p->resume = [job] { pump_call(job, 0); };
    }
    p->overtime = static_cast<bool>(job->ctx.overtime);     // TODO 836
    job->pooled = true;     // it comes back on the pool - TODO 445
    job->parked = p;
    job->parked_since = art::now();
    // a raise from here to park_yield, which a script's pcall can catch - TODO 942
    barch::test::fault("park");
    return p;
}

int park_yield(lua_State* L) {
    using namespace detail;
    auto* ctx = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    if (!ctx || L == ctx->top) {
        if (ctx)
            ctx->park_yielded = true; // TODO 942
        return lua_yield(L, 0);
    }
    // park_call checked the path, and no Luau has run since
    if (!break_path(L, *ctx, true))
        luaL_error(L, "FUNCTION a coroutine couldn't park");
    ctx->park_yielded = true; // TODO 942
    return lua_break(L);
}

uint64_t blocking_wait_cap(lua_State* L) {
    using namespace detail;
    auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    if (!rc || !rc->wall)
        return 0;
    int64_t left = rc->wall - art::now();
    return left > 1 ? static_cast<uint64_t>(left) : 1;
}

uint64_t park_wait_cap(lua_State* L) {
    using namespace detail;
    auto* rc = call_of(L);
    if (!rc || !rc->owner)
        return 0;
    auto job = rc->owner->self.lock();
    return job && job->http ? blocking_wait_cap(L) : 0;
}

int64_t blocking_wait_start(lua_State*) {
    using namespace detail;
    return art::now();
}

void blocking_wait_end(lua_State* L, int64_t started) {
    using namespace detail;
    auto* rc = static_cast<run_ctx*>(lua_callbacks(L)->userdata);
    if (!rc)
        return;
    const int64_t now = art::now();
    // unless the slice is measured, which covers it already - TODO 445
    rc->credit_wait(now - started);
    // a wait cut short at the ceiling ends the call, the way a parked one that
    // resumes past it does
    if (rc->wall && now >= rc->wall)
        luaL_error(L, "FUNCTION timeout");
}

void complete_call(const parked_call_ptr& parked, push_results push) {
    using namespace detail;
    if (!parked)
        return;
    std::function<void()> resume;
    bool overtime = false;
    {
        std::lock_guard<std::mutex> lk(parked->mu);
        const auto act = luau_park_core::complete(parked->done, parked->waiting);
        if (act == luau_park_core::on_complete::ignore)
            return; // a second completion is a no-op, not a second resume
        parked->done = true;
        parked->push = std::move(push);
        if (act == luau_park_core::on_complete::record)
            return; // the pump has not parked it yet and will see `done` itself
        parked->waiting = false;
        resume = parked->resume;
        overtime = parked->overtime;
    }
    if (resume && overtime)
        enqueue_overtime(std::move(resume));
    else if (resume)
        enqueue(std::move(resume));
}

void start_function(const std::string& space, const std::string& name,
                    const call_interface_ptr& iface,
                    const heap::vector<std::string>& args, const call_limits& limits,
                    const function_states_ptr& cache,
                    const function_done& done, const std::string& entry) {
    using namespace detail;
#ifdef BARCH_HAS_WASM
    // a module this session already runs goes straight to it, without a Luau state
    // or a read of the source - TODO 820
    if (cache && cache->wasm &&
        start_wasm_warm(cache->wasm, barch::functions::compiled_key(space, name), name, iface,
                        args, limits, done, entry))
        return;
#endif
    auto job = std::make_shared<call_job>();
    job->cache = cache ? cache : make_function_states();
    job->iface = iface;
    job->done = done;

    job->st = state_for(*job->cache);
    if (!job->st) {
        done(false, Variable(nullptr), "FUNCTION luau state");
        return;
    }
    space_state* st = job->st;
    job->scope.space = space;
    job->scope.running = job->iface->running_in;
    job->cctx.set(job->iface);
    // require needs the loader while the function compiles - TODO 430
    std::optional<call_scope> loading;
    loading.emplace(st, &job->cctx);

    // into the state's buffer rather than a new string: assign keeps the capacity,
    // so the warm path allocates nothing
    std::string& key = st->keybuf;
    key.clear();
    key.append(space);
    key.push_back('\0');
    key.append(name);
    auto it = st->functions.find(key);
    // a write since this was compiled means the source may have changed under it,
    // so the copy goes and it is built again - see TODO 243
    if (it != st->functions.end() && published_since(key, it->second)) {
        drop_compiled(st->L, it->second);
        if (statistics::luau_functions > 0)
            --statistics::luau_functions;
        st->functions.erase(it);
        it = st->functions.end();
    }
    if (it == st->functions.end()) {
        if (st->functions.size() >= max_cached_functions) {
            // the backstop. Give the refs back rather than just dropping the map, or
            // the state - which now outlives any one space - holds every one of them
            for (auto& e : st->functions)
                drop_compiled(st->L, e.second);
            if (statistics::luau_functions >= st->functions.size())
                statistics::luau_functions -= st->functions.size();
            st->functions.clear();
        }
        std::string source;
        if (!job->iface->load("", name, false, source)) {
            loading.reset();
            done(false, Variable(nullptr), "no such function");
            return;
        }
        // a WebAssembly module: the WASM driver runs it - TODO 820
        if (is_wasm_module(source)) {
            loading.reset();
#ifdef BARCH_HAS_WASM
            if (!job->cache->wasm)
                job->cache->wasm = make_wasm_sessions();
            start_wasm(job->cache->wasm, key, name, source, iface, args, limits, done, entry);
#else
            done(false, Variable(nullptr), "FUNCTION wasm not built in (ADD_WASM)");
#endif
            return;
        }
        compiled c;
        std::string err;
        // the AOT flag is stored beside the source (SETF ... AOT) and honoured
        // here, on the call path - the SETF check path compiles only to
        // validate. Whatever the flag said, a native miss still runs
        // interpreted.
        bool want_aot = barch::functions::wants_aot(key);
        bool built = compile_into(*st, key, source, c, err, false, want_aot);
        loading.reset();
        if (!built) {
            done(false, Variable(nullptr), err);
            return;
        }
        it = st->functions.emplace(key, c).first;
        ++statistics::luau_functions;
    }
    loading.reset();
    const compiled& c = it->second;
    // native entry point (SETF ... AOT, compiled on the cold path above).
    // Whichever entry is being called: the chunk was compiled from its root,
    // so a transport method is native the same as call() is, and it runs
    // hookless the same way - cheap now that the deadline watch is one
    // thread rather than one per call. TODO 394, 396.
    //
    // Every native call runs hookless, whether or not it can park: parking yields
    // through a native frame the same way it does through an interpreted one, and
    // the deadline watch ends slices and the call - see pump_call. A module it
    // requires runs hookless with it. The scan that used to decide this, and the
    // flags it set, are gone - TODO 568.
    job->aot_native = c.aot;

    /*
     * Which function in the chunk actually runs - TODO 188.
     *
     * With no entry it is `call`, as it always was. An entry names one of the
     * commands a resp transport() exposed, and the arity checked is the one that
     * method declared rather than the chunk's own.
     */
    int entry_ref = c.fn;
    int want_arity = c.arity;
    if (!entry.empty()) {
        auto m = c.methods.find(entry);
        if (m == c.methods.end()) {
            done(false, Variable(nullptr), "no such method '" + entry + "'");
            return;
        }
        entry_ref = m->second;
        auto a = c.method_arity.find(entry);
        want_arity = a == c.method_arity.end() ? 0 : a->second;
    }

    int given = (int) args.size();
    if ((want_arity > 0 && given != want_arity) || (want_arity < 0 && given < -want_arity)) {
        done(false, Variable(nullptr),
             "wrong number of arguments for '" + (entry.empty() ? name : entry) + "'");
        return;
    }

    // so a C function with only the lua_State can find this call and park it
    job->self = job;
    job->scope.owner = job.get();

    // the function's own header may lower the space's slice and deadline, and raise
    // them up to the caps - TODO 434
    const uint64_t insns = function_limit(c.meta_slice_insns, limits.slice_insns,
                                          limits.slice_max_insns);
    const uint64_t deadline_ms = function_limit(c.meta_deadline_ms, limits.deadline_ms,
                                                limits.deadline_max_ms);
    job->ctx.slice = insns;
    job->ctx.left = insns ? insns : 1;
    job->inline_only = limits.inline_only;      // TODO 552
    job->ctx.read_only = c.meta_read_only;      // TODO 672
    job->ctx.stop = limits.stop;                // TODO 874
    if (limits.from_log) {
        // the same run on every member - TODO 673
        job->ctx.from_log = true;
        job->ctx.log_left = limits.log_insns;
    }
    // a coroutine of its own, so a slice that ends where a yield is not allowed
    // can wait for the next firing rather than raising - TODO 397
    job->ctx.on_coroutine = true;
    if (deadline_ms) {
        const int64_t now = art::now();
        job->ctx.deadline = now + static_cast<int64_t>(deadline_ms);
        job->ctx.budget_ns = static_cast<int64_t>(deadline_ms) * 1000000;
        if (limits.wall_factor)
            job->ctx.wall = now + static_cast<int64_t>(deadline_ms * limits.wall_factor);
        // TODO 836
        job->ctx.wall_factor = limits.wall_factor;
        job->ctx.overtime_ok = c.meta_overtime && !limits.inline_only && !limits.from_log;
    }

    /*
     * Arguments arrive as varargs, not as one table:
     *
     *     arity = 1
     *     function call(key) return barch.store.get(key) end
     *
     * which reads the way a function of fixed arity should. A script that wants them
     * as a table writes `local argv = {...}` and has both, so the host does not have
     * to offer two shapes to give both.
     *
     * Everything from take_thread on runs outside any pcall, and a Luau error there
     * (out of memory, say) is a C++ exception. It used to leave here with the
     * coroutine taken, its thread data pointing into the job, and done never
     * called. It finishes the job now, as a raise out of the pump does - TODO 873.
     */
    try {
        // a coroutine of its own, so the interrupt has somewhere to yield to. Pinned
        // in the registry: nothing else refers to it while it is suspended
        take_thread(*st, job->T, job->tref);
        job->ctx.top = job->T;      // TODO 875
        // the coroutine carries which space this call runs in, for as long as it
        // runs. The job outlives the coroutine's use of it, and `release` clears
        // it - TODO 150
        lua_setthreaddata(job->T, &job->scope);
        if (limits.from_log) {
            // the same run on every member - TODO 673
            lua_getglobal(job->T, "math");
            if (lua_istable(job->T, -1)) {
                lua_getfield(job->T, -1, "randomseed");
                if (lua_isfunction(job->T, -1)) {
                    lua_pushinteger(job->T, (int) (limits.log_seed & 0x7fffffff));
                    lua_call(job->T, 1, 0);
                } else {
                    lua_pop(job->T, 1);
                }
            }
            lua_pop(job->T, 1);
        }
        lua_getref(job->T, entry_ref);
        for (const auto& a : args) {
            lua_pushlstring(job->T, a.data(), a.size());
        }
    } catch (const std::exception& e) {
        finish_raised(job, e.what());
        return;
    }
    /*
     * The first slice runs here, on the thread that asked.
     *
     * Parking costs about 20us - a shard latch, a pool hop, a wake and a post back -
     * which is nothing against a script that runs for milliseconds and everything
     * against one that is a single line. A GET written in Luau measured 4.7x the
     * built-in, and all of it was this, so a script that finishes immediately should
     * not pay for machinery it never uses.
     *
     * The slice is deliberately short. It is not the full one: a full slice on a
     * service thread is exactly what parking was for. Long enough for a one liner,
     * bounded enough that a script which is not one parks almost at once.
     */
    job->ctx.left = inline_insns;
    pump_call(job, (int) args.size());
}



void http_vm_start(http_vm& vm, int fn_ref, const void* req, void* res,
                   const std::vector<http_binding>* params, uint64_t deadline_ms,
                   http_call call) {
    using namespace detail;
    auto done = std::move(call.done);
    if (!vm.cache) {
        done("HTTP luau state");
        return;
    }
    auto job = std::make_shared<call_job>();
    job->cache = vm.cache;
    job->st = state_for(*job->cache);
    if (!job->st || !job->st->L) {
        done("HTTP luau state");
        return;
    }
    job->iface = vm.iface;
    job->done = [done](bool, Variable, std::string err) { done(std::move(err)); };
    job->http = true;
    job->resume_via = std::move(call.resume);
    job->ctx.stop = std::move(call.stop);
    job->self = job;
    job->scope.owner = job.get();
    job->scope.space = vm.space;
    job->scope.running = vm.iface && !vm.iface->running_in.empty() ? vm.iface->running_in
                                                                   : vm.space;
    // one slice for the whole handler: it holds a VM slot, so a slice that ended
    // would only go on again on the same thread
    job->ctx.left = (std::numeric_limits<uint64_t>::max)() / 4;
    job->ctx.slice = job->ctx.left;
    // the route's own, from its header, when it gave one - TODO 552
    const uint64_t limit_ms = deadline_ms ? deadline_ms : vm.deadline_ms;
    if (limit_ms) {
        const int64_t now = art::now();
        job->ctx.deadline = now + static_cast<int64_t>(limit_ms);
        job->ctx.budget_ns = static_cast<int64_t>(limit_ms) * 1000000;
        // the deadline moves on past a wait, the ceiling doesn't - TODO 435
        if (vm.wall_factor)
            job->ctx.wall = now + static_cast<int64_t>(limit_ms * vm.wall_factor);
    }
    // measured: an HTTP request can afford the clock, and a handler that waits
    // inline is covered by it - TODO 445
    job->pooled = true;
    job->cctx.set(job->iface);
    auto* id = barch::functions::http_ident_tls();
    if (id && id->space) {
        // the request's own identity: a store_access of its own, held by the call, and
        // through the request's transaction when its handler began one - TODO 709.
        // The transaction is read at each use, from whichever thread runs the call
        job->cctx.own_store = std::make_shared<store_access>(barch::functions::tx_aware(
            barch::functions::store_for(id->space, id->acl), id->space, id->acl,
            []() -> std::shared_ptr<barch::tx_session> {
                auto* now = barch::functions::http_ident_tls();
                return now ? now->tx : nullptr;
            }));
        job->cctx.store = job->cctx.own_store.get();
    }
    /*
     * The pushes run outside any pcall, so a Luau error there (out of memory, say)
     * is a C++ exception. finish_raised gives the coroutine and the state's pointers
     * back and answers the call, as for a stored function - TODO 575, 873.
     */
    try {
        take_thread(*job->st, job->T, job->tref);
        lua_setthreaddata(job->T, &job->scope);
        // for a coroutine the handler makes, which has no thread data - TODO 875
        job->ctx.top = job->T;
        lua_getref(job->T, fn_ref);
        crow_push_request(job->T, req);
        job->http_gen = crow_http_call();
        crow_push_response(job->T, res);
        // params and query go on every call, templated or not, so a handler that
        // takes four arguments does not have to care which kind of route it is on.
        // A two argument handler ignores them - see TODO 222.
        crow_push_params(job->T, params);
        crow_push_query(job->T, req);
    } catch (const std::exception& e) {
        crow_http_end_call();
        finish_raised(job, e.what());
        return;
    }
    // the first segment, on the Crow thread that asked. A wait parks the call and
    // returns here, and the slot's owner takes it from there - TODO 911
    struct end_call {
        ~end_call() { crow_http_end_call(); }
    } ended;
    pump_call(job, 4);
}


} // namespace foreign
} // namespace barch

#endif // BARCH_HAS_LUAU
