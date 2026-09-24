#ifndef BARCH_FOREIGN_DRIVER_H
#define BARCH_FOREIGN_DRIVER_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include "sastam.h"
#include "variable.h"

struct lua_State;

namespace barch {
class key_space;
namespace foreign {

struct result {
    enum class status { value, missing, error } status{status::error};
    std::string payload{};
};

struct driver {
    virtual ~driver() = default;
    virtual result fetch(std::string_view space, std::string_view key, uint64_t deadline_ms) = 0;
    // default: run fetch on this thread. Luau overrides so a slice can
    // yield the worker and come back on the foreign pool.
    virtual void fetch_async(std::string_view space, std::string_view key, uint64_t deadline_ms,
                             std::function<void(result)> done) {
        done(fetch(space, key, deadline_ms));
    }
};

driver& fake_driver();
driver& luau_driver();
/** compile and keep the space's script. false means leave foreign off. */
bool prepare_luau(barch::key_space& ks);
bool luau_available();
/**
 * The compiled functions a session holds, one Luau state per key space it has called
 * into. Opaque here because it is full of lua_State, which only the driver may see.
 *
 * Nothing invalidates it: a session runs whatever it compiled the first time, and new
 * code reaches new sessions. See TODO 98 C, and 137 for what that costs.
 */
struct function_states;
typedef std::shared_ptr<function_states> function_states_ptr;
function_states_ptr make_function_states();
/**
 * The same, but every byte its Luau state allocates is added to `into` as well as to
 * the global `statistics::luau_bytes`. The counter is shared and kept alive by the
 * cache, so it is still there to be decremented when the state closes.
 *
 * Counted in the allocator rather than read out of the collector for the reason
 * `luau_alloc` gives: a state belongs to the thread running it, and asking a live
 * state how big it is from another thread is a race.
 */
function_states_ptr make_function_states(std::shared_ptr<std::atomic<uint64_t>> into);

/**
 * Parking a running Luau call while something else finishes - TODO 186.
 *
 * A C function that has to wait on I/O asks to be parked, starts its work, and
 * returns `lua_yield(L, 0)`. The coroutine stops where it is and the pool thread
 * it was on goes back to other jobs. When the work lands, on whatever thread it
 * lands on, `complete_call` hands over a function that pushes the results; the
 * coroutine is resumed on a pool thread and those become the return values of
 * the C function.
 *
 * `park_call` answers null when this call cannot yield, which is the case inside
 * a Crow HTTP handler: the method runs under `lua_pcall`, and it is holding a VM
 * slot from the space's pool. Yielding there would return through `handle_route`
 * and release the slot while the coroutine is still suspended on it, so the next
 * request could take that state and run it underneath. A caller that gets null
 * has to wait inline instead.
 *
 * `complete_call` is safe to call from any thread and does nothing after the
 * first call. The pushing function only ever runs on the thread that resumes the
 * coroutine, so no lua_State is touched from a completion thread.
 */
struct parked_call;
typedef std::shared_ptr<parked_call> parked_call_ptr;
typedef std::function<int(lua_State*)> push_results;
parked_call_ptr park_call(lua_State* L);
void complete_call(const parked_call_ptr& parked, push_results push);

/**
 * A wait that couldn't park - TODO 435.
 *
 * When `park_call` answers null the caller blocks its thread instead. That wait
 * isn't running either, so it comes off the deadline the same way a park does:
 * `blocking_wait_start` before it, `blocking_wait_end` after. The thread can't be
 * interrupted while it waits, so the wall ceiling has to be enforced by the I/O
 * itself - `blocking_wait_cap` is the ms left before the ceiling (at least 1), to
 * cut the request's own timeout down to, or 0 when the call has no ceiling.
 */
uint64_t blocking_wait_cap(lua_State* L);
int64_t blocking_wait_start(lua_State* L);
void blocking_wait_end(lua_State* L, int64_t started);

/** how the driver asks for a function's source, by name.
 *  `space` empty means the function's own space. `exact` true (a dotted
 *  require) looks only there and must not create a space or fall back. */
typedef std::function<bool(const std::string& space, const std::string& name, bool exact,
                           std::string& source)> source_loader;

/**
 * how a script runs an ordinary command - `barch.call("GET", "k")`.
 *
 * The driver knows nothing about commands or callers, so this is handed in. False
 * fills err with something the script sees as a Lua error, which is what a refused
 * command and a failed one both look like from inside.
 *
 * `space` empty runs the command where the function runs, which is `barch.call`.
 * A name runs it in that key space instead - `sp:call(...)` on a `barch.space`
 * handle, TODO 378 - with the caller's rights in that space, and a name that isn't
 * a key space is refused without creating one.
 */
typedef std::function<bool(const std::string& space, const heap::vector<std::string>& argv,
                           Variable& out, std::string& err)> command_runner;

struct store_access;

/**
 * The access the script on this Lua state is running with, or null when there is no
 * script context at all - a foreign fill state, say, which nobody authenticated and
 * which is internal by construction. Implemented in luau_driver.cpp, where the state
 * to space mapping lives.
 */
const store_access* current_access(struct lua_State* L);

/**
 * Whether the script on this state is inside a `locked` region, where a shard lock
 * is held. Something that takes shard locks of its own - reading the configuration
 * space, say - has to refuse there rather than deadlock. See TODO 98 F6.
 */
bool in_locked_region(struct lua_State* L);

/**
 * Push a reply the way barch.call hands one back - the same shapes for the same
 * values, so a reply from another server looks exactly like a local one. For the
 * RESP client, TODO 379. Only on the thread running the state.
 */
void push_reply(struct lua_State* L, const Variable& v);

/**
 * Direct reads of the key space a function is running against, for the things a
 * command cannot say - the ordered-key operations especially.
 *
 * Every one of these finishes with the lock dropped before anything is handed back.
 * That is the rule the whole interface rests on: script code never runs while a shard
 * lock is held, so an iteration copies a bounded batch under the lock and returns it,
 * rather than calling into Luau from inside `sharded_store::range`, whose callback
 * runs under a shared lock. See TODO 98 F.
 */
struct store_access {
    /**
     * whether the user this script is running as may read and write here.
     *
     * Decided once by whoever builds this, because it is their business to know the
     * ACL and the driver's only to enforce it. Both default to false so a store_access
     * that nobody filled in refuses rather than allows.
     *
     * When per space rights land (135) these stop being one pair and become a
     * question asked per space the script touches.
     */
    bool may_read{false};
    bool may_write{false};
    /**
     * whether this user may see stored functions at all.
     *
     * Function keys are ordinary keys in the space, so a walk meets them. Someone
     * without the `function` category has no business knowing what functions exist,
     * so for them the store behaves as though the range is not there. See TODO 98.
     */
    bool may_see_functions{false};
    /**
     * whether this user may reach off the box - `http.request`, and whatever socket
     * client TODO 301 becomes.
     *
     * Not a store right, and it sits here anyway. `store_access` is the only object
     * a running script has that was built from the caller's ACL, so this is where a
     * per call answer can be asked for; a second channel carrying one boolean would
     * be worse than the mild lie in the name. The `outbound` category - see TODO 304.
     */
    bool may_reach_out{false};
    /**
     * present, or why not - see TODO 148.
     *
     * `tombed` means a foreign source was asked for this key and had nothing, and the
     * miss was cached so it is not asked again. That is a different fact from a key
     * nobody has looked for, and a script that treats them the same cannot tell "does
     * not exist" from "not fetched yet".
     */
    enum class read_state { absent, tombed, present };
    std::function<read_state(const std::string& key, std::string& value)> get{};
    /**
     * `get`, and on a miss in a foreign key space, the fill - waited for. Empty on a
     * space that is not foreign, which is how a caller can tell the two apart.
     *
     * Separate from `get` on purpose. A read that might go to the network and take
     * a timeout is not the same operation as a read that cannot, and a handler that
     * did the first while believing the second would hold its VM slot for the whole
     * fetch without anything saying so. See TODO 259.
     */
    std::function<read_state(const std::string& key, std::string& value,
                             std::string& err)> fetch{};
    std::function<bool(const std::string& key)> exists{};
    std::function<int64_t()> size{};
    std::function<int64_t(const std::string& lo, const std::string& hi)> count{};
    /** keys in [lo, hi), at most limit of them, copied out before the lock goes */
    std::function<void(const std::string& lo, const std::string& hi, int64_t limit,
                       heap::vector<std::string>& out)> range{};
    /**
     * `range`, leaving out the first `offset` keys of [lo, hi) first. The store
     * reaches the offset by node counts where it can rather than walking it - see
     * TODO 369. Separate from `range` so the C++ callers that page through a space
     * themselves keep their signature. See TODO 372.
     */
    std::function<void(const std::string& lo, const std::string& hi, int64_t limit,
                       int64_t offset, heap::vector<std::string>& out)> range_from{};
    std::function<bool(std::string& key)> min{};
    std::function<bool(std::string& key)> max{};
    /** what the space is configured as, read only */
    std::function<void(heap::vector<std::pair<std::string, std::string>>& out)> config{};
    /** write a key. false fills err - too large, wrong type, refused */
    std::function<bool(const std::string& key, const std::string& value,
                       std::string& err)> set{};
    /** remove a key. false means there was nothing there */
    std::function<bool(const std::string& key)> remove{};
    /**
     * one page of the space at a time, from `after` onwards, ascending.
     *
     * A page is copied under the lock and handed back with the lock dropped, so no
     * script runs while a shard is held and a key erased behind the walk cannot
     * matter - what is being read is nobody's live memory. Empty means the end.
     * See TODO 98 F2.
     */
    struct row {
        std::string type;        // "key", "list", "hash", "orderedset", "function"
        std::string container;   // empty for a plain key
        std::string key;         // empty for a container header
        std::string value;
        bool has_value{false};
    };
    /*
     * `after` and `next` are opaque: the encoded bytes of the last key handed out,
     * not its name. A decoded name cannot be a continuation - re-encoding it puts it
     * back wherever a *plain* key of that name would sort, and a function name lands
     * at the string lead rather than the function one, so the walk starts over and
     * never ends.
     */
    std::function<void(const std::string& after, size_t want,
                       heap::vector<row>& out, std::string& next)> page{};

    /*
     * Containers, which are a different key shape rather than a different store: a
     * list, hash or ordered set is `{lead, name, member}` with one lead per kind, so
     * one set of entry points covers all three once the kind is known. A hash's field
     * and an ordered set's member are the same position; only what the value means
     * differs, and that is the script's business rather than this interface's.
     */
    std::function<std::string(const std::string& name)> container_kind{};
    std::function<bool(const std::string& name, const std::string& member,
                       std::string& value)> container_get{};
    std::function<bool(const std::string& name, const std::string& member,
                       const std::string& value, std::string& err)> container_set{};
    std::function<bool(const std::string& name, const std::string& member)> container_del{};
    /** members from `after` onwards, the same paging the space walk uses */
    std::function<void(const std::string& name, const std::string& after, size_t want,
                       heap::vector<std::pair<std::string, std::string>>& out,
                       std::string& next)> container_page{};

    /**
     * Run `body` with a write lock held on the shard a key routes to, or on the whole
     * space when the key is empty - see TODO 98 F6.
     *
     * This is what makes a read-modify-write atomic. Everything else here copies under
     * the lock and lets it go, so a get followed by a set has a gap another connection
     * can land in; inside here it does not.
     *
     * `body` returns false if the script raised, which is passed back rather than
     * thrown through the lock. The lock goes back either way - the guard is on this
     * side of the boundary so that an error, the instruction cap or the deadline
     * cannot leave a shard locked.
     */
    std::function<bool(const std::string& key, const std::function<bool()>& body,
                       std::string& err)> locked{};

    /**
     * Which shard a key routes to, and whether this call already holds its lock.
     *
     * Both exist because the locked region is deliberately restrictive: one shard or
     * the whole space, never two. `shard_number` lets a script find out whether two
     * keys are on one shard *before* it tries to lock them and gets the abort, and
     * `has_lock` lets a helper work either inside a region or outside one without
     * being told which. See TODO 98 F6.
     */
    std::function<int64_t(const std::string& key)> shard_number{};
    std::function<bool(const std::string& key)> has_lock{};

    /**
     * Raw bytes of a key's value from `offset` to the end.
     *
     * `cb` runs under the read lock with a pointer into the leaf (or into a
     * decompressed copy if the leaf was compressed). Copy what you need; the
     * pointer is gone when this returns. Absent, tomb, or offset past the end
     * does not call `cb`.
     */
    std::function<read_state(const std::string& key, size_t offset,
                             const std::function<void(const void*, size_t)>& cb)> getBufferAt{};
    /**
     * Write `len` bytes at `offset`, growing the value if it has to.
     * false fills err — too large, or the write was refused.
     */
    std::function<bool(const std::string& key, size_t offset,
                       const void* data, size_t len, std::string& err)> setBufferAt{};
    /**
     * Every page of the space's leaf arena (`nodes` false) or node arena, shard by
     * shard in shard order and page number order within a shard - TODO 416. `f`
     * gets the shard number, the page number and the page's bytes up to its write
     * position, with no lock held, and answers false to stop. Inside a transaction
     * the pages are the ones as they were at BEGIN.
     *
     * False fills `err`: no rights, or the transaction ended part way through. A
     * walk `f` stopped is not a failure.
     */
    std::function<bool(bool nodes,
                       const std::function<bool(size_t shard, size_t page,
                                                const void* data, size_t len)>& f,
                       std::string& err)> pages{};
    /**
     * The rest of each shard's file for a backup, shard by shard - TODO 416: the
     * leaf and node arenas' allocator state (free lists, counters, page table) when
     * `free_lists`, the shard's stats block when `stats`, empty otherwise. Inside a
     * transaction, as they were at BEGIN. Same answer and `err` as pages.
     */
    std::function<bool(bool free_lists, bool stats,
                       const std::function<bool(size_t shard, const std::string& leaves,
                                                const std::string& nodes,
                                                const std::string& stats)>& f,
                       std::string& err)> shard_state{};
    /**
     * Streaming save and load of the whole space - TODO 418; see stream_backup.h.
     * save hands over `(data, len, block, shard)` and is the BEGIN-time state; it
     * needs a transaction open and leaves it open (TODO 424). load asks
     * `(block, shard, out)` and is refused inside a transaction. False fills `err`.
     */
    std::function<bool(const std::function<bool(const char* data, size_t len, uint64_t block,
                                                 size_t shard)>& emit,
                       std::string& err)> save_stream{};
    std::function<bool(const std::function<bool(uint64_t block, size_t shard, std::string& out)>& next,
                       std::string& err)> load_stream{};
};

/**
 * Open another key space by name, with this user's rights *in that space*.
 *
 * `barch.space.other.k` reaches somewhere the call did not start, so the rights have
 * to be asked for again rather than inherited - which is what per-space ACLs are for,
 * TODO 135. False means no such space, and touching a name must never build one.
 */
typedef std::function<bool(const std::string& space, store_access& out)> space_opener;

/**
 * Everything a script reaches, built once rather than per call.
 *
 * These four are about a dozen std::functions between them and none of them depends
 * on the script or its arguments - only on the key space and the user's rights there.
 * Building them per call was 1.8us, which was 72% of what a one line function cost.
 * See TODO 98 F5.
 */
/**
 * what a nested-call refusal says, so the message can be recognised as it comes back
 * up a chain and passed on whole rather than prefixed at every level - see TODO 98 E
 */
inline constexpr const char* too_deep_marker = "nested calls too deep";

struct call_interface {
    source_loader load{};
    command_runner run_command{};
    store_access store{};
    space_opener open_space{};
    /**
     * spaces `barch.space.NAME` has already opened, held here rather than per call.
     *
     * Building one is not cheap - `store_for` fills in fifteen or so std::functions -
     * and it used to be thrown away at the end of every call, so a function doing one
     * read through a named space paid for the whole interface to serve it. The
     * interface is the right owner: it is already rebuilt when the running space
     * changes, when the defined space changes and on `set_acl`, which are exactly the
     * three things that make a cached store_access wrong. See TODO 141.
     */
    /*
     * Held by pointer, not by value. A dense map keeps its values in one vector, so
     * inserting the second space moves the first one's `store_access` and every
     * pointer already handed out to a script goes stale - `barch.space.a` read after
     * `barch.space.b` was opened threw `bad_function_call`, because the functions in
     * the moved-from object are empty while the flags beside them still look valid.
     * A unique_ptr gives the entry an address that outlives the map's own growth.
     * See TODO 273.
     */
    heap::string_map<std::unique_ptr<store_access>> opened{};
    /** what it was built for, so a call in another space builds its own */
    std::string running_in{};
    std::string defined_in{};
};
typedef std::shared_ptr<call_interface> call_interface_ptr;

/**
 * compile a stored function's source, throwing the result away. SETF asks before it
 * writes, so a script that will not compile is refused rather than saved as a command
 * that cannot run.
 *
 * Done in a real state with `load` installed, not a bare one, because a script's top
 * level may `require` others - in a bare state require is nil and every such script
 * is refused with "attempt to call a nil value". Doing it properly also means a cycle,
 * or a require of something that is not there, is caught at write time rather than at
 * the first call. It does mean a function has to be stored after the ones it requires.
 */
/**
 * One RESP command a `transport()` of kind "resp" exposes - TODO 188.
 *
 *     return {
 *         kind = "resp",
 *         methods = {GETNAME = get_name, SETNAME = set_name},
 *         categories = {GETNAME = {"read"}, SETNAME = {"write", "data"}},
 *     }
 *
 * `name` is what a client types; the table value is the function itself, so the
 * exposed command and the function implementing it are named independently. The
 * categories are the same vocabulary the builtins declare, and they decide both
 * what rights a caller needs and whether the call is replicated onward.
 *
 * `arity` follows the same convention as the script-level one: n exactly n, -n
 * at least n, 0 whatever it is given.
 */
struct resp_method {
    std::string name;
    std::vector<std::string> categories;
    int arity{0};
};

struct resp_spec {
    bool has_transport{false};
    /** transport() was there and said kind = "resp" */
    bool is_resp{false};
    std::vector<resp_method> methods;
};

/**
 * A `transport()` of kind "cron" - TODO 249. Not the work, a schedule pointing at it:
 *
 *     return {
 *         kind = "cron",
 *         space = "media",
 *         call = "COMPACT",
 *         args = {"7"},
 *         every = "5m",        -- or: cron = "0 3 * * *"
 *         user = "jobs",
 *         jitter = "30s",
 *         overlap = "skip",    -- skip | queue | allow
 *         catchup = false,
 *         tz = "UTC",
 *         enabled = true,
 *     }
 *
 * `space` and `call` are where the tick calls into, exactly like CALLF but with no
 * connection behind it - which is why `user` is required: it is who the call runs as,
 * and `space = "default"` names the unnamed space a plain connection writes to,
 * the way `transport().user` names who an HTTP route runs as. Exactly one of `every`
 * or `cron` has to be set; the two schedule forms are documented on cron.h.
 */
struct cron_spec {
    bool has_transport{false};
    /** transport() was there and said kind = "cron" */
    bool is_cron{false};
    std::string space;
    std::string call;
    std::vector<std::string> args;
    std::string every;
    std::string cron;
    std::string user;
    std::string jitter;
    std::string overlap{"skip"};
    bool catchup{false};
    std::string tz{"UTC"};
    bool enabled{true};
};

/**
 * A `kind = "queue"` transport() - TODO 366.
 *
 * A queue declaration is a destination and a handler, and nothing else: the
 * messages arrive from whoever publishes to `name`, so unlike a cron entry
 * there is no schedule and unlike a resp one there are no commands.
 *
 * The handler is called with three arguments, in this order: the message, its
 * sequence as a decimal string, and how many times it has already been tried.
 * They arrive as varargs, the same as every other stored call, so a handler is
 * written
 *
 *     function call(message, sequence, attempts) ... end
 *
 * and one that wants them as a table writes `local argv = {...}`.
 *
 * The sequence is there because delivery is at-least-once - a crash between the
 * handler finishing and the message being removed means it arrives again - so a
 * handler that must not repeat itself has something to recognise. `attempts` is
 * how many times this message has already been handed over and failed, so a
 * handler can behave differently on a retry.
 *
 * `durability` takes the same four words as `aof_durability`: "none", "timer",
 * "each", or a size like "512kb". `poll` is only the backstop for messages a
 * crash left behind; an ordinary publish wakes the consumer directly, so it can
 * be as slow as it likes.
 */
struct queue_spec {
    bool has_transport{false};
    /** transport() was there and said kind = "queue" */
    bool is_queue{false};
    /** what senders publish to */
    std::string name;
    std::string space;
    std::string call;
    std::string user;
    std::string durability{"each"};
    /** where the queue file goes; empty means the server's queue_dir */
    std::string dir;
    /** how long the backstop poll waits, as a duration */
    std::string poll{"30s"};
    /** deliveries before a message is dead lettered */
    uint32_t max_attempts{5};
    bool enabled{true};
};

bool compile_function(const std::string& space, const std::string& name,
                      const std::string& source, const source_loader& load,
                      std::string& err, resp_spec* spec = nullptr,
                      cron_spec* cron = nullptr, queue_spec* queue = nullptr,
                      bool aot = false);

/**
 * run a stored function's `call(argv)` and hand back what it returned.
 *
 * `load` is asked for a function's source by name, and only when that function is not
 * already compiled in `cache`, so a warm call never reads the store. It is asked for
 * other names too when a script requires one. A null cache runs against a state built
 * for this call alone, which is what the contexts without a session do.
 */

/**
 * parked call completion event */
typedef std::function<void(bool ok, Variable out, std::string err)> function_done;

/**
 * Start a stored function and let it run in slices on the foreign pool.
 *
 * The caller does not wait: `done` is called when the script finishes, from whichever
 * thread finished it, which may be before this returns - a script that completes
 * inside its first slice never leaves this thread at all. Everything the script needs
 * - the loader, the command runner, the store - is copied into the job, because the
 * command that started it has returned by then and its stack is gone.
 *
 * `args` reach the script as varargs, so `function call(key)` works and a script that
 * wants them as a table writes `local argv = {...}`.
 *
 * The slice is a slice rather than a cap: the script yields when it runs out and is put
 * back on the pool, so a long script shares the pool instead of owning a thread. The
 * deadline is what actually ends a runaway. See TODO 98 H.
 */
/**
 * What a call may use - TODO 434. The slice and deadline are the space's; a function's
 * own header (`--@barch {...}`, see function_meta) may lower them freely and raise them
 * up to the caps. The deadline counts running time: time parked or queued for a worker
 * is taken off it. The wall ceiling, `wall_factor` times the deadline and fixed when
 * the call starts, is what ends a call that keeps waiting; 0 is none.
 */
struct call_limits {
    uint64_t slice_insns{0};
    uint64_t deadline_ms{0};
    uint64_t slice_max_insns{0};
    uint64_t deadline_max_ms{0};
    uint64_t wall_factor{0};
};

void start_function(const std::string& space, const std::string& name,
                    const call_interface_ptr& iface,
                    const heap::vector<std::string>& args, const call_limits& limits,
                    const function_states_ptr& cache,
                    const function_done& done, const std::string& entry = {});

/**
 * A stored function's own settings, from a comment in its header - TODO 434:
 *
 *     --@barch {"deadline_ms": 5000, "slice_insns": 200000}
 *
 * or the same JSON in a `--[[@barch ... ]]` block. Read from the comments before the
 * first token, the block `--!native` is read from. It's a plain comment, so Luau
 * ignores it and a server that doesn't know it runs the function with the defaults,
 * and the source is its record, so GETF, git sync and a restart all keep it.
 *
 * `present` is false when there is no such comment. A comment that isn't a JSON object,
 * or gives a field that isn't a positive whole number, is refused with `err`. Fields
 * it doesn't know are ignored, so it can grow.
 */
struct function_meta {
    bool present{false};
    uint64_t deadline_ms{0};    // 0: not given
    uint64_t slice_insns{0};
};
bool read_function_meta(const std::string& source, function_meta& out, std::string& err);

/**
 * A setting a function's header asked for, against what its space allows: lowering
 * always, raising up to `cap`. `own` 0 means the header didn't give one.
 */
inline uint64_t function_limit(uint64_t own, uint64_t base, uint64_t cap) {
    if (!own)
        return base;
    if (own <= base)
        return own;
    uint64_t ceiling = cap > base ? cap : base;
    return own < ceiling ? own : ceiling;
}

/** one HTTP method advertised by transport().methods */
struct http_method {
    std::string verb;
    int fn_ref{-2}; // LUA_NOREF
};

/**
 * What a function's transport() table said.
 *
 * `kind` is "resource" for a route and "http" for server startup. Missing
 * kind still infers: a route means resource, otherwise http. A function
 * with no transport() is an ordinary stored function. fn_ref values are
 * only valid on the http_vm they were loaded into.
 */
/** one segment of a parsed route: either a literal, or a `{name}` hole */
struct http_route_seg {
    std::string text;  ///< the literal, empty for a hole
    std::string name;  ///< the binding name, empty for a literal
    bool hole{false};
};

/** one `{name}` filled in from the request path */
struct http_binding {
    std::string name;
    std::string value;
};

struct http_route {
    std::string name;
    std::string kind;
    std::string route;
    /*
     * Crow matches the literal prefix and hands us the rest; the segments after
     * it are matched here. `segs` is `route` split up, `crow_route` is what the
     * rule is actually registered as, and both are empty on a plain route,
     * which still goes to Crow whole. See TODO 222.
     */
    std::vector<http_route_seg> segs;
    std::string crow_route;
    bool wild_tail{false};   ///< route ended in `*`: the rest goes in `params["*"]`
    bool templated{false};   ///< has holes or a `*`, so barch does the matching
    std::string accept;
    std::string send;
    std::string cors;
    std::vector<http_method> methods;
    std::vector<std::string> extra_keys;
    uint16_t port{0};
    std::string bind;
    std::string ssl_cert;
    std::string ssl_key;
    std::string ssl_proto;
    /** empty: session then HTTP default. set: this route always runs as that user */
    std::string user;
    /**
     * kind=files only: where under the file store this route serves from, so
     * `/static/*` with root `/assets/` answers `/static/logo.png` out of
     * `fs:m:/assets/logo.png`. Served in C++ without entering luau - see TODO 235.
     */
    std::string root;
    /**
     * what a files route serves when the url names a directory - "index.html",
     * typically. Empty means a directory is a 404, which is what a files route did
     * before there was an entry point to land on. See TODO 253.
     */
    std::string index;
    /**
     * whether this route may ask the space's file source for a file it does not
     * have. Off by default: a fetch can take a source's latency and holds a
     * handler's place while it does, so a route says so rather than a key space
     * deciding it for every reader. See TODO 263.
     */
    bool source{false};
    /**
     * the key space whose file store a files route serves, and fetches into when
     * `source` is on. Empty means the space the HTTP server runs in, which is all
     * a files route could do before. It comes from the transport, never from the
     * url, so a request can't pick a space. Read with the request user's rights in
     * that space. See TODO 371.
     */
    std::string space;
    bool has_transport{false};
    bool has_route{false};
};

/** Luau state that holds compiled HTTP functions for one Crow thread */
struct http_vm {
    function_states_ptr cache;
    call_interface_ptr iface;
    std::string space;
    uint64_t deadline_ms{5000};
    /** the wall ceiling is this times the deadline, 0 none - TODO 435 */
    uint64_t wall_factor{10};
};

bool http_vm_load(http_vm& vm, const std::string& name, const std::string& source,
                  http_route& out, std::string& err);
/**
 * Call one HTTP handler. `params` is the `{name}` bindings matched out of the
 * path, or null for an untemplated route; it reaches the handler as its third
 * argument, with the query string as its fourth.
 */
void http_vm_call(http_vm& vm, int fn_ref, const void* req, void* res,
                  const std::vector<http_binding>* params, std::string& err);

}
}

#endif
