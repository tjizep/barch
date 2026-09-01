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
 */
typedef std::function<bool(const heap::vector<std::string>& argv, Variable& out,
                           std::string& err)> command_runner;

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
     * present, or why not - see TODO 148.
     *
     * `tombed` means a foreign source was asked for this key and had nothing, and the
     * miss was cached so it is not asked again. That is a different fact from a key
     * nobody has looked for, and a script that treats them the same cannot tell "does
     * not exist" from "not fetched yet".
     */
    enum class read_state { absent, tombed, present };
    std::function<read_state(const std::string& key, std::string& value)> get{};
    std::function<bool(const std::string& key)> exists{};
    std::function<int64_t()> size{};
    std::function<int64_t(const std::string& lo, const std::string& hi)> count{};
    /** keys in [lo, hi), at most limit of them, copied out before the lock goes */
    std::function<void(const std::string& lo, const std::string& hi, int64_t limit,
                       heap::vector<std::string>& out)> range{};
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
    heap::string_map<store_access> opened{};
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

bool compile_function(const std::string& space, const std::string& name,
                      const std::string& source, const source_loader& load,
                      std::string& err, resp_spec* spec = nullptr);

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
 * `insns` is a slice rather than a cap: the script yields when it runs out and is put
 * back on the pool, so a long script shares the pool instead of owning a thread. The
 * deadline is what actually ends a runaway. See TODO 98 H.
 */
void start_function(const std::string& space, const std::string& name,
                    const call_interface_ptr& iface,
                    const heap::vector<std::string>& args, uint64_t insns,
                    uint64_t deadline_ms, const function_states_ptr& cache,
                    const function_done& done, const std::string& entry = {});

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
struct http_route {
    std::string name;
    std::string kind;
    std::string route;
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
    bool has_transport{false};
    bool has_route{false};
};

/** Luau state that holds compiled HTTP functions for one Crow thread */
struct http_vm {
    function_states_ptr cache;
    call_interface_ptr iface;
    std::string space;
    uint64_t deadline_ms{5000};
};

bool http_vm_load(http_vm& vm, const std::string& name, const std::string& source,
                  http_route& out, std::string& err);
void http_vm_call(http_vm& vm, int fn_ref, const void* req, void* res, std::string& err);

}
}

#endif
