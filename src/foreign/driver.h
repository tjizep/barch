#ifndef BARCH_FOREIGN_DRIVER_H
#define BARCH_FOREIGN_DRIVER_H

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include "sastam.h"
#include "variable.h"

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

/** how the driver asks for a function's source, by name */
typedef std::function<bool(const std::string& name, std::string& source)> source_loader;

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
    std::function<bool(const std::string& key, std::string& value)> get{};
    std::function<bool(const std::string& key)> exists{};
    std::function<int64_t(const std::string& lo, const std::string& hi)> count{};
    /** keys in [lo, hi), at most limit of them, copied out before the lock goes */
    std::function<void(const std::string& lo, const std::string& hi, int64_t limit,
                       heap::vector<std::string>& out)> range{};
    std::function<bool(std::string& key)> min{};
    std::function<bool(std::string& key)> max{};
    /** what the space is configured as, read only */
    std::function<void(heap::vector<std::pair<std::string, std::string>>& out)> config{};
};

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
bool compile_function(const std::string& space, const std::string& name,
                      const std::string& source, const source_loader& load,
                      std::string& err);

/**
 * run a stored function's `call(argv)` and hand back what it returned.
 *
 * `load` is asked for a function's source by name, and only when that function is not
 * already compiled in `cache`, so a warm call never reads the store. It is asked for
 * other names too when a script requires one. A null cache runs against a state built
 * for this call alone, which is what the contexts without a session do.
 *
 * `insns` is a hard instruction cap and `deadline_ms` a wall clock one; both end the
 * call with an error rather than slicing it, which is what a first cut can afford -
 * see TODO 98 H for why that has to become a park.
 */
bool call_function(const std::string& space, const std::string& name,
                   const source_loader& load, const command_runner& run_command,
                   const store_access& store,
                   const heap::vector<std::string>& args, uint64_t insns,
                   uint64_t deadline_ms, const function_states_ptr& cache,
                   Variable& out, std::string& err);

}
}

#endif
