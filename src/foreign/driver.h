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
 * compile a stored function's source, throwing the result away. SETF asks before it
 * writes, so a script that will not compile is refused rather than saved as a command
 * that cannot run. false fills err with what the compiler said.
 */
bool compile_function(const std::string& source, std::string& err);
/**
 * run a stored function's `call(argv)` and hand back what it returned.
 *
 * `insns` is a hard instruction cap and `deadline_ms` a wall clock one; both end the
 * call with an error rather than slicing it, which is what a first cut can afford -
 * see TODO 98 H for why that has to become a park.
 */
bool call_function(const std::string& space, const std::string& source,
                   const heap::vector<std::string>& args, uint64_t insns,
                   uint64_t deadline_ms, Variable& out, std::string& err);

}
}

#endif
