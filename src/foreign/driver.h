#ifndef BARCH_FOREIGN_DRIVER_H
#define BARCH_FOREIGN_DRIVER_H

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

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

}
}

#endif
