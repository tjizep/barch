#include "driver.h"
#include "key_space.h"

#include <chrono>
#include <thread>

namespace barch {
namespace foreign {

struct fake_driver_t : driver {
    result fetch(std::string_view space, std::string_view key, uint64_t) override {
        auto ks = get_keyspace(std::string(space));
        if (!ks)
            return {result::status::error, "FOREIGN no space"};
        uint64_t delay = 0;
        bool fail = false;
        std::string value;
        bool found = false;
        {
            std::lock_guard lk(ks->fake_mu);
            ++ks->fake_queries;
            delay = ks->fake_delay_ms;
            fail = ks->fake_fail;
            auto it = ks->fake_source.find(std::string(key));
            if (it != ks->fake_source.end()) {
                found = true;
                value = it->second;
            }
        }
        if (delay)
            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        if (fail)
            return {result::status::error, "FOREIGN fake fail"};
        if (!found)
            return {result::status::missing, {}};
        return {result::status::value, std::move(value)};
    }
};

driver& fake_driver() {
    static fake_driver_t d;
    return d;
}

}
}
