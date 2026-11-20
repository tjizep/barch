#pragma once
/*
 * Test knob, TODO 942: a raise at a chosen point, for a path only a failed
 * allocation or a failed thread start reaches.
 *
 *   BARCH_TEST_FAULT=<point>[,<point>...]
 *
 * Each pass of a named point throws std::runtime_error("test fault <point>"). The
 * points:
 *
 *   park   the end of park_call, after the call is marked parked and before the C
 *          function starts what it waits for
 *
 * A knob is a point that changes what the server does, and doesn't throw:
 *
 *   old_seed   the cluster's users seed is logged in the spelling before TODO 946,
 *              as an old node wrote it
 */
#include <cstdlib>
#include <stdexcept>
#include <string>

namespace barch::test {
    struct fault_settings {
        bool on = false;
        std::string points; // ",a,b,"
    };

    inline fault_settings read_fault() {
        fault_settings s;
        const char* v = std::getenv("BARCH_TEST_FAULT");
        if (!v || !*v)
            return s;
        s.on = true;
        s.points = "," + std::string(v) + ",";
        return s;
    }

    inline const fault_settings& fault_config() {
        static const fault_settings s = read_fault();
        return s;
    }

    [[gnu::noinline]] inline void fault_slow(const char* point) {
        if (fault_config().points.find("," + std::string(point) + ",") != std::string::npos)
            throw std::runtime_error("test fault " + std::string(point));
    }

    inline bool knob(const char* point) {
        return fault_config().on && fault_config().points.find("," + std::string(point) + ",") != std::string::npos;
    }

    inline void fault(const char* point) {
        if (!fault_config().on) [[likely]]
            return;
        fault_slow(point);
    }
}
