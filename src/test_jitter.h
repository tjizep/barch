#pragma once
/*
 * Test knob, TODO 805: random stops at chosen points, to run the tests as if on a
 * slow or crowded machine. A test that only waits on events passes the same with it
 * on; a test that bets on timing breaks.
 *
 *   BARCH_TEST_JITTER=<seed>:<max_ms>[:<point>,<point>...]
 *
 * At about one in four passes of a point barchd stops for 0 to max_ms. With no list
 * every point stops. A different seed gives different stops; the same seed doesn't
 * give the same run twice, since each thread's stops also depend on its id and the
 * threads interleave as they like. It's for shaking a test, not replaying one. The
 * points:
 *
 *   command        a RESP command, before it runs
 *   raft_msg       a Raft request sent or received (heartbeats among them)
 *   raft_append    a write, before it goes to the Raft log
 *   raft_apply     an entry, before the state machine applies it
 *   snapshot_copy  a snapshot copy, before it starts
 *   function       a Luau call, inside each slice: wall time passes, CPU time doesn't
 *   deadline_watch the deadline watch, before it fires
 *
 * Two more points sit in debuggable_server_lock, which every shard access goes
 * through. They're only on when named, so a plain "every point" run stays usable,
 * and they spin instead of sleeping: a busy loop of 0 to max_ms*1000 rounds. A spin
 * keeps the thread on the CPU, so it shifts things by microseconds where a sleep
 * shifts them by milliseconds, and it burns CPU time as well as wall time:
 *
 *   lock_wait      before a lock is taken (shared, upgradable or unique)
 *   lock_hold      just after it's taken, while it's held
 */
#include <atomic>
#include <cstdio>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <random>
#include <string>
#include <thread>

namespace barch::test {
    struct jitter_settings {
        bool on = false;
        uint64_t seed = 0;
        unsigned max_ms = 0;
        std::string points; // ",a,b," or empty for all
        // the lock points, worked out once since they run on every lock call
        bool lock_wait = false;
        bool lock_hold = false;
    };

    inline jitter_settings read_jitter() {
        jitter_settings s;
        const char* v = std::getenv("BARCH_TEST_JITTER");
        if (!v || !*v)
            return s;
        std::string t = v;
        auto a = t.find(':');
        if (a == std::string::npos)
            return s;
        auto b = t.find(':', a + 1);
        s.seed = std::strtoull(t.substr(0, a).c_str(), nullptr, 10);
        s.max_ms = (unsigned) std::strtoul(t.substr(a + 1, b == std::string::npos ? std::string::npos : b - a - 1).c_str(), nullptr, 10);
        if (b != std::string::npos && b + 1 < t.size())
            s.points = "," + t.substr(b + 1) + ",";
        s.on = s.max_ms > 0;
        s.lock_wait = s.on && s.points.find(",lock_wait,") != std::string::npos;
        s.lock_hold = s.on && s.points.find(",lock_hold,") != std::string::npos;
        // stderr, not the log: the lock includes this, and locktest has no logger
        if (s.on)
            std::fprintf(stderr, "test: jitter on, up to %u ms at %s\n", s.max_ms,
                         s.points.empty() ? "every point" : t.substr(b + 1).c_str());
        return s;
    }

    inline const jitter_settings& jitter_config() {
        static const jitter_settings s = read_jitter();
        return s;
    }

    // how many stops so far: a test reads it (INFO server, jitter_stops) to see the
    // knob is really on
    inline std::atomic<uint64_t>& jitter_stops() {
        static std::atomic<uint64_t> n{0};
        return n;
    }

    inline std::mt19937_64& jitter_rng() {
        thread_local std::mt19937_64 rng(jitter_config().seed ^ std::hash<std::thread::id>{}(std::this_thread::get_id()));
        return rng;
    }

    // a busy loop of random length, for the lock points
    inline void spin() {
        auto& rng = jitter_rng();
        if (rng() % 4 != 0)
            return;
        ++jitter_stops();
        const uint64_t rounds = rng() % (uint64_t(jitter_config().max_ms) * 1000 + 1);
        std::atomic<uint64_t> sink{0};
        for (uint64_t i = 0; i < rounds; ++i)
            sink.fetch_add(i, std::memory_order_relaxed);
    }

    inline void lock_wait() {
        if (jitter_config().lock_wait)
            spin();
    }

    inline void lock_hold() {
        if (jitter_config().lock_hold)
            spin();
    }

    inline void jitter(const char* point) {
        const auto& s = jitter_config();
        if (!s.on)
            return;
        if (!s.points.empty() && s.points.find("," + std::string(point) + ",") == std::string::npos)
            return;
        auto& rng = jitter_rng();
        if (rng() % 4 != 0)
            return;
        ++jitter_stops();
        std::this_thread::sleep_for(std::chrono::milliseconds(rng() % (s.max_ms + 1)));
    }
}
