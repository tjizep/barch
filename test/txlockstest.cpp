// a reader waits behind a transaction waiting to write the key, so a deadlock's
// victim can't take its shared lock back before the survivor goes on - TODO 869
//
// A lock asked for with its time already past stays noted as waiting, as a parked
// session's does (TODO 721). So one thread can lay out each step of a race in turn.
#include "tx_locks.h"
#include <atomic>
#include <chrono>
#include <cstdio>
#include <set>
#include <string>
#include <thread>

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-66s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

using clock_type = std::chrono::steady_clock;
// what a parked session asks with: only tried, and noted as waiting when it isn't taken
static clock_type::time_point past() { return clock_type::now() - std::chrono::seconds(1); }
// never reached: a wait that isn't woken shows as the test hanging
static clock_type::time_point never() { return clock_type::now() + std::chrono::hours(24); }

int main() {
    std::printf("two upgrades of one key: the victim's retry waits for the other\n");
    {
        barch::tx_locks l;
        l.take("a", "m", false, never());
        l.take("b", "m", false, never());
        auto r = l.take("a", "m", true, past());
        check(!r.ok && !r.deadlock && r.holder == "b", "a waits for b to upgrade m");
        r = l.take("b", "m", true, past());
        check(r.deadlock && r.holder == "a", "b's upgrade closes the ring, b is the victim");
        l.release("b", {"m"});
        r = l.take("b", "m", false, past());
        check(!r.ok && !r.deadlock && r.holder == "a", "b's retry reads m: it waits behind a's upgrade");
        r = l.take("a", "m", true, past());
        check(r.ok, "a upgrades m");
        l.release("a", {"m"});
        r = l.take("b", "m", false, past());
        check(r.ok && !l.waiting("b"), "then b reads m");
        r = l.take("b", "m", true, past());
        check(r.ok, "and upgrades it");
    }

    std::printf("a reader that holds the key already isn't held up\n");
    {
        barch::tx_locks l;
        l.take("a", "m", false, never());
        l.take("b", "m", false, never());
        l.take("a", "m", true, past());
        auto r = l.take("b", "m", false, past());
        check(r.ok, "b reads m again while a waits to upgrade it");
    }

    std::printf("the ring search follows a wait behind a writer\n");
    {
        barch::tx_locks l;
        l.take("c", "m", false, never());
        l.take("d", "x", true, never());
        auto r = l.take("a", "m", true, past());
        check(!r.ok && !r.deadlock && r.holder == "c", "a waits for c to write m");
        r = l.take("d", "m", false, past());
        check(!r.ok && !r.deadlock && r.holder == "a", "d waits behind a to read m");
        r = l.take("c", "x", true, past());
        check(r.deadlock && r.ring.size() == 2 && r.ring[0] == "d" && r.ring[1] == "a",
              "c waiting for d's x closes c, d, a");
    }

    std::printf("a writer that gives up lets the readers behind it go\n");
    {
        barch::tx_locks l;
        l.take("c", "m", false, never());
        l.take("a", "m", true, past());
        auto r = l.take("b", "m", false, past());
        check(!r.ok && r.holder == "a", "b waits behind a");
        const auto wake = l.stop_waiting("a");
        check(wake.count("m") == 1, "a times out: m is to be woken for whoever parked on it");
        r = l.take("b", "m", false, past());
        check(r.ok, "and b reads m");
    }
    {
        barch::tx_locks l;
        l.take("c", "m", false, never());
        l.take("a", "m", true, past());
        l.take("b", "m", false, past());
        const auto wake = l.release("a", {});
        check(wake.count("m") == 1, "a ends holding nothing: m is to be woken");
        check(l.take("b", "m", false, past()).ok, "and b reads m");
    }

    std::printf("a reader says when it waits behind a parked writer, not a holder\n");
    {
        barch::tx_locks l;
        l.take("c", "m", true, never());
        l.take("a", "m", true, past());
        auto r = l.take("b", "m", false, past());
        check(!r.ok && r.holder == "c" && !r.behind_parked, "behind c, which holds m");
        l.release("c", {"m"});
        r = l.take("b", "m", false, past());
        check(!r.ok && r.holder == "a" && r.behind_parked, "then behind a, parked to write m");
        check(l.take("a", "m", true, past()).ok, "which takes m");
        r = l.take("b", "m", false, past());
        check(!r.ok && r.holder == "a" && !r.behind_parked, "and holds it");
    }
    {
        // an upgrade: a holds m shared and parks to write it, held up by c
        barch::tx_locks l;
        l.take("a", "m", false, never());
        l.take("c", "m", false, never());
        check(!l.take("a", "m", true, past()).ok, "a parks to upgrade m, c reads it too");
        auto r = l.take("b", "m", true, past());
        check(!r.ok && r.holder == "a" && r.behind_parked, "a writer behind a: parked to write m it holds");
        l.release("c", {"m"});
        check(l.take("a", "m", true, past()).ok, "c goes, a upgrades");
    }
    {
        // a writer waiting on its thread is woken by the release itself
        barch::tx_locks l;
        l.take("c", "m", false, never());
        std::atomic<bool> got{false};
        std::thread a([&] { got = l.take("a", "m", true, never()).ok; });
        while (!l.waiting("a"))
            std::this_thread::yield();
        auto r = l.take("b", "m", false, past());
        check(!r.ok && r.holder == "a" && !r.behind_parked, "behind a writer waiting on its thread: not parked");
        l.release("c", {"m"});
        a.join();
        check(got, "which goes on when m goes");
        l.release("a", {"m"});
    }

    std::printf("a reader waiting on its thread is woken when the writer gives up\n");
    {
        barch::tx_locks l;
        l.take("c", "m", false, never());
        l.take("a", "m", true, past());
        std::atomic<bool> got{false};
        std::thread b([&] { got = l.take("b", "m", false, never()).ok; });
        bool queued = false;
        while (!(queued = l.waiting("b")) && !got)
            std::this_thread::yield();
        // never(), not a timeout: a missed wake hangs here and ctest's limit fails it
        if (queued)
            l.stop_waiting("a");
        b.join();
        check(queued && got, "b waited behind a, and got m once a gave up");
    }

    std::printf("\n%s\n", failures == 0 ? "all key lock checks pass" : "FAILURES above");
    return failures == 0 ? 0 : 1;
}
