// Two writers never write from one save view - TODO 939.
//
// save() lets the latch go between its freeze and write_frozen. A clear in that gap
// drops its view, and another save can freeze a new one before it gets to
// write_frozen, which then takes that one. Both writers then write from one view,
// and the first to end it merges its CoW pages under the other. This stops two
// writers between taking the view and writing from it, and checks only one writes.
#include "shard.h"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <latch>
#include <memory>
#include <string>
#include <thread>

static int failures = 0;
static void check(bool ok, const std::string& what) {
    std::printf("  %-62s %s\n", what.c_str(), ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

static std::latch* both_took = nullptr;

int main() {
    std::printf("two writers and one save view\n");
    auto s = std::make_shared<barch::shard>("saveviewshardtest", size_t{0}, barch::shard::scratch_t{});
    {
        unique_latch hold(s->get_latch());
        check(s->freeze_for_save_holding_lock(false) == barch::abstract_shard::save_freeze::frozen,
              "the view froze");
    }
    std::latch took(2);
    both_took = &took;
    barch::shard::after_save_take_for_test = [] { both_took->arrive_and_wait(); };
    std::atomic<int> wrote{0};
    std::thread a([&] { if (s->write_frozen()) ++wrote; });
    std::thread b([&] { if (s->write_frozen()) ++wrote; });
    a.join();
    b.join();
    barch::shard::after_save_take_for_test = nullptr;
    check(wrote.load() == 1, "one writer wrote, not " + std::to_string(wrote.load()));
    check(!s->frozen_for_save(), "and the view is down");
    std::printf(failures ? "%d failed\n" : "all passed\n", failures);
    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
