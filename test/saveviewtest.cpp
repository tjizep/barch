// A shard's save view rules, every interleaving of a few threads - TODO 939.
//
// src/save_view_core.h decides what a save's freeze does, whether its writer may write
// from the view it took, whether it ends the view that is up, and whether a BEGIN may
// start. This runs saves, clears, BEGINs and puts through every order, in the steps
// shard.cpp takes them:
//   - save: freeze under the write latch; take the view and the generation under a
//     shared latch, unless another writer has it; write under save_load_mutex; end the view under the write latch.
//     A busy freeze waits for the view to go and tries again.
//   - clear: under save_load_mutex and the latch, moves the generation, drops the
//     view and its pages.
//   - BEGIN: under the latch, or waits for the view to go. Its rollback comes later.
//   - put: under the latch. With a view up it lands in CoW pages, which the view
//     doesn't see.
// It checks:
//   - a save writes from live pages, and what it writes is the shard as it froze;
//   - a save never writes ahead of the change log;
//   - nothing replaces a view's CoW maps while the view is up;
//   - a freeze never takes a transaction's CoW maps;
//   - no order ends with a thread waiting on a view nobody will end.
//
// It also runs rules that are known to be wrong and checks that it catches each. One
// is the take as it was before TODO 939: a save whose view a clear dropped took the
// next save's, and both wrote from it.
#include "save_view_core.h"

#include <cstdio>
#include <functional>
#include <set>
#include <string>
#include <vector>

using namespace barch::save_view_core;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    struct rules {
        std::function<freeze(bool, bool)> on_freeze;
        std::function<bool(bool, bool)> may_take;
        std::function<bool(uint64_t, uint64_t, bool, bool)> may_write;
        std::function<bool(const void*, const void*)> ends_view;
        std::function<bool(bool)> may_begin;
    };

    enum class actor { save, clear, begin, put };

    struct view {
        int frozen{0};     // the shard's value when it froze
        bool alive{true};  // its pages still mapped
        bool sound{true};  // its CoW maps still its own
        bool claimed{false}; // a writer has taken it
    };

    struct thread {
        int pc{0};
        int taken{-1};      // the view a save took, -1 for none
        uint64_t gen{0};    // the generation it took with it
    };

    struct state {
        int value{0};
        uint64_t gen{0};
        int up{-1};         // the view that is up, -1 for none
        bool transacted{false};
        std::vector<view> views;
        std::vector<thread> t;

        [[nodiscard]] std::vector<int> key() const {
            std::vector<int> k{value, (int) gen, up, transacted};
            for (const auto& v : views) {
                k.push_back(v.frozen);
                k.push_back(v.alive);
                k.push_back(v.sound);
                k.push_back(v.claimed);
            }
            k.push_back(-9);
            for (const auto& x : t) {
                k.push_back(x.pc);
                k.push_back(x.taken);
                k.push_back((int) x.gen);
            }
            return k;
        }
    };

    // stand ins for the view pointers ends_view compares
    const void* ptr(int view) {
        static char slots[16];
        return view < 0 ? nullptr : &slots[view];
    }

    void drop_up(state& s) {
        if (s.up >= 0)
            s.views[s.up].alive = false;
        s.up = -1;
    }

    /*
     * Each step is a thread's move. Returns a fault, or empty, and sets `waits` when the
     * thread can't move now. `next` holds the states it can move to: a write has two,
     * one where the log reached the disk first and one where it didn't.
     */
    std::string step(const rules& r, const state& s, size_t i, actor a, std::vector<state>& next, bool& waits) {
        waits = false;
        const auto& me = s.t[i];
        state n = s;
        auto& th = n.t[i];
        switch (a) {
            case actor::put:
                // with a view up the put goes to CoW pages: views[up].frozen stays
                ++n.value;
                th.pc = 99;
                next.push_back(n);
                return {};
            case actor::clear:
                n.value = 0;
                ++n.gen;
                drop_up(n);
                th.pc = 99;
                next.push_back(n);
                return {};
            case actor::begin:
                if (me.pc == 0) {
                    if (s.transacted) {
                        th.pc = 99;
                        next.push_back(n);
                        return {};
                    }
                    if (!r.may_begin(s.up >= 0)) {
                        waits = s.up >= 0;
                        if (!waits) return "a BEGIN refused with no view up";
                        return {};
                    }
                    if (s.up >= 0)
                        n.views[s.up].sound = false; // its CoW maps are the transaction's now
                    n.transacted = true;
                    th.pc = 1;
                } else {
                    n.transacted = false; // the rollback
                    th.pc = 99;
                }
                next.push_back(n);
                return {};
            case actor::save:
                if (me.pc == 0) { // freeze, under the latch
                    switch (r.on_freeze(s.up >= 0, s.transacted)) {
                        case freeze::busy:
                            waits = s.up >= 0;
                            if (!waits) return "a freeze waited with no view up";
                            return {};
                        case freeze::direct:
                            th.pc = 99; // the old save, under the latch
                            next.push_back(n);
                            return {};
                        case freeze::frozen:
                            if (s.transacted)
                                return "a freeze took the transaction's CoW maps";
                            if (s.up >= 0)
                                n.views[s.up].sound = false;
                            n.views.push_back(view{s.value});
                            n.up = (int) n.views.size() - 1;
                            th.pc = 1;
                            next.push_back(n);
                            return {};
                    }
                }
                if (me.pc == 1) { // take the view, under a shared latch
                    const bool claimed = s.up >= 0 && s.views[s.up].claimed;
                    th.taken = r.may_take(s.up >= 0, claimed) ? s.up : -1;
                    if (th.taken >= 0)
                        n.views[th.taken].claimed = true;
                    th.gen = s.gen;
                    th.pc = th.taken < 0 ? 99 : 2;
                    next.push_back(n);
                    return {};
                }
                if (me.pc == 2) { // write, under save_load_mutex
                    for (bool log_first : {true, false}) {
                        state w = n;
                        if (r.may_write(me.gen, s.gen, log_first, false)) {
                            if (!log_first)
                                return "a save wrote ahead of the change log";
                            const auto& v = s.views[me.taken];
                            if (!v.alive)
                                return "a save wrote from pages already gone";
                            if (!v.sound)
                                return "a save wrote from CoW maps not its own";
                        }
                        w.t[i].pc = 3;
                        next.push_back(w);
                    }
                    return {};
                }
                if (me.pc == 3) { // end it, under the latch
                    if (r.ends_view(ptr(s.up), ptr(me.taken))) {
                        if (s.up != me.taken)
                            return "a save ended a view not its own";
                        drop_up(n);
                    }
                    th.pc = 99;
                    next.push_back(n);
                    return {};
                }
        }
        return {};
    }

    bool finished(int pc) {
        return pc == 99;
    }

    /** the first rule broken, over every order the actors can run in, or empty */
    std::string explore(const rules& r, const std::vector<actor>& actors) {
        state start;
        start.t.resize(actors.size());
        std::set<std::vector<int>> seen;
        std::vector<state> todo{start};
        while (!todo.empty()) {
            auto s = std::move(todo.back());
            todo.pop_back();
            if (!seen.insert(s.key()).second)
                continue;
            bool done = true, moved = false;
            for (size_t i = 0; i < actors.size(); ++i) {
                if (finished(s.t[i].pc))
                    continue;
                done = false;
                std::vector<state> next;
                bool waits = false;
                auto got = step(r, s, i, actors[i], next, waits);
                if (!got.empty())
                    return got;
                if (waits)
                    continue;
                moved = true;
                for (auto& x : next)
                    todo.push_back(std::move(x));
            }
            if (!done && !moved)
                return "threads waiting on a view nobody will end";
            if (done && s.up >= 0)
                return "a view left up after every save finished";
        }
        return {};
    }

    const std::vector<actor> menu{actor::save, actor::clear, actor::begin, actor::put};

    /** the first fault over every choice of `threads` actors from the menu */
    std::string every_choice(const rules& r, size_t threads, size_t* runs) {
        std::vector<size_t> pick(threads, 0);
        while (true) {
            std::vector<actor> actors;
            for (auto p : pick)
                actors.push_back(menu[p]);
            if (runs)
                ++*runs;
            auto got = explore(r, actors);
            if (!got.empty())
                return got;
            size_t i = 0;
            while (i < threads && ++pick[i] == menu.size())
                pick[i++] = 0;
            if (i == threads)
                return {};
        }
    }

    std::string every_size(const rules& r, size_t* runs = nullptr) {
        for (size_t n = 1; n <= 4; ++n) {
            auto got = every_choice(r, n, runs);
            if (!got.empty())
                return got;
        }
        return {};
    }
}

int main() {
    const rules now{
            [](bool up, bool tx) { return on_freeze(up, tx); },
            [](bool up, bool claimed) { return may_take(up, claimed); },
            [](uint64_t a, uint64_t b, bool log, bool refused) { return may_write(a, b, log, refused); },
            [](const void* up, const void* mine) { return ends_view(up, mine); },
            [](bool up) { return may_begin(up); },
    };

    std::printf("the rules directly\n");
    {
        check(on_freeze(false, false) == freeze::frozen, "a freeze with nothing up freezes");
        check(on_freeze(true, false) == freeze::busy && on_freeze(true, true) == freeze::busy,
              "a freeze with a view up waits");
        check(on_freeze(false, true) == freeze::direct, "a freeze inside a transaction saves directly");
        check(may_take(true, false) && !may_take(true, true) && !may_take(false, false),
              "a writer takes a view no other writer has");
        check(may_write(3, 3, true, false), "a save writes when nothing moved");
        check(!may_write(3, 4, true, false), "not after a clear");
        check(!may_write(3, 3, false, false), "not ahead of the log");
        check(!may_write(3, 3, true, true), "not after a failed load");
        int a, b;
        check(ends_view(&a, &a) && !ends_view(&b, &a) && !ends_view(nullptr, &a), "a save ends only its own view");
        check(may_begin(false) && !may_begin(true), "a BEGIN waits for a view");
    }

    std::printf("every interleaving\n");
    {
        size_t runs = 0;
        const auto got = every_size(now, &runs);
        if (!got.empty())
            std::printf("  %s\n", got.c_str());
        char line[128];
        std::snprintf(line, sizeof line, "as now (%zu choices of threads, up to 4)", runs);
        check(got.empty(), line);
    }

    std::printf("rules known to be wrong, each caught\n");
    {
        struct mutant {
            const char* what;
            rules r;
        };
        auto with = [&](auto change) {
            rules r = now;
            change(r);
            return r;
        };
        const std::vector<mutant> mutants{
                {"a freeze over a view that is up", with([](rules& r) {
                    r.on_freeze = [](bool, bool tx) { return tx ? freeze::direct : freeze::frozen; };
                })},
                {"a freeze inside a transaction", with([](rules& r) {
                    r.on_freeze = [](bool up, bool) { return up ? freeze::busy : freeze::frozen; };
                })},
                {"a writer takes a view another has, as before TODO 939", with([](rules& r) {
                    r.may_take = [](bool up, bool) { return up; };
                })},
                {"a write after a clear", with([](rules& r) {
                    r.may_write = [](uint64_t, uint64_t, bool log, bool refused) { return log && !refused; };
                })},
                {"a write ahead of the log", with([](rules& r) {
                    r.may_write = [](uint64_t a, uint64_t b, bool, bool refused) { return a == b && !refused; };
                })},
                {"a save ends whatever view is up", with([](rules& r) {
                    r.ends_view = [](const void* up, const void*) { return up != nullptr; };
                })},
                {"a save never ends its view", with([](rules& r) {
                    r.ends_view = [](const void*, const void*) { return false; };
                })},
                {"a BEGIN over a view that is up", with([](rules& r) {
                    r.may_begin = [](bool) { return true; };
                })},
        };
        for (const auto& m : mutants) {
            const auto got = every_size(m.r);
            char line[160];
            std::snprintf(line, sizeof line, "%s: %s", m.what, got.empty() ? "missed" : got.c_str());
            check(!got.empty(), line);
        }
    }

    std::printf(failures ? "FAILED\n" : "all passed\n");
    return failures ? 1 : 0;
}
