// The function sync poller's schedule and wake, and the queue consumer's posts -
// TODO 941.
//
// src/wake_core.h decides when the poller runs a repository, how long it sleeps,
// what wakes it, and when the consumer may post to its strand. This runs:
//   - the poller over two repositories, every interval each of 0, 150, 300 and 500
//     ms, enabled or not, with no ask or one ask, and with no change or one change
//     of the first one's interval, for two seconds of its clock;
//   - every order of the poller's check and sleep against a stop and a kick, with
//     and without the wait's lock;
//   - every order of a handler's completion and a wake against a stop that gives
//     up and the io_context going, with and without the post lock.
// It checks:
//   - a repository runs when it is due, never before and never after;
//   - one asked for runs in that pass, and one with interval 0 only when asked;
//   - a disabled one never runs;
//   - two first runs don't land in one pass;
//   - a changed interval holds from the change;
//   - the poller never sleeps 0;
//   - no stop or kick leaves the poller asleep with its flag set;
//   - nothing posts to the strand once the io_context is gone;
//   - a completion that finishes before stop gives up gets its count back.
//
// It also runs rules that are known to be wrong and checks that it catches each.
// Two are the code as it was before TODO 941: the notify outside the wait's lock,
// and the consumer's check apart from its post.
#include "wake_core.h"

#include <algorithm>
#include <cstdio>
#include <functional>
#include <string>
#include <vector>

using namespace barch::wake_core;

static int failures = 0;

static void check(bool ok, const char* what) {
    std::printf("%-72s %s\n", what, ok ? "pass" : "FAIL");
    if (!ok) ++failures;
}

namespace {
    struct rules {
        std::function<bool(bool, bool, bool, bool)> poller_wakes;
        std::function<pass(const repo&, schedule&, int64_t, size_t&)> plan;
        std::function<uint64_t(uint64_t, const pass&)> shorter;
        std::function<bool(poster, bool, bool)> may_post;
        /** the notifier sets its flag under the wait's lock */
        bool notify_locked{true};
        /** the consumer decides and posts under the post lock */
        bool post_locked{true};
    };

    // ---- the poller's schedule ----

    struct setup {
        uint64_t ms[2]{};
        bool enabled[2]{true, true};
        int64_t ask_at{-1};     // repository 0 asked for at this time
        int64_t change_at{-1};  // repository 0's interval becomes change_to then
        uint64_t change_to{0};
    };

    constexpr int64_t horizon = 2000;

    std::string run_schedule(const rules& r, const setup& u) {
        schedule s[2];
        uint64_t ms[2] = {u.ms[0], u.ms[1]};
        int64_t last_run[2] = {-1, -1};
        int64_t now = 0;
        bool asked_pending = false;
        bool changed = false;
        // where a repository's next run has to be by, if it has a rule for that
        int64_t run_by[2] = {-1, -1};
        int64_t run_from[2] = {-1, -1};
        int passes = 0;
        while (now <= horizon) {
            if (++passes > 10000)
                return "the poller passed without end";
            if (u.change_at >= 0 && !changed && now >= u.change_at) {
                ms[0] = u.change_to;
                changed = true;
                // from the change, the new interval holds: a run no later than one
                // interval on, the first after the stagger
                run_by[0] = u.enabled[0] && ms[0] > 0 ? now + (int64_t) ms[0] : -1;
                run_from[0] = -1;
            }
            size_t index = 0;
            uint64_t wait = idle_wait_ms;
            int first_runs = 0;
            for (int i = 0; i < 2; ++i) {
                repo rp{u.enabled[i], ms[i], i == 0 && asked_pending};
                const bool first = !s[i].due_set || s[i].ms != rp.ms;
                const auto p = r.plan(rp, s[i], now, index);
                if (p.run) {
                    if (!rp.enabled)
                        return "a disabled repository ran";
                    if (!rp.asked) {
                        if (rp.ms == 0)
                            return "a repository with interval 0 ran unasked";
                        if (run_from[i] >= 0 && now < run_from[i])
                            return "a repository ran before it was due";
                        if (first && last_run[i] < 0)
                            ++first_runs;
                    }
                    last_run[i] = now;
                    run_from[i] = rp.ms ? now + (int64_t) rp.ms : -1;
                    run_by[i] = rp.ms ? now + (int64_t) rp.ms : -1;
                } else if (rp.asked && rp.enabled) {
                    return "an asked for repository didn't run";
                }
                if (run_by[i] >= 0 && now > run_by[i])
                    return "a repository ran late";
                wait = r.shorter(wait, p);
            }
            if (first_runs > 1)
                return "two first runs landed in one pass";
            asked_pending = false;
            if (wait == 0)
                return "the poller slept 0";
            // first due run after the stagger: the poller has to wake for it
            for (int i = 0; i < 2; ++i)
                if (u.enabled[i] && ms[i] > 0 && s[i].due_set && (run_by[i] < 0 || s[i].due < run_by[i]))
                    run_by[i] = s[i].due;
            int64_t next = now + (int64_t) wait;
            // an ask or a change wakes it early
            for (int64_t e : {u.ask_at, u.change_at})
                if (e > now && e < next)
                    next = e;
            for (int i = 0; i < 2; ++i)
                if (run_by[i] >= 0 && run_by[i] < next && run_by[i] <= horizon)
                    return "the poller slept past a due run";
            now = next;
            if (u.ask_at == now)
                asked_pending = true;
        }
        return {};
    }

    std::string every_schedule(const rules& r) {
        const uint64_t intervals[] = {0, 150, 300, 500};
        for (auto m0 : intervals)
            for (auto m1 : intervals)
                for (int en = 0; en < 4; ++en)
                    for (int64_t ask : {-1, 0, 100, 450})
                        for (int64_t change : {-1, 0, 320})
                            for (auto to : intervals) {
                                if (change < 0 && to != 0)
                                    continue;
                                setup u;
                                u.ms[0] = m0;
                                u.ms[1] = m1;
                                u.enabled[0] = (en & 1) == 0;
                                u.enabled[1] = (en & 2) == 0;
                                u.ask_at = ask;
                                u.change_at = change;
                                u.change_to = to;
                                auto got = run_schedule(r, u);
                                if (!got.empty())
                                    return got;
                            }
        return {};
    }

    // ---- the poller's wait ----

    /*
     * The poller checks poller_wakes, then sleeps if it said no. A notifier sets its
     * flag, then notifies, which wakes a sleeping poller. With the lock the check and
     * the sleep are one step to the notifier's flag: std::condition_variable takes
     * the mutex for the check and lets it go only as it sleeps.
     */
    std::string every_wait(const rules& r) {
        // which flag the notifier sets: a stop, or a kick
        for (int which = 0; which < 2; ++which) {
            // the four steps: poller check (0), poller sleep (1), store (2), notify (3)
            std::vector<int> order{0, 1, 2, 3};
            std::sort(order.begin(), order.end());
            do {
                auto pos = [&](int step) { return std::find(order.begin(), order.end(), step) - order.begin(); };
                if (pos(0) > pos(1) || pos(2) > pos(3))
                    continue;
                if (r.notify_locked && pos(0) < pos(2) && pos(2) < pos(1))
                    continue;
                bool running = true, kick = false, said_wake = false, asleep = false;
                for (int step : order) {
                    if (step == 0)
                        said_wake = r.poller_wakes(running, kick, false, false);
                    else if (step == 1)
                        asleep = !said_wake;
                    else if (step == 2)
                        (which == 0 ? running : kick) = (which == 0 ? false : true);
                    else
                        asleep = false;
                }
                // a sleeper the notify missed sleeps with its flag set
                if (asleep && r.poller_wakes(running, kick, false, false))
                    return which == 0 ? "a stop left the poller asleep" : "a kick left the poller asleep";
                if (!asleep && !said_wake && pos(1) < pos(3)) {
                    // woken by the notify: it checks again and must see the flag
                    if (!r.poller_wakes(running, kick, false, false))
                        return "a woken poller found no reason";
                }
            } while (std::next_permutation(order.begin(), order.end()));
        }
        if (r.poller_wakes(true, false, false, false))
            return "the poller woke with nothing to do";
        for (int b = 1; b < 16; ++b) {
            const bool running = (b & 1) == 0;
            if (!r.poller_wakes(running, (b & 2) != 0, (b & 4) != 0, (b & 8) != 0))
                return "the poller slept through a reason to wake";
        }
        return {};
    }

    // ---- the consumer's posts ----

    /*
     * A completion and a wake each decide with may_post, then post. Stop sets
     * stopping, waits, sets abandoned when it gives up, and returns. Then
     * server::start destroys the io_context. Each of stop's sets is under the post
     * lock, so with the lock neither lands between a decide and its post.
     *
     * Steps: completion decide (0), completion post (1), wake decide (2), wake post
     * (3), set stopping (4), set abandoned (5), destroy (6).
     */
    std::string every_post(const rules& r) {
        std::vector<int> order{0, 1, 2, 3, 4, 5, 6};
        do {
            auto pos = [&](int step) { return std::find(order.begin(), order.end(), step) - order.begin(); };
            if (pos(0) > pos(1) || pos(2) > pos(3) || pos(4) > pos(5) || pos(5) > pos(6))
                continue;
            if (r.post_locked) {
                bool split = false;
                for (int set : {4, 5})
                    for (auto pair : {std::make_pair(0, 1), std::make_pair(2, 3)})
                        if (pos(pair.first) < pos(set) && pos(set) < pos(pair.second))
                            split = true;
                if (split)
                    continue;
            }
            bool stopping = false, abandoned = false, gone = false;
            bool may[2] = {false, false};
            bool posted[2] = {false, false};
            for (int step : order) {
                switch (step) {
                    case 0: may[0] = r.may_post(poster::completion, stopping, abandoned); break;
                    case 2: may[1] = r.may_post(poster::wake, stopping, abandoned); break;
                    case 1:
                    case 3: {
                        const int who = step == 1 ? 0 : 1;
                        if (!may[who])
                            break;
                        if (gone)
                            return "a post landed on a destroyed io_context";
                        posted[who] = true;
                        break;
                    }
                    case 4: stopping = true; break;
                    case 5: abandoned = true; break;
                    case 6: gone = true; break;
                }
            }
            // stop() waits for handlers: one done before it gives up keeps its count
            if (pos(1) < pos(5) && !posted[0])
                return "a completion before stop gave up lost its count";
            // a wake before the stop runs
            if (pos(3) < pos(4) && !posted[1])
                return "a wake before the stop was dropped";
        } while (std::next_permutation(order.begin(), order.end()));
        return {};
    }

    std::string all(const rules& r) {
        for (auto f : {every_wait, every_post, every_schedule}) {
            auto got = f(r);
            if (!got.empty())
                return got;
        }
        return {};
    }
}

int main() {
    const rules now{poller_wakes, plan, shorter, may_post, true, true};

    std::printf("the rules directly\n");
    {
        schedule s;
        size_t index = 0;
        auto first = plan(repo{true, 300, false}, s, 1000, index);
        check(first.run && first.wait == 300 && s.due == 1300, "a first run with no stagger runs now");
        auto mid = plan(repo{true, 300, false}, s, 1299, index);
        check(!mid.run && mid.wait == 1, "a pass 1 ms short sleeps 1 ms, not the idle minute");
        check(shorter(idle_wait_ms, pass{false, 0}) == idle_wait_ms, "a repository with no bound leaves the idle wait");
        check(!may_post(poster::wake, true, false) && may_post(poster::completion, true, false),
              "a stopping consumer takes completions, not wakes");
    }

    std::printf("every schedule, wait order and post order\n");
    {
        const auto got = all(now);
        if (!got.empty())
            std::printf("  %s\n", got.c_str());
        check(got.empty(), "as now");
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
                {"the notify outside the wait's lock, as before TODO 941",
                 with([](rules& r) { r.notify_locked = false; })},
                {"the consumer's check apart from its post, as before TODO 941",
                 with([](rules& r) { r.post_locked = false; })},
                {"a run's wait left at the idle cap, as before TODO 583", with([](rules& r) {
                    r.plan = [](const repo& rp, schedule& s, int64_t t, size_t& i) {
                        auto p = plan(rp, s, t, i);
                        if (p.run) p.wait = 0;
                        return p;
                    };
                })},
                {"a wait short of due rounded to 0, as before TODO 788", with([](rules& r) {
                    r.plan = [](const repo& rp, schedule& s, int64_t t, size_t& i) {
                        auto p = plan(rp, s, t, i);
                        if (!p.run && p.wait < 200) p.wait = 0;
                        return p;
                    };
                })},
                {"a changed interval keeps the old schedule, as before TODO 584", with([](rules& r) {
                    r.plan = [](const repo& rp, schedule& s, int64_t t, size_t& i) {
                        s.ms = rp.ms;
                        return plan(rp, s, t, i);
                    };
                })},
                {"no stagger", with([](rules& r) {
                    r.plan = [](const repo& rp, schedule& s, int64_t t, size_t& i) {
                        size_t zero = 0;
                        auto p = plan(rp, s, t, zero);
                        (void) i;
                        return p;
                    };
                })},
                {"interval 0 runs on its own", with([](rules& r) {
                    r.plan = [](const repo& rp, schedule& s, int64_t t, size_t& i) {
                        if (rp.enabled && rp.ms == 0) return pass{true, 0};
                        return plan(rp, s, t, i);
                    };
                })},
                {"an ask ignored", with([](rules& r) {
                    r.plan = [](const repo& rp, schedule& s, int64_t t, size_t& i) {
                        return plan(repo{rp.enabled, rp.ms, false}, s, t, i);
                    };
                })},
                {"a disabled repository runs", with([](rules& r) {
                    r.plan = [](const repo& rp, schedule& s, int64_t t, size_t& i) {
                        return plan(repo{true, rp.ms, rp.asked}, s, t, i);
                    };
                })},
                {"a run before it is due", with([](rules& r) {
                    r.plan = [](const repo& rp, schedule& s, int64_t t, size_t& i) {
                        if (s.due_set && s.ms == rp.ms && t + 100 >= s.due) s.due = t;
                        return plan(rp, s, t, i);
                    };
                })},
                {"the wait picks the longest", with([](rules& r) {
                    r.shorter = [](uint64_t w, const pass& p) { return p.wait > w ? p.wait : w; };
                })},
                {"the poller ignores a stop", with([](rules& r) {
                    r.poller_wakes = [](bool, bool a, bool n, bool s) { return a || n || s; };
                })},
                {"the poller ignores a kick", with([](rules& r) {
                    r.poller_wakes = [](bool run, bool, bool n, bool s) { return !run || n || s; };
                })},
                {"a wake posts while stopping", with([](rules& r) {
                    r.may_post = [](poster who, bool st, bool ab) {
                        return who == poster::wake ? true : may_post(who, st, ab);
                    };
                })},
                {"a completion stops at stopping", with([](rules& r) {
                    r.may_post = [](poster, bool st, bool) { return !st; };
                })},
                {"a completion posts once abandoned", with([](rules& r) {
                    r.may_post = [](poster who, bool st, bool ab) {
                        return who == poster::completion ? true : may_post(who, st, ab);
                    };
                })},
        };
        for (const auto& m : mutants) {
            const auto got = all(m.r);
            char line[200];
            std::snprintf(line, sizeof line, "%s: %s", m.what, got.empty() ? "missed" : got.c_str());
            check(!got.empty(), line);
        }
    }

    std::printf(failures ? "FAILED\n" : "all passed\n");
    return failures ? 1 : 0;
}
